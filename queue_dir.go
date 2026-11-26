package noderunner

import (
	"encoding/json"
	"fmt"
	"github.com/dracory/nodebase"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

// This file implements the queue node's "dir" storage backend
// (task folder_backends, decision folder_node):
// a materialized queue where every staged item is ONE real file,
// `{seq}-{msgid}.json`, so it can be browsed, edited and re-injected
// with ordinary OS tools — debugging and dead-letter rework.
//
// Design:
//   - Files are advisory, not indexed: LoadInbox scans the directory
//     at arm time. Files deleted behind the engine's back simply do
//     not load; corrupt files log a warning and stay on disk.
//   - The filename is NOT trustworthy after external edits — the
//     authoritative seq lives in the file body. Filenames exist for
//     humans; every lookup verifies the body's seq.
//   - Writes are atomic: the payload goes to `{name}.tmp` and is
//     os.Rename'd into place, so a partial .json is never readable
//     (the .tmp suffix is also skipped on scan).
//   - Fix-and-reinject: Queue drains through itemReloader, re-reading
//     the file at release time, so an edit between stage and drain
//     delivers the EDITED payload.
//   - Multi-runner safety is a deployment property: a local dir is
//     single-instance; an NFS/SMB/SSHFS mount is already shared. The
//     store makes no claim either way.
//   - Shared dirs are namespaced by ownership: a scoped store only
//     claims files whose body nodeId equals its owner node ID, so two
//     queue nodes on one directory cannot cross-delete or cross-reload
//     each other's staged files (task queue_dir_shared).
//     Hand-written files must carry the queue's nodeId in the body to
//     be adopted; unattributable files (empty nodeId) are skipped with
//     a warning under a scoped store. A renamed node ID strands its
//     old files (logged, left on disk) — same as the durable inbox.

// dirQueueFile is the on-disk shape of one queued item.
type dirQueueFile struct {
	Seq        int64        `json:"seq"`
	WorkflowID string       `json:"workflowId"`
	NodeID     string       `json:"nodeId"`
	MsgID      string       `json:"msgId"`
	Payload    *messageWire `json:"payload"`
	EnqueuedAt time.Time    `json:"enqueuedAt"`
}

// DirQueueStore is a DurableInboxStore persisting every queued item as
// its own JSON file in one directory. It additionally implements
// itemReloader so the queue re-reads the file at release time.
//
// Storage goes through the nodebase.Fs seam (task remote_fs): a
// local path uses the os adapter, an sftp:// or ftp:// locator uses the
// remote adapter — the materialized "one file per item" debugging
// workflow is unchanged, only the wire differs. All nodebase.Fs ops run under
// the store mutex, so a single-threaded remote connection is safe.
type DirQueueStore struct {
	fs  nodebase.Fs
	dir string
	// owner is the node ID this store claims files for. "" is an
	// unscoped store (single-queue dirs, tooling); a scoped store —
	// what queue nodes build — touches only files whose body nodeId
	// matches, so shared dirs cannot cross-delete or cross-reload.
	owner   string
	mu      sync.Mutex
	nextSeq int64
	scanned bool // whether nextSeq was initialized from a dir scan
}

// NewDirQueueStore creates (or reuses) dir as a materialized queue.
// The store is unscoped: it manages every parseable item file in dir.
// Queue nodes should use NewScopedDirQueueStore.
func NewDirQueueStore(dir string) (*DirQueueStore, error) {
	return newDirQueueStore(nodebase.LocalFs{}, dir, "")
}

// NewScopedDirQueueStore creates a DirQueueStore that only claims
// files whose body nodeId equals nodeID. Two queue nodes may then
// share one directory: each sees, reloads and deletes only its own
// staged files, and per-node seqs may collide harmlessly (filenames
// stay distinct via the msgid component).
func NewScopedDirQueueStore(dir, nodeID string) (*DirQueueStore, error) {
	return newDirQueueStore(nodebase.LocalFs{}, dir, strings.TrimSpace(nodeID))
}

// NewDirQueueStoreFS is the nodebase.Fs-locator form of NewScopedDirQueueStore:
// the caller resolves the locator via nodebase.OpenFs and hands the connected
// filesystem in. The store takes ownership of fs — Close releases it.
func NewDirQueueStoreFS(fsys nodebase.Fs, dir, nodeID string) (*DirQueueStore, error) {
	if fsys == nil {
		return nil, fmt.Errorf("dir backend requires a filesystem")
	}
	return newDirQueueStore(fsys, dir, strings.TrimSpace(nodeID))
}

func newDirQueueStore(fsys nodebase.Fs, dir, owner string) (*DirQueueStore, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, fmt.Errorf("dir backend requires a directory path")
	}
	if err := fsys.MkdirAll(dir); err != nil {
		return nil, fmt.Errorf("failed to create queue dir %q: %w", dir, err)
	}
	return &DirQueueStore{fs: fsys, dir: dir, owner: owner, nextSeq: 1}, nil
}

// Close releases the underlying nodebase.Fs (an sftp/ftp connection; a no-op
// for local dirs). Called when the owning queue stops.
func (s *DirQueueStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fs != nil {
		return s.fs.Close()
	}
	return nil
}

// owns reports whether the item file belongs to this store: always for
// an unscoped store, otherwise only when the body nodeId matches.
// Callers must hold s.mu.
func (s *DirQueueStore) owns(it dirQueueFile) bool {
	return s.owner == "" || it.NodeID == s.owner
}

// sanitizeFilename keeps workflow/node/msg IDs filename-safe; the
// authoritative identity lives in the file body, so collisions here
// are cosmetic only.
func sanitizeFilename(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
			return r
		}
		return '_'
	}, s)
}

// itemFileName builds `{seq}-{msgid}.json`.
func itemFileName(seq int64, msgID string) string {
	return fmt.Sprintf("%d-%s.json", seq, sanitizeFilename(msgID))
}

// scan reads every `*.json` in the dir (skipping `.tmp` fragments and
// anything that fails to parse — corrupt files stay on disk) and
// returns the items this store owns. A scoped store ignores files
// tagged for other nodes; files with no nodeId at all cannot be
// attributed and warn the operator.
// Callers must hold s.mu.
func (s *DirQueueStore) scan() []dirQueueFile {
	entries, err := s.fs.List(s.dir)
	if err != nil {
		log.Printf("warning: dir queue scan of %q failed: %v", s.dir, err)
		return nil
	}
	var items []dirQueueFile
	for _, e := range entries {
		name := e.Name
		if e.IsDir || !strings.HasSuffix(name, ".json") {
			continue
		}
		raw, err := s.fs.ReadFile(s.fs.Join(s.dir, name))
		if err != nil {
			log.Printf("warning: dir queue item %q unreadable: %v", name, err)
			continue
		}
		var it dirQueueFile
		if err := json.Unmarshal(raw, &it); err != nil {
			log.Printf("warning: dir queue item %q is corrupt (%v); skipping", name, err)
			continue
		}
		if !s.owns(it) {
			if s.owner != "" && it.NodeID == "" {
				log.Printf("warning: dir queue item %q has no nodeId; queue node %q cannot adopt it (set \"nodeId\" to stage it)", name, s.owner)
			}
			continue
		}
		items = append(items, it)
	}
	return items
}

// ensureSeq lazily derives nextSeq from the highest seq on disk so a
// store built over a populated dir never reuses seqs.
// Callers must hold s.mu.
func (s *DirQueueStore) ensureSeq() {
	if s.scanned {
		return
	}
	s.scanned = true
	for _, it := range s.scan() {
		if it.Seq >= s.nextSeq {
			s.nextSeq = it.Seq + 1
		}
	}
}

// writeItem persists f atomically: full payload to a `.tmp` sibling,
// then rename. A crash mid-write leaves a `.tmp` fragment that scans
// ignore — never a partial `.json`.
// Callers must hold s.mu.
func (s *DirQueueStore) writeItem(f dirQueueFile) error {
	raw, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize queue item: %w", err)
	}
	final := s.fs.Join(s.dir, itemFileName(f.Seq, f.MsgID))
	tmp := final + ".tmp"
	if err := s.fs.WriteFile(tmp, raw); err != nil {
		return fmt.Errorf("failed to write queue item: %w", err)
	}
	if err := s.fs.Rename(tmp, final); err != nil {
		_ = s.fs.Remove(tmp)
		return fmt.Errorf("failed to commit queue item: %w", err)
	}
	return nil
}

// pathForSeq locates the file whose BODY seq matches AND that this
// store owns (the filename is advisory after external edits, and seqs
// of different queue nodes sharing a dir may collide). Returns "" when
// nothing matches.
// Callers must hold s.mu.
func (s *DirQueueStore) pathForSeq(seq int64) string {
	entries, err := s.fs.List(s.dir)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		name := e.Name
		if e.IsDir || !strings.HasSuffix(name, ".json") {
			continue
		}
		raw, err := s.fs.ReadFile(s.fs.Join(s.dir, name))
		if err != nil {
			continue
		}
		var it dirQueueFile
		if err := json.Unmarshal(raw, &it); err != nil || it.Seq != seq || !s.owns(it) {
			continue
		}
		return s.fs.Join(s.dir, name)
	}
	return ""
}

// LoadInbox implements DurableInboxStore: scan the dir, return items
// in seq order. Files removed externally simply do not appear.
func (s *DirQueueStore) LoadInbox(workflowID string) ([]DurableInboxItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureSeq()
	items := s.scan()
	sort.Slice(items, func(i, j int) bool { return items[i].Seq < items[j].Seq })
	out := make([]DurableInboxItem, 0, len(items))
	for _, it := range items {
		out = append(out, DurableInboxItem{
			WorkflowID: workflowID,
			NodeID:     it.NodeID,
			Seq:        it.Seq,
			Payload:    it.Payload.toMessage(),
			EnqueuedAt: it.EnqueuedAt,
		})
	}
	return out, nil
}

// SaveInboxItem implements DurableInboxStore: assigns the next seq and
// writes `{seq}-{msgid}.json` atomically.
func (s *DirQueueStore) SaveInboxItem(item *DurableInboxItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureSeq()
	at := item.EnqueuedAt
	if at.IsZero() {
		at = time.Now()
	}
	wire, err := messageToWire(item.Payload)
	if err != nil {
		return fmt.Errorf("failed to serialize queue message: %w", err)
	}
	f := dirQueueFile{
		Seq:        s.nextSeq,
		WorkflowID: item.WorkflowID,
		NodeID:     item.NodeID,
		MsgID:      nodebase.GenerateID(),
		Payload:    wire,
		EnqueuedAt: at,
	}
	if err := s.writeItem(f); err != nil {
		return err
	}
	s.nextSeq++
	item.Seq = f.Seq
	item.EnqueuedAt = at
	return nil
}

// DeleteInboxItem implements DurableInboxStore: removes the file whose
// body seq matches. A file already gone (external delete) is not an
// error — the files are advisory.
func (s *DirQueueStore) DeleteInboxItem(workflowID string, seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.pathForSeq(seq)
	if path == "" {
		return nil
	}
	if err := s.fs.Remove(path); err != nil {
		return fmt.Errorf("failed to delete queue item %d: %w", seq, err)
	}
	return nil
}

// DeleteWorkflowInbox implements DurableInboxStore: drops every OWNED
// item file whose body workflowId matches (files without one — e.g.
// hand-written — are left alone, and a scoped store never touches
// other nodes' files even in the same workflow).
func (s *DirQueueStore) DeleteWorkflowInbox(workflowID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, it := range s.scan() {
		if it.WorkflowID != workflowID {
			continue
		}
		if path := s.pathForSeq(it.Seq); path != "" {
			if err := s.fs.Remove(path); err != nil {
				return fmt.Errorf("failed to delete queue item %d: %w", it.Seq, err)
			}
		}
	}
	return nil
}

// LoadInboxItem implements itemReloader: re-reads one item's file at
// release time so external edits are delivered. Returns an error when
// the file no longer exists or cannot be parsed — the queue treats
// that as "externally removed" and drops the entry.
func (s *DirQueueStore) LoadInboxItem(workflowID string, seq int64) (*DurableInboxItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.pathForSeq(seq)
	if path == "" {
		return nil, fmt.Errorf("queue item %d no longer on disk", seq)
	}
	raw, err := s.fs.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to re-read queue item %d: %w", seq, err)
	}
	var it dirQueueFile
	if err := json.Unmarshal(raw, &it); err != nil {
		return nil, fmt.Errorf("queue item %d is corrupt: %w", seq, err)
	}
	return &DurableInboxItem{
		WorkflowID: workflowID,
		NodeID:     it.NodeID,
		Seq:        it.Seq,
		Payload:    it.Payload.toMessage(),
		EnqueuedAt: it.EnqueuedAt,
	}, nil
}

// itemReloader is an optional DurableInboxStore extension: stores that
// can re-read a persisted item (the dir backend) get fix-and-reinject
// semantics — edits between stage and release deliver the edited
// payload. Stores that cannot (the SQL inbox) deliver the payload
// snapshotted at enqueue/arm.
type itemReloader interface {
	LoadInboxItem(workflowID string, seq int64) (*DurableInboxItem, error)
}

// reloadEntryPayload refreshes entry.payload from the store when it
// supports itemReloader. Returns false when the item is gone — the
// caller drops the entry (external delete reconciles at release too).
func (q *Queue) reloadEntryPayload(entry *queueEntry) (bool, error) {
	store, err := q.queueStore()
	if err != nil {
		return false, err
	}
	rl, ok := store.(itemReloader)
	if !ok || entry.seq <= 0 {
		return true, nil
	}
	item, err := rl.LoadInboxItem(q.workflowID, entry.seq)
	if err != nil {
		return false, nil
	}
	entry.payload = item.Payload
	return true, nil
}

// Compile-time check: the dir store satisfies both the queue
// persistence contract and the re-read extension.
var _ DurableInboxStore = (*DirQueueStore)(nil)
var _ itemReloader = (*DirQueueStore)(nil)
