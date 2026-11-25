package noderunner

import (
	"context"
	"github.com/dracory/nodebase"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- Fs seam unit tests ---

func TestParseFsLocator(t *testing.T) {
	cases := []struct {
		locator   string
		wantSchem string
		wantDir   string
		wantErr   bool
	}{
		{"/tmp/queue", "file", "/tmp/queue", false},
		{"relative/dir", "file", "relative/dir", false},
		{"file:///var/data", "file", "/var/data", false},
		{"sftp://alice@files.example.com/incoming", "sftp", "/incoming", false},
		{"sftp://alice:pw@files.example.com:2222/in", "sftp", "/in", false},
		{"ftp://user@host/dir", "ftp", "/dir", false},
		{"sftp://user@host", "sftp", "/", false},
		{"s3://bucket/prefix", "", "", true},
		{"gopher://host/x", "", "", true},
		{"", "", "", true},
	}
	for _, c := range cases {
		scheme, dir, _, err := nodebase.ParseFsLocator(c.locator)
		if c.wantErr {
			if err == nil {
				t.Errorf("locator %q: expected error, got scheme=%q dir=%q", c.locator, scheme, dir)
			}
			continue
		}
		if err != nil {
			t.Errorf("locator %q: unexpected error: %v", c.locator, err)
			continue
		}
		if scheme != c.wantSchem || dir != c.wantDir {
			t.Errorf("locator %q: got (%q,%q) want (%q,%q)", c.locator, scheme, dir, c.wantSchem, c.wantDir)
		}
	}
}

func TestLocalFsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	var f Fs = nodebase.LocalFs{}

	sub := f.Join(dir, "sub")
	if err := f.MkdirAll(sub); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	name := f.Join(sub, "a.txt")
	if err := f.WriteFile(name, []byte("one")); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := f.AppendFile(name, []byte("two")); err != nil {
		t.Fatalf("AppendFile: %v", err)
	}
	raw, err := f.ReadFile(name)
	if err != nil || string(raw) != "onetwo" {
		t.Fatalf("ReadFile = %q, %v", raw, err)
	}
	moved := f.Join(dir, "b.txt")
	if err := f.Rename(name, moved); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	entries, err := f.List(dir)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name)
	}
	sort.Strings(names)
	if strings.Join(names, ",") != "b.txt,sub" {
		t.Fatalf("List names = %v", names)
	}
	if err := f.Remove(moved); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := f.Remove(moved); err != nil {
		t.Fatalf("Remove of missing file should be a no-op: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// memFs is an in-memory Fs standing in for a remote adapter — it
// exercises DirQueueStore through the seam exactly as sftpFs does.
type memFs struct {
	mu    sync.Mutex
	files map[string][]byte
}

func newMemFs() *memFs { return &memFs{files: map[string][]byte{}} }

func (m *memFs) Join(dir, name string) string {
	if dir == "" {
		return name
	}
	return strings.TrimSuffix(dir, "/") + "/" + name
}
func (m *memFs) MkdirAll(string) error { return nil }
func (m *memFs) ReadFile(name string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.files[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	return append([]byte(nil), data...), nil
}
func (m *memFs) WriteFile(name string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[name] = append([]byte(nil), data...)
	return nil
}
func (m *memFs) AppendFile(name string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[name] = append(append([]byte(nil), m.files[name]...), data...)
	return nil
}
func (m *memFs) Rename(oldName, newName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.files[oldName]
	if !ok {
		return os.ErrNotExist
	}
	m.files[newName] = data
	delete(m.files, oldName)
	return nil
}
func (m *memFs) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, name)
	return nil
}
func (m *memFs) List(dir string) ([]FsEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	prefix := strings.TrimSuffix(dir, "/") + "/"
	var out []FsEntry
	for name, data := range m.files {
		if strings.HasPrefix(name, prefix) && !strings.Contains(name[len(prefix):], "/") {
			out = append(out, FsEntry{Name: name[len(prefix):], Size: int64(len(data))})
		}
	}
	return out, nil
}
func (m *memFs) Close() error { return nil }

var _ Fs = (*memFs)(nil)

// TestDirQueueStoreOverRemoteFs proves the store's remote semantics:
// items materialize as files through the Fs seam, body-seq is
// authoritative, hold/release reloads edited payloads.
func TestDirQueueStoreOverRemoteFs(t *testing.T) {
	mem := newMemFs()
	store, err := NewDirQueueStoreFS(mem, "/remote/q", "n1")
	if err != nil {
		t.Fatalf("NewDirQueueStoreFS: %v", err)
	}

	item := &DurableInboxItem{
		WorkflowID: "w1",
		NodeID:     "n1",
		Payload:    &Message{Payload: "hello"},
	}
	if err := store.SaveInboxItem(item); err != nil {
		t.Fatalf("SaveInboxItem: %v", err)
	}
	if item.Seq != 1 {
		t.Fatalf("seq = %d, want 1", item.Seq)
	}

	// The item materialized as ONE file on the "remote" dir, no .tmp.
	entries, _ := mem.List("/remote/q")
	if len(entries) != 1 || !strings.HasSuffix(entries[0].Name, ".json") {
		t.Fatalf("remote dir = %v, want one .json", entries)
	}

	rows, err := store.LoadInbox("w1")
	if err != nil || len(rows) != 1 || rows[0].Payload.GetPayload() != "hello" {
		t.Fatalf("LoadInbox = %v rows, %v", len(rows), err)
	}

	// Fix-and-reinject: edit the remote file's payload, reload.
	var raw []byte
	for name := range mem.files {
		raw = mem.files[name]
	}
	edited := strings.Replace(string(raw), "hello", "edited", 1)
	for name := range mem.files {
		mem.files[name] = []byte(edited)
	}
	reloaded, err := store.LoadInboxItem("w1", 1)
	if err != nil || reloaded.Payload.GetPayload() != "edited" {
		t.Fatalf("LoadInboxItem = %v, %v", reloaded, err)
	}

	// Delete drops the remote file.
	if err := store.DeleteInboxItem("w1", 1); err != nil {
		t.Fatalf("DeleteInboxItem: %v", err)
	}
	entries, _ = mem.List("/remote/q")
	if len(entries) != 0 {
		t.Fatalf("remote dir not empty after delete: %v", entries)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// --- queue node over the locator seam ---

func TestBuildQueueStoreLocator(t *testing.T) {
	e := NewEngine()
	dir := t.TempDir()

	store, err := e.BuildQueueStore("dir", dir, "q1", nil)
	if err != nil {
		t.Fatalf("local dir locator: %v", err)
	}
	dqs, ok := store.(*DirQueueStore)
	if !ok {
		t.Fatalf("store type = %T", store)
	}
	_ = dqs.Close()

	if _, err := e.BuildQueueStore("dir", "s3://bucket/prefix", "q1", nil); err == nil {
		t.Fatal("s3 locator should error (not supported in v1)")
	}
	if _, err := e.BuildQueueStore("dir", "bogus://x", "q1", nil); err == nil {
		t.Fatal("unknown scheme should error")
	}
	// Bad sftp creds surface at deploy (dial), not first use — but an
	// unreachable host must error rather than hang.
	if _, err := e.BuildQueueStore("dir", "sftp://u@127.0.0.1:1/x", "q1",
		map[string]interface{}{"password": "pw"}); err == nil {
		t.Fatal("sftp dial to closed port should error")
	}
}

// --- fsIn / fsOut nodes ---

// TestFsInLocalDir deploys fsIn → file: a file dropped in the watched
// local dir is emitted and written out by the downstream file node.
func TestFsInLocalDir(t *testing.T) {
	inDir := t.TempDir()
	outFile := filepath.Join(t.TempDir(), "out.txt")

	if err := os.WriteFile(filepath.Join(inDir, "job1.txt"), []byte("payload-one"), 0644); err != nil {
		t.Fatal(err)
	}

	workflow := &Workflow{
		ID:   "test-fsin-flow",
		Name: "fsIn flow",
		Nodes: []Node{
			{
				ID:   "fsin",
				Type: "fsIn",
				Data: map[string]interface{}{
					"location": inDir,
					"interval": 0.1,
					"format":   "utf8",
				},
			},
			{
				ID:   "sink",
				Type: "file",
				Data: map[string]interface{}{
					"filename":      outFile,
					"overwriteFile": "true",
				},
			},
		},
		Edges: []Edge{{ID: "e1", Source: "fsin", Target: "sink"}},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if raw, err := os.ReadFile(outFile); err == nil && string(raw) == "payload-one" {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	raw, _ := os.ReadFile(outFile)
	t.Fatalf("fsIn output never landed; out file = %q", raw)
}

// TestFsOutLocalDir sends a message through fsOut on a local locator
// and checks the file content (tmp+rename publish).
func TestFsOutLocalDir(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "nested")
	engine := NewEngine()

	writer := fileExecutor(t, engine, "fsOut", map[string]interface{}{
		"location":  outDir,
		"filename":  "result.txt",
		"createDir": true,
	})
	out, err := writer.Execute(context.Background(),
		&Message{Payload: "payload-out", Metadata: map[string]interface{}{}})
	if err != nil {
		t.Fatalf("fsOut execute failed: %v", err)
	}

	target := filepath.Join(outDir, "result.txt")
	if out.GetMetadata()["filename"] != target {
		t.Fatalf("metadata filename = %v, want %q", out.GetMetadata()["filename"], target)
	}
	raw, err := os.ReadFile(target)
	if err != nil || string(raw) != "payload-out" {
		t.Fatalf("result.txt = %q, %v", raw, err)
	}
	// No tmp fragment left behind.
	if _, err := os.Stat(target + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("tmp file left behind")
	}

	// Append mode rewrites via the same seam.
	appender := fileExecutor(t, engine, "fsOut", map[string]interface{}{
		"location":      outDir,
		"filename":      "result.txt",
		"overwriteFile": "false",
		"appendNewline": true,
	})
	if _, err := appender.Execute(context.Background(),
		&Message{Payload: "more", Metadata: map[string]interface{}{}}); err != nil {
		t.Fatalf("fsOut append failed: %v", err)
	}
	raw, _ = os.ReadFile(target)
	if string(raw) != "payload-outmore\n" {
		t.Fatalf("after append result.txt = %q", raw)
	}
}

// TestFsOutMsgFilename checks filenameType=msg is reduced to a base name.
func TestFsOutMsgFilename(t *testing.T) {
	outDir := t.TempDir()
	node := NewFsOutNode("w", &FsOutConfig{
		Location:     outDir,
		FilenameType: "msg",
	}, NewEngine())
	out, err := node.Execute(context.Background(), &Message{
		Payload:  "x",
		Metadata: map[string]interface{}{"filename": "../escape/evil.txt"},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outDir, "evil.txt")); err != nil {
		t.Fatalf("expected evil.txt in outDir: %v", err)
	}
	if out.GetMetadata()["filename"] != filepath.Join(outDir, "evil.txt") {
		t.Fatalf("metadata filename = %v", out.GetMetadata()["filename"])
	}
}
