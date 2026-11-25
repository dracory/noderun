package noderunner

import (
	"fmt"
	nodestore "github.com/dracory/nodestore"
	"strings"
	"sync"
	"time"
)

// This file implements the durable inbox backend (decision
// bounded_inbox_durable_queue, extended by task
// always_persisted_messages): a write-through persistence
// layer behind InboxBackend so queued messages survive a process crash
// or restart — "the queue is a place, not a pipe".
//
// Durability is now the DEFAULT whenever a DurableInboxStore is
// configured (GOD ruling 2026-11-24: multiple runners may share one DB,
// so every hop must leave a durable record). The per-node opt-in keys —
// node.Data["inboxDurable"], workflow.View["inboxDurable"],
// WorkflowEngine.InboxDurable — are SUPERSEDED and no longer consulted;
// they remain parseable only for config compatibility. An engine with
// no store keeps pure-memory inboxes (standalone dev without a DB).
//
// Semantics:
//   - Enqueue writes a 'pending' row BEFORE the item becomes visible to
//     the traversal; a persist failure fails the offer (reported as an
//     inbox error event) rather than silently degrading to memory.
//   - Ordering is preserved by the store's monotonically increasing
//     sequence column, assigned at persist time; LoadInbox returns rows
//     in seq order.
//   - Items parked on the block-policy waitlist are not yet persisted
//     (they were never admitted); capacity bounds that loss.
//   - The in-memory entry list is a traversal-time cache; the DB row is
//     the store of record.
//
// Claim lifecycle (stores implementing InboxClaimer, e.g.
// NeatInboxStore): Dequeue transitions the row pending -> claimed via
// an atomic conditional UPDATE (claimed_by = runner id, claim_expires_at
// = now + claim TTL), and Ack(seq) deletes it only AFTER the item was
// dispatched to the node and its downstream offers persisted. A crash
// between claim and ack leaves a 'claimed' row — distinguishable from
// never-picked-up 'pending' — which becomes claimable again after
// expiry (claim is a lease, never a bare delete; at-least-once, msgId
// is the dedup key). Stores without claim support keep the legacy
// delete-on-dequeue behaviour (the single in-flight item may be lost or
// delivered twice).
//
// Restart / cross-runner:
//   - On every workflow start the engine re-queues surviving rows for
//     that workflow in seq order (see requeueDurable in engine.go).
//   - Multi-runner: runners sharing one DSN share the
//     node_inbox_messages table. nodestore.WorkflowInterface leases
//     (engine_lease.go) guarantee at most one runner executes a
//     workflow; the workflow lease is the coarse lock and the claim
//     columns are the fine-grained receipt. When a runner dies its
//     lease expires; the runner that next starts the workflow claims
//     the lease and re-queues the dead runner's pending + expired
//     rows — the lease handoff IS the queue handoff.
//   - The synchronous httpTrigger -> httpResponse path is out of
//     scope: the client socket dies with the process, so a persisted
//     response buys nothing.

// DurableInboxItem is one persisted inbox row. Seq is assigned by the
// store on save and defines global dequeue order.
type DurableInboxItem struct {
	WorkflowID string
	NodeID     string
	Seq        int64
	Payload    nodestore.MessageInterface
	EnqueuedAt time.Time
}

// DurableInboxStore is the persistence contract behind the durable
// inbox backend. It lives in flowrunner (like LeaseStore) because the
// import direction is flowadmin/storage -> flowrunner.
// Implementations must return rows ordered by Seq.
type DurableInboxStore interface {
	// LoadInbox returns all surviving queued items for a workflow in
	// sequence order.
	LoadInbox(workflowID string) ([]DurableInboxItem, error)
	// SaveInboxItem persists item and fills in its Seq.
	SaveInboxItem(item *DurableInboxItem) error
	// DeleteInboxItem removes one persisted row (delivery ack).
	DeleteInboxItem(workflowID string, seq int64) error
	// DeleteWorkflowInbox drops every queued row for a workflow
	// (called when the workflow itself is deleted).
	DeleteWorkflowInbox(workflowID string) error
}

// storedItemAdmitter is the optional backend contract used by the
// restart requeue: it admits an already-persisted item together with
// its row seq, so dequeue acks the ORIGINAL row instead of inserting
// a duplicate.
type storedItemAdmitter interface {
	AdmitStored(item InboxItem, seq int64)
}

// InboxClaimer is the optional DurableInboxStore extension for the
// pending -> claimed -> deleted lifecycle. ClaimInboxItem performs one
// atomic conditional UPDATE: it succeeds only when the row is pending,
// its previous claim expired, or runnerID already holds it. claimed=false
// means a live claim by another runner — the drainer must skip the row.
type InboxClaimer interface {
	ClaimInboxItem(workflowID string, seq int64, runnerID string, ttl time.Duration) (claimed bool, err error)
}

// workflowClaimLoader is the optional DurableInboxStore extension that
// lets a restarting runner also reload rows still claimed by itself
// (crash leftovers) without waiting out the claim TTL.
type workflowClaimLoader interface {
	LoadInboxFor(workflowID, runnerID string) ([]DurableInboxItem, error)
}

// claimantSetter is the optional backend hook the engine uses to hand
// the draining identity (runner id + claim TTL) to a claim-aware
// backend. Called by newInbox once per traversal before the first offer.
type claimantSetter interface {
	SetClaimant(runnerID string, ttl time.Duration)
}

// seqAcker is the optional backend hook the engine calls AFTER an item
// was fully dispatched (node executed, downstream offers persisted) to
// delete the persisted row — the delivery ack.
type seqAcker interface {
	Ack(seq int64) error
}

// durableEntry pairs a queued item with its persisted row seq
// (0 = transient, never written to the store).
type durableEntry struct {
	item InboxItem
	seq  int64
}

// durableInboxBackend is a write-through InboxBackend: every queued
// item lives in an ordered in-memory entry list (preserving the exact
// FIFO semantics of memInboxBackend within a traversal), and items
// targeting durable-marked nodes additionally persist a row on
// enqueue and delete it on removal.
type durableInboxBackend struct {
	workflowID string
	store      DurableInboxStore
	durable    map[string]bool
	// claimant identifies the draining runner for stores that support
	// claims (InboxClaimer); empty keeps the legacy delete-on-dequeue.
	claimant string
	claimTTL time.Duration
	mu       sync.Mutex
	entries  []durableEntry
	pending  map[string]int
}

// NewDurableInboxBackend creates the backend plugged in via
// WorkflowEngine.DurableInboxStore (or returned from a custom
// InboxBackendFactory). Nodes are marked durable via SetDurableNodes,
// called by newInbox after resolving config.
func NewDurableInboxBackend(workflowID string, store DurableInboxStore) InboxBackend {
	return &durableInboxBackend{
		workflowID: workflowID,
		store:      store,
		durable:    map[string]bool{},
		pending:    map[string]int{},
	}
}

// SetDurableNodes records which node inboxes are durable for this run.
// Called by newInbox once per traversal before the first offer. Since
// the always-persisted ruling newInbox passes every node ID — the set
// remains so a custom InboxBackendFactory backend can still mark a
// subset.
func (b *durableInboxBackend) SetDurableNodes(nodes map[string]bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.durable = nodes
}

// SetClaimant implements claimantSetter: the runner id and claim TTL
// used for pending -> claimed transitions at dequeue time.
func (b *durableInboxBackend) SetClaimant(runnerID string, ttl time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.claimant = runnerID
	b.claimTTL = ttl
}

// AdmitStored implements storedItemAdmitter: the item re-enters the
// queue carrying its surviving row seq so dequeue deletes that row.
// Requeued items bypass node inbox capacity and drop/block policy —
// the restart drain admits every surviving row unconditionally, which
// is acceptable (even desirable) since those items were already
// admitted by a previous run and must not be dropped now.
func (b *durableInboxBackend) AdmitStored(item InboxItem, seq int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries = append(b.entries, durableEntry{item: item, seq: seq})
	b.pending[item.NodeID]++
}

// Enqueue persists durable-node items first, then appends the entry.
func (b *durableInboxBackend) Enqueue(item InboxItem) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	seq := int64(0)
	if b.durable[item.NodeID] && b.store != nil {
		stored := &DurableInboxItem{
			WorkflowID: b.workflowID,
			NodeID:     item.NodeID,
			Payload:    item.Payload,
			EnqueuedAt: time.Now(),
		}
		if err := b.store.SaveInboxItem(stored); err != nil {
			return fmt.Errorf("durable inbox persist failed: %w", err)
		}
		seq = stored.Seq
	}
	b.entries = append(b.entries, durableEntry{item: item, seq: seq})
	b.pending[item.NodeID]++
	return nil
}

// Dequeue removes the oldest entry. With a claim-capable store the row
// transitions pending -> claimed here and is deleted later by Ack (after
// dispatch) — a crash in between leaves a reclaimable 'claimed' row. A
// row whose live claim belongs to another runner is skipped locally and
// left for its owner. Stores without claim support keep the legacy
// delete-on-dequeue. Store failures leave the entry queued and surface
// the error so the row cannot be silently dropped.
func (b *durableInboxBackend) Dequeue() (InboxItem, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	claimer, claims := b.store.(InboxClaimer)
	for len(b.entries) > 0 {
		entry := b.entries[0]
		if entry.seq > 0 {
			if claims && b.claimant != "" && b.claimTTL > 0 {
				claimed, err := claimer.ClaimInboxItem(b.workflowID, entry.seq, b.claimant, b.claimTTL)
				if err != nil {
					return InboxItem{}, false, fmt.Errorf("durable inbox claim failed: %w", err)
				}
				if !claimed {
					// Live claim by another runner — drop the stale
					// local copy; the row stays for its owner.
					b.entries = b.entries[1:]
					b.pending[entry.item.NodeID]--
					continue
				}
				// Keep the seq on the item so the engine acks (deletes)
				// the row after dispatch completes.
				entry.item.Seq = entry.seq
			} else {
				if err := b.store.DeleteInboxItem(b.workflowID, entry.seq); err != nil {
					return InboxItem{}, false, fmt.Errorf("durable inbox ack failed: %w", err)
				}
			}
		}
		b.entries = b.entries[1:]
		b.pending[entry.item.NodeID]--
		return entry.item, true, nil
	}
	return InboxItem{}, false, nil
}

// Ack implements seqAcker: the item was fully dispatched — delete its
// persisted row. Only reached for claim-capable stores; legacy stores
// already deleted on dequeue (items leave Dequeue with Seq == 0).
func (b *durableInboxBackend) Ack(seq int64) error {
	return b.store.DeleteInboxItem(b.workflowID, seq)
}

// RemoveOldest removes the oldest entry for nodeID (drop-oldest
// policy), deleting its persisted row first.
func (b *durableInboxBackend) RemoveOldest(nodeID string) (InboxItem, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, entry := range b.entries {
		if entry.item.NodeID != nodeID {
			continue
		}
		if entry.seq > 0 {
			if err := b.store.DeleteInboxItem(b.workflowID, entry.seq); err != nil {
				return InboxItem{}, false, fmt.Errorf("durable inbox ack failed: %w", err)
			}
		}
		b.entries = append(b.entries[:i], b.entries[i+1:]...)
		b.pending[nodeID]--
		return entry.item, true, nil
	}
	return InboxItem{}, false, nil
}

// Pending reports how many items are queued for nodeID.
func (b *durableInboxBackend) Pending(nodeID string) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.pending[nodeID], nil
}

// Close intentionally does NOT delete persisted rows: entries still
// queued when the traversal ends (stop, crash) are exactly what the
// next run's requeueDurable recovers.
func (b *durableInboxBackend) Close() error { return nil }

// ParseDurableFlag normalizes an "inboxDurable" config value. It
// accepts booleans, truthy strings ("true"/"yes"/"1"/"on") and
// numbers, matching how loosely-typed editor data arrives.
func ParseDurableFlag(value interface{}) (bool, bool) {
	switch v := value.(type) {
	case bool:
		return v, true
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "true", "yes", "1", "on":
			return true, true
		case "false", "no", "0", "off":
			return false, true
		}
	case float64:
		return v != 0, true
	case int:
		return v != 0, true
	}
	return false, false
}
