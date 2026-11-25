package noderunner

import (
	"fmt"
	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
)

// This file implements per-node bounded inboxes at the send/traverse
// boundary (decision bounded_inbox_durable_queue, Option A).
//
// Each node may declare an inbox capacity and an overflow policy via
// node.GetData():
//
//	"inboxCapacity": <number>   — max pending messages (<=0 or absent: unbounded)
//	"inboxPolicy":   "block" | "drop-newest" | "drop-oldest" | "route-to-catch"
//	"inboxDurable":  <bool>     — SUPERSEDED (durable-by-default since task
//	                            always_persisted_messages; every node
//	                            inbox persists when a DurableInboxStore is
//                            configured). Parsed no longer; kept documented
//                            so stale config is understood, not honored.
//
// Flow-level defaults may be set on workflow.GetView() under the same keys;
// engine-level defaults live on WorkflowEngine.InboxCapacity /
// WorkflowEngine.InboxPolicy. With no capacity configured anywhere the
// inbox is unbounded and traversal is identical to the legacy
// sequential BFS.
//
// Overflow policies:
//   - block:          the message waits in a blocked waitlist and is
//     admitted (in order) once the node's pending count drops below
//     capacity. The traversal goroutine never sleeps, so a cyclic flow
//     cannot hard-deadlock — but producers can be starved indefinitely
//     if a cyclic flow keeps every inbox full. This is the documented
//     stall risk; bounded capacities plus the per-injection execution
//     cap bound the damage.
//   - drop-newest:    the incoming message is discarded (warn event).
//   - drop-oldest:    the node's oldest queued message is evicted to
//     make room (warn event).
//   - route-to-catch: the incoming message is discarded and delivered
//     to in-scope catch nodes as an error event (reuses dispatchCatch).
//
// Back-pressure is observable through the existing workflow event log:
// every overflow emits a "warn" NodeEvent naming the node, capacity and
// policy outcome, and route-to-catch additionally produces a catch
// error event.
//
// Storage is abstracted behind InboxBackend so the neat-backed durable
// backend (durable_message_queue task) can plug in via
// WorkflowEngine.InboxBackendFactory without touching the traversal.

// InboxItem is one queued unit of work: a target node plus the payload
// clone it will receive when executed. Seq carries the persisted row id
// for claim-capable durable stores (0 = transient or already-acked on
// dequeue): the engine passes it to inbox.ack after dispatch so the row
// outlives mid-process crashes.
type InboxItem struct {
	NodeID  string
	Payload nodestore.MessageInterface
	Seq     int64
}

// InboxBackend is the storage contract behind node inboxes. The memory
// backend (memInboxBackend) is the default; the durable neat-backed
// implementation from the durable_message_queue task plugs in through
// WorkflowEngine.InboxBackendFactory.
//
// Implementations must preserve global FIFO order for Dequeue while
// tracking a pending count per node. All calls happen on the traversal
// goroutine for the memory backend; a durable backend serving several
// runners is responsible for its own synchronization.
type InboxBackend interface {
	// Enqueue appends item to the global queue and the per-node count.
	Enqueue(item InboxItem) error
	// Dequeue removes and returns the globally-oldest queued item.
	Dequeue() (InboxItem, bool, error)
	// RemoveOldest removes and returns the oldest queued item for
	// nodeID (drop-oldest policy).
	RemoveOldest(nodeID string) (InboxItem, bool, error)
	// Pending reports how many items are queued for nodeID.
	Pending(nodeID string) (int, error)
	// Close releases backend resources (no-op for memory).
	Close() error
}

// memInboxBackend is the default in-memory InboxBackend: one shared
// FIFO slice plus per-node pending counts.
type memInboxBackend struct {
	items   []InboxItem
	pending map[string]int
}

func newMemInboxBackend() *memInboxBackend {
	return &memInboxBackend{pending: make(map[string]int)}
}

func (b *memInboxBackend) Enqueue(item InboxItem) error {
	b.items = append(b.items, item)
	b.pending[item.NodeID]++
	return nil
}

func (b *memInboxBackend) Dequeue() (InboxItem, bool, error) {
	if len(b.items) == 0 {
		return InboxItem{}, false, nil
	}
	item := b.items[0]
	b.items = b.items[1:]
	b.pending[item.NodeID]--
	return item, true, nil
}

func (b *memInboxBackend) RemoveOldest(nodeID string) (InboxItem, bool, error) {
	for i, item := range b.items {
		if item.NodeID == nodeID {
			b.items = append(b.items[:i], b.items[i+1:]...)
			b.pending[nodeID]--
			return item, true, nil
		}
	}
	return InboxItem{}, false, nil
}

func (b *memInboxBackend) Pending(nodeID string) (int, error) {
	return b.pending[nodeID], nil
}

func (b *memInboxBackend) Close() error { return nil }

// nodeInboxConfig is the resolved capacity + policy for one node.
type nodeInboxConfig struct {
	capacity int                  // <=0 means unbounded
	policy   nodebase.OverflowPolicy // only consulted when capacity > 0
}

// offerResult reports what offer did with an item so the engine can
// emit events and route drops to catch nodebase.
type offerResult struct {
	item     InboxItem
	capacity int
	policy   nodebase.OverflowPolicy
	dropped  *InboxItem // incoming item rejected (drop-newest / route-to-catch)
	evicted  *InboxItem // oldest queued item evicted (drop-oldest)
}

// inbox wraps an InboxBackend with per-node capacity + overflow policy
// and the block waitlist. One inbox exists per message traversal.
type inbox struct {
	backend InboxBackend
	def     nodeInboxConfig
	configs map[string]nodeInboxConfig
	// blocked holds block-policy messages waiting for capacity, in
	// arrival order. They are promoted into the backend by next()
	// whenever a dequeue frees a slot.
	blocked []InboxItem
}

func (b *inbox) configFor(nodeID string) nodeInboxConfig {
	if cfg, ok := b.configs[nodeID]; ok {
		return cfg
	}
	return b.def
}

// offer applies the target node's capacity + overflow policy to item.
// Accepted items (including block-policy items admitted later) reach
// the backend FIFO; rejected items are reported via offerResult so the
// engine can emit events and dispatch catch messages.
func (b *inbox) offer(item InboxItem) (offerResult, error) {
	cfg := b.configFor(item.NodeID)
	res := offerResult{item: item, capacity: cfg.capacity, policy: cfg.policy}

	full := false
	if cfg.capacity > 0 {
		pending, err := b.backend.Pending(item.NodeID)
		if err != nil {
			return res, err
		}
		full = pending >= cfg.capacity
	}
	if !full {
		return res, b.backend.Enqueue(item)
	}

	switch cfg.policy {
	case nodebase.OverflowDropOldest:
		evicted, ok, err := b.backend.RemoveOldest(item.NodeID)
		if err != nil {
			return res, err
		}
		if ok {
			ev := evicted
			res.evicted = &ev
		}
		return res, b.backend.Enqueue(item)
	case nodebase.OverflowBlock:
		// Park the message; next() promotes it once the node's pending
		// count drops below capacity.
		b.blocked = append(b.blocked, item)
		return res, nil
	default: // drop-newest, route-to-catch and unknown policies reject the new item
		dropped := item
		res.dropped = &dropped
		return res, nil
	}
}

// next returns the next runnable item. After each dequeue it promotes
// blocked waitlist entries whose nodes now have spare capacity. If the
// backend has drained but blocked items remain (their nodes stayed at
// capacity — only possible when queued items never executed), they are
// admitted anyway so the traversal cannot strand messages.
func (b *inbox) next() (InboxItem, bool, error) {
	item, ok, err := b.backend.Dequeue()
	if err != nil {
		return InboxItem{}, false, err
	}
	if ok {
		if err := b.promote(); err != nil {
			return InboxItem{}, false, err
		}
		return item, true, nil
	}
	if len(b.blocked) > 0 {
		waiting := b.blocked
		b.blocked = nil
		for _, w := range waiting {
			if err := b.backend.Enqueue(w); err != nil {
				return InboxItem{}, false, err
			}
		}
		return b.next()
	}
	return InboxItem{}, false, nil
}

// promote re-offers blocked messages whose nodes have spare capacity,
// preserving their arrival order.
func (b *inbox) promote() error {
	if len(b.blocked) == 0 {
		return nil
	}
	var still []InboxItem
	for _, item := range b.blocked {
		cfg := b.configFor(item.NodeID)
		fits := cfg.capacity <= 0
		if !fits {
			pending, err := b.backend.Pending(item.NodeID)
			if err != nil {
				return err
			}
			fits = pending < cfg.capacity
		}
		if fits {
			if err := b.backend.Enqueue(item); err != nil {
				return err
			}
		} else {
			still = append(still, item)
		}
	}
	b.blocked = still
	return nil
}

// close releases the backend.
func (b *inbox) close() {
	_ = b.backend.Close()
}

// ack deletes the persisted row for a fully-dispatched item when the
// backend supports post-dispatch acks (claim-capable durable stores).
func (b *inbox) ack(seq int64) error {
	if a, ok := b.backend.(seqAcker); ok {
		return a.Ack(seq)
	}
	return nil
}

// newInbox builds the traversal inbox for a workflow run: it resolves
// engine defaults, flow-level defaults (workflow.GetView() keys) and
// per-node overrides (node.GetData() keys) into nodeInboxConfigs.
func (e *WorkflowEngine) newInbox(workflowID string, workflow nodestore.WorkflowInterface) *inbox {
	// Durable-by-default (GOD ruling 2026-11-24): a configured
	// DurableInboxStore persists EVERY node inbox — the superseded
	// inboxDurable flags are no longer consulted. An engine with no
	// store keeps pure-memory inboxes (standalone dev without a DB).
	durableSet := map[string]bool{}
	if e.DurableInboxStore != nil {
		for _, node := range workflow.GetNodes() {
			durableSet[node.GetID()] = true
		}
	}

	var backend InboxBackend
	if e.InboxBackendFactory != nil {
		backend = e.InboxBackendFactory(workflowID)
	}
	if backend == nil && e.DurableInboxStore != nil {
		backend = NewDurableInboxBackend(workflowID, e.DurableInboxStore)
	}
	if backend == nil {
		backend = newMemInboxBackend()
	}
	// A custom factory backend may still be durable-aware — hand it
	// the resolved node set the same way.
	if aware, ok := backend.(interface{ SetDurableNodes(map[string]bool) }); ok {
		aware.SetDurableNodes(durableSet)
	}
	// Claim-capable backends get the draining identity: the runner id
	// and a claim TTL tied to the workflow lease TTL, so a dead
	// claimant's rows reopen exactly when its lease would.
	if c, ok := backend.(claimantSetter); ok {
		c.SetClaimant(e.runnerID(), e.leaseTTL())
	}

	def := nodeInboxConfig{capacity: e.InboxCapacity, policy: e.InboxPolicy}
	if def.policy == "" {
		def.policy = nodebase.OverflowBlock
	}
	if workflow.GetView() != nil {
		if v, ok := workflow.GetView()["inboxCapacity"]; ok {
			if c, err := nodebase.ToFloat(v); err == nil {
				def.capacity = int(c)
			}
		}
		if p, ok := nodebase.ParseOverflowPolicy(workflow.GetView()["inboxPolicy"]); ok {
			def.policy = p
		}
	}

	configs := make(map[string]nodeInboxConfig, len(workflow.GetNodes()))
	for _, node := range workflow.GetNodes() {
		cfg := def
		if v, ok := node.GetData()["inboxCapacity"]; ok {
			if c, err := nodebase.ToFloat(v); err == nil {
				cfg.capacity = int(c)
			}
		}
		if p, ok := nodebase.ParseOverflowPolicy(node.GetData()["inboxPolicy"]); ok {
			cfg.policy = p
		}
		configs[node.GetID()] = cfg
	}
	return &inbox{backend: backend, def: def, configs: configs}
}

// durableNodes was the per-node "inboxDurable" resolver. REMOVED by the
// always-persisted ruling (2026-11-24): durability is a property of the
// configured store, not a per-hop opt-in — newInbox marks every node
// when DurableInboxStore is set.

// reportOffer surfaces the outcome of an inbox offer through the
// workflow event log and, for route-to-catch, the catch machinery.
func (e *WorkflowEngine) reportOffer(workflowID string, byID map[string]nodestore.NodeInterface, res offerResult) {
	node := byID[res.item.NodeID]
	name := nodebase.NodeName(node)
	if res.dropped != nil {
		if res.policy == nodebase.OverflowRouteToCatch {
			e.EmitEvent(workflowID, res.item.NodeID, name, "warn",
				fmt.Sprintf("inbox full (capacity %d): routed message to catch", res.capacity))
			e.RouteFailure(workflowID, node, res.dropped.Payload,
				fmt.Errorf("node %q inbox overflow: capacity %d exceeded", res.item.NodeID, res.capacity))
		} else {
			e.EmitEvent(workflowID, res.item.NodeID, name, "warn",
				fmt.Sprintf("inbox full (capacity %d): dropped newest message", res.capacity))
		}
	}
	if res.evicted != nil {
		e.EmitEvent(workflowID, res.item.NodeID, name, "warn",
			fmt.Sprintf("inbox full (capacity %d): dropped oldest queued message", res.capacity))
	}
}
