package noderunner

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	nodestore "github.com/dracory/nodestore"
	"github.com/dracory/nodebase"
)

// This file implements the queue node (decision
// folder_node): a named, durable, inspectable staging queue
// between elements — Enfocus Switch's staging-point model applied to
// messages.
//
// Design: the queue is an INDEPENDENT drainer with its own nodebase.Stopper
// lifecycle, not a buffered hop inside the synchronous traversal. A
// queue inside the traversal would just be a delay node with
// persistence; the Switch value is a staging point whose downstream can
// be held or redeployed while items keep arriving.
//
// Structure:
//   - QueueNode is the per-execution executor shell. Executors are
//     rebuilt per message, so the real queue lives in a shared
//     Queue stored in the engine's per-node state (same pattern
//     as join/sort buffers).
//   - QueueNode implements nodebase.Stopper + nodebase.Activator. Activation (deploy)
//     arms the drainer goroutine and recovers surviving durable rows;
//     message-time Execute stages the payload into the queue.
//   - Queue keeps the runtime FIFO (entries carry their durable
//     row seq) plus the block-policy waitlist. Every staged item is
//     persisted through DurableInboxStore BEFORE it becomes drainable;
//     the row is deleted only after the item is emitted downstream
//     (at-least-once: a crash between emit and delete re-delivers).
//
// Capacity/policy resolution mirrors the inbox keys with queue-local
// aliases: node.GetData()["capacity"|"inboxCapacity"] >
// workflow.GetView()["inboxCapacity"] > engine.InboxCapacity, same for
// "policy"/"inboxPolicy". Hold comes from node.GetData()["hold"] and can be
// toggled at runtime via the API (SetHold).
//
// Restart / lease handoff: queue rows live in the same inbox_messages
// table keyed by the queue node ID. requeueDurable in engine.go skips
// rows whose target is a queue node — the queue's own activation
// recovery loads them, so they are never delivered twice.

// The inspection-API queue item type is nodebase.QueueItem — the
// runner-agnostic contract the admin surface is written against (see
// nodes/inspection.go).

// queueEntry is one queued item: its persisted row seq (0 only for
// items admitted from the block waitlist whose persist already ran)
// plus the payload clone.
type queueEntry struct {
	seq        int64
	payload    nodestore.MessageInterface
	enqueuedAt time.Time
}

// Queue is the shared staging queue behind a queue node. One
// instance per workflow+node, stored in engine.states. All fields are
// guarded by mu.
type Queue struct {
	engine     *WorkflowEngine
	workflowID string
	node       nodestore.NodeInterface
	capacity   int
	policy     nodebase.OverflowPolicy

	mu      sync.Mutex
	entries []queueEntry
	blocked []queueEntry // block-policy waitlist (transient, like inbox.go)
	hold    bool
	// manual marks a checkpoint queue: the drainer runs (so recovery
	// and the blocked-waitlist promotion still work) but never emits —
	// items leave only through ReleaseItem route releases.
	manual bool
	// routes are the checkpoint's named release choices in port order:
	// routes[i] labels output_{i+1}.
	routes   []string
	armed    bool
	notify   chan struct{} // buffered(1): wakes the drainer
	stopCh   chan struct{}
	once     sync.Once
	backend  string
	dir      string
	store    DurableInboxStore // resolved persistence (durable | dir)
	storeErr error             // sticky store-construction error
	// volatile marks a memory-only queue (the auto problem queue on an
	// engine with no durable store): items stage without persistence
	// and get negative seqs so the ack/delete paths skip store calls.
	volatile    bool
	volatileSeq int64
	// noOutputWarned latches once the drainer reports an unwired queue,
	// so the warn event fires once per disconnected period instead of
	// on every drain signal.
	noOutputWarned bool
}

// ResolveQueueConfig reads capacity/policy/hold/backend off the node
// using inbox-key precedence (node > flow view > engine default).
// Backend defaults to "durable"; "dir" also resolves node.GetData()["dir"].
func (e *WorkflowEngine) ResolveQueueConfig(node nodestore.NodeInterface, workflow nodestore.WorkflowInterface) (int, nodebase.OverflowPolicy, bool, string, string) {
	capacity := e.InboxCapacity
	if workflow != nil && workflow.GetView() != nil {
		if v, ok := workflow.GetView()["inboxCapacity"]; ok {
			if c, err := nodebase.ToFloat(v); err == nil {
				capacity = int(c)
			}
		}
	}
	if v, ok := node.GetData()["inboxCapacity"]; ok {
		if c, err := nodebase.ToFloat(v); err == nil {
			capacity = int(c)
		}
	}
	if v, ok := node.GetData()["capacity"]; ok {
		if c, err := nodebase.ToFloat(v); err == nil {
			capacity = int(c)
		}
	}

	policy := e.InboxPolicy
	if workflow != nil && workflow.GetView() != nil {
		if p, ok := nodebase.ParseOverflowPolicy(workflow.GetView()["inboxPolicy"]); ok {
			policy = p
		}
	}
	if p, ok := nodebase.ParseOverflowPolicy(node.GetData()["inboxPolicy"]); ok {
		policy = p
	}
	if p, ok := nodebase.ParseOverflowPolicy(node.GetData()["policy"]); ok {
		policy = p
	}
	if policy == "" {
		policy = nodebase.OverflowBlock
	}

	hold, holdSet := ParseDurableFlag(node.GetData()["hold"])
	if !holdSet {
		// The designated problem queue is a staging folder: it must
		// not auto-drain — items leave only via hold/release or a
		// manual requeue. An explicit hold=false in config overrides.
		if pid, ok := ProblemQueueIDFromView(workflow); ok && pid == node.GetID() {
			hold = true
		}
	}

	backend, _ := node.GetData()["backend"].(string)
	backend = strings.ToLower(strings.TrimSpace(backend))
	if backend == "" {
		backend = "durable"
	}
	dir, _ := node.GetData()["dir"].(string)
	return capacity, policy, hold, backend, dir
}

// buildStore resolves this queue's persistence backend. "durable"
// uses the engine-wide DurableInboxStore; "dir" constructs a
// DirQueueStore rooted at dir, scoped to this queue's node ID so
// several queue nodes can share one directory without cross-deleting
// or cross-reloading each other's files. Construction errors are
// stored on the queue (sticky) so every store user reports the same
// failure.
func (e *WorkflowEngine) BuildQueueStore(backend, dir, nodeID string, nodeData map[string]interface{}) (DurableInboxStore, error) {
	switch backend {
	case "durable":
		if e.DurableInboxStore == nil {
			return nil, fmt.Errorf("no DurableInboxStore configured")
		}
		return e.DurableInboxStore, nil
	case "dir":
		fsys, path, err := nodebase.OpenFs(dir, nodebase.FsCredentialsFromData(nodeData))
		if err != nil {
			return nil, fmt.Errorf("dir backend locator: %w", err)
		}
		return NewDirQueueStoreFS(fsys, path, nodeID)
	default:
		return nil, fmt.Errorf("unknown queue backend %q", backend)
	}
}

// queueStore returns the resolved persistence backend (or its sticky
// construction error).
func (q *Queue) queueStore() (DurableInboxStore, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.storeLocked()
}

// storeLocked is queueStore for callers already holding q.mu.
func (q *Queue) storeLocked() (DurableInboxStore, error) {
	if q.store == nil && q.storeErr == nil {
		// Defensive: queue() always resolves, but do not panic if a
		// caller ever reaches here first.
		q.store, q.storeErr = q.engine.BuildQueueStore(q.backend, q.dir, q.node.GetID(), q.node.GetData())
	}
	return q.store, q.storeErr
}

// signal wakes the drainer (non-blocking).
func (q *Queue) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// arm starts the drainer once and recovers surviving durable rows.
// Idempotent — activation calls it, and it is the only place rows are
// adopted from the store.
func (q *Queue) Arm(ctx context.Context) {
	q.mu.Lock()
	if q.armed {
		q.mu.Unlock()
		return
	}
	q.armed = true
	q.mu.Unlock()

	store, storeErr := q.queueStore()
	if storeErr != nil {
		q.engine.EmitEvent(q.workflowID, q.node.GetID(), nodebase.NodeName(q.node), "error",
			fmt.Sprintf("queue backend unavailable: %v", storeErr))
	}
	if store != nil {
		rows, err := store.LoadInbox(q.workflowID)
		if err != nil {
			q.engine.EmitEvent(q.workflowID, q.node.GetID(), nodebase.NodeName(q.node), "error",
				fmt.Sprintf("queue recovery failed: %v", err))
		} else {
			recovered := 0
			q.mu.Lock()
			for i := range rows {
				if rows[i].NodeID != q.node.GetID() {
					continue
				}
				q.entries = append(q.entries, queueEntry{
					seq:        rows[i].Seq,
					payload:    rows[i].Payload,
					enqueuedAt: rows[i].EnqueuedAt,
				})
				recovered++
			}
			q.mu.Unlock()
			if recovered > 0 {
				q.engine.EmitEvent(q.workflowID, q.node.GetID(), nodebase.NodeName(q.node), "info",
					fmt.Sprintf("queue recovered %d queued message(s)", recovered))
			}
		}
	}

	go q.drainLoop(ctx)
	q.signal()
}

// stop shuts the drainer down (nodebase.Stopper path, idempotent) and releases
// the store's filesystem connection (an sftp/ftp session is a real
// connection; a local dir is a no-op).
func (q *Queue) Stop() {
	q.once.Do(func() { close(q.stopCh) })
	if store, err := q.queueStore(); err == nil {
		if closer, ok := store.(io.Closer); ok {
			_ = closer.Close()
		}
	}
}

// drainLoop emits queued items downstream in FIFO order while not held.
func (q *Queue) drainLoop(ctx context.Context) {
	for {
		select {
		case <-q.stopCh:
			return
		case <-ctx.Done():
			return
		case <-q.notify:
		}
		for q.drainOne(ctx) {
		}
	}
}

// drainOne emits the oldest queued entry if draining is allowed.
// Returns false when the queue is empty, held or stopped — the loop
// then waits for the next signal.
func (q *Queue) drainOne(ctx context.Context) bool {
	q.mu.Lock()
	if q.manual || q.hold || len(q.entries) == 0 {
		q.mu.Unlock()
		return false
	}
	// An unwired queue must not drain: emitSync would deliver to zero
	// edges and the item would be silently dropped. Hold the items
	// instead — they release once an edge exists (redeploy re-arms the
	// drainer) or via a manual release after wiring.
	if !q.engine.HasOutgoingEdges(q.workflowID, q.node.GetID(), nil) {
		warned := q.noOutputWarned
		q.noOutputWarned = true
		q.mu.Unlock()
		if !warned {
			q.engine.EmitEvent(q.workflowID, q.node.GetID(), nodebase.NodeName(q.node), "warn",
				"queue has no outgoing connections; holding items until wired")
		}
		return false
	}
	q.noOutputWarned = false
	entry := q.entries[0]
	q.entries = q.entries[1:]
	q.mu.Unlock()

	// Fix-and-reinject: stores that can re-read an item (dir backend)
	// refresh the payload at release time so external edits deliver;
	// a vanished file reconciles the entry away.
	present, err := q.reloadEntryPayload(&entry)
	if err != nil {
		q.mu.Lock()
		q.entries = append([]queueEntry{entry}, q.entries...)
		q.mu.Unlock()
		return false
	}
	if !present {
		q.engine.EmitEvent(q.workflowID, q.node.GetID(), nodebase.NodeName(q.node), "warn",
			fmt.Sprintf("queue item seq=%d no longer persisted; dropped", entry.seq))
		q.promote()
		return true
	}

	// Deliver synchronously so queued items cannot overtake each other
	// on separate traversal goroutines — the queue's FIFO is the point.
	// The problem-queue loop guard rides along on the payload.
	q.engine.MarkProblemDelivery(q.workflowID, q.node.GetID(), entry.payload)
	if err := q.engine.DeliverSync(q.workflowID, q.node.GetID(), entry.payload, nil); err != nil {
		// nodestore.WorkflowInterface torn down mid-drain or context cancelled: put the
		// entry back; the row was never deleted so nothing is lost.
		q.mu.Lock()
		q.entries = append([]queueEntry{entry}, q.entries...)
		q.mu.Unlock()
		return false
	}

	// Ack the persisted row only after the emit was accepted —
	// at-least-once: a crash in between re-delivers.
	if entry.seq > 0 {
		store, err := q.queueStore()
		if err == nil {
			err = store.DeleteInboxItem(q.workflowID, entry.seq)
		}
		if err != nil {
			q.engine.EmitEvent(q.workflowID, q.node.GetID(), nodebase.NodeName(q.node), "error",
				fmt.Sprintf("queue ack failed (will retry): %v", err))
			q.mu.Lock()
			q.entries = append([]queueEntry{entry}, q.entries...)
			q.mu.Unlock()
			return false
		}
	}
	q.promote()
	return true
}

// promote admits blocked waitlist entries while the queue has spare
// capacity, persisting each before it becomes drainable.
func (q *Queue) promote() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.blocked) > 0 && (q.capacity <= 0 || len(q.entries) < q.capacity) {
		next := q.blocked[0]
		stored := &DurableInboxItem{
			WorkflowID: q.workflowID,
			NodeID:     q.node.GetID(),
			Payload:    next.payload,
			EnqueuedAt: time.Now(),
		}
		store, storeErr := q.storeLocked()
		if storeErr != nil {
			q.engine.EmitEvent(q.workflowID, q.node.GetID(), nodebase.NodeName(q.node), "error",
				fmt.Sprintf("queue backend unavailable: %v", storeErr))
			return
		}
		if err := store.SaveInboxItem(stored); err != nil {
			q.engine.EmitEvent(q.workflowID, q.node.GetID(), nodebase.NodeName(q.node), "error",
				fmt.Sprintf("queue persist failed: %v", err))
			return
		}
		next.seq = stored.Seq
		q.blocked = q.blocked[1:]
		q.entries = append(q.entries, next)
	}
	q.signal()
}

// enqueue stages a delivered payload under the queue's capacity and
// overflow policy. Persisted rows are written before the entry becomes
// drainable; a persist failure rejects the delivery (error event on the
// queue node) rather than degrading to memory.
func (q *Queue) Enqueue(p nodestore.MessageInterface) error {
	q.mu.Lock()
	full := q.capacity > 0 && len(q.entries)+len(q.blocked) >= q.capacity
	if !full {
		q.mu.Unlock()
		return q.persistAndStage(p)
	}
	policy := q.policy
	switch policy {
	case nodebase.OverflowBlock:
		q.blocked = append(q.blocked, queueEntry{payload: p, enqueuedAt: time.Now()})
		q.mu.Unlock()
		return nil
	case nodebase.OverflowDropOldest:
		if len(q.entries) == 0 {
			// Capacity is consumed by the block waitlist alone — nothing
			// persisted to evict; reject the new item instead.
			q.mu.Unlock()
			q.engine.EmitEvent(q.workflowID, q.node.GetID(), nodebase.NodeName(q.node), "warn",
				fmt.Sprintf("queue full (capacity %d): dropped newest message", q.capacity))
			return nil
		}
		evicted := q.entries[0]
		q.entries = q.entries[1:]
		q.mu.Unlock()
		if evicted.seq > 0 {
			if store, err := q.queueStore(); err == nil {
				_ = store.DeleteInboxItem(q.workflowID, evicted.seq)
			}
		}
		q.engine.EmitEvent(q.workflowID, q.node.GetID(), nodebase.NodeName(q.node), "warn",
			fmt.Sprintf("queue full (capacity %d): dropped oldest queued message", q.capacity))
		return q.persistAndStage(p)
	case nodebase.OverflowRouteToCatch:
		q.mu.Unlock()
		q.engine.EmitEvent(q.workflowID, q.node.GetID(), nodebase.NodeName(q.node), "warn",
			fmt.Sprintf("queue full (capacity %d): routed message to catch", q.capacity))
		q.engine.RouteFailure(q.workflowID, q.node, p,
			fmt.Errorf("queue %q full: capacity %d exceeded", nodebase.NodeName(q.node), q.capacity))
		return nil
	default: // drop-newest and unknown policies reject the new item
		q.mu.Unlock()
		q.engine.EmitEvent(q.workflowID, q.node.GetID(), nodebase.NodeName(q.node), "warn",
			fmt.Sprintf("queue full (capacity %d): dropped newest message", q.capacity))
		return nil
	}
}

// persistAndStage writes the durable row then appends the entry. A
// volatile queue (auto problem queue without a durable store) stages
// the entry with a negative synthetic seq instead — nothing is
// persisted and every store-touching path keys off seq > 0.
func (q *Queue) persistAndStage(p nodestore.MessageInterface) error {
	entry := queueEntry{payload: p, enqueuedAt: time.Now()}
	if q.volatile {
		q.mu.Lock()
		q.volatileSeq--
		entry.seq = q.volatileSeq
		q.mu.Unlock()
	} else {
		store, err := q.queueStore()
		if err != nil {
			return fmt.Errorf("queue persist failed: %w", err)
		}
		stored := &DurableInboxItem{
			WorkflowID: q.workflowID,
			NodeID:     q.node.GetID(),
			Payload:    p,
			EnqueuedAt: time.Now(),
		}
		if err := store.SaveInboxItem(stored); err != nil {
			return fmt.Errorf("queue persist failed: %w", err)
		}
		entry.seq = stored.Seq
		entry.enqueuedAt = stored.EnqueuedAt
	}
	q.mu.Lock()
	q.entries = append(q.entries, entry)
	q.mu.Unlock()
	q.signal()
	return nil
}

/* ---- Inspection API surface (QueueHandle) ---- */

// Items returns the queued entries in FIFO order.
func (q *Queue) Items() []nodebase.QueueItem {
	q.mu.Lock()
	defer q.mu.Unlock()
	items := make([]nodebase.QueueItem, 0, len(q.entries))
	for _, e := range q.entries {
		items = append(items, nodebase.QueueItem{Seq: e.seq, EnqueuedAt: e.enqueuedAt, Payload: e.payload})
	}
	return items
}

// Count reports the queued depth (excluding the block waitlist).
func (q *Queue) Count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries)
}

// Held reports whether the queue is held (accepting, not draining).
func (q *Queue) Held() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.hold
}

// SetHold toggles the hold gate at runtime; releasing wakes the drainer.
func (q *Queue) SetHold(hold bool) {
	q.mu.Lock()
	q.hold = hold
	q.mu.Unlock()
	if !hold {
		q.signal()
	}
}

// DeleteItem drops the queued item with the given seq, removing its
// persisted row.
func (q *Queue) DeleteItem(seq int64) error {
	q.mu.Lock()
	idx := -1
	for i, e := range q.entries {
		if e.seq == seq {
			idx = i
			break
		}
	}
	if idx < 0 {
		q.mu.Unlock()
		return fmt.Errorf("no queued item with seq %d", seq)
	}
	q.entries = append(q.entries[:idx], q.entries[idx+1:]...)
	q.mu.Unlock()
	if seq > 0 {
		store, err := q.queueStore()
		if err != nil {
			return fmt.Errorf("failed to delete queue row: %w", err)
		}
		if err := store.DeleteInboxItem(q.workflowID, seq); err != nil {
			return fmt.Errorf("failed to delete queue row: %w", err)
		}
	}
	q.promote()
	return nil
}

// RouteLabels returns the checkpoint's route labels in port order —
// routes[i] labels output_{i+1}. An empty/unlabeled route reports its
// port key ("output_2" style). Plain queues report nil.
func (q *Queue) RouteLabels() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.routes) == 0 {
		return nil
	}
	labels := make([]string, len(q.routes))
	for i, label := range q.routes {
		labels[i] = label
		if strings.TrimSpace(labels[i]) == "" {
			labels[i] = fmt.Sprintf("output_%d", i+1)
		}
	}
	return labels
}

// resolveRoutePort maps a route name to its output port key. An empty
// route resolves to "" = all outgoing edges (plain queue behaviour). A
// route may be given as its label or directly as a port key.
func (q *Queue) resolveRoutePort(route string) (string, error) {
	route = strings.TrimSpace(route)
	if route == "" {
		return "", nil
	}
	labels := q.RouteLabels()
	if len(labels) == 0 {
		return "", fmt.Errorf("queue has no named routes")
	}
	for i, label := range labels {
		port := fmt.Sprintf("output_%d", i+1)
		if route == port || strings.EqualFold(route, label) {
			return port, nil
		}
	}
	return "", fmt.Errorf("unknown route %q (routes: %s)", route, strings.Join(labels, ", "))
}

// RequeueItem releases one queued item downstream immediately, out of
// order, and removes its persisted row — the manual single-item release
// used by the inspection API / dead-letter rework.
func (q *Queue) RequeueItem(seq int64) error {
	return q.ReleaseItem(seq, "", "")
}

// ReleaseItem is the route-parameterized requeue used by checkpoints:
// route picks which outgoing edge the item takes (a route label or an
// "output_N" port key; empty = all edges, identical to RequeueItem).
// On a routed release the payload records the approval metadata under
// Metadata["checkpoint"] = {route, approvedBy, at}.
func (q *Queue) ReleaseItem(seq int64, route string, approvedBy string) error {
	port, err := q.resolveRoutePort(route)
	if err != nil {
		return err
	}
	var ports []string
	if port != "" {
		ports = []string{port}
	}

	// Same no-drop guard as drainOne: releasing onto an unwired port
	// would silently lose the item.
	if !q.engine.HasOutgoingEdges(q.workflowID, q.node.GetID(), ports) {
		return fmt.Errorf("queue has no outgoing connection for the selected route; item kept")
	}

	q.mu.Lock()
	idx := -1
	var entry queueEntry
	for i, e := range q.entries {
		if e.seq == seq {
			idx = i
			entry = e
			break
		}
	}
	if idx < 0 {
		q.mu.Unlock()
		return fmt.Errorf("no queued item with seq %d", seq)
	}
	q.entries = append(q.entries[:idx], q.entries[idx+1:]...)
	q.mu.Unlock()

	// Fix-and-reinject (see drainOne): re-read edited items; a vanished
	// file means the item was removed externally.
	present, err := q.reloadEntryPayload(&entry)
	if err != nil {
		q.mu.Lock()
		q.entries = append([]queueEntry{entry}, q.entries...)
		q.mu.Unlock()
		return err
	}
	if !present {
		q.promote()
		return fmt.Errorf("queue item seq=%d no longer persisted", seq)
	}

	// Human-in-the-loop bookkeeping: which route released the item,
	// who approved it (when the caller knows), and when.
	if port != "" && entry.payload != nil {
		if entry.payload.GetMetadata() == nil {
			entry.payload.SetMetadata(map[string]interface{}{})
		}
		entry.payload.GetMetadata()["checkpoint"] = map[string]interface{}{
			"route":      route,
			"approvedBy": approvedBy,
			"at":         time.Now().UTC().Format(time.RFC3339Nano),
		}
	}

	// Problem-queue loop guard (see drainOne).
	q.engine.MarkProblemDelivery(q.workflowID, q.node.GetID(), entry.payload)
	if err := q.engine.DeliverSync(q.workflowID, q.node.GetID(), entry.payload, ports); err != nil {
		// Put it back so the item is not lost.
		q.mu.Lock()
		q.entries = append([]queueEntry{entry}, q.entries...)
		q.mu.Unlock()
		return err
	}
	if entry.seq > 0 {
		store, err := q.queueStore()
		if err == nil {
			err = store.DeleteInboxItem(q.workflowID, entry.seq)
		}
		if err != nil {
			return fmt.Errorf("failed to delete queue row: %w", err)
		}
	}
	q.promote()
	return nil
}
