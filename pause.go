package noderunner

import (
	"context"
	"fmt"
	"time"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// PauseWorkflow gracefully closes the workflow's delivery gate. Work
// already admitted before this call completes normally; trigger output
// admitted after this call is persisted into the durable inbox (or the
// process-local fallback when no durable store is configured) without
// dispatching any node.
func (e *WorkflowEngine) PauseWorkflow(workflowID string) error {
	pauseMu := e.pauseLock(workflowID)
	pauseMu.Lock()
	e.runMu.Lock()
	workflow := e.flows[workflowID]
	_, running := e.running[workflowID]
	if !running {
		e.runMu.Unlock()
		pauseMu.Unlock()
		return fmt.Errorf("cannot pause workflow %q: workflow is not running", workflowID)
	}
	if e.paused[workflowID] {
		e.runMu.Unlock()
		pauseMu.Unlock()
		return fmt.Errorf("cannot pause workflow %q: workflow is already paused", workflowID)
	}
	if workflow == nil {
		e.runMu.Unlock()
		pauseMu.Unlock()
		return fmt.Errorf("cannot pause workflow %q: running workflow state unavailable", workflowID)
	}
	e.paused[workflowID] = true
	workflow.SetPaused(true)
	e.runMu.Unlock()
	pauseMu.Unlock()

	e.EmitEvent(workflowID, "", "", "info", "workflow paused")
	return nil
}

// ResumeWorkflow reopens the delivery gate and drains the backlog that
// accumulated while the workflow was paused. Durable rows are filtered
// to the exact seqs admitted through the pause gate, so rows still owned
// by pre-pause in-flight traversals are not duplicated.
func (e *WorkflowEngine) ResumeWorkflow(workflowID string) error {
	pauseMu := e.pauseLock(workflowID)
	pauseMu.Lock()
	e.runMu.Lock()
	workflow := e.flows[workflowID]
	_, running := e.running[workflowID]
	if !running {
		e.runMu.Unlock()
		pauseMu.Unlock()
		return fmt.Errorf("cannot resume workflow %q: workflow is not running", workflowID)
	}
	if !e.paused[workflowID] {
		e.runMu.Unlock()
		pauseMu.Unlock()
		return fmt.Errorf("cannot resume workflow %q: workflow is not paused", workflowID)
	}
	if workflow == nil {
		e.runMu.Unlock()
		pauseMu.Unlock()
		return fmt.Errorf("cannot resume workflow %q: running workflow state unavailable", workflowID)
	}

	workflow.SetPaused(false)
	delete(e.paused, workflowID)
	seqSet := make(map[int64]bool, len(e.pausedSeqs[workflowID]))
	for seq := range e.pausedSeqs[workflowID] {
		seqSet[seq] = true
	}
	delete(e.pausedSeqs, workflowID)
	memoryItems := append([]InboxItem(nil), e.pausedMemory[workflowID]...)
	delete(e.pausedMemory, workflowID)
	ctx := e.runCtx[workflowID]
	if ctx == nil {
		ctx = context.Background()
	}
	e.runMu.Unlock()
	pauseMu.Unlock()

	e.EmitEvent(workflowID, "", "", "info", "workflow resumed")

	if len(seqSet) > 0 && e.DurableInboxStore != nil {
		var rows []DurableInboxItem
		for _, row := range e.loadDurableInboxRows(workflow) {
			if seqSet[row.Seq] {
				rows = append(rows, row)
			}
		}
		if len(rows) > 0 {
			go e.requeueDurable(ctx, workflow, rows)
		}
	}
	if len(memoryItems) > 0 {
		go e.requeuePausedMemory(ctx, workflow, memoryItems)
	}
	e.signalQueues(workflowID)
	return nil
}

// signalQueues wakes every live queue drainer for the workflow. Queue
// drainers sleep while the pause gate is closed; resume is the event
// that makes their already-staged items drainable again.
func (e *WorkflowEngine) signalQueues(workflowID string) {
	e.runMu.Lock()
	var queues []*Queue
	for _, state := range e.states[workflowID] {
		if q, ok := state.(*Queue); ok {
			queues = append(queues, q)
		}
	}
	e.runMu.Unlock()
	for _, q := range queues {
		q.signal()
	}
}

// workflowPaused reports whether the workflow's delivery gate is closed.
func (e *WorkflowEngine) workflowPaused(workflowID string) bool {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	return e.paused[workflowID]
}

// notePausedDurableRows adds survivor rows discovered while deploying a
// paused workflow to the resume backlog. At a cold boot there are no live
// owners for these rows; recording their seqs keeps them intact until the
// operator explicitly resumes instead of draining them during deploy.
func (e *WorkflowEngine) notePausedDurableRows(workflowID string, rows []DurableInboxItem) {
	if len(rows) == 0 {
		return
	}
	e.runMu.Lock()
	defer e.runMu.Unlock()
	if e.pausedSeqs[workflowID] == nil {
		e.pausedSeqs[workflowID] = make(map[int64]bool, len(rows))
	}
	for _, row := range rows {
		if row.Seq > 0 {
			e.pausedSeqs[workflowID][row.Seq] = true
		}
	}
}

// pausedDownstreamItems expands the outgoing-edge seed exactly as a normal
// traversal would, producing one cloned message per target. Capacity and
// overflow policy are deliberately not applied here: the workflow-level
// pause gate is itself the back-pressure point and the contract is that
// paused trigger output is queued rather than dropped.
func pausedDownstreamItems(workflow nodestore.WorkflowInterface, fromNodeID string, payload nodestore.MessageInterface, ports []string) []InboxItem {
	var allowed map[string]bool
	if ports != nil {
		allowed = make(map[string]bool, len(ports))
		for _, p := range ports {
			allowed[p] = true
		}
	}
	var items []InboxItem
	for _, edge := range workflow.GetEdges() {
		if edge.GetSource() != fromNodeID {
			continue
		}
		if allowed != nil && !allowed[edgePort(edge)] {
			continue
		}
		items = append(items, InboxItem{
			NodeID:  edge.GetTarget(),
			Payload: nodebase.ClonePayload(payload),
		})
	}
	return items
}

// admitPausedDownstreamLocked admits one new downstream seed without
// dispatching it. The caller must hold pauseLock(workflowID); this is what
// makes the admission atomic with pause/resume transitions. With a durable
// store the rows are written before the call returns and their seqs are
// remembered for resume. Without one the cloned seed items stay in a
// process-local backlog (memory-only engines make no restart durability
// promise in the first place).
func (e *WorkflowEngine) admitPausedDownstreamLocked(workflowID, fromNodeID string, workflow nodestore.WorkflowInterface, payload nodestore.MessageInterface, ports []string) error {
	items := pausedDownstreamItems(workflow, fromNodeID, payload, ports)
	if len(items) == 0 {
		return nil
	}
	byID := make(map[string]nodestore.NodeInterface, len(workflow.GetNodes()))
	for _, node := range workflow.GetNodes() {
		byID[node.GetID()] = node
	}

	var memoryItems []InboxItem
	for _, item := range items {
		// Queue/checkpoint inputs are already staging-point handoffs.
		// Stage them directly instead of writing an inbox row the live
		// queue will never adopt (activation-time adoption only happens
		// once). The queue itself persists before becoming drainable.
		if node, ok := byID[item.NodeID]; ok && (node.GetType() == "queue" || node.GetType() == "checkpoint") {
			checkpoint := node.GetType() == "checkpoint"
			var routes []string
			if checkpoint {
				routes = nodebase.ParseCheckpointRoutes(node.GetData()["routes"])
			}
			q, err := e.EnsureQueue(node, checkpoint, routes)
			if err != nil {
				return fmt.Errorf("paused queue admission failed for node %q: %w", item.NodeID, err)
			}
			if err := q.Enqueue(item.Payload); err != nil {
				return fmt.Errorf("paused queue admission failed for node %q: %w", item.NodeID, err)
			}
			continue
		}
		if e.DurableInboxStore == nil {
			memoryItems = append(memoryItems, item)
			continue
		}
		row := &DurableInboxItem{
			WorkflowID: workflowID,
			NodeID:     item.NodeID,
			Payload:    item.Payload,
			EnqueuedAt: time.Now(),
		}
		if err := e.DurableInboxStore.SaveInboxItem(row); err != nil {
			return fmt.Errorf("paused inbox persist failed for node %q: %w", item.NodeID, err)
		}
		e.runMu.Lock()
		if e.pausedSeqs[workflowID] == nil {
			e.pausedSeqs[workflowID] = map[int64]bool{}
		}
		e.pausedSeqs[workflowID][row.Seq] = true
		e.runMu.Unlock()
	}
	if len(memoryItems) > 0 {
		e.runMu.Lock()
		e.pausedMemory[workflowID] = append(e.pausedMemory[workflowID], memoryItems...)
		e.runMu.Unlock()
	}
	return nil
}

// requeuePausedMemory drains the process-local fallback backlog through a
// normal traversal after resume. Items bypass capacity because they were
// already accepted by the pause gate.
func (e *WorkflowEngine) requeuePausedMemory(ctx context.Context, workflow nodestore.WorkflowInterface, items []InboxItem) {
	e.traverse(ctx, workflow.GetID(), workflow, func(inboxes *inbox, _ map[string][]nodestore.EdgeInterface, _ func(string, nodestore.MessageInterface) error) {
		for _, item := range items {
			if err := inboxes.backend.Enqueue(item); err != nil {
				e.EmitEvent(workflow.GetID(), item.NodeID, "", "error", fmt.Sprintf("paused memory requeue failed: %v", err))
			}
		}
	}, nil)
}
