package noderunner

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
)

// This file implements the structural default error path — the Switch
// "problem-jobs folder" model (task problem_messages).
// Node-RED has no default error path: an error no catch node claims is
// logged and the message evaporates. When the workflow designates a
// problem queue (workflow.GetView()["onError"] = <queue node id>), an
// uncaught failure is instead staged into that queue with an error
// envelope, inspectable and requeueable through the /api/queue-*
// endpoints — the problem queue IS the dead-letter queue.
//
// Precedence: a catch node in scope still wins (opt-in overrides);
// the problem queue is the fallback. The inbox/queue route-to-catch
// overflow policies share this path, so an overflowed message with no
// catch in scope lands in the problem queue too.
//
// Loop guards (a failure must never feed the problem queue back into
// itself):
//   - the problem queue node's own failure is never re-enqueued;
//   - payloads delivered downstream by the problem queue's drainer or
//     by a manual requeue are stamped Metadata["fromProblemQueue"]; a
//     failure carrying that mark is reported to catch (if any) but
//     never re-enqueued.
//
// "auto" mode: view.onError="auto" materializes a built-in hidden
// system queue under the well-known id ProblemQueueAutoNodeID
// ("__problems__") — no configured queue node required. It is a plain
// engine-level Queue stored in the same per-node state map, so the
// existing /api/queue-* endpoints inspect it unchanged (node_id
// "__problems__"). It is always held+manual (items leave only via the
// release API; there is no downstream wiring). With no durable store
// configured it degrades to memory-only staging (volatile items carry
// negative seqs so no persistence calls are attempted).

// ProblemQueueAutoNodeID is the well-known node id of the built-in
// system problem queue materialized by view.onError="auto".
const ProblemQueueAutoNodeID = "__problems__"

// ProblemQueueIDFromView returns the designated problem queue node id
// configured on the workflow view ("onError"). "auto" resolves to the
// built-in system queue id (ProblemQueueAutoNodeID).
func ProblemQueueIDFromView(workflow nodestore.WorkflowInterface) (string, bool) {
	if workflow == nil || workflow.GetView() == nil {
		return "", false
	}
	v, _ := workflow.GetView()["onError"].(string)
	v = strings.TrimSpace(v)
	if v == "" {
		return "", false
	}
	if v == "auto" {
		return ProblemQueueAutoNodeID, true
	}
	// A checkpoint is a human approval gate, not a dead-letter queue —
	// it is never eligible as the designated problem queue.
	for _, n := range workflow.GetNodes() {
		if n.GetID() == v && n.GetType() == "checkpoint" {
			return "", false
		}
	}
	return v, true
}

// problemQueueID resolves the designated problem queue for a running
// workflow.
func (e *WorkflowEngine) problemQueueID(workflowID string) (string, bool) {
	e.runMu.Lock()
	workflow, ok := e.flows[workflowID]
	e.runMu.Unlock()
	if !ok {
		return "", false
	}
	return ProblemQueueIDFromView(workflow)
}

// RouteFailure is the single failure-routing entry point. The
// precedence ladder (task error_catch_port):
//
//  1. the node's own opt-in "error" output port, when errorCatch is
//     enabled AND that port is wired (an enabled-but-unwired port
//     must not swallow the error);
//  2. catch nodes in scope (Node-RED semantics);
//  3. the designated problem queue — including messages already
//     marked fromCatch, whose catch branch failure is itself
//     uncaught.
func (e *WorkflowEngine) RouteFailure(workflowID string, node nodestore.NodeInterface, in nodestore.MessageInterface, cause error) {
	if e.dispatchErrorPort(workflowID, node, in, cause) {
		return
	}
	if e.dispatchCatch(workflowID, node, in, cause) {
		return
	}
	e.dispatchProblem(workflowID, node, in, cause)
}

// errorCatchOutputs maps the external-facing built-in types that
// support the opt-in errorCatch flag to their normal output-port
// count. With errorCatch: true the node gains a dedicated "error"
// port appended AFTER its normal outputs — port key
// output_{normal+1} — so wiring it never shifts existing port
// indices (mirrors the editor's dynamic-port rendering).
var errorCatchOutputs = map[string]int{
	"httpRequest": 1,
	"exec":        3,
	"tcpRequest":  1,
	"file":        1,
	"fileIn":      1,
	"mailOut":     0,
	"db":          1,
}

// errorCatchPort returns the node's dedicated error-port key
// ("output_N") when the type supports errorCatch and the flag is
// enabled, else "".
func errorCatchPort(node nodestore.NodeInterface) string {
	if !truthyFlag(node.GetData()["errorCatch"]) {
		return ""
	}
	normal, ok := errorCatchOutputs[node.GetType()]
	if !ok {
		return ""
	}
	return fmt.Sprintf("output_%d", normal+1)
}

// dispatchErrorPort routes a node executor error onto the node's
// dedicated error output port (errorCatch opt-in). It reports
// whether the failure was claimed: only when the flag is on AND the
// port has at least one outgoing edge — an unwired error port falls
// through to the catch/problem-queue ladder. The emitted message
// carries msg.error = {text, node, full} plus the original msg data
// under originalData, and inherits the original msgId (same
// preserveMsgId convention as catch) so a downstream httpResponse
// can still answer the triggering request.
func (e *WorkflowEngine) dispatchErrorPort(workflowID string, node nodestore.NodeInterface, in nodestore.MessageInterface, cause error) bool {
	port := errorCatchPort(node)
	if port == "" {
		return false
	}
	e.runMu.Lock()
	workflow, ok := e.flows[workflowID]
	e.runMu.Unlock()
	if !ok {
		return false
	}
	wired := false
	for _, edge := range workflow.GetEdges() {
		if edge.GetSource() == node.GetID() && edgePort(edge) == port {
			wired = true
			break
		}
	}
	if !wired {
		return false
	}

	var originalData interface{}
	var msgID string
	if in != nil {
		originalData = in.GetPayload()
		msgID, _ = in.GetMetadata()["msgId"].(string)
	}
	payload := nodestore.NewMessage().SetPayload(map[string]interface{}{
		"error": map[string]interface{}{
			"text": cause.Error(),
			"node": node.GetID(),
			"full": fmt.Sprintf("%s node %q (%s) failed: %s",
				node.GetType(), nodebase.NodeName(node), node.GetID(), cause.Error()),
		},
		"originalData": originalData,
	}).SetMetadata(map[string]interface{}{}).SetTimestamp(time.Now())
	if msgID != "" {
		payload.GetMetadata()["msgId"] = msgID
		payload.GetMetadata()["preserveMsgId"] = true
	}
	// Best effort, same as catch: a dead error branch must not mask
	// the original failure.
	if err := e.Emit(workflowID, node.GetID(), payload, []string{port}); err != nil {
		e.EmitEvent(workflowID, node.GetID(), nodebase.NodeName(node), "error",
			fmt.Sprintf("error port emission failed: %v", err))
		return false
	}
	e.EmitEvent(workflowID, node.GetID(), nodebase.NodeName(node), "warn",
		fmt.Sprintf("failure routed to %s error port", port))
	return true
}

// dispatchProblem stages an unclaimed failure into the workflow's
// designated problem queue. The staged payload carries the error
// envelope (msg.error = {node, nodebase.NodeName, type, message, ts,
// originalPayloadSummary}) plus the original msg data under
// originalData, consistent with catch semantics. It is a no-op when
// no problem queue is configured, the failure originated from the
// problem queue itself, or the failed message was delivered BY the
// problem queue (loop guard).
func (e *WorkflowEngine) dispatchProblem(workflowID string, node nodestore.NodeInterface, in nodestore.MessageInterface, cause error) {
	pid, ok := e.problemQueueID(workflowID)
	if !ok {
		return
	}
	if node.GetID() == pid {
		return // the problem queue's own failure never re-enqueues
	}
	if metadataFlag(in, "fromProblemQueue") {
		return // failure downstream of a problem-queue drain: no loop
	}
	q, ok := e.QueueHandle(workflowID, pid)
	if !ok && pid == ProblemQueueAutoNodeID {
		q = e.autoProblemQueue(workflowID)
		ok = q != nil
	}
	if !ok {
		e.EmitEvent(workflowID, node.GetID(), nodebase.NodeName(node), "error",
			fmt.Sprintf("uncaught failure dropped: problem queue %q not available", pid))
		return
	}

	var originalData interface{}
	if in != nil {
		originalData = in.GetPayload()
	}
	// The dead-letter envelope is persisted durably — mask credential
	// fields for the failed node's type (and any sentinels in flight)
	// the same way the run journal does, so secrets cannot ride the
	// original payload into queue storage.
	scrubbedData := scrubJournalValue(node.GetType(), originalData, 0)
	payload := nodestore.NewMessage().SetPayload(map[string]interface{}{
		"error": map[string]interface{}{
			"node":                   node.GetID(),
			"nodeName":               nodebase.NodeName(node),
			"type":                   node.GetType(),
			"message":                maskURICredentials(cause.Error()),
			"ts":                     time.Now().UTC().Format(time.RFC3339Nano),
			"originalPayloadSummary": summarizePayloadData(scrubbedData),
		},
		"originalData": scrubbedData,
	}).SetMetadata(map[string]interface{}{"problemMessage": true}).SetTimestamp(time.Now())

	if err := q.Enqueue(payload); err != nil {
		e.EmitEvent(workflowID, node.GetID(), nodebase.NodeName(node), "error",
			fmt.Sprintf("problem queue %q rejected failure: %v", pid, err))
		return
	}
	e.EmitEvent(workflowID, node.GetID(), nodebase.NodeName(node), "warn",
		fmt.Sprintf("uncaught failure routed to problem queue %q", pid))
}

// MarkProblemDelivery stamps the loop-guard flag on a payload about to
// be delivered downstream by the problem queue (drainer or manual
// requeue). A failure of that delivery must not re-enqueue into the
// same queue.
func (e *WorkflowEngine) MarkProblemDelivery(workflowID, nodeID string, p nodestore.MessageInterface) {
	pid, ok := e.problemQueueID(workflowID)
	if !ok || pid != nodeID || p == nil {
		return
	}
	if p.GetMetadata() == nil {
		p.SetMetadata(map[string]interface{}{})
	}
	p.GetMetadata()["fromProblemQueue"] = true
}

// autoProblemQueue materializes (once per run) and returns the
// built-in system problem queue for onError="auto". The queue is held
// and manual — it never drains on its own; items leave only through
// the release endpoints. With a durable store it persists and
// recovers rows like any queue node (its rows are keyed to
// ProblemQueueAutoNodeID and skipped by requeueDurable); without one
// it degrades to memory-only staging (volatile).
func (e *WorkflowEngine) autoProblemQueue(workflowID string) *Queue {
	state := e.GetOrCreateNodeState(workflowID, ProblemQueueAutoNodeID, func() interface{} {
		q := &Queue{
			engine:     e,
			workflowID: workflowID,
			node:       nodestore.NewNode(ProblemQueueAutoNodeID, "queue").SetData(map[string]interface{}{"name": "Problem messages"}),
			policy:     nodebase.OverflowBlock,
			hold:       true,
			manual:     true,
			notify:     make(chan struct{}, 1),
			stopCh:     make(chan struct{}),
			backend:    "durable",
		}
		q.store, q.storeErr = e.BuildQueueStore("durable", "", ProblemQueueAutoNodeID, nil)
		if q.store == nil {
			// No durable store configured: stage in memory rather than
			// rejecting every failure. Items get negative seqs so the
			// release/delete paths skip persistence calls.
			q.storeErr = nil
			q.volatile = true
		}
		// Shut the drainer down with the workflow. Registered inside
		// the factory (under runMu) so it happens exactly once per
		// queue instance.
		e.stoppers[workflowID] = append(e.stoppers[workflowID], stopperFunc(func() error {
			q.Stop()
			return nil
		}))
		return q
	})
	q, _ := state.(*Queue)
	if q == nil {
		return nil
	}

	// Arm once (idempotent): recovers surviving durable rows and starts
	// the drainer. Bound to the run context so a workflow without
	// stoppers still tears the drainer down on stop.
	e.runMu.Lock()
	ctx := e.runCtx[workflowID]
	e.runMu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	q.Arm(ctx)
	return q
}

// stopperFunc adapts a plain function to the nodebase.Stopper interface so the
// auto problem queue's drainer can ride the workflow's stopper list.
type stopperFunc func() error

// Stop implements nodebase.Stopper.
func (f stopperFunc) Stop() error { return f() }

// summarizePayloadData renders a bounded one-line preview of a failed
// message's data for the error envelope (same truncation as the queue
// item preview).
func summarizePayloadData(data interface{}) string {
	if data == nil {
		return "null"
	}
	raw, err := json.Marshal(data)
	if err != nil {
		raw = []byte(fmt.Sprintf("%v", data))
	}
	const max = 200
	if len(raw) > max {
		return string(raw[:max]) + "..."
	}
	return string(raw)
}
