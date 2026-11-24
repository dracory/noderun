package noderunner_test

import (
	"context"
	. "github.com/dracory/noderun"
	"testing"
	"time"
)

// problemTestFlow builds inj -> bad (a template that always fails)
// plus an unwired queue node "pq" designated as the problem queue via
// workflow.View["onError"]. Extra nodes/edges extend the flow for the
// catch and rework-loop tests.
func problemTestFlow(id string, extraNodes []Node, extraEdges []Edge) *Workflow {
	nodes := []Node{
		{ID: "inj", Type: "junction", Data: map[string]interface{}{}},
		{ID: "bad", Type: "template", Data: map[string]interface{}{
			"name":     "always-fails",
			"template": "{{",
		}},
		{ID: "pq", Type: "queue", Data: map[string]interface{}{"name": "problem messages"}},
	}
	edges := []Edge{
		{ID: "e1", Source: "inj", Target: "bad"},
	}
	return &Workflow{
		ID:    id,
		Name:  id,
		View:  map[string]interface{}{"onError": "pq"},
		Nodes: append(nodes, extraNodes...),
		Edges: append(edges, extraEdges...),
	}
}

// waitForProblemCount polls the problem queue until it reports n items.
func waitForProblemCount(t *testing.T, engine *Engine, workflowID string, n int, timeout time.Duration) *Queue {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var q *Queue
	for time.Now().Before(deadline) {
		if h, ok := engine.QueueHandle(workflowID, "pq"); ok {
			q = h.(*Queue)
			if q.Count() == n {
				return q
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	return q
}

// TestProblemQueueUncaught verifies the structural error path: a node
// failure no catch node claims lands in the designated problem queue
// with the error envelope (node, nodeName, message, ts,
// originalPayloadSummary) instead of evaporating.
func TestProblemQueueUncaught(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := problemTestFlow("wf-prob", nil, nil)

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	injectInto(t, engine, workflow.ID, "hello")

	q := waitForProblemCount(t, engine, workflow.ID, 1, 3*time.Second)
	if q == nil || q.Count() != 1 {
		t.Fatalf("uncaught failure never reached the problem queue: %v", q)
	}
	if !q.Held() {
		t.Fatal("designated problem queue must default to held (no auto-drain)")
	}

	items := q.Items()
	data, ok := items[0].Payload.GetPayload().(map[string]interface{})
	if !ok {
		t.Fatalf("problem payload must carry the error envelope, got %T", items[0].Payload.GetPayload())
	}
	errMap, ok := data["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("problem payload missing msg.error: %v", data)
	}
	if errMap["node"] != "bad" || errMap["nodeName"] != "always-fails" {
		t.Fatalf("error envelope must identify the failing node, got %v", errMap)
	}
	if msg, _ := errMap["message"].(string); msg == "" {
		t.Fatalf("error envelope must carry the failure message, got %v", errMap)
	}
	if ts, _ := errMap["ts"].(string); ts == "" {
		t.Fatalf("error envelope must carry a timestamp, got %v", errMap)
	}
	if s, _ := errMap["originalPayloadSummary"].(string); s != `"hello"` {
		t.Fatalf("originalPayloadSummary must preview the failed msg, got %v", errMap)
	}
	if data["originalData"] != "hello" {
		t.Fatalf("originalData must carry the failed msg data, got %v", data)
	}
	if flag, _ := items[0].Payload.GetMetadata()["problemMessage"].(bool); !flag {
		t.Fatal("problem payload must be marked problemMessage in metadata")
	}
}

// waitForAutoProblemCount polls the built-in "__problems__" system
// queue until it reports n items.
func waitForAutoProblemCount(t *testing.T, engine *Engine, workflowID string, n int, timeout time.Duration) *Queue {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var q *Queue
	for time.Now().Before(deadline) {
		if h, ok := engine.QueueHandle(workflowID, ProblemQueueAutoNodeID); ok {
			q = h.(*Queue)
			if q.Count() == n {
				return q
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	return q
}

// TestProblemQueueAutoUncaught verifies onError="auto": with no
// configured queue node the uncaught failure lands in the built-in
// "__problems__" system queue, inspectable through the same
// QueueHandle the /api/queue-* endpoints use.
func TestProblemQueueAutoUncaught(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := problemTestFlow("wf-prob-auto", nil, nil)
	// Drop the designated queue node and switch to auto mode.
	workflow.Nodes = workflow.Nodes[:2]
	workflow.View["onError"] = "auto"

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	injectInto(t, engine, workflow.ID, "hello")

	q := waitForAutoProblemCount(t, engine, workflow.ID, 1, 3*time.Second)
	if q == nil || q.Count() != 1 {
		t.Fatalf("uncaught failure never reached the auto problem queue: %v", q)
	}
	if !q.Held() {
		t.Fatal("auto problem queue must be held (no auto-drain)")
	}
	if store.RowCount(workflow.ID) != 1 {
		t.Fatal("auto problem queue must persist rows like a queue node")
	}
	items := q.Items()
	data, ok := items[0].Payload.GetPayload().(map[string]interface{})
	if !ok {
		t.Fatalf("problem payload must carry the error envelope, got %T", items[0].Payload.GetPayload())
	}
	errMap, ok := data["error"].(map[string]interface{})
	if !ok || errMap["node"] != "bad" {
		t.Fatalf("error envelope must identify the failing node, got %v", data)
	}
}

// TestProblemQueueAutoVolatile verifies the auto queue degrades to
// memory-only staging when the engine has no durable store: failures
// are still collected and deletable instead of being dropped.
func TestProblemQueueAutoVolatile(t *testing.T) {
	workflow := problemTestFlow("wf-prob-auto-volatile", nil, nil)
	workflow.Nodes = workflow.Nodes[:2]
	workflow.View["onError"] = "auto"

	engine := NewEngine() // no DurableInboxStore
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	injectInto(t, engine, workflow.ID, "hello")

	q := waitForAutoProblemCount(t, engine, workflow.ID, 1, 3*time.Second)
	if q == nil || q.Count() != 1 {
		t.Fatalf("uncaught failure never reached the volatile auto queue: %v", q)
	}
	seq := q.Items()[0].Seq
	if err := q.DeleteItem(seq); err != nil {
		t.Fatalf("volatile item must be deletable: %v", err)
	}
	if q.Count() != 0 {
		t.Fatalf("delete must drain the volatile item, count=%d", q.Count())
	}
}

// TestProblemQueueCatchWins verifies opt-in overrides structural: with
// a catch node in scope the failure goes to the catch branch and the
// problem queue stays empty.
func TestProblemQueueCatchWins(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := problemTestFlow("wf-prob-catch",
		[]Node{
			{ID: "catch1", Type: "catch", Data: map[string]interface{}{"scope": "all"}},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{}},
		},
		[]Edge{
			{ID: "e2", Source: "catch1", Target: "dbg"},
		})

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	injectInto(t, engine, workflow.ID, "x")

	got := waitForDebugValues(engine, workflow.ID, 1, 3*time.Second)
	if len(got) != 1 {
		t.Fatalf("catch branch never ran, got %v", got)
	}
	// Give the (non-existent) fallback a moment to prove it did not fire.
	time.Sleep(150 * time.Millisecond)
	q := waitForProblemCount(t, engine, workflow.ID, 0, time.Second)
	if q == nil || q.Count() != 0 {
		t.Fatalf("catch claimed the failure; problem queue must stay empty, got %v", q)
	}
}

// TestProblemQueueRequeue verifies the rework loop end to end: an item
// staged by the error path is inspectable and requeueable through the
// same Queue handle the /api/queue-* endpoints use, and a requeue
// delivers it to the problem queue's downstream.
func TestProblemQueueRequeue(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := problemTestFlow("wf-prob-rework",
		[]Node{
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{}},
		},
		[]Edge{
			{ID: "e2", Source: "pq", Target: "dbg"},
		})

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	injectInto(t, engine, workflow.ID, "broken")

	q := waitForProblemCount(t, engine, workflow.ID, 1, 3*time.Second)
	if q == nil || q.Count() != 1 {
		t.Fatalf("failure never reached the problem queue: %v", q)
	}
	seq := q.Items()[0].Seq

	// Manual requeue (the /api/queue-item-requeue path) delivers the
	// item downstream despite the hold gate.
	if err := q.RequeueItem(seq); err != nil {
		t.Fatalf("requeue failed: %v", err)
	}
	got := waitForDebugValues(engine, workflow.ID, 1, 3*time.Second)
	if len(got) != 1 {
		t.Fatalf("requeued item never reached the problem queue downstream, got %v", got)
	}
	if q.Count() != 0 || store.RowCount(workflow.ID) != 0 {
		t.Fatalf("requeue must drain the row: count=%d rows=%d", q.Count(), store.RowCount(workflow.ID))
	}
}

// TestProblemQueueNoLoop verifies the loop guard: a failure of the
// problem queue's own downstream must not re-enqueue into the problem
// queue (infinite loop), while catch — if present — could still claim
// it.
func TestProblemQueueNoLoop(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := problemTestFlow("wf-prob-loop",
		[]Node{
			{ID: "bad2", Type: "template", Data: map[string]interface{}{
				"name":     "rework-fails",
				"template": "{{",
			}},
		},
		[]Edge{
			{ID: "e2", Source: "pq", Target: "bad2"},
		})

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	injectInto(t, engine, workflow.ID, "broken")

	q := waitForProblemCount(t, engine, workflow.ID, 1, 3*time.Second)
	if q == nil || q.Count() != 1 {
		t.Fatalf("failure never reached the problem queue: %v", q)
	}
	if err := q.RequeueItem(q.Items()[0].Seq); err != nil {
		t.Fatalf("requeue failed: %v", err)
	}

	// bad2 fails downstream of the problem queue. Without the guard it
	// would re-enqueue forever; the queue must drain to zero and stay.
	time.Sleep(300 * time.Millisecond)
	if q.Count() != 0 {
		t.Fatalf("problem-queue downstream failure must not re-enqueue, count=%d", q.Count())
	}
	if n := store.RowCount(workflow.ID); n != 0 {
		t.Fatalf("no problem row may remain after the guarded failure, got %d", n)
	}

	// The failure is still surfaced in the event log — it is dropped
	// deliberately, not silently.
	_, _, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
	seen := false
	for _, ev := range events {
		if ev.NodeID == "bad2" && ev.Level == "error" {
			seen = true
		}
	}
	if !seen {
		t.Fatal("downstream failure must still be logged")
	}
}
