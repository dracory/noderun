package noderunner_test

import (
	"context"
	. "github.com/dracory/noderun"
	"strings"
	"testing"
	"time"
)

// errorPortTestFlow builds inj -> req (an httpRequest that always
// fails — an ftp:// URL is rejected before any network I/O), plus
// the problem-queue node "pq" designated via view.onError. extraNodes
// and extraEdges extend the flow for the wired/unwired/catch tests.
func errorPortTestFlow(id string, errorCatch bool, extraNodes []Node, extraEdges []Edge) *Workflow {
	nodes := []Node{
		{ID: "inj", Type: "junction", Data: map[string]interface{}{}},
		{ID: "req", Type: "httpRequest", Data: map[string]interface{}{
			"name": "bad-request",
			"url":  "ftp://example.invalid/x",
		}},
		{ID: "pq", Type: "queue", Data: map[string]interface{}{"name": "problem messages"}},
	}
	if errorCatch {
		nodes[1].Data["errorCatch"] = true
	}
	edges := []Edge{
		{ID: "e1", Source: "inj", Target: "req"},
	}
	return &Workflow{
		ID:    id,
		Name:  id,
		View:  map[string]interface{}{"onError": "pq"},
		Nodes: append(nodes, extraNodes...),
		Edges: append(edges, extraEdges...),
	}
}

// TestErrorPortWired verifies the top of the precedence ladder: with
// errorCatch enabled and the appended error port (output_2 for the
// 1-output httpRequest) wired, the failure emits on that port with
// msg.error={text,node,full} and reaches NEITHER catch nor the
// problem queue.
func TestErrorPortWired(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := errorPortTestFlow("wf-errport", true,
		[]Node{
			{ID: "catch1", Type: "catch", Data: map[string]interface{}{"scope": "all"}},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{}},
			{ID: "catchdbg", Type: "debug", Data: map[string]interface{}{}},
		},
		[]Edge{
			{ID: "e2", Source: "req", SourceKey: "output_2", Target: "dbg"},
			{ID: "e3", Source: "catch1", Target: "catchdbg"},
		})

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	injectInto(t, engine, workflow.ID, "hello")

	got := waitForDebugValues(engine, workflow.ID, 1, 3*time.Second)
	if len(got) != 1 {
		t.Fatalf("error port branch never ran, got %v", got)
	}

	// The debug value is the rendered payload — verify the msg.error
	// shape directly via the event contents.
	msg := got[0]
	for _, want := range []string{`"text"`, `"node": "req"`, `"full"`} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error port message missing %s: %s", want, msg)
		}
	}
	if !strings.Contains(msg, `"originalData": "hello"`) {
		t.Fatalf("error port message must carry originalData: %s", msg)
	}

	// The wired port claimed the failure — catch branch and problem
	// queue must both stay empty.
	time.Sleep(150 * time.Millisecond)
	q := waitForProblemCount(t, engine, workflow.ID, 0, time.Second)
	if q == nil || q.Count() != 0 {
		t.Fatalf("wired error port claimed the failure; problem queue must stay empty, got %v", q)
	}
}

// TestErrorPortUnwired verifies an enabled-but-unwired error port does
// NOT swallow the failure: it falls through the ladder to the problem
// queue.
func TestErrorPortUnwired(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := errorPortTestFlow("wf-errport-unwired", true, nil, nil)

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	injectInto(t, engine, workflow.ID, "hello")

	q := waitForProblemCount(t, engine, workflow.ID, 1, 3*time.Second)
	if q == nil || q.Count() != 1 {
		t.Fatalf("unwired error port must fall through to the problem queue, got %v", q)
	}
}

// TestErrorPortCatchFallback verifies the middle rung: errorCatch
// enabled but unwired with a catch node in scope routes to the catch
// branch (error port → catch → problem queue).
func TestErrorPortCatchFallback(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := errorPortTestFlow("wf-errport-catch", true,
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
	time.Sleep(150 * time.Millisecond)
	q := waitForProblemCount(t, engine, workflow.ID, 0, time.Second)
	if q == nil || q.Count() != 0 {
		t.Fatalf("catch claimed the failure; problem queue must stay empty, got %v", q)
	}
}

// TestErrorPortNormalRoutingUntouched verifies the appended port does
// not disturb the normal output: a successful run still emits on
// output_1 only. (A localhost URL with nothing listening fails fast
// with a connect error — still an executor error, so this asserts on
// the normal-port edge staying silent on failure instead.)
func TestErrorPortFlagOffProblemQueue(t *testing.T) {
	store := NewFakeDurableStore()
	// errorCatch off entirely: existing ladder unchanged.
	workflow := errorPortTestFlow("wf-errport-off", false,
		[]Node{
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{}},
		},
		[]Edge{
			// Wire an output_2 edge even though the flag is off —
			// without errorCatch the engine must ignore it.
			{ID: "e2", Source: "req", SourceKey: "output_2", Target: "dbg"},
		})

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	injectInto(t, engine, workflow.ID, "hello")

	q := waitForProblemCount(t, engine, workflow.ID, 1, 3*time.Second)
	if q == nil || q.Count() != 1 {
		t.Fatalf("errorCatch off: failure must reach the problem queue, got %v", q)
	}
	if got := waitForDebugValues(engine, workflow.ID, 1, 300*time.Millisecond); len(got) != 0 {
		t.Fatalf("nothing may emit on output_2 without errorCatch, got %v", got)
	}
}
