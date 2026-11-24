package noderunner_test

import (
	"context"
	"encoding/json"
	nodestore "github.com/dracory/nodestore"
	. "github.com/dracory/noderun"
	"github.com/dracory/nodebase"
	"testing"
	"time"
)

// checkpointTestFlow builds inj -> checkpoint -> [dbgA|dbgB]. The
// checkpoint declares two routes: "approve" (output_1 -> dbgA) and
// "decline" (output_2 -> dbgB).
func checkpointTestFlow(id string) *Workflow {
	return &Workflow{
		ID:   id,
		Name: id,
		Nodes: []Node{
			{ID: "inj", Type: "junction", Data: map[string]interface{}{}},
			{ID: "cp", Type: "checkpoint", Data: map[string]interface{}{
				"name":   "approval",
				"routes": []interface{}{"approve", "decline"},
			}},
			{ID: "dbgA", Type: "debug", Data: map[string]interface{}{}},
			{ID: "dbgB", Type: "debug", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "cp"},
			{ID: "e2", Source: "cp", SourceKey: "output_1", Target: "dbgA"},
			{ID: "e3", Source: "cp", SourceKey: "output_2", Target: "dbgB"},
		},
	}
}

// debugValuesFor returns one debug node's emitted messages in order
// (debugValues hard-codes the "dbg" id).
func debugValuesFor(engine *Engine, workflowID, nodeID string) []string {
	_, _, events, _ := engine.GetWorkflowSnapshot(workflowID, 0)
	var out []string
	for _, ev := range events {
		if ev.NodeID == nodeID && ev.Level == "debug" {
			out = append(out, ev.Message)
		}
	}
	return out
}

func waitForCheckpointCount(t *testing.T, engine *Engine, workflowID string, n int, timeout time.Duration) nodebase.QueueInspector {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var q nodebase.QueueInspector
	for time.Now().Before(deadline) {
		if h, ok := engine.QueueHandle(workflowID, "cp"); ok {
			q = h
			if q.Count() == n {
				return q
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	return q
}

// TestCheckpointParksAndRoutes covers the acceptance flow: an injected
// message parks in the checkpoint (nothing drains), the inspect
// surface lists both routes, and releasing via "approve" delivers the
// item only on the approve edge.
func TestCheckpointParksAndRoutes(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := checkpointTestFlow("wf-checkpoint")

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	injectInto(t, engine, workflow.ID, "job1")

	q := waitForCheckpointCount(t, engine, workflow.ID, 1, 3*time.Second)
	if q == nil || q.Count() != 1 {
		t.Fatalf("expected 1 parked item, got %v", q)
	}
	// Give the drainer a moment to prove a checkpoint never drains.
	time.Sleep(150 * time.Millisecond)
	if got := debugValuesFor(engine, workflow.ID, "dbgA"); len(got) != 0 {
		t.Fatalf("checkpoint must not drain, dbgA got %v", got)
	}
	if got := debugValuesFor(engine, workflow.ID, "dbgB"); len(got) != 0 {
		t.Fatalf("checkpoint must not drain, dbgB got %v", got)
	}
	if n := store.RowCount(workflow.ID); n != 1 {
		t.Fatalf("parked item must stay persisted, got %d rows", n)
	}

	// Both routes are listed in port order.
	labels := q.RouteLabels()
	if len(labels) != 2 || labels[0] != "approve" || labels[1] != "decline" {
		t.Fatalf("expected routes [approve decline], got %v", labels)
	}

	// An unknown route is rejected without losing the item.
	seq := q.Items()[0].Seq
	if err := q.ReleaseItem(seq, "bogus", ""); err == nil {
		t.Fatal("unknown route must fail")
	}
	if q.Count() != 1 {
		t.Fatalf("failed release must keep the item, count=%d", q.Count())
	}

	// Release "approve": delivers only to dbgA (output_1).
	if err := q.ReleaseItem(seq, "approve", "tester"); err != nil {
		t.Fatalf("release failed: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && len(debugValuesFor(engine, workflow.ID, "dbgA")) == 0 {
		time.Sleep(15 * time.Millisecond)
	}
	if got := debugValuesFor(engine, workflow.ID, "dbgA"); len(got) != 1 || got[0] != `"job1"` {
		t.Fatalf("expected approve release to dbgA, got %v", got)
	}
	time.Sleep(100 * time.Millisecond)
	if got := debugValuesFor(engine, workflow.ID, "dbgB"); len(got) != 0 {
		t.Fatalf("approve release must not reach dbgB, got %v", got)
	}
	if q.Count() != 0 || store.RowCount(workflow.ID) != 0 {
		t.Fatalf("released item must be acked: count=%d rows=%d", q.Count(), store.RowCount(workflow.ID))
	}
}

// TestCheckpointDeclineRoute covers release on the second route via
// its port key — "output_2" delivers to dbgB only.
func TestCheckpointDeclineRoute(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := checkpointTestFlow("wf-checkpoint-decline")

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	injectInto(t, engine, workflow.ID, "job2")
	q := waitForCheckpointCount(t, engine, workflow.ID, 1, 3*time.Second)
	if q == nil || q.Count() != 1 {
		t.Fatalf("expected 1 parked item, got %v", q)
	}

	if err := q.ReleaseItem(q.Items()[0].Seq, "output_2", ""); err != nil {
		t.Fatalf("release failed: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && len(debugValuesFor(engine, workflow.ID, "dbgB")) == 0 {
		time.Sleep(15 * time.Millisecond)
	}
	if got := debugValuesFor(engine, workflow.ID, "dbgB"); len(got) != 1 || got[0] != `"job2"` {
		t.Fatalf("expected decline release to dbgB, got %v", got)
	}
	if got := debugValuesFor(engine, workflow.ID, "dbgA"); len(got) != 0 {
		t.Fatalf("decline release must not reach dbgA, got %v", got)
	}
}

// TestCheckpointRestartRecovers simulates a crash: rows seeded under
// the checkpoint node id are adopted by activation recovery and stay
// parked — the checkpoint never drains, so the item waits for a human.
func TestCheckpointRestartRecovers(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := checkpointTestFlow("wf-checkpoint-restart")

	// "Previous run" leftovers, written before this engine existed.
	store.Seed(workflow.ID, "cp", "stuck1")
	store.Seed(workflow.ID, "cp", "stuck2")

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	q := waitForCheckpointCount(t, engine, workflow.ID, 2, 3*time.Second)
	if q == nil || q.Count() != 2 {
		t.Fatalf("checkpoint must recover 2 parked items, got %v", q)
	}
	time.Sleep(150 * time.Millisecond)
	if got := debugValuesFor(engine, workflow.ID, "dbgA"); len(got) != 0 {
		t.Fatalf("recovered items must stay parked, dbgA got %v", got)
	}
	if got := debugValuesFor(engine, workflow.ID, "dbgB"); len(got) != 0 {
		t.Fatalf("recovered items must stay parked, dbgB got %v", got)
	}
	if n := store.RowCount(workflow.ID); n != 2 {
		t.Fatalf("parked rows must stay persisted, got %d", n)
	}
}

// TestCheckpointNeverProblemQueue verifies a checkpoint node id in
// view.onError is not honoured as the problem queue.
func TestCheckpointNeverProblemQueue(t *testing.T) {
	workflow := checkpointTestFlow("wf-checkpoint-onerror")
	workflow.View = map[string]interface{}{"onError": "cp"}
	if pid, ok := ProblemQueueIDFromView(workflow); ok {
		t.Fatalf("checkpoint must never be the problem queue, got %q", pid)
	}
}

// TestCheckpointNodeREDRoundTrip verifies the (from Node-RED's view)
// unknown type "checkpoint" survives an import/export round-trip.
func TestCheckpointNodeREDRoundTrip(t *testing.T) {
	flow := `[
		{"id":"tab1","type":"tab","label":"Main"},
		{"id":"cp1","type":"checkpoint","z":"tab1","x":10,"y":10,
		 "routes":["approve","decline"],"wires":[["dbg1"],["dbg2"]]},
		{"id":"dbg1","type":"debug","z":"tab1","x":20,"y":10,"wires":[]},
		{"id":"dbg2","type":"debug","z":"tab1","x":20,"y":20,"wires":[]}
	]`
	w, err := nodestore.ImportNodeRED([]byte(flow))
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}
	var found bool
	for _, n := range w.GetNodes() {
		if n.GetID() == "cp1" {
			found = true
			if n.GetType() != "checkpoint" {
				t.Fatalf("checkpoint type clobbered on import: %q", n.GetType())
			}
			routes, _ := n.GetData()["routes"].([]interface{})
			if len(routes) != 2 {
				t.Fatalf("routes config lost on import: %v", n.GetData()["routes"])
			}
		}
	}
	if !found {
		t.Fatal("checkpoint node missing after import")
	}

	out, err := nodestore.ExportNodeRED(w)
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}
	var items []map[string]interface{}
	if err := json.Unmarshal(out, &items); err != nil {
		t.Fatalf("export decode failed: %v", err)
	}
	for _, obj := range items {
		if obj["id"] == "cp1" {
			if obj["type"] != "checkpoint" {
				t.Fatalf("checkpoint type clobbered on export: %q", obj["type"])
			}
			return
		}
	}
	t.Fatal("checkpoint node missing after export")
}
