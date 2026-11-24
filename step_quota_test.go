package noderunner_test

import (
	"context"
	"fmt"
	. "github.com/dracory/noderun"
	"strings"
	"testing"
	"time"
)

// quotaCycleFlow builds inj -> a -> b -> a (a wired 2-node cycle) with
// an optional problem queue node designated via view.onError.
func quotaCycleFlow(id string, withProblemQueue bool) *Workflow {
	nodes := []Node{
		{ID: "inj", Type: "junction", Data: map[string]interface{}{}},
		{ID: "a", Type: "junction", Data: map[string]interface{}{}},
		{ID: "b", Type: "junction", Data: map[string]interface{}{}},
	}
	view := map[string]interface{}{}
	if withProblemQueue {
		nodes = append(nodes, Node{ID: "pq", Type: "queue", Data: map[string]interface{}{"name": "problem messages"}})
		view["onError"] = "pq"
	}
	return &Workflow{
		ID:    id,
		Name:  id,
		View:  view,
		Nodes: nodes,
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "a"},
			{ID: "e2", Source: "a", Target: "b"},
			{ID: "e3", Source: "b", Target: "a"},
		},
	}
}

// waitForQuotaBreach polls the event log for the quota error event.
func waitForQuotaBreach(engine *Engine, workflowID string, timeout time.Duration) *NodeEvent {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, _, events, _ := engine.GetWorkflowSnapshot(workflowID, 0)
		for _, ev := range events {
			if ev.Level == "error" && strings.Contains(ev.Message, "step quota reached") {
				return &ev
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

// TestStepQuotaCycleHalts verifies a wired cycle stops dispatching at
// the configured quota and records an error event naming the limit
// and the last-executed node — instead of looping forever.
func TestStepQuotaCycleHalts(t *testing.T) {
	workflow := quotaCycleFlow("wf-quota-cycle", false)

	engine := NewEngine()
	engine.MaxSteps = 50
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	injectInto(t, engine, workflow.ID, "go")

	ev := waitForQuotaBreach(engine, workflow.ID, 5*time.Second)
	if ev == nil {
		t.Fatal("cycle never tripped the step quota")
	}
	if ev.NodeID != "a" && ev.NodeID != "b" {
		t.Fatalf("quota event must name the last-executed node, got %q", ev.NodeID)
	}
	if !strings.Contains(ev.Message, "50") {
		t.Fatalf("quota event must name the limit, got %q", ev.Message)
	}
}

// TestStepQuotaFanOutCompletes verifies the quota counts executions,
// not enqueued messages: a split fanning out below the limit must
// complete every branch — the old per-offer accounting would have
// tripped on the same numbers.
func TestStepQuotaFanOutCompletes(t *testing.T) {
	const fanout = 25
	workflow := &Workflow{
		ID:   "wf-quota-fanout",
		Name: "wf-quota-fanout",
		View: map[string]interface{}{},
		Nodes: []Node{
			{ID: "inj", Type: "junction", Data: map[string]interface{}{}},
			{ID: "sp", Type: "split", Data: map[string]interface{}{
				// split an array into one message per element
				"splt": "\\n",
			}},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "sp"},
			{ID: "e2", Source: "sp", Target: "dbg"},
		},
	}

	engine := NewEngine()
	// executions = split(1) + fanout debug runs; offers = fanout.
	// A per-message counter would reach ~2*fanout and breach.
	engine.MaxSteps = fanout + 5
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	parts := make([]string, fanout)
	for i := range parts {
		parts[i] = fmt.Sprintf("%d", i)
	}
	msgID, _, err := engine.InjectMessage(workflow.ID, "inj", &Message{Payload: parts})
	if err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	engine.DiscardPending(msgID)

	got := waitForDebugValues(engine, workflow.ID, fanout, 5*time.Second)
	if len(got) < fanout {
		t.Fatalf("fan-out incomplete: %d/%d debug messages arrived", len(got), fanout)
	}
	if ev := waitForQuotaBreach(engine, workflow.ID, 150*time.Millisecond); ev != nil {
		t.Fatalf("legitimate fan-out must not trip the quota: %v", ev.Message)
	}
}

// TestStepQuotaRoutesToProblemQueue verifies the breach failure rides
// the standard routeFailure ladder: with a designated problem queue
// the quota error is staged there with the error envelope.
func TestStepQuotaRoutesToProblemQueue(t *testing.T) {
	workflow := quotaCycleFlow("wf-quota-pq", true)

	engine := NewEngine()
	engine.DurableInboxStore = NewFakeDurableStore()
	engine.MaxSteps = 20
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	injectInto(t, engine, workflow.ID, "go")

	q := waitForProblemCount(t, engine, workflow.ID, 1, 5*time.Second)
	if q == nil || q.Count() != 1 {
		t.Fatalf("quota breach never reached the problem queue: %v", q)
	}
	data, ok := q.Items()[0].Payload.GetPayload().(map[string]interface{})
	if !ok {
		t.Fatalf("problem payload must carry the error envelope, got %T", q.Items()[0].Payload.GetPayload())
	}
	errMap, _ := data["error"].(map[string]interface{})
	msg, _ := errMap["message"].(string)
	if !strings.Contains(msg, "step quota reached") {
		t.Fatalf("problem envelope must carry the quota failure, got %v", errMap)
	}
	if errMap["node"] != "a" && errMap["node"] != "b" {
		t.Fatalf("problem envelope must name the last-executed node, got %v", errMap)
	}
}

// TestStepQuotaRoutesToCatch verifies a catch node in scope claims the
// quota breach ahead of the problem queue (ladder precedence).
func TestStepQuotaRoutesToCatch(t *testing.T) {
	workflow := quotaCycleFlow("wf-quota-catch", true)
	workflow.Nodes = append(workflow.Nodes,
		Node{ID: "catch1", Type: "catch", Data: map[string]interface{}{"scope": "all"}},
		Node{ID: "dbg", Type: "debug", Data: map[string]interface{}{}},
	)
	workflow.Edges = append(workflow.Edges,
		Edge{ID: "e4", Source: "catch1", Target: "dbg"},
	)

	engine := NewEngine()
	engine.DurableInboxStore = NewFakeDurableStore()
	engine.MaxSteps = 20
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	injectInto(t, engine, workflow.ID, "go")

	got := waitForDebugValues(engine, workflow.ID, 1, 5*time.Second)
	if len(got) != 1 {
		t.Fatalf("catch branch never claimed the quota breach, got %v", got)
	}
	q := waitForProblemCount(t, engine, workflow.ID, 0, time.Second)
	if q != nil && q.Count() != 0 {
		t.Fatalf("catch claimed the breach; problem queue must stay empty, got %d", q.Count())
	}
}

// TestStepQuotaResolution verifies precedence: view.maxSteps >
// engine MaxSteps > DefaultMaxSteps, negative disables.
func TestStepQuotaResolution(t *testing.T) {
	engine := NewEngine()
	if got := engine.MaxStepsLimit(&Workflow{}); got != DefaultMaxSteps {
		t.Fatalf("empty config must fall back to DefaultMaxSteps, got %d", got)
	}
	engine.MaxSteps = 42
	if got := engine.MaxStepsLimit(&Workflow{}); got != 42 {
		t.Fatalf("engine MaxSteps must apply, got %d", got)
	}
	wf := &Workflow{View: map[string]interface{}{"maxSteps": float64(7)}}
	if got := engine.MaxStepsLimit(wf); got != 7 {
		t.Fatalf("view maxSteps must override the engine default, got %d", got)
	}
	wf.View["maxSteps"] = float64(-1)
	if got := engine.MaxStepsLimit(wf); got != 0 {
		t.Fatalf("negative view maxSteps must disable the quota, got %d", got)
	}
	engine.MaxSteps = -1
	if got := engine.MaxStepsLimit(&Workflow{}); got != 0 {
		t.Fatalf("negative engine MaxSteps must disable the quota, got %d", got)
	}
}
