package noderunner

import (
	"context"
	"testing"
	"time"
)

// TestTriggerNodeTwoOutputs verifies the trigger node emits op1
// immediately on output_1 and op2 after the configured delay on
// output_2: inject -> trigger (op1type payload, op2 "done", 50ms) with
// a log node on each port. log1 must reach ok quickly, log2 only after
// the delay.
func TestTriggerNodeTwoOutputs(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-trigger-flow",
		Name: "Trigger Flow",
		Nodes: []Node{
			{
				ID:   "inj",
				Type: "inject",
				Data: map[string]interface{}{
					"payload": `"go"`,
					"once":    true,
				},
			},
			{
				ID:   "trg",
				Type: "trigger",
				Data: map[string]interface{}{
					"op1type":  "payload",
					"op2":      "done",
					"op2type":  "str",
					"duration": 50.0,
					"units":    "ms",
					"outputs":  2,
				},
			},
			{
				ID:   "log1",
				Type: "log",
				Data: map[string]interface{}{"message": "first"},
			},
			{
				ID:   "log2",
				Type: "log",
				Data: map[string]interface{}{"message": "second"},
			},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "trg"},
			{ID: "e2", Source: "trg", SourceKey: "output_1", Target: "log1"},
			{ID: "e3", Source: "trg", SourceKey: "output_2", Target: "log2"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	// op1 should reach log1 almost immediately
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		_, nodes, _, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		if nodes["log1"] == "ok" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	_, nodes, _, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
	if nodes["log1"] != "ok" {
		t.Fatalf("log1 (output_1) never reached ok; statuses=%v", nodes)
	}
	if nodes["log2"] == "ok" {
		t.Fatalf("log2 (output_2) completed before the 50ms delay elapsed")
	}

	// op2 should arrive shortly after the 50ms delay
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, nodes, _, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		if nodes["log2"] == "ok" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, nodes, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
	t.Fatalf("log2 (output_2) never reached ok; statuses=%v events=%v", nodes, events)
}

// TestTriggerNodeSelectPorts verifies the immediate output always
// routes to output_1.
func TestTriggerNodeSelectPorts(t *testing.T) {
	engine := NewEngine()
	node := NewTriggerNode("trg", &TriggerConfig{Outputs: 2}, engine, "wf")
	ports := node.SelectPorts(nil, nil)
	if len(ports) != 1 || ports[0] != "output_1" {
		t.Fatalf("expected [output_1], got %v", ports)
	}
}
