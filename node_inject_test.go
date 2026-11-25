package noderunner_test

import (
	"context"
	. "github.com/dracory/noderun"
	"strings"
	"testing"
	"time"
)

// TestInjectNodeFireOnce verifies that an inject node configured with
// once=true fires its payload on deploy and the message reaches the
// downstream log node (which ends up with status "ok").
func TestInjectNodeFireOnce(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-inject-flow",
		Name: "Inject Flow",
		Nodes: []Node{
			{
				ID:   "inj",
				Type: "inject",
				Data: map[string]interface{}{
					"payload": `{"a":1}`,
					"once":    true,
				},
			},
			{
				ID:   "log1",
				Type: "log",
				Data: map[string]interface{}{"message": "got it"},
			},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "log1"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	// Poll briefly for the log node to reach "ok"
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		_, nodes, _, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		if nodes["log1"] == "ok" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	_, nodes, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
	t.Fatalf("log node never reached ok; statuses=%v events=%v", nodes, events)
}

// TestFireInjectNodeConfiguredPayload — the fire-button path must emit
// the node's configured payload (json type parses the payload field),
// not a generic marker or a timestamp. Regression: the API used to
// inject a hard-coded {manual:true} downstream.
func TestFireInjectNodeConfiguredPayload(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-fire-inject",
		Name: "Fire Inject Flow",
		Nodes: []Node{
			{
				ID:   "inj",
				Type: "inject",
				Data: map[string]interface{}{
					"payload":     `{"a":1}`,
					"payloadType": "json",
				},
			},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{}},
		},
		Edges: []Edge{{ID: "e1", Source: "inj", Target: "dbg"}},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	// Deploy is async: wait for the inject to register as a stopper
	// before firing (the API path has the same race — the fire button
	// can only be clicked after the deploy status arrives anyway).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := engine.FireInjectNode(workflow.ID, "inj"); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := engine.FireInjectNode(workflow.ID, "inj"); err != nil {
		t.Fatalf("FireInjectNode: %v", err)
	}

	for time.Now().Before(deadline) {
		_, _, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		for _, ev := range events {
			// Compact JSON: nodebase debug renders Node-RED-style.
			if ev.NodeID == "dbg" && strings.Contains(ev.Message, `"a":1`) {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, _, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
	t.Fatalf("debug never received the configured JSON payload; events=%v", events)
}

// TestFireInjectNodeNotRunning covers the error path when the target
// is not a live inject node.
func TestFireInjectNodeNotRunning(t *testing.T) {
	engine := NewEngine()
	if err := engine.FireInjectNode("nope", "inj"); err == nil {
		t.Fatal("expected error for unknown workflow")
	}
}
