package noderunner_test

import (
	"context"
	. "github.com/dracory/noderun"
	"strings"
	"testing"
	"time"
)

// TestDebugNode verifies that a debug node emits a "debug"-level event
// carrying the pretty-printed payload once the inject fires, and that a
// muted node (active=false) emits nothing.
func TestDebugNode(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-debug-flow",
		Name: "Debug Flow",
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
				ID:   "dbg",
				Type: "debug",
				Data: map[string]interface{}{
					"complete": "payload",
				},
			},
			{
				ID:   "dbgOff",
				Type: "debug",
				Data: map[string]interface{}{
					"active": false,
				},
			},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "dbg"},
			{ID: "e2", Source: "inj", Target: "dbgOff"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, _, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)

		var found bool
		for _, ev := range events {
			if ev.NodeID == "dbg" && ev.Level == "debug" &&
				strings.Contains(ev.Message, `"a": 1`) {
				found = true
			}
			if ev.NodeID == "dbgOff" && ev.Level == "debug" {
				t.Fatalf("muted debug node emitted an event: %q", ev.Message)
			}
		}
		if found {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("debug node never emitted its payload event")
}

// TestDebugNodeCompleteTrue verifies complete="true" displays the whole
// payload including metadata.
func TestDebugNodeCompleteTrue(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-debug-full-flow",
		Name: "Debug Full Flow",
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
				ID:   "dbg",
				Type: "debug",
				Data: map[string]interface{}{
					"complete": "true",
				},
			},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "dbg"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, _, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		for _, ev := range events {
			if ev.NodeID == "dbg" && ev.Level == "debug" {
				if !strings.Contains(ev.Message, `"a": 1`) ||
					!strings.Contains(ev.Message, "metadata") {
					t.Fatalf("expected full payload incl. metadata, got: %s", ev.Message)
				}
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("debug node never emitted its payload event")
}

// TestDebugNodeCompletePath — Node-RED parity: a "complete" value other
// than payload/true/false is a message property path (msg.<path>).
func TestDebugNodeCompletePath(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-debug-path-flow",
		Name: "Debug Path Flow",
		Nodes: []Node{
			{
				ID:   "inj",
				Type: "inject",
				Data: map[string]interface{}{
					"payload":     `{"a":1}`,
					"payloadType": "json",
					"topic":       "sensors/temp",
					"once":        true,
				},
			},
			{
				ID:   "dbg",
				Type: "debug",
				Data: map[string]interface{}{"complete": "topic"},
			},
			{
				ID:   "dbg2",
				Type: "debug",
				Data: map[string]interface{}{"complete": "payload.a"},
			},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "dbg"},
			{ID: "e2", Source: "inj", Target: "dbg2"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	deadline := time.Now().Add(3 * time.Second)
	gotTopic, gotSub := false, false
	for time.Now().Before(deadline) && !(gotTopic && gotSub) {
		_, _, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		for _, ev := range events {
			if ev.NodeID == "dbg" && strings.Contains(ev.Message, "sensors/temp") {
				gotTopic = true
			}
			if ev.NodeID == "dbg2" && strings.Contains(ev.Message, "1") {
				gotSub = true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, _, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
	if !gotTopic {
		t.Fatalf("debug never emitted the topic path; events=%v", events)
	}
	if !gotSub {
		t.Fatalf("debug never emitted the payload.a path; events=%v", events)
	}
}
