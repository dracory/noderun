package noderunner

import (
	"context"
	"testing"
	"time"
	stdlibuuid "uuid"
)

// TestUUIDNodeThroughEngine verifies the builtin uuid registration through
// the real workflow deployment and injection path.
func TestUUIDNodeThroughEngine(t *testing.T) {
	workflow := &Workflow{
		ID:   "uuid-flow",
		Name: "UUID Flow",
		Nodes: []Node{
			{ID: "inj", Type: "inject", Data: map[string]any{}},
			{ID: "id", Type: "uuid", Data: map[string]any{"version": "v7", "target": "payload"}},
			{ID: "resp", Type: "httpResponse", Data: map[string]any{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "id"},
			{ID: "e2", Source: "id", Target: "resp"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	_, ch, err := engine.InjectMessage(workflow.ID, "inj", &Message{Payload: "input", Timestamp: time.Now()})
	if err != nil {
		t.Fatalf("inject failed: %v", err)
	}

	select {
	case out := <-ch:
		s, ok := out.GetPayload().(string)
		if !ok {
			t.Fatalf("uuid output is %T, want string", out.GetPayload())
		}
		parsed, err := stdlibuuid.Parse(s)
		if err != nil {
			t.Fatalf("uuid output does not parse: %v", err)
		}
		if got := parsed[6] >> 4; got != 7 {
			t.Fatalf("uuid version bits = %d, want 7", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("uuid flow produced no response")
	}
}
