package noderunner

import (
	"context"
	"testing"
	"time"
)

// TestLinkOutToLinkIn verifies a linkOut pushes a clone of the message
// through the downstream wiring of its target linkIn:
// inject -> linkOut(links=[in1]); linkIn(in1) -> log.
func TestLinkOutToLinkIn(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-linkout-flow",
		Name: "LinkOut Flow",
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
				ID:   "lout",
				Type: "linkOut",
				Data: map[string]interface{}{
					"links": []string{"in1"},
					"mode":  "link",
				},
			},
			{
				ID:   "in1",
				Type: "linkIn",
				Data: map[string]interface{}{},
			},
			{
				ID:   "log1",
				Type: "log",
				Data: map[string]interface{}{"message": "via link"},
			},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "lout"},
			{ID: "e2", Source: "in1", Target: "log1"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	nodes, events := waitForStatus(engine, workflow.ID, "log1", "ok", 2*time.Second)
	if nodes["log1"] != "ok" {
		t.Fatalf("log1 never reached ok; statuses=%v events=%v", nodes, events)
	}
}

// TestLinkCall verifies the RPC-style round trip:
// inject -> linkCall(link=in2) -> log2
// linkIn(in2) -> change(set answered=true) -> linkOut(mode=return).
// The linkCall awaits the linkOut return and passes the processed
// payload downstream to log2.
func TestLinkCall(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-linkcall-flow",
		Name: "LinkCall Flow",
		Nodes: []Node{
			{
				ID:   "inj",
				Type: "inject",
				Data: map[string]interface{}{
					"payload": `{"question":"?"}`,
					"once":    true,
				},
			},
			{
				ID:   "call",
				Type: "linkCall",
				Data: map[string]interface{}{
					"link":     "in2",
					"linkType": "static",
					"timeout":  5.0,
				},
			},
			{
				ID:   "log2",
				Type: "log",
				Data: map[string]interface{}{"message": "answered"},
			},
			{
				ID:   "in2",
				Type: "linkIn",
				Data: map[string]interface{}{},
			},
			{
				ID:   "chg",
				Type: "change",
				Data: map[string]interface{}{
					"rules": []interface{}{
						map[string]interface{}{"op": "set", "field": "answered", "value": "true"},
					},
				},
			},
			{
				ID:   "ret",
				Type: "linkOut",
				Data: map[string]interface{}{
					"mode": "return",
				},
			},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "call"},
			{ID: "e2", Source: "call", Target: "log2"},
			{ID: "e3", Source: "in2", Target: "chg"},
			{ID: "e4", Source: "chg", Target: "ret"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	nodes, events := waitForStatus(engine, workflow.ID, "log2", "ok", 3*time.Second)
	if nodes["log2"] != "ok" {
		t.Fatalf("log2 never reached ok; statuses=%v events=%v", nodes, events)
	}
	if nodes["chg"] != "ok" || nodes["ret"] != "ok" {
		t.Fatalf("subflow nodes did not complete; statuses=%v", nodes)
	}
	for _, ev := range events {
		if ev.Level == "error" {
			t.Fatalf("unexpected error event: %+v", ev)
		}
	}
}

// TestLinkCallTimeout verifies a linkCall with no linkOut return fails
// after its configured timeout instead of hanging forever.
func TestLinkCallTimeout(t *testing.T) {
	engine := NewEngine()
	workflow := &Workflow{
		ID:    "test-linkcall-timeout",
		Name:  "LinkCall Timeout",
		Nodes: []Node{{ID: "in3", Type: "linkIn", Data: map[string]interface{}{}}},
	}
	engine.runMu.Lock()
	engine.flows[workflow.ID] = workflow
	engine.runMu.Unlock()

	node := NewLinkCallNode("call", &LinkCallConfig{Link: "in3", Timeout: 0.05}, engine, workflow.ID)
	_, err := node.Execute(context.Background(), &Message{
		Payload:  "ping",
		Metadata: map[string]interface{}{},
	})
	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}
}
