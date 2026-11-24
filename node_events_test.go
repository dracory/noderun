package noderunner_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	. "github.com/dracory/noderun"
	"testing"
	"time"
)

// TestCatchNode verifies the engine-side catch routing: a node whose
// executor fails pushes an error message into in-scope catch nodes, so
// an httpTrigger → failing-template → catch → httpResponse flow answers
// the HTTP request from the catch branch instead of timing out.
func TestCatchNode(t *testing.T) {
	port := freePort(t)

	workflow := &Workflow{
		ID:   "test-catch-flow",
		Name: "Catch Flow",
		Nodes: []Node{
			{
				ID:   "trig",
				Type: "httpTrigger",
				Data: map[string]interface{}{
					"method": "GET",
					"path":   "/go",
					"port":   float64(port),
				},
			},
			{
				ID:   "tmpl",
				Type: "template",
				Data: map[string]interface{}{
					"template": "{{", // invalid Go template — always errors
				},
			},
			{
				ID:   "resp",
				Type: "httpResponse",
				Data: map[string]interface{}{},
			},
			{
				ID:   "catch1",
				Type: "catch",
				Data: map[string]interface{}{"scope": "all"},
			},
			{
				ID:   "tmpl2",
				Type: "template",
				Data: map[string]interface{}{
					"template": `{"caught": true}`,
				},
			},
			{
				ID:   "resp2",
				Type: "httpResponse",
				Data: map[string]interface{}{},
			},
		},
		Edges: []Edge{
			{ID: "e1", Source: "trig", Target: "tmpl"},
			{ID: "e2", Source: "tmpl", Target: "resp"},
			{ID: "e3", Source: "catch1", Target: "tmpl2"},
			{ID: "e4", Source: "tmpl2", Target: "resp2"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	url := fmt.Sprintf("http://127.0.0.1:%d/go", port)
	var resp *http.Response
	var err error
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err = http.Get(url)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("trigger listener never came up: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 from the catch branch, got %d\nbody: %s", resp.StatusCode, body)
	}

	body, _ := io.ReadAll(resp.Body)
	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		t.Fatalf("response is not JSON: %v\nbody: %s", err, body)
	}
	if data["caught"] != true {
		t.Fatalf("expected {caught:true} from the catch branch, got: %s", body)
	}
}

// TestCompleteNode verifies that a complete node scoped to the
// triggering node fires once the injected message's downstream
// traversal has drained: inject → change → log completes, then the
// complete node's own branch (complete → log2) runs.
func TestCompleteNode(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-complete-flow",
		Name: "Complete Flow",
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
				ID:   "chg",
				Type: "change",
				Data: map[string]interface{}{
					"rules": []interface{}{
						map[string]interface{}{"op": "set", "field": "b", "value": "2"},
					},
				},
			},
			{
				ID:   "log1",
				Type: "log",
				Data: map[string]interface{}{"message": "main branch", "level": "info"},
			},
			{
				ID:   "comp",
				Type: "complete",
				Data: map[string]interface{}{"scope": "all"},
			},
			{
				ID:   "log2",
				Type: "log",
				Data: map[string]interface{}{"message": "completed", "level": "info"},
			},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "chg"},
			{ID: "e2", Source: "chg", Target: "log1"},
			{ID: "e3", Source: "comp", Target: "log2"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	// Poll the event log until log2 ran (the complete node's branch)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, _, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		for _, ev := range events {
			if ev.NodeID == "log2" && ev.Message == "completed" {
				return // complete fired and its branch executed
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("complete node never fired — log2 never executed")
}
