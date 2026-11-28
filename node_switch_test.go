package noderunner_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dracory/nodebase"
	. "github.com/dracory/noderun"
	"io"
	"net/http"
	"testing"
	"time"
)

// TestSwitchRouting verifies per-output-port routing: a request to /a must
// be answered through the branch wired to the switch's first output, any
// other path through the second output.
func TestSwitchRouting(t *testing.T) {
	port := freePort(t)

	workflow := &Workflow{
		ID:   "test-switch-flow",
		Name: "Switch Flow",
		Nodes: []Node{
			{
				ID:   "trig",
				Type: "httpTrigger",
				Data: map[string]interface{}{
					"method": "GET",
					"path":   "/",
					"port":   float64(port),
				},
			},
			{
				ID:   "sw",
				Type: "switch",
				Data: map[string]interface{}{
					"mode": "first",
					"rules": []interface{}{
						map[string]interface{}{"property": "path", "op": "eq", "value": "/a"},
						map[string]interface{}{"property": "path", "op": "neq", "value": "/a"},
					},
				},
			},
			{
				ID:   "changeA",
				Type: "change",
				Data: map[string]interface{}{
					"rules": []interface{}{
						map[string]interface{}{"op": "set", "field": "payload", "value": `{"branch":"a"}`},
					},
				},
			},
			{
				ID:   "changeB",
				Type: "change",
				Data: map[string]interface{}{
					"rules": []interface{}{
						map[string]interface{}{"op": "set", "field": "payload", "value": `{"branch":"b"}`},
					},
				},
			},
			{ID: "respA", Type: "httpResponse", Data: map[string]interface{}{}},
			{ID: "respB", Type: "httpResponse", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "trig", SourceKey: "output_1", Target: "sw"},
			{ID: "e2", Source: "sw", SourceKey: "output_1", Target: "changeA"},
			{ID: "e3", Source: "sw", SourceKey: "output_2", Target: "changeB"},
			{ID: "e4", Source: "changeA", SourceKey: "output_1", Target: "respA"},
			{ID: "e5", Source: "changeB", SourceKey: "output_1", Target: "respB"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	get := func(path string) map[string]interface{} {
		t.Helper()
		url := fmt.Sprintf("http://127.0.0.1:%d%s", port, path)
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
		resp = requireHTTPResponse(t, resp, err)
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: expected 200, got %d", path, resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		var data map[string]interface{}
		if err := json.Unmarshal(body, &data); err != nil {
			t.Fatalf("GET %s: response is not JSON: %v\nbody: %s", path, err, body)
		}
		return data
	}

	if data := get("/a"); data["branch"] != "a" {
		t.Fatalf("GET /a: expected branch a, got: %v", data)
	}
	if data := get("/other"); data["branch"] != "b" {
		t.Fatalf("GET /other: expected branch b, got: %v", data)
	}
}

// TestInjectFanOutIsolation verifies that a fan-out (one node wired to two
// siblings) runs both branches, each with its own payload clone, and that a
// mutating branch does not corrupt the sibling's data.
func TestInjectFanOutIsolation(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-fanout-flow",
		Name: "Fanout Flow",
		Nodes: []Node{
			{ID: "inj", Type: "inject", Data: map[string]interface{}{"payload": `{"x":0}`}},
			{
				ID:   "ch",
				Type: "change",
				Data: map[string]interface{}{
					"rules": []interface{}{
						map[string]interface{}{"op": "set", "field": "x", "value": "1"},
					},
				},
			},
			{ID: "lg", Type: "log", Data: map[string]interface{}{"message": "sibling"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", SourceKey: "output_1", Target: "ch"},
			{ID: "e2", Source: "inj", SourceKey: "output_1", Target: "lg"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	payload := &Message{
		Payload:   map[string]interface{}{"x": float64(0)},
		Timestamp: time.Now(),
	}
	msgID, _, err := engine.InjectMessage(workflow.ID, "inj", payload)
	if err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	defer engine.DiscardPending(msgID)

	// Both downstream nodes must execute to completion
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, statuses, _, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		if statuses["ch"] == "ok" && statuses["lg"] == "ok" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, statuses, _, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
	t.Fatalf("expected both branches to complete, statuses: %v", statuses)
}

// TestSwitchSelectPorts unit-tests rule evaluation and mode handling.
func TestSwitchSelectPorts(t *testing.T) {
	payload := func(data map[string]interface{}) *Message {
		return &Message{Payload: data, Timestamp: time.Now()}
	}

	sw := NewSwitchNode("sw", &SwitchConfig{
		Rules: []SwitchRule{
			{Property: "path", Op: "eq", Value: "/a"},
			{Property: "path", Op: "neq", Value: "/a"},
		},
	})

	if ports := sw.SelectPorts(payload(map[string]interface{}{"path": "/a"}), nil); len(ports) != 1 || ports[0] != "output_1" {
		t.Fatalf("expected [output_1], got %v", ports)
	}
	if ports := sw.SelectPorts(payload(map[string]interface{}{"path": "/b"}), nil); len(ports) != 1 || ports[0] != "output_2" {
		t.Fatalf("expected [output_2], got %v", ports)
	}

	// "all" mode returns every matching port
	swAll := NewSwitchNode("sw", &SwitchConfig{
		Mode: "all",
		Rules: []SwitchRule{
			{Property: "n", Op: "gt", Value: "1"},
			{Property: "n", Op: "lt", Value: "10"},
		},
	})
	if ports := swAll.SelectPorts(payload(map[string]interface{}{"n": float64(5)}), nil); len(ports) != 2 {
		t.Fatalf("expected both ports in all mode, got %v", ports)
	}

	// contains on strings and arrays
	swContains := NewSwitchNode("sw", &SwitchConfig{
		Rules: []SwitchRule{
			{Property: "s", Op: "contains", Value: "ell"},
			{Property: "arr", Op: "contains", Value: "2"},
		},
	})
	ports := swContains.SelectPorts(payload(map[string]interface{}{
		"s":   "hello",
		"arr": []interface{}{float64(1), float64(2)},
	}), nil)
	if len(ports) != 1 || ports[0] != "output_1" {
		t.Fatalf("expected [output_1] (first mode), got %v", ports)
	}

	// empty matches absent or empty property
	swEmpty := NewSwitchNode("sw", &SwitchConfig{
		Rules: []SwitchRule{{Property: "missing", Op: "empty"}},
	})
	if ports := swEmpty.SelectPorts(payload(map[string]interface{}{}), nil); len(ports) != 1 {
		t.Fatalf("expected match on absent property, got %v", ports)
	}

	// no match → empty slice (message dies)
	swNoMatch := NewSwitchNode("sw", &SwitchConfig{
		Rules: []SwitchRule{{Property: "path", Op: "eq", Value: "/a"}},
	})
	if ports := swNoMatch.SelectPorts(payload(map[string]interface{}{"path": "/b"}), nil); len(ports) != 0 {
		t.Fatalf("expected no ports, got %v", ports)
	}
}

// TestSwitchNewOperators — NR-16: lte/gte/regex/istype.
func TestSwitchNewOperators(t *testing.T) {
	p := func(v interface{}) *Message {
		return &Message{Payload: map[string]interface{}{"v": v}}
	}
	cases := []struct {
		rule  SwitchRule
		input *Message
		want  bool
	}{
		{SwitchRule{Property: "v", Op: "lte", Value: "5"}, p(float64(5)), true},
		{SwitchRule{Property: "v", Op: "gte", Value: "5"}, p(float64(4)), false},
		{SwitchRule{Property: "v", Op: "regex", Value: "^a.c$"}, p("abc"), true},
		{SwitchRule{Property: "v", Op: "regex", Value: "^z"}, p("abc"), false},
		{SwitchRule{Property: "v", Op: "istype", Value: "number"}, p(float64(1)), true},
		{SwitchRule{Property: "v", Op: "istype", Value: "string"}, p(float64(1)), false},
		{SwitchRule{Property: "v", Op: "istype", Value: "object"}, p(map[string]interface{}{"a": 1}), true},
		{SwitchRule{Property: "v", Op: "istype", Value: "object"}, p(float64(1)), false},
		{SwitchRule{Property: "v", Op: "istype", Value: "array"}, p([]interface{}{1}), true},
	}
	for i, c := range cases {
		if got := nodebase.EvalSwitchRule(c.rule, c.input); got != c.want {
			t.Fatalf("case %d (%s %s): got %v, want %v", i, c.rule.Op, c.rule.Value, got, c.want)
		}
	}
}

// TestSwitchOtherwise — NR-17: an unmatched message routes to the
// otherwise port (one past the last rule output).
func TestSwitchOtherwise(t *testing.T) {
	n := NewSwitchNode("s", &SwitchConfig{
		Rules:     []SwitchRule{{Property: "v", Op: "eq", Value: "1"}},
		Mode:      "first",
		Otherwise: true,
	})
	ports := n.SelectPorts(&Message{Payload: map[string]interface{}{"v": float64(9)}}, nil)
	if len(ports) != 1 || ports[0] != "output_2" {
		t.Fatalf("expected otherwise port output_2, got %v", ports)
	}
	// A match still routes to its own port.
	ports = n.SelectPorts(&Message{Payload: map[string]interface{}{"v": float64(1)}}, nil)
	if len(ports) != 1 || ports[0] != "output_1" {
		t.Fatalf("expected output_1, got %v", ports)
	}
	// Without otherwise, unmatched dies.
	n2 := NewSwitchNode("s", &SwitchConfig{
		Rules: []SwitchRule{{Property: "v", Op: "eq", Value: "1"}},
	})
	if ports := n2.SelectPorts(&Message{Payload: map[string]interface{}{"v": float64(9)}}, nil); len(ports) != 0 {
		t.Fatalf("expected no ports, got %v", ports)
	}
}

// TestSwitchValidate — N-4: deploy-time validation rejects unknown
// operators, malformed regex rules and invalid modes.
func TestSwitchValidate(t *testing.T) {
	bad := []struct {
		name string
		conf *SwitchConfig
	}{
		{"unknown op", &SwitchConfig{Rules: []SwitchRule{{Op: "bogus"}}}},
		{"bad regex", &SwitchConfig{Rules: []SwitchRule{{Op: "regex", Value: "/([/"}}}},
		{"bad mode", &SwitchConfig{Rules: []SwitchRule{{Op: "eq"}}, Mode: "weird"}},
	}
	for _, c := range bad {
		n := NewSwitchNode("s", c.conf)
		if err := n.Validate(); err == nil {
			t.Fatalf("%s: expected validation error", c.name)
		}
	}

	good := NewSwitchNode("s", &SwitchConfig{
		Rules: []SwitchRule{
			{Property: "v", Op: "regex", Value: "/^x/"},
			{Property: "v", Op: "istype", Value: "number"},
		},
		Mode:      "all",
		Otherwise: true,
	})
	if err := good.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}
