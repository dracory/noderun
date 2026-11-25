package noderunner_test

import (
	"context"
	"testing"
	"time"

	. "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

// Change/switch rules can read and write flow/global context and read
// environment variables — the Node-RED pt/tot serialization.

// runChangeRules deploys inj -> change(rules) -> resp and returns the
// payload after the change node ran.
func runChangeRules(t *testing.T, rules []map[string]interface{}, in interface{}) nodestore.MessageInterface {
	t.Helper()
	wf := &Workflow{
		ID: "ctx-change-" + sanitizeName(t.Name()),
		Nodes: []Node{
			{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
			{ID: "chg", Type: "change", Data: map[string]interface{}{"rules": rules}},
			specResponseNode("resp"),
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "chg"},
			{ID: "e2", Source: "chg", Target: "resp"},
		},
	}
	engine := specDeployFlow(t, wf)
	ch := specInjectInto(t, engine, wf.ID, "inj", &Message{
		Payload:   in,
		Timestamp: time.Now(),
	})
	return specAwaitResponse(t, ch, 3*time.Second)
}

// TestChangeFlowContextRoundTrip: pt:"flow" writes the context store,
// tot:"flow" reads it back (flow 05's second rule pair).
func TestChangeFlowContextRoundTrip(t *testing.T) {
	out := runChangeRules(t, []map[string]interface{}{
		{"t": "set", "p": "count", "pt": "flow", "to": "7", "tot": "num"},
		{"t": "set", "p": "payload", "pt": "msg", "to": "count", "tot": "flow"},
	}, "orig")
	if out.GetPayload() != float64(7) {
		t.Fatalf("expected flow context readback 7, got %#v", out.GetPayload())
	}
}

// TestChangeGlobalContextRoundTrip: pt:"global" writes the engine-wide
// scope, tot:"global" reads it.
func TestChangeGlobalContextRoundTrip(t *testing.T) {
	out := runChangeRules(t, []map[string]interface{}{
		{"t": "set", "p": "g", "pt": "global", "to": "hello", "tot": "str"},
		{"t": "set", "p": "payload", "pt": "msg", "to": "g", "tot": "global"},
	}, "orig")
	if out.GetPayload() != "hello" {
		t.Fatalf("expected global context readback, got %#v", out.GetPayload())
	}
}

// TestChangeDeleteFlowContext: a delete rule on a flow key removes it —
// a later read resolves to undefined.
func TestChangeDeleteFlowContext(t *testing.T) {
	out := runChangeRules(t, []map[string]interface{}{
		{"t": "set", "p": "tmp", "pt": "flow", "to": "x", "tot": "str"},
		{"t": "delete", "p": "tmp", "pt": "flow"},
		{"t": "set", "p": "payload", "pt": "msg", "to": "tmp", "tot": "flow"},
	}, "orig")
	if out.GetPayload() != nil {
		t.Fatalf("expected deleted key to read undefined, got %#v", out.GetPayload())
	}
}

// TestChangeEnvValue: tot:"env" resolves an environment variable
// (host NodeEnv first, then the OS environment when allowed).
func TestChangeEnvValue(t *testing.T) {
	t.Setenv("CTX_RULE_TEST_VAR", "from-env")
	wf := &Workflow{
		ID: "ctx-env",
		Nodes: []Node{
			{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
			{ID: "chg", Type: "change", Data: map[string]interface{}{"rules": []interface{}{
				map[string]interface{}{"t": "set", "p": "payload", "pt": "msg", "to": "CTX_RULE_TEST_VAR", "tot": "env"},
			}}},
			specResponseNode("resp"),
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "chg"},
			{ID: "e2", Source: "chg", Target: "resp"},
		},
	}
	engine := NewEngine()
	engine.AllowOSEnv = true
	if _, err := engine.ExecuteWorkflow(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()
	ch := specInjectInto(t, engine, wf.ID, "inj", &Message{Payload: "orig", Timestamp: time.Now()})
	out := specAwaitResponse(t, ch, 3*time.Second)
	if out.GetPayload() != "from-env" {
		t.Fatalf("expected env var value, got %#v", out.GetPayload())
	}
}

// TestChangeJSONataFlowContext: the supported expression subset —
// $flowContext("k") plus arithmetic — evaluated against live context.
func TestChangeJSONataFlowContext(t *testing.T) {
	out := runChangeRules(t, []map[string]interface{}{
		{"t": "set", "p": "count", "pt": "flow", "to": "41", "tot": "num"},
		{"t": "set", "p": "payload", "pt": "msg", "to": `$flowContext("count")+1`, "tot": "jsonata"},
	}, "orig")
	if out.GetPayload() != float64(42) {
		t.Fatalf("expected jsonata increment 42, got %#v", out.GetPayload())
	}
}

// TestChangeNativeContextRule: the native {op,field,fieldType} shape
// honours context targets too.
func TestChangeNativeContextRule(t *testing.T) {
	out := runChangeRules(t, []map[string]interface{}{
		{"op": "set", "field": "k", "fieldType": "flow", "value": "9", "valueType": "num"},
		{"op": "set", "field": "payload", "value": "k", "valueType": "flow"},
	}, "orig")
	if out.GetPayload() != float64(9) {
		t.Fatalf("expected native context rule readback 9, got %#v", out.GetPayload())
	}
}

// TestSwitchContextRouting: a switch whose property lives in flow
// context routes per the stored value (flow 09).
func TestSwitchContextRouting(t *testing.T) {
	wf := &Workflow{
		ID: "ctx-switch",
		Nodes: []Node{
			{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
			{ID: "setstate", Type: "change", Data: map[string]interface{}{"rules": []interface{}{
				map[string]interface{}{"t": "set", "p": "state", "pt": "flow", "to": "payload", "tot": "msg"},
			}}},
			{ID: "sw", Type: "switch", Data: map[string]interface{}{
				"property":     "state",
				"propertyType": "flow",
				"rules": []interface{}{
					map[string]interface{}{"t": "eq", "v": "1", "vt": "num"},
					map[string]interface{}{"t": "eq", "v": "2", "vt": "num"},
				},
			}},
			{ID: "tagA", Type: "change", Data: map[string]interface{}{"rules": []interface{}{
				map[string]interface{}{"op": "set", "field": "payload", "value": "one"},
			}}},
			{ID: "tagB", Type: "change", Data: map[string]interface{}{"rules": []interface{}{
				map[string]interface{}{"op": "set", "field": "payload", "value": "two"},
			}}},
			specResponseNode("resp"),
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "setstate"},
			{ID: "e2", Source: "setstate", Target: "sw"},
			{ID: "e3", Source: "sw", SourceKey: "output_1", Target: "tagA"},
			{ID: "e4", Source: "sw", SourceKey: "output_2", Target: "tagB"},
			{ID: "e5", Source: "tagA", Target: "resp"},
			{ID: "e6", Source: "tagB", Target: "resp"},
		},
	}
	engine := specDeployFlow(t, wf)

	for state, want := range map[float64]string{1: "one", 2: "two"} {
		ch := specInjectInto(t, engine, wf.ID, "inj", &Message{
			Payload:   state,
			Timestamp: time.Now(),
		})
		out := specAwaitResponse(t, ch, 3*time.Second)
		if out.GetPayload() != want {
			t.Fatalf("state %v: expected branch %q, got %#v", state, want, out.GetPayload())
		}
	}
}

// TestSwitchContextValueType: vt:"flow" compares the property against a
// context value.
func TestSwitchContextValueType(t *testing.T) {
	wf := &Workflow{
		ID: "ctx-switch-vt",
		Nodes: []Node{
			{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
			{ID: "setstate", Type: "change", Data: map[string]interface{}{"rules": []interface{}{
				map[string]interface{}{"t": "set", "p": "expected", "pt": "flow", "to": "5", "tot": "num"},
			}}},
			{ID: "sw", Type: "switch", Data: map[string]interface{}{
				"rules": []interface{}{
					map[string]interface{}{"t": "eq", "p": "payload", "pt": "msg", "v": "expected", "vt": "flow"},
				},
			}},
			specResponseNode("hit"),
			specResponseNode("miss"),
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "setstate"},
			{ID: "e2", Source: "setstate", Target: "sw"},
			{ID: "e3", Source: "sw", SourceKey: "output_1", Target: "hit"},
			{ID: "e4", Source: "sw", SourceKey: "output_2", Target: "miss"},
		},
	}
	engine := specDeployFlow(t, wf)

	ch := specInjectInto(t, engine, wf.ID, "inj", &Message{Payload: float64(5), Timestamp: time.Now()})
	out := specAwaitResponse(t, ch, 3*time.Second)
	if out.GetPayload() != float64(5) {
		t.Fatalf("expected match passthrough, got %#v", out.GetPayload())
	}
}
