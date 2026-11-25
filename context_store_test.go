package noderunner_test

import (
	"context"
	"github.com/dracory/nodebase"
	. "github.com/dracory/nodebase/function"
	. "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
	"testing"
	"time"
)

// runFunctionIn executes a function-node script in the given workflow
// against the engine — enough to exercise the context scopes without
// a full deploy. Note the engine caches the compiled program per
// (workflow, node) pair, so each call must use a fresh node ID.
func runFunctionIn(t *testing.T, engine *Engine, workflowID, nodeID, script string) nodestore.MessageInterface {
	t.Helper()
	n := NewFunctionNode(nodestore.NewNode(nodeID, "function").SetWorkflowID(workflowID), &FunctionConfig{Func: script}, engine)
	out, err := n.Execute(context.Background(), nodestore.NewMessage().SetPayload(map[string]interface{}{}).SetTimestamp(time.Now()))
	if err != nil {
		t.Fatalf("script %q failed: %v", script, err)
	}
	return out
}

// TestContextScopeIsolation proves Node-RED scoping: two workflows
// cannot see each other's flow context, node context is private to
// the writing node, and global context is shared engine-wide.
func TestContextScopeIsolation(t *testing.T) {
	engine := NewEngine()

	// The writer sets all three scopes and reads its own values back.
	out := runFunctionIn(t, engine, "wf-a", "n1",
		"flow.set('secret', 'a-value'); global.set('shared', 'g-value'); context.set('mine', 7);"+
			"msg.payload = {f: flow.get('secret'), n: context.get('mine'), keys: context.keys()}; return msg;")
	got, ok := out.GetPayload().(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %#v", out.GetPayload())
	}
	if got["f"] != "a-value" || got["n"] != int64(7) {
		t.Fatalf("own context not readable: %#v", got)
	}
	keys, ok := got["keys"].([]string)
	if !ok || len(keys) != 1 || keys[0] != "mine" {
		t.Fatalf("context.keys() returned %#v", got["keys"])
	}

	// A node in ANOTHER workflow sees neither the flow nor the node
	// values, but does see the global one.
	out = runFunctionIn(t, engine, "wf-b", "n2",
		"msg.payload = {flowLeak: typeof flow.get('secret'), "+
			"nodeLeak: typeof context.get('mine'), "+
			"global: global.get('shared')}; return msg;")
	got, ok = out.GetPayload().(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %#v", out.GetPayload())
	}
	if got["flowLeak"] != "undefined" {
		t.Fatalf("flow context leaked across workflows: %#v", got["flowLeak"])
	}
	if got["nodeLeak"] != "undefined" {
		t.Fatalf("node context leaked across nodes: %#v", got["nodeLeak"])
	}
	if got["global"] != "g-value" {
		t.Fatalf("global context not shared: %#v", got["global"])
	}
}

// TestContextSurvivesRedeploy proves flow/global/node values persist
// across undeploy/redeploy — the whole point of the engine-level
// store (e.states is wiped on full deploy).
func TestContextSurvivesRedeploy(t *testing.T) {
	mk := func() nodestore.WorkflowInterface {
		return nodestore.NewWorkflow("ctx").SetID("ctx-redeploy").SetNodes([]nodestore.NodeInterface{
			nodestore.NewNode("inj", "inject").SetData(map[string]interface{}{}),
			nodestore.NewNode("fn", "function").SetData(map[string]interface{}{
				"func": "flow.set('n', (flow.get('n') || 0) + 1); msg.payload = flow.get('n'); return msg;",
			}),
			nodestore.NewNode("resp", "httpResponse").SetData(map[string]interface{}{}),
		}).SetEdges([]nodestore.EdgeInterface{
			nodestore.NewEdge("e1", "inj", "fn"),
			nodestore.NewEdge("e2", "fn", "resp"),
		})
	}

	inject := func(engine *Engine) float64 {
		_, ch, err := engine.InjectMessage("ctx-redeploy", "inj", nodestore.NewMessage().SetTimestamp(time.Now()))
		if err != nil {
			t.Fatalf("inject failed: %v", err)
		}
		select {
		case out := <-ch:
			v, _ := out.GetPayload().(float64)
			return v
		case <-time.After(3 * time.Second):
			t.Fatal("no response from flow")
			return 0
		}
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), mk(), nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	if v := inject(engine); v != 1 {
		t.Fatalf("first run: n=%v, want 1", v)
	}
	if err := engine.StopWorkflow("ctx-redeploy"); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	// Undeploy/redeploy on the SAME engine (in-memory fallback store).
	if _, err := engine.ExecuteWorkflow(context.Background(), mk(), nil); err != nil {
		t.Fatalf("redeploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow("ctx-redeploy") }()
	if v := inject(engine); v != 2 {
		t.Fatalf("after redeploy: n=%v, want 2 (context must survive)", v)
	}
}

// TestContextSharedStoreAcrossEngines simulates an engine restart with
// a persistent backend: a second engine bound to the same nodebase.ContextStore
// must observe everything the first engine wrote — this is the wiring
// main.go performs against the disk-backed stores.
func TestContextSharedStoreAcrossEngines(t *testing.T) {
	shared := nodebase.NewMemoryContextStore()

	e1 := NewEngine()
	e1.ContextStore = shared
	runFunctionIn(t, e1, "wf-a", "n1",
		"flow.set('counter', 41); global.set('boot', 'yes'); return msg;")

	e2 := NewEngine()
	e2.ContextStore = shared
	out := runFunctionIn(t, e2, "wf-a", "n1",
		"msg.payload = {n: flow.get('counter'), g: global.get('boot')}; return msg;")
	got := out.GetPayload().(map[string]interface{})
	// goja exports whole numbers as int64; accept either numeric type.
	if got["n"] != int64(41) && got["n"] != float64(41) {
		t.Fatalf("flow context not visible to second engine: %#v", got)
	}
	if got["g"] != "yes" {
		t.Fatalf("global context not visible to second engine: %#v", got)
	}
}

// TestContextDelete verifies set(key, undefined) removes the key and
// that flow.keys() reflects the deletion.
func TestContextDelete(t *testing.T) {
	engine := NewEngine()
	out := runFunctionIn(t, engine, "wf", "n",
		"flow.set('x', 1); flow.set('y', 2); flow.set('x', undefined);"+
			"msg.payload = {x: typeof flow.get('x'), keys: flow.keys()}; return msg;")
	got := out.GetPayload().(map[string]interface{})
	if got["x"] != "undefined" {
		t.Fatalf("deleted key still readable: %#v", got)
	}
	keys, ok := got["keys"].([]string)
	if !ok || len(keys) != 1 || keys[0] != "y" {
		t.Fatalf("unexpected keys after delete: %#v", got["keys"])
	}
}
