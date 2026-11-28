package noderunner

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestEnvScopingChain verifies the Node-RED env.get resolution order:
// group env (view.groups[].env) → component overlay (node.Data["_env"])
// → flow env (view.env) → global-config → OS env. Each layer is
// peeled away to prove the next one takes over.
func TestEnvScopingChain(t *testing.T) {
	t.Setenv("NODERUN_ENV_SCOPED", "os")

	entry := func(v string) map[string]interface{} {
		return map[string]interface{}{"name": "NODERUN_ENV_SCOPED", "type": "str", "value": v}
	}

	workflow := &Workflow{
		ID:   "test-env-scoping",
		Name: "Env Scoping",
		Nodes: []Node{
			{
				ID:   "fn",
				Type: "function",
				Data: map[string]interface{}{
					"func": `msg.payload = env.get("NODERUN_ENV_SCOPED"); return msg;`,
					// Component-style overlay: sits between group and
					// flow in the chain.
					componentEnvKey: map[string]interface{}{"NODERUN_ENV_SCOPED": "component"},
				},
			},
		},
		View: map[string]interface{}{
			"env": []interface{}{entry("flow")},
			"groups": []interface{}{
				map[string]interface{}{
					"id":        "g1",
					"childFlow": "Home",
					"nodeIds":   []interface{}{"fn"},
					"env":       []interface{}{entry("group")},
				},
			},
		},
	}

	engine := NewEngine()
	engine.SetGlobalEnv([]interface{}{entry("global")})
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	// Activation runs async and reads view/node data without runMu —
	// wait for it to finish before peeling layers off the live flow.
	waitForEvents(engine, workflow.ID, 3*time.Second, func(ev NodeEvent) bool {
		return ev.Message == "workflow deployed"
	})

	// Group env wins over every other layer.
	if v, ok := engine.NodeEnv(workflow.ID, "fn", "NODERUN_ENV_SCOPED"); !ok || v != "group" {
		t.Fatalf("expected group env to win, got %#v (ok=%v)", v, ok)
	}

	// Peel the layers off the live run in place — RunningFlow returns
	// a masked clone by design, so the test mutates e.flows directly
	// under runMu to stay race-free against the running traversal.
	engine.runMu.Lock()
	running := engine.flows[workflow.ID]
	if running == nil {
		engine.runMu.Unlock()
		t.Fatal("running workflow missing")
	}
	running.GetView()["groups"] = []interface{}{}
	engine.runMu.Unlock()
	if v, ok := engine.NodeEnv(workflow.ID, "fn", "NODERUN_ENV_SCOPED"); !ok || v != "component" {
		t.Fatalf("expected component overlay to win, got %#v (ok=%v)", v, ok)
	}

	// Without the overlay the flow env applies.
	engine.runMu.Lock()
	fnNode := running.FindNodeByID("fn")
	if fnNode == nil {
		engine.runMu.Unlock()
		t.Fatal("function node missing from running workflow")
	}
	delete(fnNode.GetData(), componentEnvKey)
	engine.runMu.Unlock()
	if v, ok := engine.NodeEnv(workflow.ID, "fn", "NODERUN_ENV_SCOPED"); !ok || v != "flow" {
		t.Fatalf("expected flow env to win, got %#v (ok=%v)", v, ok)
	}

	// Without flow env the global-config layer applies.
	engine.runMu.Lock()
	delete(running.GetView(), "env")
	engine.runMu.Unlock()
	if v, ok := engine.NodeEnv(workflow.ID, "fn", "NODERUN_ENV_SCOPED"); !ok || v != "global" {
		t.Fatalf("expected global-config env to win, got %#v (ok=%v)", v, ok)
	}

	// Without global env the lookup misses — env.get falls back to
	// os.Getenv (verified end-to-end below).
	engine.SetGlobalEnv(nil)
	if _, ok := engine.NodeEnv(workflow.ID, "fn", "NODERUN_ENV_SCOPED"); ok {
		t.Fatal("expected all scoped layers to miss")
	}
}

// TestEnvScopingEnvGet proves the chain is wired into the function
// node end to end: a script reading env.get sees the group env value
// through a deployed flow, and the OS env is the final fallback.
func TestEnvScopingEnvGet(t *testing.T) {
	t.Setenv("NODERUN_ENV_E2E", "os-value")

	workflow := &Workflow{
		ID:   "test-env-e2e",
		Name: "Env E2E",
		Nodes: []Node{
			{ID: "inj", Type: "inject", Data: map[string]interface{}{"payload": "go", "payloadType": "str", "once": true}},
			{
				ID:   "fn",
				Type: "function",
				Data: map[string]interface{}{
					"func": `msg.payload = {scoped: env.get("NODERUN_ENV_E2E"), os: env.get("NODERUN_ENV_OS_ONLY")}; return msg;`,
				},
			},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{"name": "out"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "fn"},
			{ID: "e2", Source: "fn", Target: "dbg"},
		},
		View: map[string]interface{}{
			"env": []interface{}{
				map[string]interface{}{"name": "NODERUN_ENV_E2E", "type": "str", "value": "flow-value"},
			},
		},
	}
	t.Setenv("NODERUN_ENV_OS_ONLY", "os-only")

	engine := NewEngine()
	engine.AllowOSEnv = true // the OS env fallback is opt-in
	engine.SetGlobalEnv([]interface{}{
		map[string]interface{}{"name": "NODERUN_ENV_E2E", "type": "str", "value": "global-value"},
	})
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	_, events := waitForStatus(engine, workflow.ID, "dbg", "ok", 3*time.Second)
	var seen bool
	for _, ev := range events {
		if ev.NodeID == "dbg" && strings.Contains(ev.Message, "flow-value") && strings.Contains(ev.Message, "os-only") {
			seen = true
		}
	}
	if !seen {
		t.Fatalf("expected flow env + OS fallback in debug output; events=%v", events)
	}
}

// TestEnvScopingGroupComponentInner verifies a grouped component instance's
// env reaches its inner (namespaced) nodes.
func TestEnvScopingGroupComponentInner(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-env-group-component",
		Name: "Group Component Env",
		Nodes: []Node{
			componentDefNodeShim(
				"defG", "Greeter",
				[]interface{}{map[string]interface{}{"wires": []interface{}{map[string]interface{}{"id": "fn"}}}},
				[]interface{}{map[string]interface{}{"wires": []interface{}{map[string]interface{}{"id": "fn", "port": 0.0}}}},
				nil,
				[]interface{}{map[string]interface{}{
					"id":   "fn",
					"type": "function",
					"data": map[string]interface{}{"func": `msg.payload = env.get("G_VAR"); return msg;`},
				}},
				nil,
			),
			{ID: "inj", Type: "inject", Data: map[string]interface{}{"payload": "x", "payloadType": "str", "once": true}},
			{ID: "inst", Type: "component:defG", Data: map[string]interface{}{}},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{"name": "out"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "inst"},
			{ID: "e2", Source: "inst", Target: "dbg"},
		},
		View: map[string]interface{}{
			"groups": []interface{}{
				map[string]interface{}{
					"id":        "g1",
					"childFlow": "Home",
					"nodeIds":   []interface{}{"inst"},
					"env": []interface{}{
						map[string]interface{}{"name": "G_VAR", "type": "str", "value": "from-group"},
					},
				},
			},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	_, events := waitForStatus(engine, workflow.ID, "dbg", "ok", 3*time.Second)
	var seen bool
	for _, ev := range events {
		// Bare string: nodebase debug renders payloads without JSON quotes.
		if ev.NodeID == "dbg" && strings.Contains(ev.Message, `from-group`) {
			seen = true
		}
	}
	if !seen {
		t.Fatalf("expected group env to reach inner component node; events=%v", events)
	}
}
