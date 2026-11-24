package noderunner

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestEnvScopingChain verifies the Node-RED env.get resolution order:
// group env (view.groups[].env) → subflow overlay (node.Data["_env"])
// → flow env (view.env) → global-config → OS env. Each layer is
// peeled away to prove the next one takes over.
func TestEnvScopingChain(t *testing.T) {
	t.Setenv("GOFLOW_ENV_SCOPED", "os")

	entry := func(v string) map[string]interface{} {
		return map[string]interface{}{"name": "GOFLOW_ENV_SCOPED", "type": "str", "value": v}
	}

	workflow := &Workflow{
		ID:   "test-env-scoping",
		Name: "Env Scoping",
		Nodes: []Node{
			{
				ID:   "fn",
				Type: "function",
				Data: map[string]interface{}{
					"func": `msg.payload = env.get("GOFLOW_ENV_SCOPED"); return msg;`,
					// Subflow-style overlay: sits between group and
					// flow in the chain.
					subflowEnvKey: map[string]interface{}{"GOFLOW_ENV_SCOPED": "subflow"},
				},
			},
		},
		View: map[string]interface{}{
			"env": []interface{}{entry("flow")},
			"groups": []interface{}{
				map[string]interface{}{
					"id":      "g1",
					"module":  "Home",
					"nodeIds": []interface{}{"fn"},
					"env":     []interface{}{entry("group")},
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

	// Group env wins over every other layer.
	if v, ok := engine.NodeEnv(workflow.ID, "fn", "GOFLOW_ENV_SCOPED"); !ok || v != "group" {
		t.Fatalf("expected group env to win, got %#v (ok=%v)", v, ok)
	}

	// Without group membership the subflow overlay takes over.
	running, _ := engine.RunningFlow(workflow.ID)
	running.GetView()["groups"] = []interface{}{}
	if v, ok := engine.NodeEnv(workflow.ID, "fn", "GOFLOW_ENV_SCOPED"); !ok || v != "subflow" {
		t.Fatalf("expected subflow overlay to win, got %#v (ok=%v)", v, ok)
	}

	// Without the overlay the flow env applies.
	delete(running.FindNodeByID("fn").GetData(), subflowEnvKey)
	if v, ok := engine.NodeEnv(workflow.ID, "fn", "GOFLOW_ENV_SCOPED"); !ok || v != "flow" {
		t.Fatalf("expected flow env to win, got %#v (ok=%v)", v, ok)
	}

	// Without flow env the global-config layer applies.
	delete(running.GetView(), "env")
	if v, ok := engine.NodeEnv(workflow.ID, "fn", "GOFLOW_ENV_SCOPED"); !ok || v != "global" {
		t.Fatalf("expected global-config env to win, got %#v (ok=%v)", v, ok)
	}

	// Without global env the lookup misses — env.get falls back to
	// os.Getenv (verified end-to-end below).
	engine.SetGlobalEnv(nil)
	if _, ok := engine.NodeEnv(workflow.ID, "fn", "GOFLOW_ENV_SCOPED"); ok {
		t.Fatal("expected all scoped layers to miss")
	}
}

// TestEnvScopingEnvGet proves the chain is wired into the function
// node end to end: a script reading env.get sees the group env value
// through a deployed flow, and the OS env is the final fallback.
func TestEnvScopingEnvGet(t *testing.T) {
	t.Setenv("GOFLOW_ENV_E2E", "os-value")

	workflow := &Workflow{
		ID:   "test-env-e2e",
		Name: "Env E2E",
		Nodes: []Node{
			{ID: "inj", Type: "inject", Data: map[string]interface{}{"payload": "go", "payloadType": "str", "once": true}},
			{
				ID:   "fn",
				Type: "function",
				Data: map[string]interface{}{
					"func": `msg.payload = {scoped: env.get("GOFLOW_ENV_E2E"), os: env.get("GOFLOW_ENV_OS_ONLY")}; return msg;`,
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
				map[string]interface{}{"name": "GOFLOW_ENV_E2E", "type": "str", "value": "flow-value"},
			},
		},
	}
	t.Setenv("GOFLOW_ENV_OS_ONLY", "os-only")

	engine := NewEngine()
	engine.SetGlobalEnv([]interface{}{
		map[string]interface{}{"name": "GOFLOW_ENV_E2E", "type": "str", "value": "global-value"},
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

// TestEnvScopingGroupSubflowInner verifies a grouped subflow instance's
// env reaches its inner (namespaced) nodes.
func TestEnvScopingGroupSubflowInner(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-env-group-subflow",
		Name: "Group Subflow Env",
		Nodes: []Node{
			subflowDefNodeShim(
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
			{ID: "inst", Type: "subflow:defG", Data: map[string]interface{}{}},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{"name": "out"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "inst"},
			{ID: "e2", Source: "inst", Target: "dbg"},
		},
		View: map[string]interface{}{
			"groups": []interface{}{
				map[string]interface{}{
					"id":      "g1",
					"module":  "Home",
					"nodeIds": []interface{}{"inst"},
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
		if ev.NodeID == "dbg" && strings.Contains(ev.Message, `"from-group"`) {
			seen = true
		}
	}
	if !seen {
		t.Fatalf("expected group env to reach inner subflow node; events=%v", events)
	}
}
