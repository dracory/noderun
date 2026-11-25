package noderunner_test

import (
	"context"
	function "github.com/dracory/nodebase/function"
	. "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
	"testing"
	"time"
)

// TestFunctionNodeTransform verifies the basic Node-RED contract:
// the script mutates msg.payload and returns msg; the downstream
// httpResponse must observe the transformed value — which also proves
// Metadata (msgId) survives the JS round-trip.
func TestFunctionNodeTransform(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-function-flow",
		Name: "Function Flow",
		Nodes: []Node{
			{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
			{
				ID:   "fn",
				Type: "function",
				Data: map[string]interface{}{
					"func": "msg.payload.n = msg.payload.n * 2; return msg;",
				},
			},
			{ID: "resp", Type: "httpResponse", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "fn"},
			{ID: "e2", Source: "fn", Target: "resp"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	_, ch, err := engine.InjectMessage(workflow.ID, "inj", &Message{
		Payload:   map[string]interface{}{"n": 1},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("inject failed: %v", err)
	}

	select {
	case out := <-ch:
		data, ok := out.GetPayload().(map[string]interface{})
		if !ok {
			t.Fatalf("expected map payload, got %#v", out.GetPayload())
		}
		if data["n"] != float64(2) {
			t.Fatalf("expected n=2, got %#v", data["n"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("function flow produced no response")
	}
}

// TestFunctionNodeMultiSend verifies node.send([m1, m2]) fans out to the
// two output ports: both downstream log nodes must run, each receiving
// its own port's message.
func TestFunctionNodeMultiSend(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-function-multi",
		Name: "Function Multi",
		Nodes: []Node{
			{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
			{
				ID:   "fn",
				Type: "function",
				Data: map[string]interface{}{
					"func":    "node.send([{payload:{p:1}}, {payload:{p:2}}]);",
					"outputs": float64(2),
				},
			},
			{ID: "log1", Type: "log", Data: map[string]interface{}{"message": "p1"}},
			{ID: "log2", Type: "log", Data: map[string]interface{}{"message": "p2"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "fn"},
			{ID: "e2", Source: "fn", SourceKey: "output_1", Target: "log1"},
			{ID: "e3", Source: "fn", SourceKey: "output_2", Target: "log2"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	msgID, _, err := engine.InjectMessage(workflow.ID, "inj", &Message{
		Payload:   map[string]interface{}{"start": true},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	engine.DiscardPending(msgID)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, statuses, _, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		if statuses["log1"] == "ok" && statuses["log2"] == "ok" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, statuses, _, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
	t.Fatalf("expected both logs to run, statuses: %v", statuses)
}

// TestFunctionNodeTimeout verifies a runaway script is interrupted and
// surfaces as a node error rather than blocking the flow forever.
func TestFunctionNodeTimeout(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-function-timeout",
		Name: "Function Timeout",
		Nodes: []Node{
			{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
			{
				ID:   "fn",
				Type: "function",
				Data: map[string]interface{}{
					"func":    "while (true) {}",
					"timeout": float64(100),
				},
			},
			{ID: "log", Type: "log", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "fn"},
			{ID: "e2", Source: "fn", Target: "log"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	msgID, _, err := engine.InjectMessage(workflow.ID, "inj", &Message{
		Payload:   map[string]interface{}{},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	engine.DiscardPending(msgID)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, statuses, _, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		if statuses["fn"] == "error" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, statuses, _, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
	t.Fatalf("expected function node to error on timeout, statuses: %v", statuses)
}

// TestFunctionNodeTimeoutRecovery — BUG-9: after a timeout the same
// node must still run normally (the interrupted VM is discarded).
func TestFunctionNodeTimeoutRecovery(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-function-timeout-recovery",
		Name: "Timeout Recovery",
		Nodes: []Node{
			{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
			{
				ID:   "fn",
				Type: "function",
				Data: map[string]interface{}{
					"func":    "if (msg.payload && msg.payload.spin) { while(true){} } return msg;",
					"timeout": 50,
				},
			},
			{ID: "resp", Type: "httpResponse", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "fn"},
			{ID: "e2", Source: "fn", Target: "resp"},
		},
	}
	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	// First message spins forever and times out.
	if _, _, err := engine.InjectMessage(workflow.ID, "inj", &Message{
		Payload:   map[string]interface{}{"spin": true},
		Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	events := waitForEvents(engine, workflow.ID, 3*time.Second, func(ev NodeEvent) bool {
		return ev.NodeID == "fn" && ev.Level == "error"
	})
	if countEvents(events, func(ev NodeEvent) bool {
		return ev.NodeID == "fn" && ev.Level == "error"
	}) == 0 {
		t.Fatal("expected a timeout error event from the function node")
	}

	// Second message through the SAME node must run normally — a stale
	// interrupt flag on the pooled VM would fail it immediately.
	_, ch, err := engine.InjectMessage(workflow.ID, "inj", &Message{Payload: map[string]interface{}{"n": 1}, Timestamp: time.Now()})
	if err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	select {
	case out := <-ch:
		data, ok := out.GetPayload().(map[string]interface{})
		if !ok || data["n"] != float64(1) {
			t.Fatalf("expected {n:1}, got %v", out.GetPayload())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no response after timeout recovery")
	}
}

// TestFunctionNodeWarnLog — NR-13: node.warn/node.log emit events.
func TestFunctionNodeWarnLog(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-function-warnlog",
		Name: "WarnLog",
		Nodes: []Node{
			{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
			{
				ID:   "fn",
				Type: "function",
				Data: map[string]interface{}{
					"func": "node.warn('careful'); node.log('fyi'); return msg;",
				},
			},
			{ID: "resp", Type: "httpResponse", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "fn"},
			{ID: "e2", Source: "fn", Target: "resp"},
		},
	}
	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	if _, _, err := engine.InjectMessage(workflow.ID, "inj", &Message{Timestamp: time.Now()}); err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	events := waitForEvents(engine, workflow.ID, 3*time.Second, func(ev NodeEvent) bool {
		return ev.NodeID == "fn" && ev.Message == "careful"
	})
	foundWarn, foundLog := false, false
	for _, ev := range events {
		if ev.NodeID == "fn" && ev.Level == "warn" && ev.Message == "careful" {
			foundWarn = true
		}
		if ev.NodeID == "fn" && ev.Level == "info" && ev.Message == "fyi" {
			foundLog = true
		}
	}
	if !foundWarn || !foundLog {
		t.Fatalf("expected warn+info events, got %v", events)
	}
}

// runFunctionScript executes a script body through a bare FunctionNode
// (no workflow needed — injectGlobals only touches node-local state and
// the engine's event/state maps).
func runFunctionScript(t *testing.T, script string) (nodestore.MessageInterface, error) {
	t.Helper()
	engine := NewEngine()
	engine.AllowOSEnv = true // the env.get OS fallback is opt-in
	n := function.NewFunctionNode(nodestore.NewNode("fn", "function").SetWorkflowID("sandbox-wf"), &function.FunctionConfig{Func: script}, engine)
	return n.Execute(context.Background(), &Message{
		Payload:   map[string]interface{}{},
		Timestamp: time.Now(),
	})
}

// TestFunctionNodeSandboxIsolation proves the goja VM exposes NO host
// bindings beyond the documented Node-RED API surface (msg, node,
// context, env). User JS must not reach require/fs/process/network or
// any Go function/object: the VM is built with bare goja.New() and
// injectGlobals is the only injection point (see node_function.go).
func TestFunctionNodeSandboxIsolation(t *testing.T) {
	// Host-capability globals must simply not exist inside the VM.
	// console and setTimeout/clearTimeout are NOT host bindings — they
	// are Node-RED-parity sandbox APIs confined to the VM (console.*
	// maps to node events, timers run script callbacks on the VM), so
	// they are expected to exist.
	out, err := runFunctionScript(t,
		"return {payload:{require:typeof require,process:typeof process,"+
			"console:typeof console,setTimeout:typeof setTimeout,"+
			"fetch:typeof fetch,module:typeof module,"+
			"XMLHttpRequest:typeof XMLHttpRequest}};")
	if err != nil {
		t.Fatalf("typeof probe failed: %v", err)
	}
	m, ok := out.GetPayload().(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %#v", out.GetPayload())
	}
	for _, name := range []string{
		"require", "process",
		"fetch", "module", "XMLHttpRequest",
	} {
		if m[name] != "undefined" {
			t.Errorf("host binding leaked into sandbox: %s is %v", name, m[name])
		}
	}
	if m["console"] != "object" || m["setTimeout"] != "function" {
		t.Errorf("Node-RED parity globals missing: console=%v setTimeout=%v", m["console"], m["setTimeout"])
	}
	// `global` exists but is the Node-RED global-context API object —
	// not the JS global object — so it must expose only get/set/keys
	// and no host surface.
	out, err = runFunctionScript(t,
		"return {payload:{global:typeof global,proc:typeof global.process,"+
			"set:typeof global.set,get:typeof global.get}};")
	if err != nil {
		t.Fatalf("global probe failed: %v", err)
	}
	m = out.GetPayload().(map[string]interface{})
	if m["global"] != "object" || m["proc"] != "undefined" ||
		m["set"] != "function" || m["get"] != "function" {
		t.Fatalf("unexpected global context surface: %#v", m)
	}

	// Attempts to invoke host facilities must hard-fail the script.
	for _, script := range []string{
		"require('fs').readFileSync('/etc/passwd');",
		"require('child_process').exec('id');",
		"process.exit(1);",
		"process.env.HOME;",
		"global.process.exit(1);",
		// Function-constructor escape hatch: still inside the sandbox.
		"(function(){}).constructor('return process')().exit(1);",
		// Exported Go values must not expose reflection-style escapes.
		"msg.constructor.constructor('return process')().exit(1);",
	} {
		if _, err := runFunctionScript(t, script); err == nil {
			t.Errorf("hostile script unexpectedly succeeded: %s", script)
		}
	}

	// env.get is the single deliberate host read (Node-RED parity for
	// credentials — documented on FunctionNode). It must keep working.
	t.Setenv("NODERUN_SANDBOX_PROBE", "probe-ok")
	out, err = runFunctionScript(t,
		"msg.payload = env.get('NODERUN_SANDBOX_PROBE'); return msg;")
	if err != nil {
		t.Fatalf("env.get probe failed: %v", err)
	}
	if out.GetPayload() != "probe-ok" {
		t.Fatalf("env.get returned %#v, want probe-ok", out.GetPayload())
	}
}
