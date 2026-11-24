package noderunner_test

import (
	"context"
	nodestore "github.com/dracory/nodestore"
	. "github.com/dracory/noderun"
	_ "github.com/dracory/nodebase/builtin"
	"reflect"
	"strings"
	"testing"
	"time"
)

// subflowDefNode builds the type:"subflow" definition node as stored
// in a workflow: name, in/out ports, env defaults and the internal
// nodes/edges all live in Data.
func subflowDefNode(id, name string, in, out []interface{}, env []interface{}, nodes, edges []interface{}) nodestore.NodeInterface {
	return nodestore.NewNode(id, "subflow").SetData(map[string]interface{}{
		"name":  name,
		"in":    in,
		"out":   out,
		"env":   env,
		"nodes": nodes,
		"edges": edges,
	})
}

// TestSubflowEndToEnd deploys a flow whose subflow instance wraps an
// internal function node; the message must enter via the in port,
// be transformed inside the namespaced copy, and leave via the out
// port to the debug node.
func TestSubflowEndToEnd(t *testing.T) {
	workflow := nodestore.NewWorkflow("Subflow Flow").SetID("test-subflow-e2e").SetNodes([]nodestore.NodeInterface{
		subflowDefNode(
			"def1", "Wrap",
			[]interface{}{map[string]interface{}{"wires": []interface{}{map[string]interface{}{"id": "fn"}}}},
			[]interface{}{map[string]interface{}{"wires": []interface{}{map[string]interface{}{"id": "fn", "port": 0.0}}}},
			nil,
			[]interface{}{map[string]interface{}{
				"id":   "fn",
				"type": "function",
				"data": map[string]interface{}{"func": `msg.payload = "inner:" + msg.payload; return msg;`},
			}},
			nil,
		),
		nodestore.NewNode("inj", "inject").SetData(map[string]interface{}{
			"payload":     "hi",
			"payloadType": "str",
			"once":        true,
		}),
		nodestore.NewNode("inst", "subflow:def1").SetData(map[string]interface{}{"name": "Wrap 1"}),
		nodestore.NewNode("dbg", "debug").SetData(map[string]interface{}{"name": "out"}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("e1", "inj", "inst"),
		nodestore.NewEdge("e2", "inst", "dbg"),
	})

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.GetID()) }()

	nodes, events := waitForStatus(engine, workflow.GetID(), "dbg", "ok", 3*time.Second)
	if nodes["dbg"] != "ok" {
		t.Fatalf("debug never reached ok; statuses=%v events=%v", nodes, events)
	}

	// The expansion must have produced the namespaced internal node
	// and removed the instance/definition from the running graph.
	if _, ok := engine.LookupNode(workflow.GetID(), "inst/fn"); !ok {
		t.Fatalf("expanded internal node inst/fn not found in running workflow")
	}
	if _, ok := engine.LookupNode(workflow.GetID(), "inst"); ok {
		t.Fatalf("subflow instance node should not survive expansion")
	}
	if _, ok := engine.LookupNode(workflow.GetID(), "def1"); ok {
		t.Fatalf("subflow definition node should not be part of the running graph")
	}

	var sawPayload bool
	for _, ev := range events {
		if ev.Level == "error" {
			t.Fatalf("unexpected error event: %+v", ev)
		}
		if ev.NodeID == "dbg" && strings.Contains(ev.Message, `"inner:hi"`) {
			sawPayload = true
		}
	}
	if !sawPayload {
		t.Fatalf("debug did not receive transformed payload; events=%v", events)
	}
}

// TestSubflowInstancesIsolated deploys two instances of the same
// subflow: each internal copy keeps its own node context, so the
// per-instance counter returns 1 on both, not 2 on the second.
func TestSubflowInstancesIsolated(t *testing.T) {
	workflow := nodestore.NewWorkflow("Isolation Flow").SetID("test-subflow-isolation").SetNodes([]nodestore.NodeInterface{
		subflowDefNode(
			"def2", "Counter",
			[]interface{}{map[string]interface{}{"wires": []interface{}{map[string]interface{}{"id": "fn"}}}},
			[]interface{}{map[string]interface{}{"wires": []interface{}{map[string]interface{}{"id": "fn", "port": 0.0}}}},
			nil,
			[]interface{}{map[string]interface{}{
				"id":   "fn",
				"type": "function",
				"data": map[string]interface{}{"func": `var c = context.get("c") || 0; c++; context.set("c", c); msg.payload = c; return msg;`},
			}},
			nil,
		),
		nodestore.NewNode("inj1", "inject").SetData(map[string]interface{}{"payload": "a", "payloadType": "str", "once": true}),
		nodestore.NewNode("inj2", "inject").SetData(map[string]interface{}{"payload": "b", "payloadType": "str", "once": true}),
		nodestore.NewNode("instA", "subflow:def2").SetData(map[string]interface{}{}),
		nodestore.NewNode("instB", "subflow:def2").SetData(map[string]interface{}{}),
		nodestore.NewNode("dbgA", "debug").SetData(map[string]interface{}{"name": "A"}),
		nodestore.NewNode("dbgB", "debug").SetData(map[string]interface{}{"name": "B"}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("e1", "inj1", "instA"),
		nodestore.NewEdge("e2", "instA", "dbgA"),
		nodestore.NewEdge("e3", "inj2", "instB"),
		nodestore.NewEdge("e4", "instB", "dbgB"),
	})

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.GetID()) }()

	nodes, events := waitForStatus(engine, workflow.GetID(), "dbgA", "ok", 3*time.Second)
	nodes, events = waitForStatus(engine, workflow.GetID(), "dbgB", "ok", 3*time.Second)
	if nodes["dbgA"] != "ok" || nodes["dbgB"] != "ok" {
		t.Fatalf("instances did not complete; statuses=%v events=%v", nodes, events)
	}

	// Both debug nodes must have seen counter == 1: each instance
	// owns its copy of the function node's context.
	seen := map[string]bool{}
	for _, ev := range events {
		if ev.Level == "error" {
			t.Fatalf("unexpected error event: %+v", ev)
		}
		if ev.NodeID == "dbgA" && strings.Contains(ev.Message, "1") {
			seen["A"] = true
		}
		if ev.NodeID == "dbgB" && strings.Contains(ev.Message, "1") {
			seen["B"] = true
		}
		if (ev.NodeID == "dbgA" || ev.NodeID == "dbgB") && strings.Contains(ev.Message, "2") {
			t.Fatalf("context leaked between instances: %+v", ev)
		}
	}
	if !seen["A"] || !seen["B"] {
		t.Fatalf("expected counter=1 on both instances; events=%v", events)
	}
}

// TestSubflowEnvOverride verifies per-instance env overrides reach
// env.get inside the subflow, falling back to the definition default
// when the instance does not override.
func TestSubflowEnvOverride(t *testing.T) {
	workflow := nodestore.NewWorkflow("Env Flow").SetID("test-subflow-env").SetNodes([]nodestore.NodeInterface{
		subflowDefNode(
			"def3", "Greeter",
			[]interface{}{map[string]interface{}{"wires": []interface{}{map[string]interface{}{"id": "fn"}}}},
			[]interface{}{map[string]interface{}{"wires": []interface{}{map[string]interface{}{"id": "fn", "port": 0.0}}}},
			[]interface{}{map[string]interface{}{"name": "GREETING", "type": "str", "value": "default"}},
			[]interface{}{map[string]interface{}{
				"id":   "fn",
				"type": "function",
				"data": map[string]interface{}{"func": `msg.payload = env.get("GREETING"); return msg;`},
			}},
			nil,
		),
		nodestore.NewNode("inj1", "inject").SetData(map[string]interface{}{"payload": "a", "payloadType": "str", "once": true}),
		nodestore.NewNode("inj2", "inject").SetData(map[string]interface{}{"payload": "b", "payloadType": "str", "once": true}),
		nodestore.NewNode("instA", "subflow:def3").SetData(map[string]interface{}{
			"env": []interface{}{map[string]interface{}{"name": "GREETING", "type": "str", "value": "hi"}},
		}),
		nodestore.NewNode("instB", "subflow:def3").SetData(map[string]interface{}{}),
		nodestore.NewNode("dbgA", "debug").SetData(map[string]interface{}{"name": "A"}),
		nodestore.NewNode("dbgB", "debug").SetData(map[string]interface{}{"name": "B"}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("e1", "inj1", "instA"),
		nodestore.NewEdge("e2", "instA", "dbgA"),
		nodestore.NewEdge("e3", "inj2", "instB"),
		nodestore.NewEdge("e4", "instB", "dbgB"),
	})

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.GetID()) }()

	nodes, events := waitForStatus(engine, workflow.GetID(), "dbgA", "ok", 3*time.Second)
	nodes, events = waitForStatus(engine, workflow.GetID(), "dbgB", "ok", 3*time.Second)
	if nodes["dbgA"] != "ok" || nodes["dbgB"] != "ok" {
		t.Fatalf("instances did not complete; statuses=%v", nodes)
	}

	var sawOverride, sawDefault bool
	for _, ev := range events {
		if ev.NodeID == "dbgA" && strings.Contains(ev.Message, `"hi"`) {
			sawOverride = true
		}
		if ev.NodeID == "dbgB" && strings.Contains(ev.Message, `"default"`) {
			sawDefault = true
		}
	}
	if !sawOverride || !sawDefault {
		t.Fatalf("env override/default not observed; events=%v", events)
	}
}

// TestNodeREDImportRuns proves a Node-RED flow export — tab, subflow
// definition with internal nodes at z=<defId>, and a subflow:<id>
// instance — imports and deploys unmodified.
func TestNodeREDImportRuns(t *testing.T) {
	flowJSON := `[
		{"id":"tab1","type":"tab","label":"Main"},
		{"id":"defX","type":"subflow","name":"Wrap","info":"",
		 "in":[{"x":40,"y":40,"wires":[{"id":"innerFn"}]}],
		 "out":[{"x":300,"y":40,"wires":[{"id":"innerFn","port":0}]}],
		 "env":[{"name":"GREETING","type":"str","value":"fromNR"}]},
		{"id":"innerFn","type":"function","z":"defX","x":170,"y":40,
		 "func":"msg.payload = env.get('GREETING') + ':' + msg.payload; return msg;","outputs":1,"wires":[[]]},
		{"id":"nrInj","type":"inject","z":"tab1","x":100,"y":100,
		 "payload":"hi","payloadType":"str","once":true,"wires":[["nrInst"]]},
		{"id":"nrInst","type":"subflow:defX","z":"tab1","x":250,"y":100,"name":"Wrap 1","wires":[["nrDbg"]]},
		{"id":"nrDbg","type":"debug","z":"tab1","x":400,"y":100,"active":true,"wires":[[]]}
	]`

	workflow, err := nodestore.ImportNodeRED([]byte(flowJSON))
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}

	// The subflow definition must be folded into a type:"subflow"
	// node with its internal node carried in Data.
	// Import re-mints source ids — locate by type: the def folds
	// into a "subflow" node, the instance into "subflow:<newDefID>".
	var def, inst nodestore.NodeInterface
	for i := range workflow.GetNodes() {
		switch {
		case workflow.GetNodes()[i].GetType() == "subflow":
			def = workflow.GetNodes()[i]
		case strings.HasPrefix(workflow.GetNodes()[i].GetType(), "subflow:"):
			inst = workflow.GetNodes()[i]
		}
	}
	if def == nil {
		t.Fatalf("subflow definition not imported; nodes=%+v", workflow.GetNodes())
	}
	if inst == nil {
		t.Fatalf("subflow instance not imported; nodes=%+v", workflow.GetNodes())
	}
	if inst.GetType() != "subflow:"+def.GetID() {
		t.Fatalf("instance type %q does not reference def %q", inst.GetType(), def.GetID())
	}
	inner, _ := def.GetData()["nodes"].([]interface{})
	if len(inner) != 1 {
		t.Fatalf("definition carries %d internal nodes, want 1", len(inner))
	}

	// Round-trip: export back to Node-RED JSON and re-import.
	raw, err := nodestore.ExportNodeRED(workflow)
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}
	reimported, err := nodestore.ImportNodeRED(raw)
	if err != nil {
		t.Fatalf("re-import failed: %v (exported %s)", err, raw)
	}
	var reDef nodestore.NodeInterface
	for i := range reimported.GetNodes() {
		if reimported.GetNodes()[i].GetType() == "subflow" {
			reDef = reimported.GetNodes()[i]
		}
	}
	if reDef == nil {
		t.Fatalf("exported flow lost the subflow definition: %s", raw)
	}
	inner, _ = reDef.GetData()["nodes"].([]interface{})
	if len(inner) != 1 {
		t.Fatalf("exported definition lost its internal nodes: %s", raw)
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.GetID()) }()

	var dbgID string
	for _, n := range workflow.GetNodes() {
		if n.GetType() == "debug" {
			dbgID = n.GetID()
		}
	}
	nodes, events := waitForStatus(engine, workflow.GetID(), dbgID, "ok", 3*time.Second)
	if nodes[dbgID] != "ok" {
		t.Fatalf("debug never reached ok; statuses=%v events=%v", nodes, events)
	}
	var sawPayload bool
	for _, ev := range events {
		if ev.NodeID == dbgID && strings.Contains(ev.Message, `"fromNR:hi"`) {
			sawPayload = true
		}
	}
	if !sawPayload {
		t.Fatalf("subflow env + in/out wiring failed; events=%v", events)
	}
}

// TestSubflowRecursiveRejected documents the v1 boundary: a subflow
// that contains an instance of itself fails deploy with a clear error
// instead of expanding forever. Non-recursive nesting works.
func TestSubflowRecursiveRejected(t *testing.T) {
	selfRef := subflowDefNode(
		"loop", "Loop",
		nil, nil, nil,
		[]interface{}{map[string]interface{}{
			"id":   "innerInst",
			"type": "subflow:loop",
			"data": map[string]interface{}{},
		}},
		nil,
	)
	workflow := nodestore.NewWorkflow("Recursive Flow").SetID("test-subflow-recursion").SetNodes([]nodestore.NodeInterface{selfRef, nodestore.NewNode("inst", "subflow:loop").SetData(map[string]interface{}{})})

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err == nil {
		t.Fatalf("recursive subflow deployed; want a clear error")
	} else if !strings.Contains(err.Error(), "recursive") {
		t.Fatalf("unexpected error: %v", err)
	}

	// Non-recursive nesting: outer def contains an instance of the
	// inner def — must expand and run.
	nested := nodestore.NewWorkflow("Nested Flow").SetID("test-subflow-nested").SetNodes([]nodestore.NodeInterface{
		subflowDefNode(
			"inner", "Inner",
			[]interface{}{map[string]interface{}{"wires": []interface{}{map[string]interface{}{"id": "j"}}}},
			[]interface{}{map[string]interface{}{"wires": []interface{}{map[string]interface{}{"id": "j", "port": 0.0}}}},
			nil,
			[]interface{}{map[string]interface{}{"id": "j", "type": "junction", "data": map[string]interface{}{}}},
			nil,
		),
		subflowDefNode(
			"outer", "Outer",
			[]interface{}{map[string]interface{}{"wires": []interface{}{map[string]interface{}{"id": "innerInst"}}}},
			[]interface{}{map[string]interface{}{"wires": []interface{}{map[string]interface{}{"id": "innerInst", "port": 0.0}}}},
			nil,
			[]interface{}{map[string]interface{}{
				"id":   "innerInst",
				"type": "subflow:inner",
				"data": map[string]interface{}{},
			}},
			nil,
		),
		nodestore.NewNode("inj", "inject").SetData(map[string]interface{}{"payload": "x", "payloadType": "str", "once": true}),
		nodestore.NewNode("inst", "subflow:outer").SetData(map[string]interface{}{}),
		nodestore.NewNode("dbg", "debug").SetData(map[string]interface{}{"name": "out"}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("e1", "inj", "inst"),
		nodestore.NewEdge("e2", "inst", "dbg"),
	})
	if _, err := engine.ExecuteWorkflow(context.Background(), nested, nil); err != nil {
		t.Fatalf("nested subflow deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(nested.GetID()) }()

	nodes, events := waitForStatus(engine, nested.GetID(), "dbg", "ok", 3*time.Second)
	if nodes["dbg"] != "ok" {
		t.Fatalf("nested subflow did not complete; statuses=%v events=%v", nodes, events)
	}
	if _, ok := engine.LookupNode(nested.GetID(), "inst/innerInst/j"); !ok {
		t.Fatalf("nested internal node inst/innerInst/j not found")
	}
}

// TestSubflowOrphanedInstanceFailsDeploy: a subflow:<id> instance
// whose definition is absent must fail deploy with the clear
// "definition not found" error — even when the workflow carries no
// subflow definitions at all (previously ExpandSubflows early-
// returned and the instance died later as an unknown node type).
func TestSubflowOrphanedInstanceFailsDeploy(t *testing.T) {
	workflow := nodestore.NewWorkflow("Orphan Flow").SetID("test-subflow-orphan").SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inst", "subflow:missing").SetData(map[string]interface{}{}),
		nodestore.NewNode("dbg", "debug").SetData(map[string]interface{}{}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("e1", "inst", "dbg"),
	})

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err == nil {
		t.Fatalf("orphaned subflow instance deployed; want a definition-not-found error")
	} else if !strings.Contains(err.Error(), `definition "missing" not found`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestExpandSubflowsErrorLeavesWorkflowUnchanged: a failed expansion
// must not partially mutate the input workflow — definition nodes
// stay in place and nodes/edges are untouched.
func TestExpandSubflowsErrorLeavesWorkflowUnchanged(t *testing.T) {
	nodes := []nodestore.NodeInterface{
		subflowDefNode(
			"d1", "D",
			nil, nil, nil,
			[]interface{}{map[string]interface{}{"id": "j", "type": "junction", "data": map[string]interface{}{}}},
			nil,
		),
		nodestore.NewNode("orphan", "subflow:nope").SetData(map[string]interface{}{}),
		nodestore.NewNode("x", "junction").SetData(map[string]interface{}{}),
	}
	edges := []nodestore.EdgeInterface{
		nodestore.NewEdge("e1", "orphan", "x"),
	}
	w := nodestore.NewWorkflow("").SetID("test-subflow-no-mutation").SetNodes(append([]nodestore.NodeInterface(nil), nodes...)).SetEdges(append([]nodestore.EdgeInterface(nil), edges...))

	if err := ExpandSubflows(w, false); err == nil {
		t.Fatalf("expansion of unknown definition succeeded; want error")
	} else if !strings.Contains(err.Error(), `definition "nope" not found`) {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(w.GetNodes(), nodes) {
		t.Fatalf("workflow nodes mutated on failed expansion: %+v", w.GetNodes())
	}
	if !reflect.DeepEqual(w.GetEdges(), edges) {
		t.Fatalf("workflow edges mutated on failed expansion: %+v", w.GetEdges())
	}
}

// TestExpandSubflowsDanglingEdgesFail: edges left pointing at removed
// definition nodes, and definition-internal edges with unknown
// endpoints, must fail expansion instead of silently passing through
// or being dropped.
func TestExpandSubflowsDanglingEdgesFail(t *testing.T) {
	// nodestore.EdgeInterface from a regular node into a definition node.
	w := nodestore.NewWorkflow("").SetID("test-subflow-def-edge").SetNodes([]nodestore.NodeInterface{
		subflowDefNode(
			"d2", "D",
			nil, nil, nil,
			[]interface{}{map[string]interface{}{"id": "j", "type": "junction", "data": map[string]interface{}{}}},
			nil,
		),
		nodestore.NewNode("inj", "inject").SetData(map[string]interface{}{}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("e1", "inj", "d2"),
	})
	if err := ExpandSubflows(w, false); err == nil {
		t.Fatalf("edge into a subflow definition expanded cleanly; want error")
	} else if !strings.Contains(err.Error(), "subflow definition") {
		t.Fatalf("unexpected error: %v", err)
	}

	// Internal definition edge with an unknown endpoint.
	w2 := nodestore.NewWorkflow("").SetID("test-subflow-inner-edge").SetNodes([]nodestore.NodeInterface{
		subflowDefNode(
			"d3", "D",
			nil, nil, nil,
			[]interface{}{map[string]interface{}{"id": "j", "type": "junction", "data": map[string]interface{}{}}},
			[]interface{}{map[string]interface{}{
				"id": "ie1", "source": "j", "sourceKey": "output_1", "target": "ghost",
			}},
		),
		nodestore.NewNode("inst", "subflow:d3").SetData(map[string]interface{}{}),
	})
	if err := ExpandSubflows(w2, false); err == nil {
		t.Fatalf("internal edge to unknown node expanded cleanly; want error")
	} else if !strings.Contains(err.Error(), "unknown node") {
		t.Fatalf("unexpected error: %v", err)
	}
}
