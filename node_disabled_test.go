package noderunner

import (
	"context"
	"encoding/json"
	nodestore "github.com/dracory/nodestore"
	"strings"
	"testing"
	"time"
)

// debugEvents collects every event message text recorded so far.
func debugEvents(engine *Engine, workflowID string) []string {
	_, _, events, _ := engine.GetWorkflowSnapshot(workflowID, 0)
	var msgs []string
	for _, ev := range events {
		msgs = append(msgs, ev.Message)
	}
	return msgs
}

// TestDisabledInjectDoesNotFire — a node carrying Node-RED's "d": true
// never activates: the once-shot inject produces no message.
func TestDisabledInjectDoesNotFire(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-disabled-inject",
		Name: "Disabled Inject Flow",
		Nodes: []Node{
			{
				ID:   "inj",
				Type: "inject",
				Data: map[string]interface{}{
					"payload":     "orig",
					"payloadType": "str",
					"once":        true,
					"d":           true,
				},
			},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "dbg"},
		},
	}

	engine := NewEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	// A disabled inject is never registered as a stopper — there is
	// nothing to fire, manually or on a timer.
	engine.runMu.Lock()
	stoppers := len(engine.stoppers[workflow.ID])
	engine.runMu.Unlock()
	if stoppers != 0 {
		t.Fatalf("disabled inject registered %d stoppers", stoppers)
	}
	if err := engine.FireInjectNode(workflow.ID, "inj"); err == nil {
		t.Fatal("FireInjectNode succeeded on a disabled inject")
	}

	time.Sleep(200 * time.Millisecond)
	for _, m := range debugEvents(engine, workflow.ID) {
		if strings.Contains(m, "orig") {
			t.Fatalf("disabled inject fired: event %q", m)
		}
	}
}

// TestDisabledHTTPTriggerDoesNotBind — a disabled httpTrigger never
// binds its port: the same port stays free for another workflow.
func TestDisabledHTTPTriggerDoesNotBind(t *testing.T) {
	port := freePort(t)
	wfDisabled := triggerWorkflow("test-disabled-trigger", "/hook")
	wfDisabled.Nodes[0].Data["port"] = port
	wfDisabled.Nodes[0].Data["d"] = true

	engine := NewEngine()
	if _, err := engine.DeployWorkflow(context.Background(), wfDisabled, nil); err != nil {
		t.Fatalf("deploy of disabled trigger failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wfDisabled.ID) }()

	engine.runMu.Lock()
	stoppers := len(engine.stoppers[wfDisabled.ID])
	engine.runMu.Unlock()
	if stoppers != 0 {
		t.Fatalf("disabled httpTrigger registered %d stoppers", stoppers)
	}

	// The port is still free: a second workflow with an enabled
	// trigger on the same port binds successfully.
	wfEnabled := triggerWorkflow("test-enabled-trigger", "/hook")
	wfEnabled.Nodes[0].Data["port"] = port
	if _, err := engine.DeployWorkflow(context.Background(), wfEnabled, nil); err != nil {
		t.Fatalf("enabled trigger could not bind the disabled trigger's port: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wfEnabled.ID) }()
}

// TestDisabledFunctionPassesThrough — messages reaching a disabled
// processing node flow downstream unchanged (the wire stays live).
func TestDisabledFunctionPassesThrough(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-disabled-function",
		Name: "Disabled Function Flow",
		Nodes: []Node{
			{
				ID:   "inj",
				Type: "inject",
				Data: map[string]interface{}{
					"payload":     "orig",
					"payloadType": "str",
					"once":        true,
				},
			},
			{
				ID:   "fn",
				Type: "function",
				Data: map[string]interface{}{
					"func": "msg.payload = 'changed'; return msg;",
					"d":    true,
				},
			},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "fn"},
			{ID: "e2", Source: "fn", Target: "dbg"},
		},
	}

	engine := NewEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	if nodes, _ := waitForStatus(engine, workflow.ID, "dbg", "ok", 2*time.Second); nodes["dbg"] != "ok" {
		t.Fatalf("debug node never ran; statuses=%v", nodes)
	}

	msgs := debugEvents(engine, workflow.ID)
	seenOrig := false
	for _, m := range msgs {
		if strings.Contains(m, "orig") {
			seenOrig = true
		}
		if strings.Contains(m, "changed") {
			t.Fatalf("disabled function executed: event %q", m)
		}
	}
	if !seenOrig {
		t.Fatalf("payload did not pass through unchanged; events=%v", msgs)
	}
}

// TestDisabledFlowTab — every node of a child flow listed in
// view.disabledFlows is inert (Node-RED disabled tab equivalent).
func TestDisabledFlowTab(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-disabled-tab",
		Name: "Disabled Tab Flow",
		Nodes: []Node{
			{
				ID:   "inj",
				Type: "inject",
				Data: map[string]interface{}{
					"payload":     "orig",
					"payloadType": "str",
					"once":        true,
					"childFlow":   "Tab2",
				},
			},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{"childFlow": "Tab2"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "dbg"},
		},
		View: map[string]interface{}{
			"disabledFlows": []interface{}{"Tab2"},
		},
	}

	engine := NewEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	engine.runMu.Lock()
	stoppers := len(engine.stoppers[workflow.ID])
	engine.runMu.Unlock()
	if stoppers != 0 {
		t.Fatalf("inject on a disabled tab registered %d stoppers", stoppers)
	}

	time.Sleep(200 * time.Millisecond)
	_, nodes, _, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
	if nodes["dbg"] == "ok" {
		t.Fatal("node on a disabled tab executed")
	}
}

// TestDisabledGroup — a node inside a view group marked disabled is
// inert; nodes outside the group keep working.
func TestDisabledGroup(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-disabled-group",
		Name: "Disabled Group Flow",
		Nodes: []Node{
			{
				ID:   "inj",
				Type: "inject",
				Data: map[string]interface{}{
					"payload":     "orig",
					"payloadType": "str",
					"once":        true,
				},
			},
			{
				ID:   "fn",
				Type: "function",
				Data: map[string]interface{}{
					"func": "msg.payload = 'changed'; return msg;",
				},
			},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "fn"},
			{ID: "e2", Source: "fn", Target: "dbg"},
		},
		View: map[string]interface{}{
			"groups": []interface{}{
				map[string]interface{}{
					"id":       "g1",
					"name":     "g",
					"nodeIds":  []interface{}{"fn"},
					"disabled": true,
				},
			},
		},
	}

	engine := NewEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	if nodes, _ := waitForStatus(engine, workflow.ID, "dbg", "ok", 2*time.Second); nodes["dbg"] != "ok" {
		t.Fatalf("debug node never ran; statuses=%v", nodes)
	}
	for _, m := range debugEvents(engine, workflow.ID) {
		if strings.Contains(m, "changed") {
			t.Fatalf("function inside a disabled group executed: event %q", m)
		}
	}
}

// TestDisabledNodeRoundTrip — the "d" flag survives a Node-RED
// export/import round-trip; disabled tabs/groups map to the view model.
func TestDisabledNodeRoundTrip(t *testing.T) {
	src := `[
		{"id":"t1","type":"tab","label":"Main","disabled":true},
		{"id":"t2","type":"tab","label":"Second"},
		{"id":"g1","type":"group","z":"t1","name":"grp","x":10,"y":20,"w":200,"h":100,"nodes":["n2"],"disabled":true},
		{"id":"n1","type":"inject","z":"t1","name":"","x":100,"y":100,"wires":[["n2"]],"d":true},
		{"id":"n2","type":"debug","z":"t1","x":200,"y":100,"wires":[]},
		{"id":"n3","type":"debug","z":"t2","x":100,"y":200,"wires":[]}
	]`

	w, err := nodestore.ImportNodeRED([]byte(src))
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}

	// Node-level d survives in node Data. Source ids are re-minted
	// on import — n1 is the inject, n2/n3 the debug nodes (x=200/100).
	var n1, n2, n3 nodestore.NodeInterface
	for _, nd := range w.GetNodes() {
		switch {
		case nd.GetType() == "inject":
			n1 = nd
		case nd.GetType() == "debug" && nd.GetPositionX() == 200:
			n2 = nd
		case nd.GetType() == "debug":
			n3 = nd
		}
	}
	if n1 == nil || n1.GetData()["d"] != true {
		t.Fatalf("imported node lost d flag: %+v", n1)
	}
	if !nodeDisabled(w, n1) {
		t.Fatal("nodeDisabled false for d:true node")
	}
	// n2 is disabled by tab AND by group; n3 is enabled.
	if !nodeDisabled(w, n2) {
		t.Fatal("nodeDisabled false for node on disabled tab+group")
	}
	if nodeDisabled(w, n3) {
		t.Fatal("nodeDisabled true for node on enabled tab")
	}

	// Export round-trip: tab t1 emits disabled:true, the group object
	// comes back with its member list and disabled flag.
	out, err := nodestore.ExportNodeRED(w)
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}
	var items []map[string]interface{}
	if err := json.Unmarshal(out, &items); err != nil {
		t.Fatalf("export produced invalid JSON: %v", err)
	}
	var tab1, grp, node1 map[string]interface{}
	for _, it := range items {
		switch {
		case it["id"] == nodeREDTabID("Main"):
			tab1 = it
		case it["type"] == "group":
			grp = it
		case it["type"] == "inject":
			node1 = it
		}
	}
	if tab1 == nil || tab1["disabled"] != true {
		t.Fatalf("exported tab lost disabled flag: %v", tab1)
	}
	if grp == nil || grp["disabled"] != true {
		t.Fatalf("exported group lost disabled flag: %v", grp)
	}
	if node1 == nil || node1["d"] != true {
		t.Fatalf("exported node lost d flag: %v", node1)
	}

	// Re-importing the export restores the same disabled resolution.
	w2, err := nodestore.ImportNodeRED(out)
	if err != nil {
		t.Fatalf("re-import failed: %v", err)
	}
	for _, nd := range w2.GetNodes() {
		if nd.GetID() == "n2" && !nodeDisabled(w2, nd) {
			t.Fatal("round-trip lost group/tab disabled resolution")
		}
	}
}
