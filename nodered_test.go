package noderunner_test

import (
	"context"
	nodestore "github.com/dracory/nodestore"
	. "github.com/dracory/noderun"
	"testing"
	"time"

	"encoding/json"
	"fmt"
)

// nodeByID looks up an imported node by id (port of the flowrunner
// test helper).
func nodeByID(w nodestore.WorkflowInterface, id string) nodestore.NodeInterface {
	for _, n := range w.GetNodes() {
		if n.GetID() == id {
			return n
		}
	}
	return nil
}

// stringOf coerces an exported JSON value to string for assertions.
func stringOf(v interface{}) string { return fmt.Sprint(v) }

// TestNodeREDLinkNodesEndToEnd imports a Node-RED link in/out pair and
// proves the mapped linkOut executor actually pushes the message
// through the linkIn's downstream wiring at run time.
func TestNodeREDLinkNodesEndToEnd(t *testing.T) {
	flowJSON := `[
		{"id":"tab1","type":"tab","label":"Main"},
		{"id":"inj","type":"inject","z":"tab1","payload":"via-link","payloadType":"str","once":true,"wires":[["lout"]]},
		{"id":"lout","type":"link out","z":"tab1","links":["lin"],"mode":"link","wires":[[]]},
		{"id":"lin","type":"link in","z":"tab1","wires":[["dbg"]]},
		{"id":"dbg","type":"debug","z":"tab1","active":true,"wires":[[]]}
	]`

	w, err := nodestore.ImportNodeRED([]byte(flowJSON))
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}
	types := map[string]string{}
	for _, n := range w.GetNodes() {
		types[n.GetType()] = n.GetType()
	}
	if types["linkOut"] == "" || types["linkIn"] == "" {
		t.Fatalf("link types not mapped: %+v", w.GetNodes())
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), w, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(w.GetID()) }()

	// Import re-mints source ids — locate the debug node by type.
	var dbgID string
	for _, n := range w.GetNodes() {
		if n.GetType() == "debug" {
			dbgID = n.GetID()
		}
	}
	nodes, events := waitForStatus(engine, w.GetID(), dbgID, "ok", 3*time.Second)
	if nodes[dbgID] != "ok" {
		t.Fatalf("debug never received the linkOut push; statuses=%v events=%v", nodes, events)
	}
}

// TestNodeREDTypeMappingRoundTrip proves an imported Node-RED flow
// exports back to the original type spellings.
func TestNodeREDTypeMappingRoundTrip(t *testing.T) {
	flowJSON := `[
		{"id":"tab1","type":"tab","label":"Main"},
		{"id":"h","type":"http in","z":"tab1","url":"/x","wires":[["r"]]},
		{"id":"r","type":"http response","z":"tab1","wires":[[]]},
		{"id":"m","type":"mqtt out","z":"tab1","topic":"t","wires":[[]]}
	]`
	w, err := nodestore.ImportNodeRED([]byte(flowJSON))
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}
	raw, err := nodestore.ExportNodeRED(w)
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}
	var items []map[string]interface{}
	if err := json.Unmarshal(raw, &items); err != nil {
		t.Fatalf("bad export JSON: %v", err)
	}
	got := map[string]bool{}
	for _, it := range items {
		got[stringOf(it["type"])] = true
	}
	for _, wt := range []string{"http in", "http response", "mqtt out"} {
		if !got[wt] {
			t.Errorf("round-trip type %q missing from export", wt)
		}
	}
}

// TestNodeREDEnvRoundTrip proves flow env on tab objects imports into
// workflow.GetView()["env"] and exports back onto emitted tabs (Node-RED
// parity: tab env is exported).
func TestNodeREDEnvRoundTrip(t *testing.T) {
	flowJSON := `[
		{"id":"tab1","type":"tab","label":"Main",
		 "env":[{"name":"SHARED","type":"str","value":"one"}]},
		{"id":"inj","type":"inject","z":"tab1","payload":"x","wires":[["dbg"]]},
		{"id":"dbg","type":"debug","z":"tab1","wires":[[]]}
	]`

	w, err := nodestore.ImportNodeRED([]byte(flowJSON))
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}
	env, _ := w.GetView()["env"].([]interface{})
	if len(env) != 1 {
		t.Fatalf("expected 1 env entry in view.env, got %v", w.GetView()["env"])
	}
	m, _ := env[0].(map[string]interface{})
	if m["name"] != "SHARED" || m["value"] != "one" {
		t.Fatalf("unexpected env entry: %v", m)
	}

	out, err := nodestore.ExportNodeRED(w)
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}
	var items []map[string]interface{}
	if err := json.Unmarshal(out, &items); err != nil {
		t.Fatalf("export produced invalid JSON: %v", err)
	}
	var tab map[string]interface{}
	for _, it := range items {
		if stringOf(it["type"]) == "tab" {
			tab = it
		}
	}
	if tab == nil {
		t.Fatal("export produced no tab object")
	}
	tabEnv, _ := tab["env"].([]interface{})
	if len(tabEnv) != 1 {
		t.Fatalf("expected env on exported tab, got %v", tab["env"])
	}
	em, _ := tabEnv[0].(map[string]interface{})
	if em["name"] != "SHARED" || em["value"] != "one" {
		t.Fatalf("exported tab env mismatch: %v", em)
	}
}
