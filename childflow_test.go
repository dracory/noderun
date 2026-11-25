package noderunner

import (
	"context"
	"encoding/json"
	nodestore "github.com/dracory/nodestore"
	"testing"
	"time"
)

// Child flow id migration (task childflow_id_migration): nodes
// store data.childFlow = child flow id; view.childFlows carries the entities
// {id,label,role,enabled,env}; legacy documents carry names. Every
// consumer resolves either form through view.childFlows.

// TestChildFlowLabel — id resolves to the entity label; names and
// unknown references pass through unchanged.
func TestChildFlowLabel(t *testing.T) {
	w := &Workflow{
		View: map[string]interface{}{
			"childFlows": []interface{}{
				map[string]interface{}{"id": "mod-1", "label": "Router"},
			},
		},
	}
	if got := flowLabel(w, "mod-1"); got != "Router" {
		t.Errorf("flowLabel(id) = %q, want %q", got, "Router")
	}
	if got := flowLabel(w, "Home"); got != "Home" {
		t.Errorf("flowLabel(legacy name) = %q, want %q", got, "Home")
	}
	if got := flowLabel(nil, "x"); got != "x" {
		t.Errorf("flowLabel(nil workflow) = %q, want %q", got, "x")
	}
}

// TestChildFlowRefMatches — references match across forms: id==id,
// name==name, and id==name when the entity list maps them.
func TestChildFlowRefMatches(t *testing.T) {
	w := &Workflow{
		View: map[string]interface{}{
			"childFlows": []interface{}{
				map[string]interface{}{"id": "mod-1", "label": "Router"},
			},
		},
	}
	if !flowRefMatches(w, "mod-1", "Router") {
		t.Error("id should match its entity label")
	}
	if !flowRefMatches(w, "Router", "mod-1") {
		t.Error("label should match its entity id")
	}
	if flowRefMatches(w, "mod-1", "Other") {
		t.Error("different child flows should not match")
	}
	if flowRefMatches(w, "", "Router") {
		t.Error("empty ref should not match")
	}
}

// TestDisabledChildFlowByID — a node whose data.childFlow is a child flow id is
// inert when view.disabledFlows lists that id, and also when a
// legacy name-valued disabledFlows entry names the same child flow.
func TestDisabledChildFlowByID(t *testing.T) {
	mk := func(disabled []interface{}) *Workflow {
		return &Workflow{
			ID:   "test-disabled-mod-id",
			Name: "Disabled Child Flow ID Flow",
			Nodes: []Node{
				{
					ID:   "inj",
					Type: "inject",
					Data: map[string]interface{}{
						"payload":     "orig",
						"payloadType": "str",
						"once":        true,
						"childFlow":   "mod-1",
					},
				},
				{ID: "dbg", Type: "debug", Data: map[string]interface{}{"childFlow": "mod-1"}},
			},
			Edges: []Edge{
				{ID: "e1", Source: "inj", Target: "dbg"},
			},
			View: map[string]interface{}{
				"childFlows": []interface{}{
					map[string]interface{}{"id": "mod-1", "label": "Tab2"},
				},
				"disabledFlows": disabled,
			},
		}
	}

	for _, disabled := range [][]interface{}{{"mod-1"}, {"Tab2"}} {
		workflow := mk(disabled)
		engine := NewEngine()
		if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
			t.Fatalf("deploy failed: %v", err)
		}
		engine.runMu.Lock()
		stoppers := len(engine.stoppers[workflow.ID])
		engine.runMu.Unlock()
		if stoppers != 0 {
			t.Fatalf("disabledFlows=%v: inject on a disabled child flow registered %d stoppers", disabled, stoppers)
		}
		time.Sleep(200 * time.Millisecond)
		_, nodes, _, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		if nodes["dbg"] == "ok" {
			t.Fatalf("disabledFlows=%v: node on a disabled child flow executed", disabled)
		}
		_ = engine.StopWorkflow(workflow.ID)
	}
}

// TestDisabledChildFlowEnabledFlag — a child flow entity flagged
// enabled:false disables every node on it even without a
// disabledFlows entry.
func TestDisabledChildFlowEnabledFlag(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-disabled-mod-enabled",
		Name: "Disabled Enabled Flag Flow",
		Nodes: []Node{
			{
				ID:   "inj",
				Type: "inject",
				Data: map[string]interface{}{
					"payload":     "orig",
					"payloadType": "str",
					"once":        true,
					"childFlow":   "mod-1",
				},
			},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{"childFlow": "mod-1"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "dbg"},
		},
		View: map[string]interface{}{
			"childFlows": []interface{}{
				map[string]interface{}{"id": "mod-1", "label": "Tab2", "enabled": false},
			},
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
		t.Fatalf("inject on an enabled:false child flow registered %d stoppers", stoppers)
	}
}

// TestExportNodeREDChildFlowID — id-keyed documents export with the
// child flow label on the emitted tab and disabled tab flag resolved
// through view.childFlows.
func TestExportNodeREDChildFlowID(t *testing.T) {
	w := &Workflow{
		ID:   "wf-mod-id",
		Name: "Child Flow ID Export",
		Nodes: []Node{
			{ID: "n1", Type: "inject", Data: map[string]interface{}{"childFlow": "mod-1"}},
		},
		View: map[string]interface{}{
			"childFlows": []interface{}{
				map[string]interface{}{"id": "mod-1", "label": "Pages"},
			},
			"disabledFlows": []interface{}{"mod-1"},
		},
	}
	raw, err := nodestore.ExportNodeRED(w)
	if err != nil {
		t.Fatalf("ExportNodeRED: %v", err)
	}
	var items []map[string]interface{}
	if err := json.Unmarshal(raw, &items); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var tab map[string]interface{}
	for _, it := range items {
		if it["type"] == "tab" {
			tab = it
		}
	}
	if tab == nil {
		t.Fatal("no tab object exported")
	}
	if tab["label"] != "Pages" {
		t.Errorf("tab label = %v, want %q (resolved from child flow id)", tab["label"], "Pages")
	}
	if tab["disabled"] != true {
		t.Error("id-listed disabledFlows should export disabled:true on the tab")
	}
}
