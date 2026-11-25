package noderunner

import (
	"context"
	"encoding/json"
	nodestore "github.com/dracory/nodestore"
	"testing"
	"time"
)

// Module id migration (task module_id_migration): nodes
// store data.module = module id; view.modules carries the entities
// {id,label,role,enabled,env}; legacy documents carry names. Every
// consumer resolves either form through view.modules.

// TestModuleLabel — id resolves to the entity label; names and
// unknown references pass through unchanged.
func TestModuleLabel(t *testing.T) {
	w := &Workflow{
		View: map[string]interface{}{
			"modules": []interface{}{
				map[string]interface{}{"id": "mod-1", "label": "Router"},
			},
		},
	}
	if got := moduleLabel(w, "mod-1"); got != "Router" {
		t.Errorf("moduleLabel(id) = %q, want %q", got, "Router")
	}
	if got := moduleLabel(w, "Home"); got != "Home" {
		t.Errorf("moduleLabel(legacy name) = %q, want %q", got, "Home")
	}
	if got := moduleLabel(nil, "x"); got != "x" {
		t.Errorf("moduleLabel(nil workflow) = %q, want %q", got, "x")
	}
}

// TestModuleRefMatches — references match across forms: id==id,
// name==name, and id==name when the entity list maps them.
func TestModuleRefMatches(t *testing.T) {
	w := &Workflow{
		View: map[string]interface{}{
			"modules": []interface{}{
				map[string]interface{}{"id": "mod-1", "label": "Router"},
			},
		},
	}
	if !moduleRefMatches(w, "mod-1", "Router") {
		t.Error("id should match its entity label")
	}
	if !moduleRefMatches(w, "Router", "mod-1") {
		t.Error("label should match its entity id")
	}
	if moduleRefMatches(w, "mod-1", "Other") {
		t.Error("different modules should not match")
	}
	if moduleRefMatches(w, "", "Router") {
		t.Error("empty ref should not match")
	}
}

// TestDisabledModuleByID — a node whose data.module is a module id is
// inert when view.disabledModules lists that id, and also when a
// legacy name-valued disabledModules entry names the same module.
func TestDisabledModuleByID(t *testing.T) {
	mk := func(disabled []interface{}) *Workflow {
		return &Workflow{
			ID:   "test-disabled-mod-id",
			Name: "Disabled Module ID Flow",
			Nodes: []Node{
				{
					ID:   "inj",
					Type: "inject",
					Data: map[string]interface{}{
						"payload":     "orig",
						"payloadType": "str",
						"once":        true,
						"module":      "mod-1",
					},
				},
				{ID: "dbg", Type: "debug", Data: map[string]interface{}{"module": "mod-1"}},
			},
			Edges: []Edge{
				{ID: "e1", Source: "inj", Target: "dbg"},
			},
			View: map[string]interface{}{
				"modules": []interface{}{
					map[string]interface{}{"id": "mod-1", "label": "Tab2"},
				},
				"disabledModules": disabled,
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
			t.Fatalf("disabledModules=%v: inject on a disabled module registered %d stoppers", disabled, stoppers)
		}
		time.Sleep(200 * time.Millisecond)
		_, nodes, _, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		if nodes["dbg"] == "ok" {
			t.Fatalf("disabledModules=%v: node on a disabled module executed", disabled)
		}
		_ = engine.StopWorkflow(workflow.ID)
	}
}

// TestDisabledModuleEnabledFlag — a module entity flagged
// enabled:false disables every node on it even without a
// disabledModules entry.
func TestDisabledModuleEnabledFlag(t *testing.T) {
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
					"module":      "mod-1",
				},
			},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{"module": "mod-1"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "dbg"},
		},
		View: map[string]interface{}{
			"modules": []interface{}{
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
		t.Fatalf("inject on an enabled:false module registered %d stoppers", stoppers)
	}
}

// TestExportNodeREDModuleID — id-keyed documents export with the
// module label on the emitted tab and disabled tab flag resolved
// through view.modules.
func TestExportNodeREDModuleID(t *testing.T) {
	w := &Workflow{
		ID:   "wf-mod-id",
		Name: "Module ID Export",
		Nodes: []Node{
			{ID: "n1", Type: "inject", Data: map[string]interface{}{"module": "mod-1"}},
		},
		View: map[string]interface{}{
			"modules": []interface{}{
				map[string]interface{}{"id": "mod-1", "label": "Pages"},
			},
			"disabledModules": []interface{}{"mod-1"},
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
		t.Errorf("tab label = %v, want %q (resolved from module id)", tab["label"], "Pages")
	}
	if tab["disabled"] != true {
		t.Error("id-listed disabledModules should export disabled:true on the tab")
	}
}
