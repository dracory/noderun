package noderunner_test

import (
	. "github.com/dracory/noderun"
	"testing"
	"time"
)

// TestSwitchSpec ports assertions from
// test/nodes/core/function/10-switch_spec.js (upstream describe
// "switch Node"). Upstream routes msg.payload; GoFlow rules evaluate a
// dotted property path inside the payload object — the ported cases
// use property "v" to stand in for msg.payload. Port routing is
// observed via per-port debug sinks.
func TestSwitchSpec(t *testing.T) {
	describe(t, "test/nodes/core/function/10-switch_spec.js", "switch Node", func(s *suite) {

		// build makes inj -> switch -> (d1, d2 debug sinks), one edge
		// per output port.
		build := func(id string, rules []map[string]interface{}, mode string) *Workflow {
			nodes := []Node{
				{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
				{ID: "sw", Type: "switch", Data: map[string]interface{}{"rules": rules, "mode": mode}},
				specDebugNode("d1"),
				specDebugNode("d2"),
			}
			edges := []Edge{
				{ID: "e-in", Source: "inj", Target: "sw"},
				{ID: "e-d1", Source: "sw", SourceKey: "output_1", Target: "d1"},
				{ID: "e-d2", Source: "sw", SourceKey: "output_2", Target: "d2"},
			}
			return &Workflow{ID: id, Nodes: nodes, Edges: edges}
		}

		send := func(t *testing.T, engine *Engine, wfID string, v interface{}) {
			specInjectInto(t, engine, wfID, "inj", &Message{
				Payload:   map[string]interface{}{"v": v},
				Timestamp: time.Now(),
			})
		}

		s.it("should check if payload equals given value", func(t *testing.T) {
			wf := build("spec-sw-eq", []map[string]interface{}{
				{"property": "v", "op": "eq", "value": "a"},
				{"property": "v", "op": "eq", "value": "other"},
			}, "all")
			engine := specDeployFlow(t, wf)
			send(t, engine, wf.ID, "a")
			specAwaitEvent(t, engine, wf.ID, func(ev NodeEvent) bool {
				return ev.NodeID == "d1" && ev.Level == "debug"
			}, 3*time.Second)
		})

		s.it("should check if payload NOT equals given value", func(t *testing.T) {
			wf := build("spec-sw-neq", []map[string]interface{}{
				{"property": "v", "op": "neq", "value": "a"},
				{"property": "v", "op": "eq", "value": "never"},
			}, "all")
			engine := specDeployFlow(t, wf)
			send(t, engine, wf.ID, "b")
			specAwaitEvent(t, engine, wf.ID, func(ev NodeEvent) bool {
				return ev.NodeID == "d1" && ev.Level == "debug"
			}, 3*time.Second)
		})

		s.it("should check if payload is less than given value", func(t *testing.T) {
			wf := build("spec-sw-lt", []map[string]interface{}{
				{"property": "v", "op": "lt", "value": "5"},
				{"property": "v", "op": "eq", "value": "never"},
			}, "all")
			engine := specDeployFlow(t, wf)
			send(t, engine, wf.ID, 4)
			specAwaitEvent(t, engine, wf.ID, func(ev NodeEvent) bool {
				return ev.NodeID == "d1" && ev.Level == "debug"
			}, 3*time.Second)
		})

		s.it("should match regex", func(t *testing.T) {
			wf := build("spec-sw-regex", []map[string]interface{}{
				{"property": "v", "op": "regex", "value": "^abc"},
				{"property": "v", "op": "eq", "value": "never"},
			}, "all")
			engine := specDeployFlow(t, wf)
			send(t, engine, wf.ID, "abcdef")
			specAwaitEvent(t, engine, wf.ID, func(ev NodeEvent) bool {
				return ev.NodeID == "d1" && ev.Level == "debug"
			}, 3*time.Second)
		})

		// upstream title has a trailing space — verbatim.
		s.it("should check if payload if of type string ", func(t *testing.T) {
			wf := build("spec-sw-istype", []map[string]interface{}{
				{"property": "v", "op": "istype", "value": "string"},
				{"property": "v", "op": "eq", "value": "never"},
			}, "all")
			engine := specDeployFlow(t, wf)
			send(t, engine, wf.ID, "hello")
			specAwaitEvent(t, engine, wf.ID, func(ev NodeEvent) bool {
				return ev.NodeID == "d1" && ev.Level == "debug"
			}, 3*time.Second)
		})

		s.it("handles more than one switch statement", func(t *testing.T) {
			wf := build("spec-sw-all", []map[string]interface{}{
				{"property": "v", "op": "eq", "value": "a"},
				{"property": "v", "op": "neq", "value": "b"},
			}, "all")
			engine := specDeployFlow(t, wf)
			send(t, engine, wf.ID, "a")
			specAwaitEvent(t, engine, wf.ID, func(ev NodeEvent) bool {
				return ev.NodeID == "d1" && ev.Level == "debug"
			}, 3*time.Second)
			specAwaitEvent(t, engine, wf.ID, func(ev NodeEvent) bool {
				return ev.NodeID == "d2" && ev.Level == "debug"
			}, 3*time.Second)
		})

		s.it("stops after first statement", func(t *testing.T) {
			wf := build("spec-sw-first", []map[string]interface{}{
				{"property": "v", "op": "eq", "value": "a"},
				{"property": "v", "op": "neq", "value": "b"},
			}, "first")
			engine := specDeployFlow(t, wf)
			send(t, engine, wf.ID, "a")
			specAwaitEvent(t, engine, wf.ID, func(ev NodeEvent) bool {
				return ev.NodeID == "d1" && ev.Level == "debug"
			}, 3*time.Second)
			time.Sleep(300 * time.Millisecond)
			_, _, events, _ := engine.GetWorkflowSnapshot(wf.ID, 0)
			for _, ev := range events {
				if ev.NodeID == "d2" {
					t.Fatalf("second rule port received a message in first mode")
				}
			}
		})

		s.it("should handle JSONata expression", func(t *testing.T) {
			skipf(t, "JSONata expressions not supported")
		})

		s.it("should handle env var expression", func(t *testing.T) {
			skipf(t, "env var rule operands not implemented")
		})
	})
}
