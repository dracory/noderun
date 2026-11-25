package noderunner_test

import (
	. "github.com/dracory/noderun"
	"strings"
	"testing"
	"time"
)

// TestInjectSpec ports assertions from
// test/nodes/core/common/20-inject_spec.js (upstream describe "inject
// node" / "post"). Node-RED drives the inject via its admin HTTP API;
// the engine's equivalents are FireInjectNode (the inject "button") and
// InjectMessage (posted message). A debug node observing the event log
// plays the role of upstream's helper-node capture.
func TestInjectSpec(t *testing.T) {
	describe(t, "test/nodes/core/common/20-inject_spec.js", "inject node", func(s *suite) {

		// upstream: POSTs an arbitrary message to the inject endpoint;
		// our equivalent is InjectMessage at the inject node with the
		// posted payload resolved by a terminal httpResponse.
		s.it("should inject message", func(t *testing.T) {
			wf := &Workflow{
				ID: "spec-inject-message",
				Nodes: []Node{
					{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
					specResponseNode("resp"),
				},
				Edges: []Edge{
					{ID: "e1", Source: "inj", Target: "resp"},
				},
			}
			engine := specDeployFlow(t, wf)
			ch := specInjectInto(t, engine, wf.ID, "inj", &Message{
				Payload:   map[string]interface{}{"a": 1},
				Timestamp: time.Now(),
			})
			out := specAwaitResponse(t, ch, 3*time.Second)
			if m, ok := out.GetPayload().(map[string]interface{}); !ok || m["a"] != float64(1) {
				t.Fatalf("unexpected payload: %#v", out.GetPayload())
			}
		})

		// upstream: a message posted with extra properties arrives with
		// those properties intact; here via InjectMessage Metadata.
		s.it("should inject custom properties in posted message", func(t *testing.T) {
			wf := &Workflow{
				ID: "spec-inject-props",
				Nodes: []Node{
					{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
					specResponseNode("resp"),
				},
				Edges: []Edge{
					{ID: "e1", Source: "inj", Target: "resp"},
				},
			}
			engine := specDeployFlow(t, wf)
			ch := specInjectInto(t, engine, wf.ID, "inj", &Message{
				Payload:   "hi",
				Metadata:  map[string]interface{}{"custom": "kept"},
				Timestamp: time.Now(),
			})
			out := specAwaitResponse(t, ch, 3*time.Second)
			if out.GetMetadata()["custom"] != "kept" {
				t.Fatalf("custom property lost: %#v", out.GetMetadata())
			}
		})

		// upstream: once=true fires on deploy; verified via the debug
		// event carrying the configured string payload.
		s.it("should inject once with default delay property", func(t *testing.T) {
			wf := &Workflow{
				ID: "spec-inject-once",
				Nodes: []Node{
					{ID: "inj", Type: "inject", Data: map[string]interface{}{
						"payload": "once payload", "payloadType": "str", "once": true,
					}},
					specDebugNode("dbg"),
				},
				Edges: []Edge{
					{ID: "e1", Source: "inj", Target: "dbg"},
				},
			}
			engine := specDeployFlow(t, wf)
			specAwaitEvent(t, engine, wf.ID, func(ev NodeEvent) bool {
				return ev.NodeID == "dbg" && strings.Contains(ev.Message, "once payload")
			}, 3*time.Second)
		})

		// upstream: repeat interval re-fires; assert at least two debug
		// events arrive from a 100ms ticker.
		s.it("should inject repeatedly", func(t *testing.T) {
			wf := &Workflow{
				ID: "spec-inject-repeat",
				Nodes: []Node{
					{ID: "inj", Type: "inject", Data: map[string]interface{}{
						"payload": "tick", "payloadType": "str",
						"interval": 0.1, "intervalUnits": "s",
					}},
					specDebugNode("dbg"),
				},
				Edges: []Edge{
					{ID: "e1", Source: "inj", Target: "dbg"},
				},
			}
			engine := specDeployFlow(t, wf)
			seen := 0
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				_, _, events, _ := engine.GetWorkflowSnapshot(wf.ID, 0)
				seen = 0
				for _, ev := range events {
					if ev.NodeID == "dbg" && strings.Contains(ev.Message, "tick") {
						seen++
					}
				}
				if seen >= 2 {
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
			t.Fatalf("expected repeat fires, saw %d", seen)
		})

		// upstream title includes the trailing space — kept verbatim.
		s.it("inject value of environment variable ", func(t *testing.T) {
			t.Setenv("NODERUN_SPEC_ENV", "seekrit")
			wf := &Workflow{
				ID: "spec-inject-env",
				Nodes: []Node{
					{ID: "inj", Type: "inject", Data: map[string]interface{}{
						"payload": "NODERUN_SPEC_ENV", "payloadType": "env", "once": true,
					}},
					specDebugNode("dbg"),
				},
				Edges: []Edge{
					{ID: "e1", Source: "inj", Target: "dbg"},
				},
			}
			engine := specDeployFlow(t, wf)
			specAwaitEvent(t, engine, wf.ID, func(ev NodeEvent) bool {
				return ev.NodeID == "dbg" && strings.Contains(ev.Message, "seekrit")
			}, 3*time.Second)
		})

		s.it("should inject with cron", func(t *testing.T) {
			skipf(t, "cron schedules not implemented (repeat is interval-only)")
		})

		s.it("should report invalid JSONata expression", func(t *testing.T) {
			skipf(t, "JSONata expressions not supported")
		})

		// upstream title has a trailing space — verbatim.
		s.it("inject name of node as environment variable by substitution ", func(t *testing.T) {
			skipf(t, "$(VAR) env substitution in config not implemented")
		})
	})
}
