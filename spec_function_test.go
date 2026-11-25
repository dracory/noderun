package noderunner_test

import (
	. "github.com/dracory/noderun"
	"strings"
	"testing"
	"time"
)

// TestFunctionSpec ports assertions from
// test/nodes/core/function/10-function_spec.js (upstream describe
// "function node"). Function scripts run on goja rather than node's
// vm; assertions observe downstream debug events or an httpResponse
// resolving the injected message.
func TestFunctionSpec(t *testing.T) {
	describe(t, "test/nodes/core/function/10-function_spec.js", "function node", func(s *suite) {

		s.it("should send returned message", func(t *testing.T) {
			wf := &Workflow{
				ID: "spec-fn-return",
				Nodes: []Node{
					{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
					{ID: "fn", Type: "function", Data: map[string]interface{}{
						"func": "msg.payload = 'changed'; return msg;",
					}},
					specResponseNode("resp"),
				},
				Edges: []Edge{
					{ID: "e1", Source: "inj", Target: "fn"},
					{ID: "e2", Source: "fn", Target: "resp"},
				},
			}
			engine := specDeployFlow(t, wf)
			ch := specInjectInto(t, engine, wf.ID, "inj", &Message{
				Payload:   "orig",
				Timestamp: time.Now(),
			})
			out := specAwaitResponse(t, ch, 3*time.Second)
			if out.GetPayload() != "changed" {
				t.Fatalf("expected 'changed', got %#v", out.GetPayload())
			}
		})

		// node.send builds a fresh message without the pending msgId,
		// so a debug sink observes it rather than httpResponse.
		s.it("should send returned message using send()", func(t *testing.T) {
			wf := &Workflow{
				ID: "spec-fn-send",
				Nodes: []Node{
					{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
					{ID: "fn", Type: "function", Data: map[string]interface{}{
						"func": "node.send({payload: 'via send'}); return null;",
					}},
					specDebugNode("dbg"),
				},
				Edges: []Edge{
					{ID: "e1", Source: "inj", Target: "fn"},
					{ID: "e2", Source: "fn", Target: "dbg"},
				},
			}
			engine := specDeployFlow(t, wf)
			specInjectInto(t, engine, wf.ID, "inj", &Message{
				Payload:   "orig",
				Timestamp: time.Now(),
			})
			specAwaitEvent(t, engine, wf.ID, func(ev NodeEvent) bool {
				return ev.NodeID == "dbg" && strings.Contains(ev.Message, "via send")
			}, 3*time.Second)
		})

		s.it("should send to multiple outputs", func(t *testing.T) {
			wf := &Workflow{
				ID: "spec-fn-multi",
				Nodes: []Node{
					{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
					{ID: "fn", Type: "function", Data: map[string]interface{}{
						"func":    "return [{payload:'p1'},{payload:'p2'}];",
						"outputs": 2,
					}},
					specDebugNode("d1"),
					specDebugNode("d2"),
				},
				Edges: []Edge{
					{ID: "e1", Source: "inj", Target: "fn"},
					{ID: "e2", Source: "fn", SourceKey: "output_1", Target: "d1"},
					{ID: "e3", Source: "fn", SourceKey: "output_2", Target: "d2"},
				},
			}
			engine := specDeployFlow(t, wf)
			specInjectInto(t, engine, wf.ID, "inj", &Message{
				Payload:   "go",
				Timestamp: time.Now(),
			})
			specAwaitEvent(t, engine, wf.ID, func(ev NodeEvent) bool {
				return ev.NodeID == "d1" && strings.Contains(ev.Message, "p1")
			}, 3*time.Second)
			specAwaitEvent(t, engine, wf.ID, func(ev NodeEvent) bool {
				return ev.NodeID == "d2" && strings.Contains(ev.Message, "p2")
			}, 3*time.Second)
		})

		s.it("should allow input to be discarded by returning null", func(t *testing.T) {
			wf := &Workflow{
				ID: "spec-fn-null",
				Nodes: []Node{
					{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
					{ID: "fn", Type: "function", Data: map[string]interface{}{
						"func": "return null;",
					}},
					specDebugNode("dbg"),
				},
				Edges: []Edge{
					{ID: "e1", Source: "inj", Target: "fn"},
					{ID: "e2", Source: "fn", Target: "dbg"},
				},
			}
			engine := specDeployFlow(t, wf)
			specInjectInto(t, engine, wf.ID, "inj", &Message{
				Payload:   "drop me",
				Timestamp: time.Now(),
			})
			// Assert the debug node stays silent for a grace window.
			time.Sleep(500 * time.Millisecond)
			_, _, events, _ := engine.GetWorkflowSnapshot(wf.ID, 0)
			for _, ev := range events {
				if ev.NodeID == "dbg" {
					t.Fatalf("discarded message reached debug: %s", ev.Message)
				}
			}
		})

		s.it("should handle and log script error", func(t *testing.T) {
			wf := &Workflow{
				ID: "spec-fn-error",
				Nodes: []Node{
					{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
					{ID: "fn", Type: "function", Data: map[string]interface{}{
						"func": "throw new Error('boom');",
					}},
				},
				Edges: []Edge{
					{ID: "e1", Source: "inj", Target: "fn"},
				},
			}
			engine := specDeployFlow(t, wf)
			specInjectInto(t, engine, wf.ID, "inj", &Message{
				Payload:   "go",
				Timestamp: time.Now(),
			})
			specAwaitEvent(t, engine, wf.ID, func(ev NodeEvent) bool {
				return ev.NodeID == "fn" && ev.Level == "error" && strings.Contains(ev.Message, "boom")
			}, 3*time.Second)
		})

		s.it("should allow accessing env vars", func(t *testing.T) {
			t.Setenv("NODERUN_SPEC_FN_ENV", "fn-seekrit")
			wf := &Workflow{
				ID: "spec-fn-env",
				Nodes: []Node{
					{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
					{ID: "fn", Type: "function", Data: map[string]interface{}{
						"func": "msg.payload = env.get('NODERUN_SPEC_FN_ENV'); return msg;",
					}},
					specResponseNode("resp"),
				},
				Edges: []Edge{
					{ID: "e1", Source: "inj", Target: "fn"},
					{ID: "e2", Source: "fn", Target: "resp"},
				},
			}
			engine := specDeployFlow(t, wf)
			engine.AllowOSEnv = true // the env.get OS fallback is opt-in
			ch := specInjectInto(t, engine, wf.ID, "inj", &Message{
				Payload:   "go",
				Timestamp: time.Now(),
			})
			out := specAwaitResponse(t, ch, 3*time.Second)
			if out.GetPayload() != "fn-seekrit" {
				t.Fatalf("expected env value, got %#v", out.GetPayload())
			}
		})

		s.it("should set flow context", func(t *testing.T) {
			wf := &Workflow{
				ID: "spec-fn-flowctx",
				Nodes: []Node{
					{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
					{ID: "fn", Type: "function", Data: map[string]interface{}{
						"func": "flow.set('spec_k', 42); msg.payload = flow.get('spec_k'); return msg;",
					}},
					specResponseNode("resp"),
				},
				Edges: []Edge{
					{ID: "e1", Source: "inj", Target: "fn"},
					{ID: "e2", Source: "fn", Target: "resp"},
				},
			}
			engine := specDeployFlow(t, wf)
			engine.AllowOSEnv = true // the env.get OS fallback is opt-in
			ch := specInjectInto(t, engine, wf.ID, "inj", &Message{
				Payload:   "go",
				Timestamp: time.Now(),
			})
			out := specAwaitResponse(t, ch, 3*time.Second)
			if out.GetPayload() != float64(42) {
				t.Fatalf("expected 42 from flow context, got %#v", out.GetPayload())
			}
		})

		s.it("should handle setTimeout()", func(t *testing.T) {
			wf := &Workflow{
				ID:   "spec-fn-settimeout",
				Name: "fn spec",
				Nodes: []Node{
					{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
					{ID: "fn", Type: "function", Data: map[string]interface{}{
						"func": "setTimeout(function(){node.send(msg);},50);",
					}},
					{ID: "resp", Type: "httpResponse", Data: map[string]interface{}{}},
				},
				Edges: []Edge{
					{ID: "e1", Source: "inj", Target: "fn"},
					{ID: "e2", Source: "fn", Target: "resp"},
				},
			}
			engine := specDeployFlow(t, wf)
			ch := specInjectInto(t, engine, wf.ID, "inj", &Message{
				Payload:   "go",
				Timestamp: time.Now(),
			})
			out := specAwaitResponse(t, ch, 3*time.Second)
			if out.GetPayload() != "go" {
				t.Fatalf("expected 'go' via setTimeout send, got %#v", out.GetPayload())
			}
		})

		s.it("should use the same Date object from outside the sandbox", func(t *testing.T) {
			skipf(t, "not applicable — goja runtime, not node vm sandbox sharing")
		})
	})
}
