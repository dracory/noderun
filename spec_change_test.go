package noderunner_test

import (
	. "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
	"testing"
	"time"
)

// TestChangeSpec ports assertions from
// test/nodes/core/function/15-change_spec.js (upstream describe
// "change Node" and its nested #set/#change/#delete/#move describes).
// The join key is the it() title, so the ported suite keeps one
// describe block while recording titles verbatim from every nested
// upstream describe.
func TestChangeSpec(t *testing.T) {
	describe(t, "test/nodes/core/function/15-change_spec.js", "change Node", func(s *suite) {

		// changeFlow runs inj -> change(rules) -> httpResponse.
		changeFlow := func(rules []map[string]interface{}) *Workflow {
			return &Workflow{
				ID: "spec-change",
				Nodes: []Node{
					{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
					{ID: "chg", Type: "change", Data: map[string]interface{}{"rules": rules}},
					specResponseNode("resp"),
				},
				Edges: []Edge{
					{ID: "e1", Source: "inj", Target: "chg"},
					{ID: "e2", Source: "chg", Target: "resp"},
				},
			}
		}

		run := func(t *testing.T, rules []map[string]interface{}, in interface{}) nodestore.MessageInterface {
			wf := changeFlow(rules)
			wf.ID = "spec-change-" + sanitizeName(t.Name())[len("TestChangeSpec_"):]
			engine := specDeployFlow(t, wf)
			ch := specInjectInto(t, engine, wf.ID, "inj", &Message{
				Payload:   in,
				Timestamp: time.Now(),
			})
			return specAwaitResponse(t, ch, 3*time.Second)
		}

		s.it("sets the value of the message property", func(t *testing.T) {
			out := run(t, []map[string]interface{}{
				{"op": "set", "field": "payload", "value": "newval"},
			}, "orig")
			if out.GetPayload() != "newval" {
				t.Fatalf("expected 'newval', got %#v", out.GetPayload())
			}
		})

		s.it("changes the value to a number", func(t *testing.T) {
			out := run(t, []map[string]interface{}{
				{"op": "set", "field": "payload", "value": "42"},
			}, "orig")
			if out.GetPayload() != float64(42) {
				t.Fatalf("expected numeric 42, got %#v", out.GetPayload())
			}
		})

		s.it("deletes the value of the message property", func(t *testing.T) {
			out := run(t, []map[string]interface{}{
				{"op": "delete", "field": "gone"},
			}, map[string]interface{}{"gone": 1, "kept": 2})
			m, ok := out.GetPayload().(map[string]interface{})
			if !ok {
				t.Fatalf("expected map payload, got %#v", out.GetPayload())
			}
			if _, found := m["gone"]; found {
				t.Fatalf("property not deleted: %#v", m)
			}
			if m["kept"] != float64(2) {
				t.Fatalf("sibling property lost: %#v", m)
			}
		})

		s.it("moves the value of the message property", func(t *testing.T) {
			out := run(t, []map[string]interface{}{
				{"op": "move", "field": "src", "value": "dst"},
			}, map[string]interface{}{"src": "moved"})
			m, ok := out.GetPayload().(map[string]interface{})
			if !ok {
				t.Fatalf("expected map payload, got %#v", out.GetPayload())
			}
			if m["dst"] != "moved" {
				t.Fatalf("expected dst='moved', got %#v", m)
			}
			if _, found := m["src"]; found {
				t.Fatalf("source property still present: %#v", m)
			}
		})

		s.it("applies multiple rules in order", func(t *testing.T) {
			out := run(t, []map[string]interface{}{
				{"op": "set", "field": "payload", "value": "first"},
				{"op": "set", "field": "payload", "value": "second"},
			}, "orig")
			if out.GetPayload() != "second" {
				t.Fatalf("rules not applied in order: %#v", out.GetPayload())
			}
		})

		// upstream #change describe: search/replace — our change node
		// has no change op.
		s.it("changes the value of the message property", func(t *testing.T) {
			skipf(t, "change (find/replace) op not implemented")
		})

		// upstream env var describe.
		s.it("sets the value using env property", func(t *testing.T) {
			skipf(t, "env value source not implemented in change rules")
		})

		s.it("changes the value using jsonata", func(t *testing.T) {
			skipf(t, "JSONata expressions not supported")
		})
	})
}
