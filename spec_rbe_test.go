package noderunner_test

import (
	"context"
	. "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
	"testing"
	"time"
)

// TestRBESpec ports assertions from test/nodes/core/function/rbe_spec.js
// (upstream describe "rbe node"). Sequence assertions drive the
// RBENode executor directly — deterministic and equivalent to
// upstream's synchronous n1.emit("input", ...) calls. Upstream msg
// properties map onto Message fields: msg.payload → Data,
// msg.topic / msg.reset / msg-level properties → Metadata entries.
// Upstream septopics defaults to true, so SepTopics is set explicitly
// where the upstream config relies on the default.
func TestRBESpec(t *testing.T) {
	describe(t, "test/nodes/core/function/rbe_spec.js", "rbe node", func(s *suite) {

		newNode := func(cfg *RBEConfig) *RBENode {
			return NewRBENode("n1", cfg, NewEngine(), "wf")
		}

		// msg builds an input payload; extra pairs set Metadata keys
		// (msg.topic, msg.reset, watched non-payload properties).
		msg := func(payload interface{}, kv ...interface{}) *Message {
			p := &Message{Payload: payload, Timestamp: time.Now()}
			for i := 0; i+1 < len(kv); i += 2 {
				if p.Metadata == nil {
					p.Metadata = map[string]interface{}{}
				}
				p.Metadata[kv[i].(string)] = kv[i+1]
			}
			return p
		}

		// runSeq feeds inputs through the node and returns the payloads
		// it passed (nil/error drops are skipped).
		runSeq := func(t *testing.T, node *RBENode, inputs []nodestore.MessageInterface) []nodestore.MessageInterface {
			t.Helper()
			var out []nodestore.MessageInterface
			for _, in := range inputs {
				o, err := node.Execute(context.Background(), in)
				if err != nil {
					t.Fatalf("execute failed: %v", err)
				}
				if o != nil {
					out = append(out, o)
				}
			}
			return out
		}

		// wantPayloads asserts the output sequence's Data values.
		wantPayloads := func(t *testing.T, out []nodestore.MessageInterface, want ...interface{}) {
			t.Helper()
			if len(out) != len(want) {
				t.Fatalf("expected %d outputs, got %d (%v)", len(want), len(out), out)
			}
			for i, w := range want {
				if !equalValue(out[i].GetPayload(), w) {
					t.Fatalf("output %d: expected %#v, got %#v", i, w, out[i].GetPayload())
				}
			}
		}

		s.it("should be loaded with correct defaults", func(t *testing.T) {
			wf := &Workflow{
				ID: "spec-rbe-defaults",
				Nodes: []Node{
					{ID: "n1", Type: "rbe", Data: map[string]interface{}{"name": "rbe1"}},
					specDebugNode("n2"),
				},
				Edges: []Edge{{ID: "e1", Source: "n1", Target: "n2"}},
			}
			engine := specDeployFlow(t, wf)
			n, ok := engine.LookupNode(wf.ID, "n1")
			if !ok || n.GetType() != "rbe" {
				t.Fatalf("rbe node not loaded: %v", n)
			}
			if name, _ := n.GetData()["name"].(string); name != "rbe1" {
				t.Fatalf("expected name rbe1, got %q", name)
			}
			// Upstream defaults: func="rbe", gap="0" — both are the
			// zero-value fallbacks in RBEConfig.
		})

		s.it("should only send output if payload changes - with multiple topics (rbe)", func(t *testing.T) {
			// septopics defaults to true upstream → separate prev per topic
			n := newNode(&RBEConfig{Func: "rbe", SepTopics: true})
			out := runSeq(t, n, []nodestore.MessageInterface{
				msg("a"), msg("a"), msg("a"),
				msg(float64(2)), msg(float64(2)),
				msg(map[string]interface{}{"b": 1, "c": 2}),
				msg(map[string]interface{}{"c": 2, "b": 1}),
				msg(map[string]interface{}{"c": 2, "b": 1}),
				msg(true), msg(false), msg(false), msg(true),
				msg(float64(1), "topic", "a"),
				msg(float64(1), "topic", "b"),
				msg(float64(1), "topic", "b"),
				msg(float64(1), "topic", "a"),
				msg(float64(1), "topic", "c"),
			})
			wantPayloads(t, out,
				"a", float64(2),
				map[string]interface{}{"b": 1, "c": 2},
				true, false, true,
				float64(1), float64(1), float64(1))
			// The last three carry topics a, b, c respectively
			for i, topic := range []string{"a", "b", "c"} {
				if got, _ := out[6+i].GetMetadata()["topic"].(string); got != topic {
					t.Fatalf("output %d: expected topic %q, got %q", 6+i, topic, got)
				}
			}
		})

		s.it("should only send output if a Date payload changes (rbe)", func(t *testing.T) {
			n := newNode(&RBEConfig{Func: "rbe"})
			d1 := time.UnixMilli(1000)
			d2 := time.UnixMilli(2000)
			out := runSeq(t, n, []nodestore.MessageInterface{
				msg(d1), msg(d1), msg(d2),
			})
			wantPayloads(t, out, d1, d2)
		})

		s.it("should ignore multiple topics if told to (rbe)", func(t *testing.T) {
			// septopics=false → one shared bucket regardless of topic
			n := newNode(&RBEConfig{Func: "rbe", SepTopics: false})
			obj := map[string]interface{}{"b": 1, "c": 2}
			out := runSeq(t, n, []nodestore.MessageInterface{
				msg("a", "topic", "a"),
				msg("a", "topic", "b"),
				msg("a", "topic", "c"),
				msg(float64(2), "topic", "a"),
				msg(float64(2), "topic", "b"),
				msg(map[string]interface{}{"b": 1, "c": 2}),
				msg(map[string]interface{}{"c": 2, "b": 1}),
				msg(map[string]interface{}{"c": 2, "b": 1}),
				msg(true, "topic", "a"),
				msg(false, "topic", "b"),
				msg(false, "topic", "c"),
				msg(true, "topic", "d"),
				msg(float64(1), "topic", "a"),
				msg(float64(1), "topic", "b"),
				msg(float64(1), "topic", "c"),
				msg(float64(1), "topic", "d"),
				msg(float64(2), "topic", "a"),
			})
			wantPayloads(t, out, "a", float64(2), obj, true, false, true, float64(1), float64(2))
		})

		s.it("should only send output if another chosen property changes - foo (rbe)", func(t *testing.T) {
			// property "foo": messages lacking it are dropped without
			// touching prev (upstream value===undefined gate). The
			// GoFlow message model puts msg-level properties in the
			// Data map, so {foo:...} arrives as Data{"foo":...}.
			n := newNode(&RBEConfig{Func: "rbe", Property: "foo"})
			foo := func(v interface{}) *Message {
				return msg(map[string]interface{}{"foo": v})
			}
			out := runSeq(t, n, []nodestore.MessageInterface{
				foo("a"),
				msg("a"),
				foo("a"),
				msg("a"),
				foo("a"),
				foo("b"),
				foo(map[string]interface{}{"b": 1, "c": 2}),
				foo(map[string]interface{}{"c": 2, "b": 1}),
				msg(map[string]interface{}{"c": 2, "b": 1}),
			})
			if len(out) != 3 {
				t.Fatalf("expected 3 outputs, got %d", len(out))
			}
			if m, _ := out[0].GetPayload().(map[string]interface{}); m["foo"] != "a" {
				t.Fatalf("expected foo=a, got %v", m)
			}
			if m, _ := out[1].GetPayload().(map[string]interface{}); m["foo"] != "b" {
				t.Fatalf("expected foo=b, got %v", m)
			}
			m, _ := out[2].GetPayload().(map[string]interface{})
			inner, _ := m["foo"].(map[string]interface{})
			if inner["b"] != 1 {
				t.Fatalf("expected foo object, got %v", m)
			}
		})

		s.it("should only send output if payload changes - ignoring first value (rbei)", func(t *testing.T) {
			n := newNode(&RBEConfig{Func: "rbei", SepTopics: true})
			out := runSeq(t, n, []nodestore.MessageInterface{
				msg("a", "topic", "a"),
				msg("a", "topic", "b"),
				msg("a", "topic", "a"),
				msg("b", "topic", "a"),
				msg("b", "topic", "b"),
				msg("c", "topic", "a"),
				msg("c", "topic", "b"),
			})
			// rbei blocks the first message per topic: a/a, a/b blocked;
			// then b/a, b/b, c/a, c/b pass
			if len(out) != 4 {
				t.Fatalf("expected 4 outputs, got %d", len(out))
			}
			want := []struct {
				payload string
				topic   string
			}{{"b", "a"}, {"b", "b"}, {"c", "a"}, {"c", "b"}}
			for i, w := range want {
				if out[i].GetPayload() != w.payload {
					t.Fatalf("output %d: expected payload %q, got %#v", i, w.payload, out[i].GetPayload())
				}
				if got, _ := out[i].GetMetadata()["topic"].(string); got != w.topic {
					t.Fatalf("output %d: expected topic %q, got %q", i, w.topic, got)
				}
			}
		})

		s.it("should send output if queue is reset (rbe)", func(t *testing.T) {
			n := newNode(&RBEConfig{Func: "rbe", SepTopics: true})
			out := runSeq(t, n, []nodestore.MessageInterface{
				msg("a", "topic", "a"),
				msg("a", "topic", "a"),
				msg("b", "topic", "b"),
				msg(nil, "reset", true), // reset all
				msg("a", "topic", "a"),
				msg("b", "topic", "b"),
				msg("b", "topic", "b"),
				msg(nil, "topic", "b", "reset", ""), // reset b
				msg("b", "topic", "b"),
				msg("a", "topic", "a"),
				msg(nil, "reset", ""), // reset all
				msg("b", "topic", "b"),
				msg("a", "topic", "a"),
				msg(nil, "topic", "c"), // don't reset a non topic
				msg("b", "topic", "b"),
				msg("a", "topic", "a"),
				msg("c", "topic", "c"),
			})
			// sends: a,a? — no: a(first), b(first), a(post-reset), b(post-reset),
			// b(post topic-reset), b(post all-reset), a(post all-reset), c(first)
			wantPayloads(t, out, "a", "b", "a", "b", "b", "b", "a", "c")
		})

		s.it("should only send output if x away from original value (deadbandEq)", func(t *testing.T) {
			n := newNode(&RBEConfig{Func: "deadbandEq", Gap: "10"})
			out := runSeq(t, n, []nodestore.MessageInterface{
				msg(float64(0)), msg(float64(2)), msg(float64(4)), msg(float64(6)),
				msg(float64(8)), msg(float64(10)), msg(float64(15)), msg(float64(20)),
			})
			wantPayloads(t, out, float64(0), float64(10), float64(20))
		})

		s.it("should only send output if more than x away from original value (deadband)", func(t *testing.T) {
			skipf(t, "upstream parseFloat parses \"6 deg\"→6; GoFlow reports non-numeric")
		})

		s.it("should accept a numeric gap (deadband)", func(t *testing.T) {
			n := newNode(&RBEConfig{Func: "deadband", Gap: "10"})
			out := runSeq(t, n, []nodestore.MessageInterface{
				msg(float64(0)), msg(float64(5)), msg(float64(10)), msg(float64(20)),
			})
			wantPayloads(t, out, float64(0), float64(20))
		})

		s.it("should only send output if more than x% away from original value (deadband)", func(t *testing.T) {
			n := newNode(&RBEConfig{Func: "deadband", Gap: "10%"})
			out := runSeq(t, n, []nodestore.MessageInterface{
				msg(float64(100)), msg(float64(95)), msg(float64(105)),
				msg(float64(111)), msg(float64(120)), msg(float64(135)),
			})
			wantPayloads(t, out, float64(100), float64(111), float64(135))
		})

		s.it("should warn if no number found in deadband mode", func(t *testing.T) {
			// GoFlow surfaces the warning as a node error (catchable)
			// instead of a node.warn log line; the message is dropped
			// either way — including the very first one.
			n := newNode(&RBEConfig{Func: "deadband", Gap: "10"})
			out, err := n.Execute(context.Background(), msg("banana"))
			if err == nil {
				t.Fatalf("expected non-numeric error, got out=%v", out)
			}
			if out != nil {
				t.Fatalf("expected no output, got %v", out.GetPayload())
			}
		})

		s.it("should not send output if x away or greater from original value (narrowbandEq)", func(t *testing.T) {
			n := newNode(&RBEConfig{Func: "narrowbandEq", Gap: "10", Start: "1"})
			out := runSeq(t, n, []nodestore.MessageInterface{
				msg(float64(100)), msg(float64(0)), msg(float64(10)), msg(float64(5)),
				msg(float64(15)), msg(float64(10)), msg(float64(20)), msg(float64(25)),
			})
			wantPayloads(t, out, float64(0), float64(5), float64(10))
		})

		s.it("should not send output if more than x away from original value (narrowband)", func(t *testing.T) {
			skipf(t, "upstream parseFloat parses \"6 deg\"→6; GoFlow reports non-numeric")
		})

		s.it("should send output if gap is 0 and input doesnt change (narrowband)", func(t *testing.T) {
			n := newNode(&RBEConfig{Func: "narrowband", Gap: "0"})
			out := runSeq(t, n, []nodestore.MessageInterface{
				msg(float64(1)), msg(float64(1)), msg(float64(1)), msg(float64(1)),
				msg(float64(0)), msg(float64(1)),
			})
			wantPayloads(t, out, float64(1), float64(1), float64(1), float64(1), float64(1))
		})

		s.it("should not send output if more than x away from original value (narrowband in step mode)", func(t *testing.T) {
			skipf(t, "inout=\"in\" step mode (compare vs last received value) not implemented")
		})
	})
}
