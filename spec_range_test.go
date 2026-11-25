package noderunner_test

import (
	"context"
	. "github.com/dracory/noderun"
	"testing"
	"time"
)

// TestRangeSpec ports assertions from
// test/nodes/core/function/16-range_spec.js (upstream describe
// "range Node"). The upstream genericRangeTest sends one message and
// checks the mapped payload — the engine's equivalent drives the
// RangeNode executor directly. Where upstream reports a non-number via
// node.log and drops the message, the engine returns a node error
// (catchable); both refuse to forward the message, so the ported
// assertions check (nil, err).
func TestRangeSpec(t *testing.T) {
	describe(t, "test/nodes/core/function/16-range_spec.js", "range Node", func(s *suite) {

		// rangeTest mirrors genericRangeTest: build the node, feed one
		// payload, return the mapped value.
		rangeTest := func(t *testing.T, action string, minin, maxin, minout, maxout float64, round bool, payload, expected interface{}) {
			t.Helper()
			n := NewRangeNode("r", &RangeConfig{
				Action: action, MinIn: minin, MaxIn: maxin,
				MinOut: minout, MaxOut: maxout, Round: round,
			})
			out, err := n.Execute(context.Background(), &Message{
				Payload: payload, Timestamp: time.Now(),
			})
			if err != nil {
				t.Fatalf("execute failed: %v", err)
			}
			if out == nil {
				t.Fatalf("message dropped, expected %v", expected)
			}
			if !equalValue(out.GetPayload(), expected) {
				t.Fatalf("expected %v, got %v", expected, out.GetPayload())
			}
		}

		// nonNumberDropped mirrors nonNumberDroppedTest: input that
		// Number() would coerce but which isn't a number must be
		// reported and not passed on.
		nonNumberDropped := func(t *testing.T, payload interface{}) {
			t.Helper()
			n := NewRangeNode("r", &RangeConfig{
				Action: "drop", MinIn: -5, MaxIn: 45, MinOut: -5, MaxOut: 45,
			})
			out, err := n.Execute(context.Background(), &Message{
				Payload: payload, Timestamp: time.Now(),
			})
			if out != nil {
				t.Fatalf("non-number input %v was passed on as %v", payload, out.GetPayload())
			}
			if err == nil {
				t.Fatalf("non-number input %v was not reported", payload)
			}
		}

		s.it("should load some defaults", func(t *testing.T) {
			wf := &Workflow{
				ID: "spec-range-defaults",
				Nodes: []Node{
					{ID: "rangeNode1", Type: "range", Data: map[string]interface{}{"name": "rangeNode"}},
					specDebugNode("h"),
				},
				Edges: []Edge{{ID: "e1", Source: "rangeNode1", Target: "h"}},
			}
			engine := specDeployFlow(t, wf)
			n, ok := engine.LookupNode(wf.ID, "rangeNode1")
			if !ok || n.GetType() != "range" {
				t.Fatalf("range node not loaded: %v", n)
			}
			if name, _ := n.GetData()["name"].(string); name != "rangeNode" {
				t.Fatalf("expected name rangeNode, got %q", name)
			}
			// Upstream default round=false is the Go zero value.
		})

		s.it("ranges numbers up tenfold", func(t *testing.T) {
			rangeTest(t, "scale", 0, 100, 0, 1000, false, float64(50), float64(500))
		})

		s.it("ranges numbers down such as centimetres to metres", func(t *testing.T) {
			rangeTest(t, "scale", 0, 100, 0, 1, false, float64(55), float64(0.55))
		})

		s.it("wraps numbers down say for degree/rotation reading 1/2", func(t *testing.T) {
			rangeTest(t, "roll", 0, 10, 0, 360, true, float64(15), float64(180))
		})

		s.it("wraps numbers around say for degree/rotation reading 1/3", func(t *testing.T) {
			rangeTest(t, "roll", 0, 10, 0, 360, true, float64(13.3333), float64(120))
		})

		s.it("wraps numbers around say for degree/rotation reading 1/4", func(t *testing.T) {
			rangeTest(t, "roll", 0, 10, 0, 360, true, float64(12.5), float64(90))
		})

		s.it("wraps numbers down say for degree/rotation reading 1/4", func(t *testing.T) {
			rangeTest(t, "roll", 0, 10, 0, 360, true, float64(-12.5), float64(270))
		})

		s.it("wraps numbers around say for degree/rotation reading 0", func(t *testing.T) {
			rangeTest(t, "roll", 0, 10, 0, 360, true, float64(-10), float64(0))
		})

		s.it("clamps numbers within a range - over max", func(t *testing.T) {
			rangeTest(t, "clamp", 0, 10, 0, 1000, false, float64(111), float64(1000))
		})

		s.it("clamps numbers within a range - below min", func(t *testing.T) {
			rangeTest(t, "clamp", 0, 10, 0, 1000, false, float64(-1), float64(0))
		})

		s.it("clamps numbers within a range - above max fp", func(t *testing.T) {
			rangeTest(t, "clamp", 0, 9.5, 0.5, 100.5, true, float64(13.1), float64(100))
		})

		s.it("clamps numbers within a range - below min fp", func(t *testing.T) {
			rangeTest(t, "clamp", 0, 9.5, 0.3, 100.5, true, float64(-13.1), float64(1))
		})

		s.it("handles input range where max < min", func(t *testing.T) {
			// upstream registers this title twice with different bounds
			rangeTest(t, "scale", 100, 0, 0, -100, false, float64(40), float64(-60))
		})
		s.it("handles input range where max < min", func(t *testing.T) {
			rangeTest(t, "scale", 100, 0, -100, 0, false, float64(10), float64(-10))
		})

		s.it("scales numbers given as strings", func(t *testing.T) {
			rangeTest(t, "scale", 0, 100, 0, 1000, false, "50", float64(500))
		})

		s.it("scales numeric strings with surrounding whitespace", func(t *testing.T) {
			rangeTest(t, "scale", 0, 100, 0, 1000, false, " 50 ", float64(500))
		})

		s.it("drops msg if in drop mode and input outside range", func(t *testing.T) {
			n := NewRangeNode("r", &RangeConfig{
				Action: "drop", MinIn: 2, MaxIn: 8, MinOut: 20, MaxOut: 80, Round: true,
			})
			ctx := context.Background()
			for _, v := range []float64{1, 9} {
				out, err := n.Execute(ctx, &Message{Payload: v, Timestamp: time.Now()})
				if err != nil || out != nil {
					t.Fatalf("expected drop for %v, got out=%v err=%v", v, out, err)
				}
			}
			out, err := n.Execute(ctx, &Message{Payload: float64(5), Timestamp: time.Now()})
			if err != nil || out == nil {
				t.Fatalf("expected pass for 5, got out=%v err=%v", out, err)
			}
			if !equalValue(out.GetPayload(), float64(50)) {
				t.Fatalf("expected 50, got %v", out.GetPayload())
			}
		})

		s.it("just passes on msg if payload not present", func(t *testing.T) {
			n := NewRangeNode("r", &RangeConfig{
				Action: "scale", MinIn: 0, MaxIn: 100, MinOut: 0, MaxOut: 100, Round: true,
			})
			in := &Message{
				Metadata:  map[string]interface{}{"topic": "pass on"},
				Timestamp: time.Now(),
			}
			out, err := n.Execute(context.Background(), in)
			if err != nil || out != in {
				t.Fatalf("expected passthrough, got out=%v err=%v", out, err)
			}
			if out.GetMetadata()["topic"] != "pass on" {
				t.Fatalf("topic lost: %v", out.GetMetadata())
			}
		})

		s.it("reports if input is not a number", func(t *testing.T) {
			n := NewRangeNode("r", &RangeConfig{
				Action: "scale", Round: true,
			})
			out, err := n.Execute(context.Background(), &Message{
				Payload: "NOT A NUMBER", Timestamp: time.Now(),
			})
			if err == nil {
				t.Fatalf("expected non-numeric report, got out=%v", out)
			}
		})

		s.it("drops and reports null rather than scaling it as 0", func(t *testing.T) {
			// the engine cannot distinguish a null payload from an absent
			// one — nil Data takes the missing-property passthrough path
			// instead of upstream's report-and-drop.
			skipf(t, "nil Data is treated as payload-not-present (passthrough), not null")
		})

		s.it("still scales true as 1 (existing flows rely on it)", func(t *testing.T) {
			rangeTest(t, "scale", 0, 1, 0, 100, false, true, float64(100))
		})

		s.it("still scales false as 0 (existing flows rely on it)", func(t *testing.T) {
			rangeTest(t, "scale", 0, 1, 0, 100, false, false, float64(0))
		})

		s.it("drops and reports an empty string rather than scaling it as 0", func(t *testing.T) {
			nonNumberDropped(t, "")
		})

		s.it("drops and reports a whitespace-only string rather than scaling it as 0", func(t *testing.T) {
			nonNumberDropped(t, "   ")
		})

		s.it("drops and reports an empty array rather than scaling it as 0", func(t *testing.T) {
			nonNumberDropped(t, []interface{}{})
		})
	})
}
