package noderunner_test

import (
	. "github.com/dracory/noderun"
	"testing"
	"time"
)

// TestDelaySpec ports assertions from
// test/nodes/core/function/89-delay_spec.js (upstream describe
// "delay Node"). GoFlow's delay node is a fixed millisecond delay —
// everything else upstream covers (unit-scaled delays, rate limiting,
// queue flush/reset) is registered as skipped.
func TestDelaySpec(t *testing.T) {
	describe(t, "test/nodes/core/function/89-delay_spec.js", "delay Node", func(s *suite) {

		s.it("delays the message in milliseconds", func(t *testing.T) {
			wf := &Workflow{
				ID: "spec-delay-ms",
				Nodes: []Node{
					{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
					{ID: "dly", Type: "delay", Data: map[string]interface{}{"duration": 150}},
					specResponseNode("resp"),
				},
				Edges: []Edge{
					{ID: "e1", Source: "inj", Target: "dly"},
					{ID: "e2", Source: "dly", Target: "resp"},
				},
			}
			engine := specDeployFlow(t, wf)
			start := time.Now()
			ch := specInjectInto(t, engine, wf.ID, "inj", &Message{
				Payload:   "wait",
				Timestamp: time.Now(),
			})
			out := specAwaitResponse(t, ch, 3*time.Second)
			if out.GetPayload() != "wait" {
				t.Fatalf("payload changed in transit: %#v", out.GetPayload())
			}
			if elapsed := time.Since(start); elapsed < 140*time.Millisecond {
				t.Fatalf("message not delayed: %s", elapsed)
			}
		})

		s.it("delays the message in seconds", func(t *testing.T) {
			skipf(t, "delay node only supports millisecond duration")
		})

		s.it("delays the message in minutes", func(t *testing.T) {
			skipf(t, "delay node only supports millisecond duration")
		})

		s.it("variable delay set by msg.delay the message in milliseconds", func(t *testing.T) {
			skipf(t, "per-message msg.delay not implemented")
		})

		s.it("limits the message rate to 1 per second", func(t *testing.T) {
			skipf(t, "rate limiting not implemented")
		})

		s.it("can flush delay queue", func(t *testing.T) {
			skipf(t, "delay queue + flush/reset control messages not implemented")
		})
	})
}
