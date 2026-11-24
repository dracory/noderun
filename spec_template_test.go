package noderunner_test

import (
	"context"
	nodestore "github.com/dracory/nodestore"
	. "github.com/dracory/noderun"
	"testing"
	"time"
)

// TestTemplateSpec ports assertions from
// test/nodes/core/function/80-template_spec.js (upstream describe
// "template node"). NOTE: upstream renders MUSTACHE templates;
// GoFlow's template node renders Go text/template syntax against
// payload.GetPayload() — a fundamentally different template language, so the
// rendering assertions are registered as skipped rather than faked.
func TestTemplateSpec(t *testing.T) {
	describe(t, "test/nodes/core/function/80-template_spec.js", "template node", func(s *suite) {

		render := func(t *testing.T, tpl, format string, data interface{}) (nodestore.MessageInterface, error) {
			t.Helper()
			n := NewTemplateNode("n1", &TemplateConfig{
				Template: tpl, Format: format,
			})
			return n.Execute(context.Background(), &Message{
				Payload: data, Timestamp: time.Now(),
			})
		}

		s.it("should modify payload using node-configured template", func(t *testing.T) {
			t.Skip("reason: upstream templates are Mustache; GoFlow renders Go text/template")
		})

		s.it("should modify the configured property using msg.template", func(t *testing.T) {
			t.Skip("reason: msg.template override and arbitrary field targets not implemented (Mustache)")
		})

		s.it("should be able to overwrite msg.template using the template from msg.template", func(t *testing.T) {
			t.Skip("reason: msg.template override not implemented (Mustache)")
		})

		s.it("should modify payload from msg.template", func(t *testing.T) {
			t.Skip("reason: msg.template override not implemented (Mustache)")
		})

		s.it("should modify payload from env variable", func(t *testing.T) {
			t.Skip("reason: {{env.X}} Mustache context tags not supported")
		})

		s.it("should modify payload from flow context", func(t *testing.T) {
			t.Skip("reason: {{flow.X}} Mustache context tags not supported")
		})

		s.it("should modify payload from persistable flow context", func(t *testing.T) {
			t.Skip("reason: persistable (named) context stores not supported")
		})

		s.it("should handle nested context tags - property not set", func(t *testing.T) {
			t.Skip("reason: Mustache context tags not supported")
		})

		s.it("should handle nested context tags - property set", func(t *testing.T) {
			t.Skip("reason: Mustache context tags not supported")
		})

		s.it("should modify payload from two persistable flow context", func(t *testing.T) {
			t.Skip("reason: persistable (named) context stores not supported")
		})

		s.it("should modify payload from global context", func(t *testing.T) {
			t.Skip("reason: {{global.X}} Mustache context tags not supported")
		})

		s.it("should modify payload from persistable global context", func(t *testing.T) {
			t.Skip("reason: persistable (named) context stores not supported")
		})

		s.it("should modify payload from two persistable global context", func(t *testing.T) {
			t.Skip("reason: persistable (named) context stores not supported")
		})

		s.it("should modify payload from persistable flow & global context", func(t *testing.T) {
			t.Skip("reason: persistable (named) context stores not supported")
		})

		s.it("should handle missing node context", func(t *testing.T) {
			t.Skip("reason: Mustache context tags not supported")
		})

		s.it("should handle escape characters in Mustache format and JSON output mode", func(t *testing.T) {
			t.Skip("reason: Mustache rendering not supported (Go text/template)")
		})

		s.it("should modify payload in plain text mode", func(t *testing.T) {
			t.Skip("reason: syntax=plain literal passthrough not implemented")
		})

		s.it("should modify flow context", func(t *testing.T) {
			t.Skip("reason: fieldType=flow (render into context) not implemented")
		})

		s.it("should modify persistable flow context", func(t *testing.T) {
			t.Skip("reason: fieldType=flow + persistable context not implemented")
		})

		s.it("should modify global context", func(t *testing.T) {
			t.Skip("reason: fieldType=global (render into context) not implemented")
		})

		s.it("should modify persistable global context", func(t *testing.T) {
			t.Skip("reason: fieldType=global + persistable context not implemented")
		})

		s.it("should handle if the field isn't set", func(t *testing.T) {
			t.Skip("reason: Mustache rendering not supported (Go text/template)")
		})

		s.it("should handle deeper objects", func(t *testing.T) {
			t.Skip("reason: Mustache rendering + dotted field targets not supported")
		})

		s.it("should handle block contexts objects", func(t *testing.T) {
			t.Skip("reason: Mustache block sections not supported (Go text/template)")
		})

		s.it("should raise error if passed bad template", func(t *testing.T) {
			// An unclosed tag is a parse error in both template
			// languages; GoFlow surfaces it as a node error rather
			// than a logged "Unclosed tag" event.
			out, err := render(t, "payload={{payload", "text", "foo")
			if err == nil {
				t.Fatalf("expected template parse error, got out=%v", out)
			}
		})
	})
}
