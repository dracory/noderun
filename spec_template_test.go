package noderunner_test

import (
	"context"
	"testing"
	"time"

	"github.com/dracory/nodebase"
	. "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

// TestTemplateSpec ports assertions from
// test/nodes/core/function/80-template_spec.js (upstream describe
// "template node"). GoFlow's template node renders Mustache against the
// whole message (payload + metadata props) like Node-RED; flow/global/
// env context tags are pre-resolved through the host context store and
// env chain, matching upstream's NodeContext two-phase lookup.
// Remaining gaps are named persistable context stores
// ({{flow[store].X}} / "#:(store)::key") — the host ContextStore
// contract has no store dimension.
func TestTemplateSpec(t *testing.T) {
	describe(t, "test/nodes/core/function/80-template_spec.js", "template node", func(s *suite) {

		newEngine := func() *Engine { return NewEngine() }

		render := func(t *testing.T, engine *Engine, cfg *TemplateConfig, in *Message) nodestore.MessageInterface {
			t.Helper()
			if cfg.Syntax == "" {
				cfg.Syntax = "mustache"
			}
			n := NewTemplateNode("n1", cfg, engine)
			out, err := n.Execute(context.Background(), in)
			if err != nil {
				t.Fatalf("execute failed: %v", err)
			}
			return out
		}

		s.it("should modify payload using node-configured template", func(t *testing.T) {
			out := render(t, newEngine(), &TemplateConfig{
				Field: "payload", Template: "payload={{payload}}",
			}, &Message{
				Payload:   "foo",
				Metadata:  map[string]interface{}{"topic": "bar", "template": "this should be ignored as the node has its own template {{payload}}"},
				Timestamp: time.Now(),
			})
			if out.GetMetadata()["topic"] != "bar" {
				t.Fatalf("topic lost: %v", out.GetMetadata()["topic"])
			}
			if out.GetPayload() != "payload=foo" {
				t.Fatalf("payload wrong: %v", out.GetPayload())
			}
			if _, ok := out.GetMetadata()["template"]; !ok {
				t.Fatal("msg.template should be preserved")
			}
		})

		s.it("should modify the configured property using msg.template", func(t *testing.T) {
			out := render(t, newEngine(), &TemplateConfig{
				Field: "randomProperty", Template: "",
			}, &Message{
				Payload:   "foo",
				Metadata:  map[string]interface{}{"topic": "bar", "template": "payload={{payload}}"},
				Timestamp: time.Now(),
			})
			if out.GetPayload() != "foo" {
				t.Fatalf("payload should be untouched: %v", out.GetPayload())
			}
			if out.GetMetadata()["randomProperty"] != "payload=foo" {
				t.Fatalf("randomProperty wrong: %v", out.GetMetadata()["randomProperty"])
			}
		})

		s.it("should be able to overwrite msg.template using the template from msg.template", func(t *testing.T) {
			out := render(t, newEngine(), &TemplateConfig{
				Field: "payload", Template: "",
			}, &Message{
				Payload:   "foo",
				Metadata:  map[string]interface{}{"topic": "bar", "template": "topic={{topic}}"},
				Timestamp: time.Now(),
			})
			if out.GetPayload() != "topic=bar" {
				t.Fatalf("payload wrong: %v", out.GetPayload())
			}
		})

		s.it("should modify payload from msg.template", func(t *testing.T) {
			engine := newEngine()
			cfg := &TemplateConfig{Field: "payload", Template: ""}
			inputs := []*Message{
				{Payload: "foo", Metadata: map[string]interface{}{"topic": "bar", "template": "topic={{topic}}"}},
				{Payload: "foo", Metadata: map[string]interface{}{"topic": "another bar", "template": "topic={{topic}}"}},
				{Payload: "foo", Metadata: map[string]interface{}{"topic": "bar", "template": "payload={{payload}}"}},
			}
			want := []string{"topic=bar", "topic=another bar", "payload=foo"}
			for i, in := range inputs {
				out := render(t, engine, cfg, in)
				if out.GetPayload() != want[i] {
					t.Fatalf("message %d: payload=%v want %v", i, out.GetPayload(), want[i])
				}
			}
		})

		s.it("should modify payload from env variable", func(t *testing.T) {
			t.Setenv("TEST", "xyzzy")
			engine := newEngine()
			engine.AllowOSEnv = true
			out := render(t, engine, &TemplateConfig{
				Field: "payload", Template: "payload={{env.TEST}}",
			}, &Message{Payload: "foo", Metadata: map[string]interface{}{"topic": "bar"}})
			if out.GetPayload() != "payload=xyzzy" {
				t.Fatalf("payload wrong: %v", out.GetPayload())
			}
		})

		s.it("should modify payload from flow context", func(t *testing.T) {
			engine := newEngine()
			if err := engine.ResolvedContextStore().SetContext(nodebase.ContextScopeFlow, "", "value", "foo"); err != nil {
				t.Fatal(err)
			}
			out := render(t, engine, &TemplateConfig{
				Field: "payload", Template: "payload={{flow.value}}",
			}, &Message{Payload: "foo", Metadata: map[string]interface{}{"topic": "bar"}})
			if out.GetPayload() != "payload=foo" {
				t.Fatalf("payload wrong: %v", out.GetPayload())
			}
		})

		s.it("should modify payload from persistable flow context", func(t *testing.T) {
			skipf(t, "named persistable context stores (flow[store].X) not supported — ContextStore has no store dimension")
		})

		s.it("should handle nested context tags - property not set", func(t *testing.T) {
			tpl := `{{#flow.time}}time={{flow.time}}{{/flow.time}}{{^flow.time}}!time{{/flow.time}}{{#flow.random}}random={{flow.random}}randomtime={{flow.randomtime}}{{/flow.random}}{{^flow.random}}!random{{/flow.random}}`
			out := render(t, newEngine(), &TemplateConfig{
				Field: "payload", Template: tpl,
			}, &Message{Payload: "foo", Metadata: map[string]interface{}{"topic": "bar"}})
			if out.GetPayload() != "!time!random" {
				t.Fatalf("payload wrong: %v", out.GetPayload())
			}
		})

		s.it("should handle nested context tags - property set", func(t *testing.T) {
			engine := newEngine()
			store := engine.ResolvedContextStore()
			for k, v := range map[string]string{"time": "123", "random": "456", "randomtime": "789"} {
				if err := store.SetContext(nodebase.ContextScopeFlow, "", k, v); err != nil {
					t.Fatal(err)
				}
			}
			tpl := `{{#flow.time}}time={{flow.time}}{{/flow.time}}{{^flow.time}}!time{{/flow.time}}{{#flow.random}}random={{flow.random}}randomtime={{flow.randomtime}}{{/flow.random}}{{^flow.random}}!random{{/flow.random}}`
			out := render(t, engine, &TemplateConfig{
				Field: "payload", Template: tpl,
			}, &Message{Payload: "foo", Metadata: map[string]interface{}{"topic": "bar"}})
			if out.GetPayload() != "time=123random=456randomtime=789" {
				t.Fatalf("payload wrong: %v", out.GetPayload())
			}
		})

		s.it("should modify payload from two persistable flow context", func(t *testing.T) {
			skipf(t, "named persistable context stores (flow[store].X) not supported — ContextStore has no store dimension")
		})

		s.it("should modify payload from global context", func(t *testing.T) {
			engine := newEngine()
			if err := engine.ResolvedContextStore().SetContext(nodebase.ContextScopeGlobal, "", "value", "foo"); err != nil {
				t.Fatal(err)
			}
			out := render(t, engine, &TemplateConfig{
				Field: "payload", Template: "payload={{global.value}}",
			}, &Message{Payload: "foo", Metadata: map[string]interface{}{"topic": "bar"}})
			if out.GetPayload() != "payload=foo" {
				t.Fatalf("payload wrong: %v", out.GetPayload())
			}
		})

		s.it("should modify payload from persistable global context", func(t *testing.T) {
			skipf(t, "named persistable context stores (global[store].X) not supported — ContextStore has no store dimension")
		})

		s.it("should modify payload from two persistable global context", func(t *testing.T) {
			skipf(t, "named persistable context stores (global[store].X) not supported — ContextStore has no store dimension")
		})

		s.it("should modify payload from persistable flow & global context", func(t *testing.T) {
			skipf(t, "named persistable context stores not supported — ContextStore has no store dimension")
		})

		s.it("should handle missing node context", func(t *testing.T) {
			out := render(t, newEngine(), &TemplateConfig{
				Field: "payload", Template: "payload={{flow.value}},{{global.value}}",
			}, &Message{Payload: "foo", Metadata: map[string]interface{}{"topic": "bar"}})
			if out.GetPayload() != "payload=," {
				t.Fatalf("payload wrong: %v", out.GetPayload())
			}
		})

		s.it("should handle escape characters in Mustache format and JSON output mode", func(t *testing.T) {
			payload := "line\t1\nline\\2\r\nline\b3\f"
			out := render(t, newEngine(), &TemplateConfig{
				Field: "payload", Syntax: "mustache", Output: "json",
				Template: `{"data":"{{payload}}"}`,
			}, &Message{Payload: payload})
			m, ok := out.GetPayload().(map[string]interface{})
			if !ok {
				t.Fatalf("payload not a map: %v", out.GetPayload())
			}
			if m["data"] != payload {
				t.Fatalf("data wrong: %q want %q", m["data"], payload)
			}
		})

		s.it("should modify payload in plain text mode", func(t *testing.T) {
			out := render(t, newEngine(), &TemplateConfig{
				Field: "payload", Syntax: "plain", Template: "payload={{payload}}",
			}, &Message{Payload: "foo", Metadata: map[string]interface{}{"topic": "bar"}})
			if out.GetPayload() != "payload={{payload}}" {
				t.Fatalf("payload wrong: %v", out.GetPayload())
			}
		})

		s.it("should modify flow context", func(t *testing.T) {
			engine := newEngine()
			out := render(t, engine, &TemplateConfig{
				Field: "payload", FieldType: "flow", Template: "payload={{payload}}",
			}, &Message{Payload: "foo", Metadata: map[string]interface{}{"topic": "bar"}})
			// message is intact
			if out.GetPayload() != "foo" {
				t.Fatalf("payload should be untouched: %v", out.GetPayload())
			}
			v, ok, err := engine.ResolvedContextStore().GetContext(nodebase.ContextScopeFlow, "", "payload")
			if err != nil || !ok || v != "payload=foo" {
				t.Fatalf("flow context wrong: v=%v ok=%v err=%v", v, ok, err)
			}
		})

		s.it("should modify persistable flow context", func(t *testing.T) {
			skipf(t, "named persistable context stores (#:(store)::key) not supported — ContextStore has no store dimension")
		})

		s.it("should modify global context", func(t *testing.T) {
			engine := newEngine()
			out := render(t, engine, &TemplateConfig{
				Field: "payload", FieldType: "global", Template: "payload={{payload}}",
			}, &Message{Payload: "foo", Metadata: map[string]interface{}{"topic": "bar"}})
			if out.GetPayload() != "foo" {
				t.Fatalf("payload should be untouched: %v", out.GetPayload())
			}
			v, ok, err := engine.ResolvedContextStore().GetContext(nodebase.ContextScopeGlobal, "", "payload")
			if err != nil || !ok || v != "payload=foo" {
				t.Fatalf("global context wrong: v=%v ok=%v err=%v", v, ok, err)
			}
		})

		s.it("should modify persistable global context", func(t *testing.T) {
			skipf(t, "named persistable context stores (#:(store)::key) not supported — ContextStore has no store dimension")
		})

		s.it("should handle if the field isn't set", func(t *testing.T) {
			out := render(t, newEngine(), &TemplateConfig{
				Template: "payload={{payload}}",
			}, &Message{Payload: "foo", Metadata: map[string]interface{}{"topic": "bar"}})
			if out.GetPayload() != "payload=foo" {
				t.Fatalf("payload wrong: %v", out.GetPayload())
			}
		})

		s.it("should handle deeper objects", func(t *testing.T) {
			out := render(t, newEngine(), &TemplateConfig{
				Field: "topic.foo.bar", Template: "payload={{payload.doh.rei.me}}",
			}, &Message{Payload: map[string]interface{}{
				"doh": map[string]interface{}{"rei": map[string]interface{}{"me": "foo"}},
			}})
			topic, ok := out.GetMetadata()["topic"].(map[string]interface{})
			if !ok {
				t.Fatalf("topic not a map: %v", out.GetMetadata()["topic"])
			}
			foo, ok := topic["foo"].(map[string]interface{})
			if !ok || foo["bar"] != "payload=foo" {
				t.Fatalf("topic.foo.bar wrong: %v", topic)
			}
		})

		s.it("should handle block contexts objects", func(t *testing.T) {
			out := render(t, newEngine(), &TemplateConfig{
				Template: "A{{#payload.A}}{{payload.A}}{{.}}{{/payload.A}}B",
			}, &Message{Payload: map[string]interface{}{"A": "abc"}})
			if out.GetPayload() != "AabcabcB" {
				t.Fatalf("payload wrong: %v", out.GetPayload())
			}
		})

		s.it("should raise error if passed bad template", func(t *testing.T) {
			// An unclosed tag is a parse error; GoFlow surfaces it as a
			// node error rather than a logged "Unclosed tag" event.
			n := NewTemplateNode("n1", &TemplateConfig{
				Field: "payload", Syntax: "mustache", Template: "payload={{payload",
			}, newEngine())
			out, err := n.Execute(context.Background(), &Message{Payload: "foo"})
			if err == nil {
				t.Fatalf("expected template parse error, got out=%v", out)
			}
		})
	})
}
