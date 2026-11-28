package noderunner_test

import (
	"context"
	"encoding/json"
	. "github.com/dracory/noderun"
	"testing"
)

// TestChangeMovePayloadIntoField — BUG-3: moving the whole payload into
// a field must move it, not silently no-op.
func TestChangeMovePayloadIntoField(t *testing.T) {
	n := NewChangeNode("c", &ChangeConfig{Rules: []ChangeRule{
		{Op: "move", Field: "payload", Value: "wrapped.x"},
	}})
	out, _ := n.Execute(context.Background(), &Message{
		Payload: map[string]interface{}{"a": float64(1)},
	})
	out = requireNonNilMessage(t, out)
	if _, err := json.Marshal(out.GetPayload()); err != nil {
		t.Fatalf("output must be JSON-safe (no cycles): %v", err)
	}
	m := out.GetPayload().(map[string]interface{})
	wrapped, ok := m["wrapped"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected payload moved under wrapped.x, got %v", m)
	}
	if x, ok := wrapped["x"].(map[string]interface{}); !ok || x["a"] != float64(1) {
		t.Fatalf("expected wrapped.x == {a:1}, got %v", wrapped["x"])
	}
}

// TestChangeMovePayloadToMsg — BUG-3: moving payload into a msg.*
// property clears Data and writes Metadata.
func TestChangeMovePayloadToMsg(t *testing.T) {
	n := NewChangeNode("c", &ChangeConfig{Rules: []ChangeRule{
		{Op: "move", Field: "payload", Value: "msg.saved"},
	}})
	out, _ := n.Execute(context.Background(), &Message{
		Payload: map[string]interface{}{"a": float64(1)},
	})
	out = requireNonNilMessage(t, out)
	if out.GetPayload() != nil {
		t.Fatalf("expected cleared payload, got %v", out.GetPayload())
	}
	saved, ok := out.GetMetadata()["saved"].(map[string]interface{})
	if !ok || saved["a"] != float64(1) {
		t.Fatalf("expected Metadata.saved == {a:1}, got %v", out.GetMetadata()["saved"])
	}
}

// TestChangeMovePayloadToPayloadNoop — BUG-3: payload -> payload stays
// a no-op.
func TestChangeMovePayloadToPayloadNoop(t *testing.T) {
	n := NewChangeNode("c", &ChangeConfig{Rules: []ChangeRule{
		{Op: "move", Field: "payload", Value: "payload"},
	}})
	in := &Message{Payload: map[string]interface{}{"a": float64(1)}}
	out, _ := n.Execute(context.Background(), in)
	out = requireNonNilMessage(t, out)
	if m := out.GetPayload().(map[string]interface{}); m["a"] != float64(1) {
		t.Fatalf("expected unchanged payload, got %v", out.GetPayload())
	}
}
