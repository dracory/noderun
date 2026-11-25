package noderunner_test

import (
	"context"
	. "github.com/dracory/noderun"
	"testing"
)

// TestChangeNode verifies set/move/delete rules applied to a
// request-shaped payload (map[string]interface{} like an HTTP trigger
// produces)
func TestChangeNode(t *testing.T) {
	payload := &Message{
		Payload: map[string]interface{}{
			"method": "GET",
			"path":   "/go",
			"body": map[string]interface{}{
				"name": "world",
			},
		},
	}

	node := NewChangeNode("chg1", &ChangeConfig{
		Rules: []ChangeRule{
			{Op: "set", Field: "nested.greeting", Value: "hello"},
			{Op: "set", Field: "nested.count", Value: "42"},
			{Op: "set", Field: "msg.topic", Value: `"t1"`},
			{Op: "move", Field: "path", Value: "nested.path"},
			{Op: "delete", Field: "method"},
			{Op: "delete", Field: "nonexistent.deep"}, // skipped silently
			{Op: "bogus", Field: "x"},                 // unknown op skipped
		},
	})

	out, err := node.Execute(context.Background(), payload)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	data, ok := out.GetPayload().(map[string]interface{})
	if !ok {
		t.Fatalf("payload.GetPayload() is not a map: %T", out.GetPayload())
	}
	nested, ok := data["nested"].(map[string]interface{})
	if !ok {
		t.Fatalf("nested field missing or not a map: %v", data)
	}
	if nested["greeting"] != "hello" {
		t.Fatalf("expected nested.greeting=hello, got %v", nested["greeting"])
	}
	if nested["count"] != float64(42) {
		t.Fatalf("expected nested.count=42 (JSON number), got %v (%T)", nested["count"], nested["count"])
	}
	if nested["path"] != "/go" {
		t.Fatalf("expected nested.path=/go after move, got %v", nested["path"])
	}
	// "msg." fields are message-level properties → Metadata
	if out.GetMetadata()["topic"] != "t1" {
		t.Fatalf("expected msg.topic=t1 in Metadata, got %v", out.GetMetadata()["topic"])
	}
	if _, exists := data["path"]; exists {
		t.Fatalf("expected path removed after move")
	}
	if _, exists := data["method"]; exists {
		t.Fatalf("expected method deleted")
	}
}

// TestChangeNodeSetPayload verifies that field "payload" replaces
// payload.GetPayload() entirely
func TestChangeNodeSetPayload(t *testing.T) {
	payload := &Message{
		Payload: map[string]interface{}{"old": true},
	}
	node := NewChangeNode("chg2", &ChangeConfig{
		Rules: []ChangeRule{
			{Op: "set", Field: "payload", Value: `{"msg":"hi"}`},
		},
	})
	out, err := node.Execute(context.Background(), payload)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	data, ok := out.GetPayload().(map[string]interface{})
	if !ok || data["msg"] != "hi" {
		t.Fatalf("expected payload replaced with {\"msg\":\"hi\"}, got %v", out.GetPayload())
	}
}

// TestTemplateNode verifies JSON and text rendering against a
// request-shaped payload
func TestTemplateNode(t *testing.T) {
	payload := &Message{
		Payload: map[string]interface{}{
			"path": "/go",
			"body": map[string]interface{}{
				"msg": "hi there",
			},
		},
	}

	node := NewTemplateNode("tpl1", &TemplateConfig{
		Syntax:   "gotemplate",
		Template: `{"echo": "{{.body.msg}}", "path": "{{.path}}"}`,
		Format:   "json",
	}, nil)
	out, err := node.Execute(context.Background(), payload)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	data, ok := out.GetPayload().(map[string]interface{})
	if !ok {
		t.Fatalf("payload.GetPayload() is not a map: %T", out.GetPayload())
	}
	if data["echo"] != "hi there" || data["path"] != "/go" {
		t.Fatalf("unexpected rendered data: %v", data)
	}

	textNode := NewTemplateNode("tpl2", &TemplateConfig{
		Syntax:   "gotemplate",
		Template: "you said {{.body.msg}}",
		Format:   "text",
	}, nil)
	out, err = textNode.Execute(context.Background(), &Message{Payload: map[string]interface{}{
		"body": map[string]interface{}{"msg": "hi there"},
	}})
	if err != nil {
		t.Fatalf("text execute failed: %v", err)
	}
	if out.GetPayload() != "you said hi there" {
		t.Fatalf("expected rendered text, got %v", out.GetPayload())
	}
}

// TestTemplateNodeErrors verifies parse/execute/JSON errors surface
// as node errors
func TestTemplateNodeErrors(t *testing.T) {
	badParse := NewTemplateNode("tpl3", &TemplateConfig{Syntax: "gotemplate", Template: "{{", Format: "json"}, nil)
	if _, err := badParse.Execute(context.Background(), &Message{Payload: map[string]interface{}{}}); err == nil {
		t.Fatal("expected template parse error")
	}

	badJSON := NewTemplateNode("tpl4", &TemplateConfig{Syntax: "gotemplate", Template: "not json", Format: "json"}, nil)
	if _, err := badJSON.Execute(context.Background(), &Message{Payload: map[string]interface{}{}}); err == nil {
		t.Fatal("expected JSON parse error")
	}
}
