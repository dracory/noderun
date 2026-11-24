package noderunner_test

import (
	"context"
	. "github.com/dracory/noderun"
	"github.com/dracory/nodebase"
	"strings"
	"testing"
)

// TestJSONAutoStringify verifies auto mode turns an object payload into
// a JSON string.
func TestJSONAutoStringify(t *testing.T) {
	node := NewJSONNode("j1", &JSONConfig{})

	input := &Message{Payload: map[string]interface{}{"a": float64(1)}}
	out, err := node.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if out.GetPayload() != `{"a":1}` {
		t.Fatalf("expected JSON string, got %v", out.GetPayload())
	}
	if ports := node.SelectPorts(input, out); len(ports) != 1 || ports[0] != "output_1" {
		t.Fatalf("expected output_1, got %v", ports)
	}
}

// TestJSONAutoParse verifies auto mode parses a JSON string into an
// object.
func TestJSONAutoParse(t *testing.T) {
	node := NewJSONNode("j2", &JSONConfig{})

	input := &Message{Payload: `{"a":1}`}
	out, err := node.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	m, ok := out.GetPayload().(map[string]interface{})
	if !ok || m["a"] != float64(1) {
		t.Fatalf("expected map with a=1, got %#v", out.GetPayload())
	}
}

// TestJSONPretty verifies pretty mode indents the output.
func TestJSONPretty(t *testing.T) {
	node := NewJSONNode("j3", &JSONConfig{Pretty: true, Action: "str"})

	input := &Message{Payload: map[string]interface{}{"a": float64(1)}}
	out, err := node.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	s, ok := out.GetPayload().(string)
	if !ok || !strings.Contains(s, "\n  \"a\": 1") {
		t.Fatalf("expected indented JSON, got %v", out.GetPayload())
	}
}

// TestJSONForcedActions verifies str stringifies a string (JSON-encoded
// string) and obj rejects non-string input.
func TestJSONForcedActions(t *testing.T) {
	node := NewJSONNode("j4", &JSONConfig{Action: "str"})
	out, err := node.Execute(context.Background(), &Message{Payload: "plain"})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if out.GetPayload() != `"plain"` {
		t.Fatalf("expected JSON-encoded string, got %v", out.GetPayload())
	}

	nodeObj := NewJSONNode("j5", &JSONConfig{Action: "obj"})
	if _, err := nodeObj.Execute(context.Background(), &Message{
		Payload: map[string]interface{}{"a": 1},
	}); err == nil {
		t.Fatal("expected error for non-string input in obj mode")
	}
}

// TestJSONParseErrorOutput2 verifies that with outputs=2 a parse error
// routes the unchanged message to output_2 with error metadata.
func TestJSONParseErrorOutput2(t *testing.T) {
	node := NewJSONNode("j6", &JSONConfig{Outputs: 2})

	input := &Message{Payload: "{invalid"}
	out, err := node.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("Execute must not error when outputs=2: %v", err)
	}
	if out.GetPayload() != "{invalid" {
		t.Fatalf("message must pass through unchanged, got %v", out.GetPayload())
	}
	if _, ok := out.GetMetadata()["error"]; !ok {
		t.Fatal("expected Metadata.error to be set")
	}
	if ports := node.SelectPorts(input, out); len(ports) != 1 || ports[0] != "output_2" {
		t.Fatalf("expected output_2, got %v", ports)
	}
}

// TestJSONParseErrorOutput1 verifies that with a single output a parse
// error fails the node.
func TestJSONParseErrorOutput1(t *testing.T) {
	node := NewJSONNode("j7", &JSONConfig{Outputs: 1})
	if _, err := node.Execute(context.Background(), &Message{Payload: "{invalid"}); err == nil {
		t.Fatal("expected parse error")
	}
}

// TestJSONDottedProperty verifies property targeting inside the data map.
func TestJSONDottedProperty(t *testing.T) {
	node := NewJSONNode("j8", &JSONConfig{Property: "body.raw"})

	input := &Message{Payload: map[string]interface{}{
		"body": map[string]interface{}{"raw": `{"x":2}`},
	}}
	out, err := node.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	m := out.GetPayload().(map[string]interface{})
	v, found := nodebase.GetPath(m, "body.raw")
	if !found {
		t.Fatal("body.raw not found")
	}
	inner, ok := v.(map[string]interface{})
	if !ok || inner["x"] != float64(2) {
		t.Fatalf("expected parsed object at body.raw, got %#v", v)
	}
}

// TestXMLRoundTrip parses a simple XML document into map form and back.
func TestXMLRoundTrip(t *testing.T) {
	node := NewXMLNode("x1", &XMLConfig{})

	input := &Message{Payload: `<user id="7"><name>Ada</name><tag>go</tag><tag>flow</tag></user>`}
	out, err := node.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	root, ok := out.GetPayload().(map[string]interface{})
	if !ok {
		t.Fatalf("expected map, got %T", out.GetPayload())
	}
	user, ok := root["user"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected user map, got %#v", root["user"])
	}
	if user["@id"] != "7" {
		t.Fatalf("expected @id=7, got %v", user["@id"])
	}
	if user["name"] != "Ada" {
		t.Fatalf("expected name=Ada, got %v", user["name"])
	}
	tags, ok := user["tag"].([]interface{})
	if !ok || len(tags) != 2 {
		t.Fatalf("expected two tags, got %#v", user["tag"])
	}

	// Marshal the map back to XML
	node2 := NewXMLNode("x2", &XMLConfig{})
	out2, err := node2.Execute(context.Background(), out)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	s, ok := out2.GetPayload().(string)
	if !ok {
		t.Fatalf("expected XML string, got %T", out2.GetPayload())
	}
	for _, want := range []string{`<user id="7">`, `<name>Ada</name>`, `<tag>go</tag>`, `<tag>flow</tag>`, `</user>`} {
		if !strings.Contains(s, want) {
			t.Fatalf("marshalled XML missing %q: %s", want, s)
		}
	}
}

// TestCSVParseRows verifies parsing emits one message per row with
// parts metadata and header-keyed maps.
func TestCSVParseRows(t *testing.T) {
	node := NewCSVNode("c1", &CSVConfig{Headers: true})

	input := &Message{Payload: "name,age\nAda,36\nBob,41\n"}
	out, err := node.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	rows := node.Expand(out)
	if len(rows) != 2 {
		t.Fatalf("expected 2 row messages, got %d", len(rows))
	}
	for i, p := range rows {
		if _, ok := p.GetPayload().(map[string]interface{}); !ok {
			t.Fatalf("row %d: expected map, got %T", i, p.GetPayload())
		}
		if got := partField(t, p, "index"); got != i {
			t.Fatalf("row %d: parts.index = %v", i, got)
		}
		if got := partField(t, p, "count"); got != 2 {
			t.Fatalf("row %d: parts.count = %v", i, got)
		}
		if got := partField(t, p, "type"); got != "csv" {
			t.Fatalf("row %d: parts.type = %v", i, got)
		}
	}
	first := rows[0].GetPayload().(map[string]interface{})
	if first["name"] != "Ada" || first["age"] != "36" {
		t.Fatalf("unexpected first row: %v", first)
	}
	if partField(t, rows[0], "id") != partField(t, rows[1], "id") {
		t.Fatal("all rows must share the same parts id")
	}
}

// TestCSVParseNoHeaders verifies rows become string slices without
// headers.
func TestCSVParseNoHeaders(t *testing.T) {
	node := NewCSVNode("c2", &CSVConfig{})

	input := &Message{Payload: "1,2\n3,4\n"}
	out, err := node.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	rows := node.Expand(out)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	r0, ok := rows[0].GetPayload().([]string)
	if !ok || len(r0) != 2 || r0[0] != "1" {
		t.Fatalf("unexpected row data: %#v", rows[0].GetPayload())
	}
}

// TestCSVSerialize verifies arrays of maps/rows serialize to CSV with
// a header row.
func TestCSVSerialize(t *testing.T) {
	node := NewCSVNode("c3", &CSVConfig{Headers: true})

	input := &Message{Payload: []interface{}{
		map[string]interface{}{"name": "Ada", "age": "36"},
		map[string]interface{}{"name": "Bob", "age": "41"},
	}}
	out, err := node.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	s, ok := out.GetPayload().(string)
	if !ok {
		t.Fatalf("expected CSV string, got %T", out.GetPayload())
	}
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 CSV lines, got %q", s)
	}
	if lines[0] != "age,name" {
		t.Fatalf("expected header line, got %q", lines[0])
	}
	if lines[1] != "36,Ada" {
		t.Fatalf("unexpected row: %q", lines[1])
	}

	// serialize mode: Expand must return the single output
	outs := node.Expand(out)
	if len(outs) != 1 || outs[0] != out {
		t.Fatal("serialize mode must pass the single output through")
	}
}
