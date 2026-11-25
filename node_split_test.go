package noderunner_test

import (
	"context"
	. "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
	"testing"
	"time"
)

// partField reads a key from a payload's parts metadata map.
func partField(t *testing.T, p nodestore.MessageInterface, key string) interface{} {
	t.Helper()
	parts, ok := p.GetMetadata()["parts"].(map[string]interface{})
	if !ok {
		t.Fatalf("payload missing parts metadata: %+v", p.GetMetadata())
	}
	v, ok := parts[key]
	if !ok {
		t.Fatalf("parts missing key %q: %+v", key, parts)
	}
	return v
}

// TestSplitArray verifies that expanding an array payload yields one
// message per chunk with correct parts metadata (index, count, id).
func TestSplitArray(t *testing.T) {
	node := NewSplitNode("sp", &SplitConfig{})

	input := &Message{
		Payload:   []interface{}{float64(1), float64(2), float64(3)},
		Metadata:  map[string]interface{}{"origin": "test"},
		Timestamp: time.Now(),
	}

	out := node.Expand(input)
	if len(out) != 3 {
		t.Fatalf("expected 3 payloads, got %d", len(out))
	}
	for i, p := range out {
		// Node-RED emits the element itself when arraySplt is 1; only
		// larger chunk lengths produce array payloads. (nodebase split
		// parity change.)
		if p.GetPayload() != float64(i+1) {
			t.Fatalf("payload %d: expected value %d, got %v", i, i+1, p.GetPayload())
		}
		if got := partField(t, p, "index"); got != i {
			t.Fatalf("payload %d: parts.index = %v", i, got)
		}
		if got := partField(t, p, "count"); got != 3 {
			t.Fatalf("payload %d: parts.count = %v", i, got)
		}
		if got := partField(t, p, "type"); got != "array" {
			t.Fatalf("payload %d: parts.type = %v", i, got)
		}
	}
	if partField(t, out[0], "id") != partField(t, out[1], "id") {
		t.Fatal("all parts must share the same group id")
	}
}

// TestSplitArrayChunked verifies arraySplt chunks multiple elements per message.
func TestSplitArrayChunked(t *testing.T) {
	node := NewSplitNode("sp", &SplitConfig{ArraySplt: 2})
	input := &Message{Payload: []interface{}{1, 2, 3, 4, 5}}

	out := node.Expand(input)
	if len(out) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(out))
	}
	if got := len(out[2].GetPayload().([]interface{})); got != 1 {
		t.Fatalf("last chunk should hold 1 element, got %d", got)
	}
}

// TestSplitString verifies delimiter splitting with escape unescaping.
func TestSplitString(t *testing.T) {
	node := NewSplitNode("sp", &SplitConfig{Splt: `,`, SpltType: "str"})
	input := &Message{Payload: "a,b,c"}

	out := node.Expand(input)
	if len(out) != 3 {
		t.Fatalf("expected 3 payloads, got %d", len(out))
	}
	for i, want := range []string{"a", "b", "c"} {
		if out[i].GetPayload() != want {
			t.Fatalf("payload %d: expected %q, got %v", i, want, out[i].GetPayload())
		}
	}
	if got := partField(t, out[0], "ch"); got != "," {
		t.Fatalf("parts.ch = %v, expected \",\"", got)
	}
	if got := partField(t, out[0], "type"); got != "string" {
		t.Fatalf("parts.type = %v", got)
	}
}

// TestSplitStringEscapedDelimiter verifies that a literal "\n" in the
// config is unescaped before splitting.
func TestSplitStringEscapedDelimiter(t *testing.T) {
	node := NewSplitNode("sp", &SplitConfig{Splt: `\n`, SpltType: "str"})
	out := node.Expand(&Message{Payload: "x\ny"})
	if len(out) != 2 || out[0].GetPayload() != "x" || out[1].GetPayload() != "y" {
		t.Fatalf("unexpected output: %+v", out)
	}
}

// TestSplitStringLen verifies fixed-length splitting (spltType "len").
func TestSplitStringLen(t *testing.T) {
	node := NewSplitNode("sp", &SplitConfig{Splt: "2", SpltType: "len"})
	out := node.Expand(&Message{Payload: "abcde"})
	if len(out) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(out))
	}
	for i, want := range []string{"ab", "cd", "e"} {
		if out[i].GetPayload() != want {
			t.Fatalf("chunk %d: expected %q, got %v", i, want, out[i].GetPayload())
		}
	}
}

// TestSplitObject verifies one message per key with parts.key set and
// the message data carrying the value.
func TestSplitObject(t *testing.T) {
	node := NewSplitNode("sp", &SplitConfig{})
	input := &Message{Payload: map[string]interface{}{"a": 1, "b": 2}}

	out := node.Expand(input)
	if len(out) != 2 {
		t.Fatalf("expected 2 payloads, got %d", len(out))
	}
	// Keys are sorted for determinism
	for i, want := range []string{"a", "b"} {
		if got := partField(t, out[i], "key"); got != want {
			t.Fatalf("payload %d: parts.key = %v, expected %q", i, got, want)
		}
	}
	if out[0].GetPayload() != 1 || out[1].GetPayload() != 2 {
		t.Fatalf("unexpected values: %v %v", out[0].GetPayload(), out[1].GetPayload())
	}
	if got := partField(t, out[0], "type"); got != "object" {
		t.Fatalf("parts.type = %v", got)
	}
}

// TestSplitNestedProperty verifies splitting a dotted property: emitted
// messages keep the full data map with only the property replaced.
func TestSplitNestedProperty(t *testing.T) {
	node := NewSplitNode("sp", &SplitConfig{Property: "items"})
	input := &Message{Payload: map[string]interface{}{
		"items": []interface{}{"x", "y"},
		"other": "keep",
	}}

	out := node.Expand(input)
	if len(out) != 2 {
		t.Fatalf("expected 2 payloads, got %d", len(out))
	}
	m := out[0].GetPayload().(map[string]interface{})
	if m["other"] != "keep" {
		t.Fatalf("sibling fields must be preserved: %v", m)
	}
	// With the default chunk length 1 the source property is replaced by
	// the element itself, not a one-element array (Node-RED parity).
	if m["items"] != "x" {
		t.Fatalf("property not replaced by element: %v", m["items"])
	}
	// The rewrite records the full message path in parts.property; the
	// legacy bare property name is interpreted as payload.items.
	if got := partField(t, out[0], "property"); got != "payload.items" {
		t.Fatalf("parts.property = %v", got)
	}
}

// TestSplitNestedParts verifies a previous parts block is preserved
// under parts.parts.
func TestSplitNestedParts(t *testing.T) {
	node := NewSplitNode("sp", &SplitConfig{})
	input := &Message{
		Payload: []interface{}{1},
		Metadata: map[string]interface{}{
			"parts": map[string]interface{}{"id": "outer", "index": 0},
		},
	}
	out := node.Expand(input)
	if len(out) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(out))
	}
	parts := out[0].GetMetadata()["parts"].(map[string]interface{})
	prev, ok := parts["parts"].(map[string]interface{})
	if !ok || prev["id"] != "outer" {
		t.Fatalf("previous parts not preserved: %+v", parts)
	}
}

// TestSplitNonSplittable verifies numbers/bools pass through unchanged.
func TestSplitNonSplittable(t *testing.T) {
	node := NewSplitNode("sp", &SplitConfig{})
	out := node.Expand(&Message{Payload: float64(42)})
	if len(out) != 1 || out[0].GetPayload() != float64(42) {
		t.Fatalf("expected passthrough, got %+v", out)
	}
}

// TestSplitIntegration deploys inject → split → log and asserts the
// split fans out to three messages (three log message events).
func TestSplitIntegration(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-split-flow",
		Name: "Split Flow",
		Nodes: []Node{
			{
				ID:   "inj",
				Type: "inject",
				Data: map[string]interface{}{
					"payload": `[1,2,3]`,
					"once":    true,
				},
			},
			{
				ID:   "sp",
				Type: "split",
				Data: map[string]interface{}{},
			},
			{
				ID:   "log1",
				Type: "log",
				Data: map[string]interface{}{"message": "got it"},
			},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "sp"},
			{ID: "e2", Source: "sp", Target: "log1"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, nodes, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		started := 0
		for _, ev := range events {
			if ev.NodeID == "log1" && ev.Message == "got it" {
				started++
			}
		}
		if started == 3 {
			if nodes["log1"] != "ok" {
				t.Fatalf("log node status = %q", nodes["log1"])
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	_, _, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
	t.Fatalf("expected 3 log message events; events=%v", events)
}
