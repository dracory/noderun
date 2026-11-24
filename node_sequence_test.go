package noderunner_test

import (
	"context"
	. "github.com/dracory/noderun"
	"testing"
	"time"
)

// partsMsg builds a payload carrying split-style parts metadata.
func partsMsg(data interface{}, groupID string, index, count int) *Message {
	return &Message{
		Payload: data,
		Metadata: map[string]interface{}{
			"parts": map[string]interface{}{
				"id":    groupID,
				"type":  "array",
				"index": index,
				"count": count,
			},
		},
		Timestamp: time.Now(),
	}
}

// TestSortMessagePayload verifies msg-target sorting of a payload array.
func TestSortMessagePayload(t *testing.T) {
	node := NewSortNode("s", &SortConfig{}, nil, "")
	out, err := node.Execute(context.Background(), &Message{
		Payload: []interface{}{float64(3), float64(1), float64(2)},
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	arr := out.GetPayload().([]interface{})
	for i, want := range []float64{1, 2, 3} {
		if arr[i] != want {
			t.Fatalf("index %d: expected %v, got %v", i, want, arr[i])
		}
	}
}

// TestSortMessageDescending verifies desc ordering and string compare.
func TestSortMessageDescending(t *testing.T) {
	node := NewSortNode("s", &SortConfig{Order: "desc"}, nil, "")
	out, err := node.Execute(context.Background(), &Message{
		Payload: []interface{}{"a", "c", "b"},
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	arr := out.GetPayload().([]interface{})
	for i, want := range []string{"c", "b", "a"} {
		if arr[i] != want {
			t.Fatalf("index %d: expected %q, got %v", i, want, arr[i])
		}
	}
}

// TestSortMessageAsNum verifies numeric coercion of string payloads.
func TestSortMessageAsNum(t *testing.T) {
	node := NewSortNode("s", &SortConfig{AsNum: true}, nil, "")
	out, err := node.Execute(context.Background(), &Message{
		Payload: []interface{}{"10", "9", "100"},
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	arr := out.GetPayload().([]interface{})
	for i, want := range []string{"9", "10", "100"} {
		if arr[i] != want {
			t.Fatalf("index %d: expected %q, got %v", i, want, arr[i])
		}
	}
}

// TestJoinAutoArray verifies auto mode buffers a parts sequence and
// emits a single joined array message when the group completes.
func TestJoinAutoArray(t *testing.T) {
	engine := NewEngine()
	node := NewJoinNode("j", &JoinConfig{Mode: "auto", Build: "array"}, engine, "wf")

	out, err := node.Execute(context.Background(), partsMsg("a", "g1", 0, 3))
	if err != nil || out != nil {
		t.Fatalf("first message should be dropped, got out=%v err=%v", out, err)
	}
	out, err = node.Execute(context.Background(), partsMsg("c", "g1", 2, 3))
	if err != nil || out != nil {
		t.Fatalf("second message should be dropped, got out=%v err=%v", out, err)
	}
	out, err = node.Execute(context.Background(), partsMsg("b", "g1", 1, 3))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out == nil {
		t.Fatal("third message should complete the group")
	}
	arr, ok := out.GetPayload().([]interface{})
	if !ok || len(arr) != 3 {
		t.Fatalf("expected joined array of 3, got %v", out.GetPayload())
	}
	// Arrived out of order (a, c, b) but parts.index must restore order
	for i, want := range []string{"a", "b", "c"} {
		if arr[i] != want {
			t.Fatalf("index %d: expected %q, got %v", i, want, arr[i])
		}
	}
}

// TestJoinAutoNoParts verifies auto mode passes through messages
// lacking parts metadata.
func TestJoinAutoNoParts(t *testing.T) {
	engine := NewEngine()
	node := NewJoinNode("j", &JoinConfig{}, engine, "wf")
	in := &Message{Payload: "solo"}
	out, err := node.Execute(context.Background(), in)
	if err != nil || out != in {
		t.Fatalf("expected passthrough, got out=%v err=%v", out, err)
	}
}

// TestJoinCustomCount verifies custom mode groups by a key path and
// flushes at the configured count.
func TestJoinCustomCount(t *testing.T) {
	engine := NewEngine()
	node := NewJoinNode("j", &JoinConfig{
		Mode:   "custom",
		Key:    "topic",
		Count:  2,
		Build:  "string",
		Joiner: ",",
	}, engine, "wf")

	msg := func(topic, val string) *Message {
		return &Message{Payload: map[string]interface{}{"topic": topic, "val": val}}
	}

	out, _ := node.Execute(context.Background(), msg("x", "1"))
	if out != nil {
		t.Fatalf("first message should be dropped, got %v", out)
	}
	out, _ = node.Execute(context.Background(), msg("y", "other"))
	if out != nil {
		t.Fatalf("different topic must not complete the group, got %v", out)
	}
	// property defaults to "payload" → joined value is the whole data map;
	// use property "val" instead so the string join reads the field.
	node = NewJoinNode("j2", &JoinConfig{
		Mode:     "custom",
		Key:      "topic",
		Property: "val",
		Count:    2,
		Build:    "string",
		Joiner:   ",",
	}, engine, "wf")
	out, _ = node.Execute(context.Background(), msg("x", "1"))
	if out != nil {
		t.Fatalf("first message should be dropped, got %v", out)
	}
	out, err := node.Execute(context.Background(), msg("x", "2"))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	m, ok := out.GetPayload().(map[string]interface{})
	if !ok || m["val"] != "1,2" {
		t.Fatalf("expected joined string \"1,2\" at val, got %v", out.GetPayload())
	}
}

// TestJoinCustomTimeout verifies an incomplete group flushes on timeout.
func TestJoinCustomTimeout(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-join-timeout",
		Name: "Join Timeout Flow",
		Nodes: []Node{
			{ID: "src", Type: "process", Data: map[string]interface{}{}},
			{
				ID:   "jn",
				Type: "join",
				Data: map[string]interface{}{
					"mode":    "custom",
					"key":     "topic",
					"count":   5,
					"timeout": 0.05,
					"build":   "array",
				},
			},
			{ID: "log1", Type: "log", Data: map[string]interface{}{"message": "flushed"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "src", Target: "jn"},
			{ID: "e2", Source: "jn", Target: "log1"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	payload := &Message{Payload: map[string]interface{}{"topic": "t"}}
	msgID, _, err := engine.InjectMessage(workflow.ID, "src", payload)
	if err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	engine.DiscardPending(msgID)

	// The single message should time out (~50ms) and flush downstream.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, nodes, _, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		if nodes["log1"] == "ok" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, nodes, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
	t.Fatalf("timeout flush never reached log; statuses=%v events=%v", nodes, events)
}

// TestBatchCount verifies count mode emits one message per batch and
// keeps the remainder buffered.
func TestBatchCount(t *testing.T) {
	engine := NewEngine()
	node := NewBatchNode("b", &BatchConfig{Mode: "count", Count: 2}, engine, "wf")

	in := func(v int) *Message {
		return &Message{Payload: v}
	}

	out, _ := node.Execute(context.Background(), in(1))
	if out != nil {
		t.Fatalf("first message should be dropped, got %v", out)
	}
	out, err := node.Execute(context.Background(), in(2))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	arr, ok := out.GetPayload().([]interface{})
	if !ok || len(arr) != 2 || arr[0] != 1 || arr[1] != 2 {
		t.Fatalf("expected batch [1 2], got %v", out.GetPayload())
	}
	// Third message starts a new incomplete batch
	out, _ = node.Execute(context.Background(), in(3))
	if out != nil {
		t.Fatalf("remainder should stay buffered, got %v", out)
	}
}

// TestBatchCountOverlap verifies overlap keeps trailing messages for
// the next batch.
func TestBatchCountOverlap(t *testing.T) {
	engine := NewEngine()
	node := NewBatchNode("b", &BatchConfig{Mode: "count", Count: 2, Overlap: 1}, engine, "wf")

	in := func(v int) *Message {
		return &Message{Payload: v}
	}

	node.Execute(context.Background(), in(1))
	out, _ := node.Execute(context.Background(), in(2)) // batch [1 2], keeps [2]
	if out == nil {
		t.Fatal("expected first batch")
	}
	out, _ = node.Execute(context.Background(), in(3)) // batch [2 3]
	arr, ok := out.GetPayload().([]interface{})
	if !ok || len(arr) != 2 || arr[0] != 2 || arr[1] != 3 {
		t.Fatalf("expected overlapped batch [2 3], got %v", out.GetPayload())
	}
}

// TestBatchConcat verifies concat mode flattens a parts sequence into
// one message.
func TestBatchConcat(t *testing.T) {
	engine := NewEngine()
	node := NewBatchNode("b", &BatchConfig{Mode: "concat"}, engine, "wf")

	node.Execute(context.Background(), partsMsg([]interface{}{1, 2}, "g", 0, 2))
	out, err := node.Execute(context.Background(), partsMsg([]interface{}{3}, "g", 1, 2))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	arr, ok := out.GetPayload().([]interface{})
	if !ok || len(arr) != 3 {
		t.Fatalf("expected concatenated array of 3, got %v", out.GetPayload())
	}
}

// TestJoinIntegration deploys inject → split → join → log and asserts
// the three-element sequence is reassembled into exactly one message.
func TestJoinIntegration(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-join-flow",
		Name: "Join Flow",
		Nodes: []Node{
			{
				ID:   "inj",
				Type: "inject",
				Data: map[string]interface{}{
					"payload": `["a","b","c"]`,
					"once":    true,
				},
			},
			{ID: "sp", Type: "split", Data: map[string]interface{}{"splt": ","}},
			{ID: "jn", Type: "join", Data: map[string]interface{}{}},
			{ID: "log1", Type: "log", Data: map[string]interface{}{"message": "joined"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "sp"},
			{ID: "e2", Source: "sp", Target: "jn"},
			{ID: "e3", Source: "jn", Target: "log1"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	countLogStarts := func() int {
		_, _, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		n := 0
		for _, ev := range events {
			if ev.NodeID == "log1" && ev.Message == "joined" {
				n++
			}
		}
		return n
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if countLogStarts() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Settle, then assert exactly one message reached the log.
	time.Sleep(100 * time.Millisecond)
	if got := countLogStarts(); got != 1 {
		t.Fatalf("expected exactly 1 joined message, got %d", got)
	}
}

// TestSortSeqIntegration verifies seq mode buffers a parts sequence and
// re-emits all messages downstream.
func TestSortSeqIntegration(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-sort-seq-flow",
		Name: "Sort Seq Flow",
		Nodes: []Node{
			{ID: "src", Type: "process", Data: map[string]interface{}{}},
			{
				ID:   "st",
				Type: "sort",
				Data: map[string]interface{}{"target": "seq", "seqKey": "n"},
			},
			{ID: "log1", Type: "log", Data: map[string]interface{}{"message": "sorted"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "src", Target: "st"},
			{ID: "e2", Source: "st", Target: "log1"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	// Inject an out-of-order 3-message sequence through the source node.
	for i, n := range []int{3, 1, 2} {
		payload := partsMsg(map[string]interface{}{"n": n}, "g1", i, 3)
		msgID, _, err := engine.InjectMessage(workflow.ID, "src", payload)
		if err != nil {
			t.Fatalf("inject failed: %v", err)
		}
		engine.DiscardPending(msgID)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, _, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		started := 0
		for _, ev := range events {
			if ev.NodeID == "log1" && ev.Message == "sorted" {
				started++
			}
		}
		if started == 3 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, _, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
	t.Fatalf("expected 3 re-emitted messages at log; events=%v", events)
}
