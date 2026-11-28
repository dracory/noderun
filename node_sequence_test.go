package noderunner_test

import (
	"context"
	"crypto/tls"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/dracory/nodebase"
	. "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

// sequenceCaptureHost is a minimal NodeHost for direct sequence-node
// tests. Node-RED batch/sort nodes re-emit completed groups as multiple
// parts-tagged messages through ExecuteDownstream rather than returning
// one array from Execute, so the host capture is the meaningful output.
type sequenceCaptureHost struct {
	states  map[string]interface{}
	mu      sync.Mutex
	emitted []nodestore.MessageInterface
}

func newSequenceCaptureHost() *sequenceCaptureHost {
	return &sequenceCaptureHost{states: map[string]interface{}{}}
}

func (h *sequenceCaptureHost) Emit(wf, nodeID string, p nodestore.MessageInterface, ports []string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.emitted = append(h.emitted, p)
	return nil
}
func (h *sequenceCaptureHost) EmitEvent(wf, nodeID, name, level, message string) {}
func (h *sequenceCaptureHost) InjectMessage(wf, from string, p nodestore.MessageInterface) (string, chan nodestore.MessageInterface, error) {
	return "", nil, nil
}
func (h *sequenceCaptureHost) ResolveResponse(id string, p nodestore.MessageInterface) error {
	return nil
}
func (h *sequenceCaptureHost) DiscardPending(id string) {}
func (h *sequenceCaptureHost) LookupNode(wf, nodeID string) (nodestore.NodeInterface, bool) {
	return nil, false
}
func (h *sequenceCaptureHost) RunningFlow(wf string) (nodestore.WorkflowInterface, bool) {
	return nodestore.NewWorkflow("wf"), true
}
func (h *sequenceCaptureHost) RunContext(wf string) context.Context { return context.Background() }
func (h *sequenceCaptureHost) GetOrCreateNodeState(wf, nodeID string, factory func() interface{}) interface{} {
	key := wf + "/" + nodeID
	if state, ok := h.states[key]; ok {
		return state
	}
	state := factory()
	h.states[key] = state
	return state
}
func (h *sequenceCaptureHost) ExecuteDownstream(ctx context.Context, wf, from string, workflow nodestore.WorkflowInterface, p nodestore.MessageInterface, ports []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.emitted = append(h.emitted, p)
}
func (h *sequenceCaptureHost) NodeEnv(wf, nodeID, name string) (interface{}, bool) {
	return nil, false
}
func (h *sequenceCaptureHost) ResolvedContextStore() nodebase.ContextStore { return nil }
func (h *sequenceCaptureHost) LoadTLSConfig(wf, ref string) (*tls.Config, error) {
	return nil, nil
}
func (h *sequenceCaptureHost) FileRootPath() string                  { return "" }
func (h *sequenceCaptureHost) ExecAllowed() bool                     { return false }
func (h *sequenceCaptureHost) SharedHTTPRouter() nodebase.HTTPRouter { return nil }
func (h *sequenceCaptureHost) HTTPAuthFunc() func(*http.Request) (string, bool) {
	return nil
}
func (h *sequenceCaptureHost) QueueService() nodebase.QueueService { return nil }

func (h *sequenceCaptureHost) emittedMessages() []nodestore.MessageInterface {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]nodestore.MessageInterface(nil), h.emitted...)
}

func (h *sequenceCaptureHost) waitForMessages(count int, timeout time.Duration) []nodestore.MessageInterface {
	deadline := time.Now().Add(timeout)
	for len(h.emittedMessages()) < count && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	return h.emittedMessages()
}

var _ nodebase.NodeHost = (*sequenceCaptureHost)(nil)

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
	out = requireNonNilMessage(t, out)
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
	out = requireNonNilMessage(t, out)
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
	out = requireNonNilMessage(t, out)
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

// TestJoinAutoNoParts verifies auto mode drops messages lacking parts
// metadata. Node-RED warns "cannot join in 'auto' mode" and does not
// pass them through. (nodebase join parity change.)
func TestJoinAutoNoParts(t *testing.T) {
	engine := NewEngine()
	node := NewJoinNode("j", &JoinConfig{}, engine, "wf")
	in := &Message{Payload: "solo"}
	out, err := node.Execute(context.Background(), in)
	if err != nil || out != nil {
		t.Fatalf("expected no-parts message to be dropped, got out=%v err=%v", out, err)
	}
}

// TestJoinCustomCount verifies custom mode flushes at the configured
// count. Node-RED does not partition custom groups by key/topic: key is
// only the object-build key selector, so the second message completes
// the group even when its topic differs. Message property paths also
// follow Node-RED semantics — payload fields are addressed as
// "payload.<field>". (nodebase join parity change.)
func TestJoinCustomCount(t *testing.T) {
	engine := NewEngine()
	node := NewJoinNode("j", &JoinConfig{
		Mode:     "custom",
		Key:      "topic",
		Property: "payload.val",
		Count:    2,
		Build:    "string",
		Joiner:   ",",
	}, engine, "wf")

	msg := func(topic, val string) *Message {
		return &Message{
			Payload:  map[string]interface{}{"val": val},
			Metadata: map[string]interface{}{"topic": topic},
		}
	}

	out, _ := node.Execute(context.Background(), msg("x", "1"))
	if out != nil {
		t.Fatalf("first message should be dropped, got %v", out)
	}
	out, err := node.Execute(context.Background(), msg("y", "2"))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out == nil {
		t.Fatal("second message should complete the shared custom group despite a different topic")
	}
	m, ok := out.GetPayload().(map[string]interface{})
	if !ok || m["val"] != "1,2" {
		t.Fatalf("expected joined string \"1,2\" at payload.val, got %v", out.GetPayload())
	}
}

// TestJoinCustomTimeout verifies an incomplete group flushes on timeout.
func TestJoinCustomTimeout(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-join-timeout",
		Name: "Join Timeout Flow",
		Nodes: []Node{
			// junction is the pass-through source; the placeholder
			// "process" node type was removed from nodebase.
			{ID: "src", Type: "junction", Data: map[string]interface{}{}},
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

// TestBatchCount verifies count mode re-emits a completed batch as a
// parts-tagged sequence through the downstream host and keeps the
// remainder buffered. Node-RED does not return one array message from
// Execute. (nodebase batch parity change.)
func TestBatchCount(t *testing.T) {
	host := newSequenceCaptureHost()
	node := NewBatchNode("b", &BatchConfig{Mode: "count", Count: 2}, host, "wf")

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
	if out != nil {
		t.Fatalf("completing message should also be dropped, got %v", out)
	}
	emitted := host.waitForMessages(2, 2*time.Second)
	if len(emitted) != 2 || emitted[0].GetPayload() != 1 || emitted[1].GetPayload() != 2 {
		t.Fatalf("expected emitted sequence [1 2], got %v", emitted)
	}
	for i, msg := range emitted {
		parts, ok := msg.GetMetadata()["parts"].(map[string]interface{})
		if !ok || parts["index"] != i || parts["count"] != 2 || parts["id"] == "" {
			t.Fatalf("emitted message %d has bad parts metadata: %v", i, parts)
		}
	}
	// Third message starts a new incomplete batch.
	out, _ = node.Execute(context.Background(), in(3))
	if out != nil {
		t.Fatalf("remainder should stay buffered, got %v", out)
	}
	if got := len(host.emittedMessages()); got != 2 {
		t.Fatalf("incomplete remainder emitted too early; total emitted=%d", got)
	}
}

// TestBatchCountOverlap verifies overlap keeps trailing messages for
// the next emitted parts sequence.
func TestBatchCountOverlap(t *testing.T) {
	host := newSequenceCaptureHost()
	node := NewBatchNode("b", &BatchConfig{Mode: "count", Count: 2, Overlap: 1}, host, "wf")

	in := func(v int) *Message {
		return &Message{Payload: v}
	}

	for _, v := range []int{1, 2, 3} {
		out, err := node.Execute(context.Background(), in(v))
		if err != nil || out != nil {
			t.Fatalf("input %d should be dropped while batching: out=%v err=%v", v, out, err)
		}
	}
	emitted := host.waitForMessages(4, 2*time.Second)
	if len(emitted) != 4 {
		t.Fatalf("expected two 2-message sequences, got %d messages", len(emitted))
	}
	want := []int{1, 2, 2, 3}
	for i, msg := range emitted {
		if msg.GetPayload() != want[i] {
			t.Fatalf("emitted payload %d: expected %d, got %v", i, want[i], msg.GetPayload())
		}
	}
}

// TestBatchConcat verifies concat mode concatenates complete topic
// sequences and re-emits their messages as one parts sequence; it does
// not flatten their payloads into one array. (nodebase batch parity
// change.)
func TestBatchConcat(t *testing.T) {
	host := newSequenceCaptureHost()
	node := NewBatchNode("b", &BatchConfig{
		Mode: "concat",
		Topics: []struct {
			Topic string `json:"topic"`
		}{{Topic: "t"}},
	}, host, "wf")

	first := partsMsg([]interface{}{1, 2}, "g", 0, 2)
	first.Metadata["topic"] = "t"
	second := partsMsg([]interface{}{3}, "g", 1, 2)
	second.Metadata["topic"] = "t"
	if out, err := node.Execute(context.Background(), first); err != nil || out != nil {
		t.Fatalf("first concat message should buffer: out=%v err=%v", out, err)
	}
	out, err := node.Execute(context.Background(), second)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out != nil {
		t.Fatalf("completing concat message should be dropped, got %v", out)
	}
	emitted := host.waitForMessages(2, 2*time.Second)
	if len(emitted) != 2 {
		t.Fatalf("expected the two input messages re-emitted as a sequence, got %d", len(emitted))
	}
	if got, ok := emitted[0].GetPayload().([]interface{}); !ok || len(got) != 2 {
		t.Fatalf("first payload should remain the two-element chunk, got %v", emitted[0].GetPayload())
	}
	if got, ok := emitted[1].GetPayload().([]interface{}); !ok || len(got) != 1 {
		t.Fatalf("second payload should remain the one-element chunk, got %v", emitted[1].GetPayload())
	}
	for i, msg := range emitted {
		parts, ok := msg.GetMetadata()["parts"].(map[string]interface{})
		if !ok || parts["index"] != i || parts["count"] != 2 {
			t.Fatalf("concatenated message %d has bad parts metadata: %v", i, parts)
		}
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
			// junction is the pass-through source; the placeholder
			// "process" node type was removed from nodebase.
			{ID: "src", Type: "junction", Data: map[string]interface{}{}},
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
