package noderunner

import (
	"context"
	"github.com/dracory/nodebase"
	"strings"
	"testing"
	"time"
)

// item builds a minimal queued message for a node.
func item(nodeID string) InboxItem {
	return InboxItem{NodeID: nodeID, Payload: &Message{Timestamp: time.Now()}}
}

// collect drains every queued item via next() in dequeue order.
func collect(t *testing.T, ib *inbox) []InboxItem {
	t.Helper()
	var out []InboxItem
	for {
		it, ok, err := ib.next()
		if err != nil {
			t.Fatalf("next failed: %v", err)
		}
		if !ok {
			return out
		}
		out = append(out, it)
	}
}

// TestInboxUnboundedPreservesFIFO verifies that with no configured
// capacity the inbox is a plain FIFO — the legacy sequential-BFS
// ordering must be unchanged.
func TestInboxUnboundedPreservesFIFO(t *testing.T) {
	ib := &inbox{
		backend: newMemInboxBackend(),
		configs: map[string]nodeInboxConfig{},
	}
	for _, id := range []string{"a", "b", "a", "c"} {
		res, err := ib.offer(item(id))
		if err != nil {
			t.Fatalf("offer failed: %v", err)
		}
		if res.dropped != nil || res.evicted != nil {
			t.Fatalf("unbounded inbox dropped a message: %+v", res)
		}
	}
	got := collect(t, ib)
	want := []string{"a", "b", "a", "c"}
	if len(got) != len(want) {
		t.Fatalf("expected %d items, got %d", len(want), len(got))
	}
	for i, it := range got {
		if it.NodeID != want[i] {
			t.Fatalf("position %d: expected %q, got %q", i, want[i], it.NodeID)
		}
	}
}

// TestInboxDropNewest verifies a full inbox rejects the incoming
// message while the queued one survives.
func TestInboxDropNewest(t *testing.T) {
	ib := &inbox{
		backend: newMemInboxBackend(),
		configs: map[string]nodeInboxConfig{
			"n": {capacity: 1, policy: nodebase.OverflowDropNewest},
		},
	}
	first := item("n")
	if _, err := ib.offer(first); err != nil {
		t.Fatalf("offer failed: %v", err)
	}
	res, err := ib.offer(item("n"))
	if err != nil {
		t.Fatalf("offer failed: %v", err)
	}
	if res.dropped == nil {
		t.Fatal("expected the second message to be dropped")
	}
	got := collect(t, ib)
	if len(got) != 1 || got[0].Payload != first.Payload {
		t.Fatalf("expected only the first message to survive, got %+v", got)
	}
}

// TestInboxDropOldest verifies a full inbox evicts the oldest queued
// message and accepts the incoming one.
func TestInboxDropOldest(t *testing.T) {
	ib := &inbox{
		backend: newMemInboxBackend(),
		configs: map[string]nodeInboxConfig{
			"n": {capacity: 1, policy: nodebase.OverflowDropOldest},
		},
	}
	first := item("n")
	second := item("n")
	if _, err := ib.offer(first); err != nil {
		t.Fatalf("offer failed: %v", err)
	}
	res, err := ib.offer(second)
	if err != nil {
		t.Fatalf("offer failed: %v", err)
	}
	if res.evicted == nil || res.evicted.Payload != first.Payload {
		t.Fatalf("expected the first message to be evicted, got %+v", res.evicted)
	}
	if res.dropped != nil {
		t.Fatal("drop-oldest must not report the incoming message as dropped")
	}
	got := collect(t, ib)
	if len(got) != 1 || got[0].Payload != second.Payload {
		t.Fatalf("expected only the newest message to survive, got %+v", got)
	}
}

// TestInboxRouteToCatch verifies a full inbox rejects the incoming
// message and reports it with the route-to-catch policy so the engine
// can dispatch it to catch nodebase.
func TestInboxRouteToCatch(t *testing.T) {
	ib := &inbox{
		backend: newMemInboxBackend(),
		configs: map[string]nodeInboxConfig{
			"n": {capacity: 1, policy: nodebase.OverflowRouteToCatch},
		},
	}
	if _, err := ib.offer(item("n")); err != nil {
		t.Fatalf("offer failed: %v", err)
	}
	res, err := ib.offer(item("n"))
	if err != nil {
		t.Fatalf("offer failed: %v", err)
	}
	if res.dropped == nil || res.policy != nodebase.OverflowRouteToCatch {
		t.Fatalf("expected a route-to-catch drop, got %+v", res)
	}
	if got := collect(t, ib); len(got) != 1 {
		t.Fatalf("expected 1 queued item, got %d", len(got))
	}
}

// TestInboxBlockPromotes verifies a blocked message waits for capacity
// and is admitted in arrival order as the queue drains.
func TestInboxBlockPromotes(t *testing.T) {
	ib := &inbox{
		backend: newMemInboxBackend(),
		configs: map[string]nodeInboxConfig{
			"n": {capacity: 1, policy: nodebase.OverflowBlock},
		},
	}
	first := item("n")
	second := item("n")
	if _, err := ib.offer(first); err != nil {
		t.Fatalf("offer failed: %v", err)
	}
	res, err := ib.offer(second)
	if err != nil {
		t.Fatalf("offer failed: %v", err)
	}
	if res.dropped != nil || res.evicted != nil {
		t.Fatalf("block policy must not drop messages, got %+v", res)
	}
	if len(ib.blocked) != 1 {
		t.Fatalf("expected 1 blocked message, got %d", len(ib.blocked))
	}
	got := collect(t, ib)
	if len(got) != 2 || got[0].Payload != first.Payload || got[1].Payload != second.Payload {
		t.Fatalf("expected both messages in order, got %+v", got)
	}
}

// TestParseOverflowPolicy covers accepted policy names and rejects
// unknown values.
func TestParseOverflowPolicy(t *testing.T) {
	for name, want := range map[string]nodebase.OverflowPolicy{
		"block":          nodebase.OverflowBlock,
		"drop-newest":    nodebase.OverflowDropNewest,
		"drop-oldest":    nodebase.OverflowDropOldest,
		"route-to-catch": nodebase.OverflowRouteToCatch,
		" Drop-Newest ":  nodebase.OverflowDropNewest, // normalized
	} {
		got, ok := nodebase.ParseOverflowPolicy(name)
		if !ok || got != want {
			t.Fatalf("nodebase.ParseOverflowPolicy(%q) = %q,%v; want %q", name, got, ok, want)
		}
	}
	if _, ok := nodebase.ParseOverflowPolicy("explode"); ok {
		t.Fatal("unknown policy must be rejected")
	}
	if _, ok := nodebase.ParseOverflowPolicy(nil); ok {
		t.Fatal("nil policy must be rejected")
	}
}

// TestNewInboxConfigResolution verifies the precedence chain:
// per-node config > flow-level defaults > engine defaults.
func TestNewInboxConfigResolution(t *testing.T) {
	engine := NewEngine()
	engine.InboxCapacity = 9
	engine.InboxPolicy = nodebase.OverflowDropNewest

	workflow := &Workflow{
		ID:   "cfg",
		View: map[string]interface{}{"inboxCapacity": float64(5), "inboxPolicy": "drop-oldest"},
		Nodes: []Node{
			{ID: "a", Type: "junction", Data: map[string]interface{}{}},
			{ID: "b", Type: "junction", Data: map[string]interface{}{
				"inboxCapacity": float64(2), "inboxPolicy": "route-to-catch",
			}},
		},
	}
	ib := engine.newInbox("cfg", workflow)
	defer ib.close()

	if got := ib.configFor("a"); got.capacity != 5 || got.policy != nodebase.OverflowDropOldest {
		t.Fatalf("flow defaults not applied to unconfigured node: %+v", got)
	}
	if got := ib.configFor("b"); got.capacity != 2 || got.policy != nodebase.OverflowRouteToCatch {
		t.Fatalf("per-node config must override flow defaults: %+v", got)
	}
}

// inboxTestWorkflow builds inj → split → junction → log plus an
// optional catch → log2 branch, with the given inbox config on the
// junction. The inject fires once with ["a","b","c"] so the split node
// pushes 3 messages into the junction's inbox.
func inboxTestWorkflow(id string, junctionData map[string]interface{}, withCatch bool) *Workflow {
	nodes := []Node{
		{ID: "inj", Type: "inject", Data: map[string]interface{}{
			"payload": `["a","b","c"]`, "once": true,
		}},
		{ID: "sp", Type: "split", Data: map[string]interface{}{"splt": ","}},
		{ID: "jn", Type: "junction", Data: junctionData},
		{ID: "log1", Type: "log", Data: map[string]interface{}{"message": "arrived"}},
	}
	edges := []Edge{
		{ID: "e1", Source: "inj", Target: "sp"},
		{ID: "e2", Source: "sp", Target: "jn"},
		{ID: "e3", Source: "jn", Target: "log1"},
	}
	if withCatch {
		nodes = append(nodes,
			Node{ID: "catch1", Type: "catch", Data: map[string]interface{}{"scope": "all"}},
			Node{ID: "log2", Type: "log", Data: map[string]interface{}{"message": "caught"}},
		)
		edges = append(edges, Edge{ID: "e4", Source: "catch1", Target: "log2"})
	}
	return &Workflow{ID: id, Name: id, Nodes: nodes, Edges: edges}
}

// countEvents returns how many workflow events match nodeID ("" = any)
// and a message substring.
func countInboxEvents(engine *Engine, workflowID, nodeID, substr string) int {
	_, _, events, _ := engine.GetWorkflowSnapshot(workflowID, 0)
	n := 0
	for _, ev := range events {
		if nodeID != "" && ev.NodeID != nodeID {
			continue
		}
		if strings.Contains(ev.Message, substr) {
			n++
		}
	}
	return n
}

// waitForEvents polls until pred(events) holds or the deadline passes.
func waitForInboxEvents(engine *Engine, workflowID string, timeout time.Duration, pred func([]NodeEvent) bool) []NodeEvent {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, _, events, _ := engine.GetWorkflowSnapshot(workflowID, 0)
		if pred(events) {
			return events
		}
		time.Sleep(15 * time.Millisecond)
	}
	_, _, events, _ := engine.GetWorkflowSnapshot(workflowID, 0)
	return events
}

func runInboxFlow(t *testing.T, workflow *Workflow) *Engine {
	t.Helper()
	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })
	return engine
}

// TestInboxDefaultUnboundedE2E is the regression guard: with no inbox
// config a 3-way fan-out still delivers every message, in order, with
// no overflow warnings.
func TestInboxDefaultUnboundedE2E(t *testing.T) {
	workflow := inboxTestWorkflow("test-inbox-default", map[string]interface{}{}, false)
	engine := runInboxFlow(t, workflow)

	waitForInboxEvents(engine, workflow.ID, 3*time.Second, func(events []NodeEvent) bool {
		n := 0
		for _, ev := range events {
			if ev.NodeID == "log1" && ev.Message == "arrived" {
				n++
			}
		}
		return n >= 3
	})
	time.Sleep(50 * time.Millisecond)
	if got := countInboxEvents(engine, workflow.ID, "log1", "arrived"); got != 3 {
		t.Fatalf("expected 3 arrivals, got %d", got)
	}
	if got := countInboxEvents(engine, workflow.ID, "", "inbox full"); got != 0 {
		t.Fatalf("unbounded inbox must not warn, got %d warnings", got)
	}
}

// TestInboxDropNewestE2E verifies only the first of three offered
// messages executes and each rejection emits a warning.
func TestInboxDropNewestE2E(t *testing.T) {
	workflow := inboxTestWorkflow("test-inbox-drop-newest", map[string]interface{}{
		"inboxCapacity": float64(1), "inboxPolicy": "drop-newest",
	}, false)
	engine := runInboxFlow(t, workflow)

	waitForInboxEvents(engine, workflow.ID, 3*time.Second, func(events []NodeEvent) bool {
		return countInboxEvents(engine, workflow.ID, "", "inbox full") >= 2
	})
	time.Sleep(50 * time.Millisecond)
	if got := countInboxEvents(engine, workflow.ID, "log1", "arrived"); got != 1 {
		t.Fatalf("expected exactly 1 arrival, got %d", got)
	}
	if got := countInboxEvents(engine, workflow.ID, "jn", "dropped newest"); got != 2 {
		t.Fatalf("expected 2 drop-newest warnings, got %d", got)
	}
}

// TestInboxDropOldestE2E verifies the newest message wins: one arrival
// and two eviction warnings.
func TestInboxDropOldestE2E(t *testing.T) {
	workflow := inboxTestWorkflow("test-inbox-drop-oldest", map[string]interface{}{
		"inboxCapacity": float64(1), "inboxPolicy": "drop-oldest",
	}, false)
	engine := runInboxFlow(t, workflow)

	waitForInboxEvents(engine, workflow.ID, 3*time.Second, func(events []NodeEvent) bool {
		return countInboxEvents(engine, workflow.ID, "", "inbox full") >= 2
	})
	time.Sleep(50 * time.Millisecond)
	if got := countInboxEvents(engine, workflow.ID, "log1", "arrived"); got != 1 {
		t.Fatalf("expected exactly 1 arrival, got %d", got)
	}
	if got := countInboxEvents(engine, workflow.ID, "jn", "dropped oldest"); got != 2 {
		t.Fatalf("expected 2 drop-oldest warnings, got %d", got)
	}
}

// TestInboxBlockE2E verifies no message is lost under the block policy:
// all three offers eventually execute despite capacity 1.
func TestInboxBlockE2E(t *testing.T) {
	workflow := inboxTestWorkflow("test-inbox-block", map[string]interface{}{
		"inboxCapacity": float64(1), "inboxPolicy": "block",
	}, false)
	engine := runInboxFlow(t, workflow)

	waitForInboxEvents(engine, workflow.ID, 3*time.Second, func(events []NodeEvent) bool {
		n := 0
		for _, ev := range events {
			if ev.NodeID == "log1" && ev.Message == "arrived" {
				n++
			}
		}
		return n >= 3
	})
	time.Sleep(50 * time.Millisecond)
	if got := countInboxEvents(engine, workflow.ID, "log1", "arrived"); got != 3 {
		t.Fatalf("block policy must deliver all messages, got %d", got)
	}
	if got := countInboxEvents(engine, workflow.ID, "", "inbox full"); got != 0 {
		t.Fatalf("block waits instead of dropping; got %d warnings", got)
	}
}

// TestInboxRouteToCatchE2E verifies the overflowed messages reach the
// in-scope catch node as error events end-to-end.
func TestInboxRouteToCatchE2E(t *testing.T) {
	workflow := inboxTestWorkflow("test-inbox-route-catch", map[string]interface{}{
		"inboxCapacity": float64(1), "inboxPolicy": "route-to-catch",
	}, true)
	engine := runInboxFlow(t, workflow)

	// Two of three messages overflow; each must fire the catch branch.
	waitForInboxEvents(engine, workflow.ID, 3*time.Second, func(events []NodeEvent) bool {
		n := 0
		for _, ev := range events {
			if ev.NodeID == "log2" && ev.Message == "caught" {
				n++
			}
		}
		return n >= 2
	})
	time.Sleep(50 * time.Millisecond)
	if got := countInboxEvents(engine, workflow.ID, "log1", "arrived"); got != 1 {
		t.Fatalf("expected exactly 1 arrival on the main branch, got %d", got)
	}
	if got := countInboxEvents(engine, workflow.ID, "log2", "caught"); got != 2 {
		t.Fatalf("expected 2 catch deliveries, got %d", got)
	}
	if got := countInboxEvents(engine, workflow.ID, "jn", "routed message to catch"); got != 2 {
		t.Fatalf("expected 2 route-to-catch warnings, got %d", got)
	}
}
