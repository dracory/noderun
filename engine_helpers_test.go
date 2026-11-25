package noderunner_test

import (
	"fmt"
	"github.com/dracory/nodebase"
	_ "github.com/dracory/nodebase/builtin"
	. "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
	"testing"
	"time"
)

// Test helpers ported from flowrunner's node_queue_test.go and
// node_link_test.go (they are used by the ported engine/queue/component
// test files).

// queueTestFlow builds inj -> queue -> dbg. The queue node carries
// the given Data (name/hold/capacity/policy); dbg is a debug node whose
// event log records delivery order.
func queueTestFlow(id string, queueData map[string]interface{}) nodestore.WorkflowInterface {
	if queueData == nil {
		queueData = map[string]interface{}{}
	}
	if _, ok := queueData["name"]; !ok {
		queueData["name"] = "staging"
	}
	return nodestore.NewWorkflow(id).SetID(id).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inj", "junction").SetData(map[string]interface{}{}),
		nodestore.NewNode("f", "queue").SetData(queueData),
		nodestore.NewNode("dbg", "debug").SetData(map[string]interface{}{}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("e1", "inj", "f"),
		nodestore.NewEdge("e2", "f", "dbg"),
	})
}

func injectInto(t *testing.T, engine *Engine, workflowID, data string) {
	t.Helper()
	msgID, _, err := engine.InjectMessage(workflowID, "inj", nodestore.NewMessage().SetPayload(data))
	if err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	engine.DiscardPending(msgID)
}

// waitForQueueCount polls the live queue until it reports n
// items or the deadline passes.
func waitForQueueCount(t *testing.T, engine *Engine, workflowID string, n int, timeout time.Duration) nodebase.QueueInspector {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var q nodebase.QueueInspector
	for time.Now().Before(deadline) {
		if h, ok := engine.QueueHandle(workflowID, "f"); ok {
			q = h
			if q.Count() == n {
				return q
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	return q
}

// injectSequentially injects payloads one at a time, waiting for each
// to appear in the queue before sending the next. InjectMessage runs
// its upstream traversal on a separate goroutine, so back-to-back
// injects race at queue.Execute — the queue can only preserve
// *arrival* order, and the FIFO assertions need deterministic arrivals.
func injectSequentially(t *testing.T, engine *Engine, workflowID string, capacity int, payloads ...string) {
	t.Helper()
	for i, data := range payloads {
		injectInto(t, engine, workflowID, data)
		want := i + 1
		if capacity > 0 && want > capacity {
			want = capacity
		}
		// For bounded queues the depth caps out — wait for the newest
		// payload to be the tail instead of for a count increase.
		deadline := time.Now().Add(3 * time.Second)
		for {
			q := waitForQueueCount(t, engine, workflowID, want, 3*time.Second)
			if q == nil {
				t.Fatalf("queue never appeared waiting for %q", data)
			}
			items := q.Items()
			if len(items) == want && fmt.Sprint(items[len(items)-1].Payload.GetPayload()) == data {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("queue never staged %q: items=%v", data, items)
			}
			time.Sleep(15 * time.Millisecond)
		}
	}
}

// waitForStatus polls the workflow snapshot until the node reaches the
// wanted status or the deadline expires.
func waitForStatus(engine *Engine, workflowID, nodeID, want string, timeout time.Duration) (map[string]string, []NodeEvent) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, nodes, events, _ := engine.GetWorkflowSnapshot(workflowID, 0)
		if nodes[nodeID] == want {
			return nodes, events
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, nodes, events, _ := engine.GetWorkflowSnapshot(workflowID, 0)
	return nodes, events
}

// debugValues returns the debug node's emitted messages in order.
func debugValues(engine *Engine, workflowID string) []string {
	_, _, events, _ := engine.GetWorkflowSnapshot(workflowID, 0)
	var out []string
	for _, ev := range events {
		if ev.NodeID == "dbg" && ev.Level == "debug" {
			out = append(out, ev.Message)
		}
	}
	return out
}

func waitForDebugValues(engine *Engine, workflowID string, n int, timeout time.Duration) []string {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := debugValues(engine, workflowID); len(got) >= n {
			return got
		}
		time.Sleep(15 * time.Millisecond)
	}
	return debugValues(engine, workflowID)
}

// waitForEvents polls until pred(events) holds or the deadline passes.
func waitForEvents(engine *Engine, workflowID string, timeout time.Duration, pred func(NodeEvent) bool) []NodeEvent {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, _, events, _ := engine.GetWorkflowSnapshot(workflowID, 0)
		for _, ev := range events {
			if pred(ev) {
				return events
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, _, events, _ := engine.GetWorkflowSnapshot(workflowID, 0)
	return events
}

// countEvents returns how many workflow events match pred.
func countEvents(events []NodeEvent, pred func(NodeEvent) bool) int {
	n := 0
	for _, ev := range events {
		if pred(ev) {
			n++
		}
	}
	return n
}
