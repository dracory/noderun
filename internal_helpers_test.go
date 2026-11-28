package noderunner

// Shared helpers for the ported flowrunner tests that live in the
// internal package (they inspect unexported engine state). Mirrors of
// the noderunner_test helpers in engine_helpers_test.go — duplicated
// because internal and external test packages cannot share unexported
// symbols.

import (
	"net"
	"testing"
	"time"
)

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

// triggerWorkflow builds httpTrigger -> httpResponse on a random port.
func triggerWorkflow(id string, path string) *Workflow {
	return &Workflow{
		ID:   id,
		Name: "Trigger Flow",
		Nodes: []Node{
			{ID: "trig", Type: "httpTrigger", Data: map[string]interface{}{
				"method": "POST", "path": path, "port": 0,
			}},
			{ID: "resp", Type: "httpResponse", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "trig", Target: "resp"},
		},
	}
}

// func injectInto(t *testing.T, engine *Engine, workflowID, data string) {
// 	t.Helper()
// 	msgID, _, err := engine.InjectMessage(workflowID, "inj", nodestore.NewMessage().SetPayload(data))
// 	if err != nil {
// 		t.Fatalf("inject failed: %v", err)
// 	}
// 	engine.DiscardPending(msgID)
// }

// waitForQueueCount polls the live queue until it reports n items or
// the deadline passes.
// func waitForQueueCount(t *testing.T, engine *Engine, workflowID string, n int, timeout time.Duration) nodebase.QueueInspector {
// 	t.Helper()
// 	deadline := time.Now().Add(timeout)
// 	var q nodebase.QueueInspector
// 	for time.Now().Before(deadline) {
// 		if h, ok := engine.QueueHandle(workflowID, "f"); ok {
// 			q = h
// 			if q.Count() == n {
// 				return q
// 			}
// 		}
// 		time.Sleep(15 * time.Millisecond)
// 	}
// 	return q
// }

// freeTCPPort grabs an ephemeral port by opening and closing a listener.
func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot reserve port: %v", err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

// waitStatus polls the workflow status map until pred holds.
func waitStatus(t *testing.T, engine *Engine, workflowID string, timeout time.Duration, pred func(map[string]string) bool) map[string]string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, statuses, _, _ := engine.GetWorkflowSnapshot(workflowID, 0)
		if pred(statuses) {
			return statuses
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, statuses, _, _ := engine.GetWorkflowSnapshot(workflowID, 0)
	return statuses
}
