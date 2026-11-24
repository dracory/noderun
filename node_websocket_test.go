package noderunner_test

import (
	"context"
	"fmt"
	. "github.com/dracory/noderun"
	"github.com/dracory/nodebase"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// TestWebSocketInReceives deploys websocketIn -> log, dials the endpoint
// with a coder/websocket client, sends a message and asserts the log
// node completed.
func TestWebSocketInReceives(t *testing.T) {
	port := freeTCPPort(t)
	workflow := &Workflow{
		ID:   "test-wsin-flow",
		Name: "WS In Flow",
		Nodes: []Node{
			{ID: "win", Type: "websocketIn", Data: map[string]interface{}{"path": "/ws", "port": float64(port)}},
			{ID: "lg", Type: "log", Data: map[string]interface{}{"message": "got"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "win", Target: "lg"},
		},
	}
	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	statuses := waitStatus(t, engine, workflow.ID, 2*time.Second, func(n map[string]string) bool {
		return n["win"] == "ok"
	})
	if statuses["win"] != "ok" {
		t.Fatalf("websocketIn never activated; statuses=%v", statuses)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, fmt.Sprintf("ws://127.0.0.1:%d/ws", port), nil)
	if err != nil {
		t.Fatalf("ws dial failed: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")

	if err := conn.Write(ctx, websocket.MessageText, []byte("hello ws")); err != nil {
		t.Fatalf("ws write failed: %v", err)
	}

	statuses = waitStatus(t, engine, workflow.ID, 2*time.Second, func(n map[string]string) bool {
		return n["lg"] == "ok"
	})
	if statuses["lg"] != "ok" {
		t.Fatalf("log never reached ok; statuses=%v", statuses)
	}
}

// TestWebSocketOutBroadcast deploys websocketIn (no downstream) plus
// inject -> websocketOut(broadcast), dials a client and asserts it
// receives the broadcast payload.
func TestWebSocketOutBroadcast(t *testing.T) {
	port := freeTCPPort(t)
	workflow := &Workflow{
		ID:   "test-wsout-flow",
		Name: "WS Out Flow",
		Nodes: []Node{
			{ID: "win", Type: "websocketIn", Data: map[string]interface{}{"path": "/ws", "port": float64(port)}},
			{ID: "inj", Type: "inject", Data: map[string]interface{}{"payload": `"pong"`, "once": false}},
			{ID: "wout", Type: "websocketOut", Data: map[string]interface{}{"target": "broadcast"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "wout"},
		},
	}
	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	statuses := waitStatus(t, engine, workflow.ID, 2*time.Second, func(n map[string]string) bool {
		return n["win"] == "ok"
	})
	if statuses["win"] != "ok" {
		t.Fatalf("websocketIn never activated; statuses=%v", statuses)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, fmt.Sprintf("ws://127.0.0.1:%d/ws", port), nil)
	if err != nil {
		t.Fatalf("ws dial failed: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")

	// Wait until the session is registered, then inject a message
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		sessions := engine.GetOrCreateNodeState(workflow.ID, nodebase.WSSessionsKey, func() interface{} {
			return nodebase.NewWSSessions()
		}).(*nodebase.WSSessions)
		n := len(sessions.All())
		if n > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if _, _, err := engine.InjectMessage(workflow.ID, "inj", &Message{Payload: "pong", Timestamp: time.Now()}); err != nil {
		t.Fatalf("inject failed: %v", err)
	}

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("ws read failed: %v", err)
	}
	if string(data) != "pong" {
		t.Fatalf("expected broadcast payload 'pong', got %q", data)
	}
}
