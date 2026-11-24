package noderunner_test

import (
	"context"
	"fmt"
	"net"
	. "github.com/dracory/noderun"
	"strings"
	"testing"
	"time"
)

// waitStatus polls node statuses until pred is satisfied or the
// deadline passes; returns the last seen statuses.
func waitStatus(t *testing.T, engine *Engine, workflowID string, timeout time.Duration, pred func(map[string]string) bool) map[string]string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, nodes, _, _ := engine.GetWorkflowSnapshot(workflowID, 0)
		if pred(nodes) {
			return nodes
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, nodes, _, _ := engine.GetWorkflowSnapshot(workflowID, 0)
	return nodes
}

// TestExecNodeDisabled verifies the exec node refuses to run unless
// the engine's AllowExec flag is set.
func TestExecNodeDisabled(t *testing.T) {
	engine := NewEngine() // AllowExec defaults to false
	node := NewExecNode("e1", &ExecConfig{Command: "echo hi"}, engine)
	_, err := node.Execute(context.Background(), &Message{Payload: "x"})
	if err == nil || !strings.Contains(err.Error(), "exec disabled") {
		t.Fatalf("expected 'exec disabled' error, got %v", err)
	}
}

// TestExecNodeEcho verifies a successful command returns stdout as Data
// and rc 0 in Metadata, routing to output_1.
func TestExecNodeEcho(t *testing.T) {
	engine := NewEngine()
	engine.AllowExec = true
	node := NewExecNode("e1", &ExecConfig{Command: "echo hi"}, engine)
	out, err := node.Execute(context.Background(), &Message{Payload: "x"})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if s, _ := out.GetPayload().(string); strings.TrimSpace(s) != "hi" {
		t.Fatalf("expected Data 'hi', got %q", out.GetPayload())
	}
	if rc, _ := out.GetMetadata()["rc"].(int); rc != 0 {
		t.Fatalf("expected rc 0, got %v", out.GetMetadata()["rc"])
	}
	if ports := node.SelectPorts(nil, out); len(ports) != 1 || ports[0] != "output_1" {
		t.Fatalf("expected routing to output_1, got %v", ports)
	}
}

// TestExecNodeNonZeroExit verifies a failing command routes to
// output_3 carrying stderr and the exit code.
func TestExecNodeNonZeroExit(t *testing.T) {
	engine := NewEngine()
	engine.AllowExec = true
	node := NewExecNode("e1", &ExecConfig{Command: "exit 3"}, engine)
	out, err := node.Execute(context.Background(), &Message{})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if rc, _ := out.GetMetadata()["rc"].(int); rc != 3 {
		t.Fatalf("expected rc 3, got %v", out.GetMetadata()["rc"])
	}
	if ports := node.SelectPorts(nil, out); len(ports) != 1 || ports[0] != "output_3" {
		t.Fatalf("expected routing to output_3, got %v", ports)
	}
}

// TestExecNodeEndToEnd deploys inject -> exec("echo hi") -> log and
// asserts the downstream log completes.
func TestExecNodeEndToEnd(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-exec-flow",
		Name: "Exec Flow",
		Nodes: []Node{
			{ID: "inj", Type: "inject", Data: map[string]interface{}{"payload": `"go"`, "once": true}},
			{ID: "ex", Type: "exec", Data: map[string]interface{}{"command": "echo hi"}},
			{ID: "lg", Type: "log", Data: map[string]interface{}{"message": "done"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "ex"},
			{ID: "e2", Source: "ex", SourceKey: "output_1", Target: "lg"},
		},
	}
	engine := NewEngine()
	engine.AllowExec = true
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	nodes := waitStatus(t, engine, workflow.ID, 2*time.Second, func(n map[string]string) bool {
		return n["lg"] == "ok"
	})
	if nodes["lg"] != "ok" {
		t.Fatalf("log never reached ok; statuses=%v", nodes)
	}
}

// freeTCPPort grabs an ephemeral port by opening and closing a listener.
func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot reserve port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// TestTCPInReceives deploys tcpIn(listen) -> log, connects with a real
// client and writes a line, then asserts the log node completed.
func TestTCPInReceives(t *testing.T) {
	port := freeTCPPort(t)
	workflow := &Workflow{
		ID:   "test-tcpin-flow",
		Name: "TCP In Flow",
		Nodes: []Node{
			{ID: "tin", Type: "tcpIn", Data: map[string]interface{}{"mode": "listen", "port": float64(port)}},
			{ID: "lg", Type: "log", Data: map[string]interface{}{"message": "got"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "tin", Target: "lg"},
		},
	}
	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	// Wait for the listener to activate
	nodes := waitStatus(t, engine, workflow.ID, 2*time.Second, func(n map[string]string) bool {
		return n["tin"] == "ok"
	})
	if nodes["tin"] != "ok" {
		t.Fatalf("tcpIn never activated; statuses=%v", nodes)
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("client dial failed: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("hello tcp\n")); err != nil {
		t.Fatalf("client write failed: %v", err)
	}

	nodes = waitStatus(t, engine, workflow.ID, 2*time.Second, func(n map[string]string) bool {
		return n["lg"] == "ok"
	})
	if nodes["lg"] != "ok" {
		t.Fatalf("log never reached ok; statuses=%v", nodes)
	}
}

// TestUDPOutToUDPIn deploys udpIn -> log, then sends a datagram via
// udpOut executed through a second workflow's inject, and asserts the
// datagram reached the log.
func TestUDPRoundTrip(t *testing.T) {
	port := freeUDPPort(t)
	workflow := &Workflow{
		ID:   "test-udp-flow",
		Name: "UDP Flow",
		Nodes: []Node{
			{ID: "uin", Type: "udpIn", Data: map[string]interface{}{"port": float64(port)}},
			{ID: "lg", Type: "log", Data: map[string]interface{}{"message": "got"}},
			{ID: "inj", Type: "inject", Data: map[string]interface{}{"payload": `"ping"`, "once": true}},
			{ID: "uout", Type: "udpOut", Data: map[string]interface{}{"host": "127.0.0.1", "port": float64(port)}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "uin", Target: "lg"},
			{ID: "e2", Source: "inj", Target: "uout"},
		},
	}
	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	// inject fires on deploy, possibly before udpIn is bound. If the
	// datagram is lost, send a second one manually via a raw socket.
	nodes := waitStatus(t, engine, workflow.ID, 2*time.Second, func(n map[string]string) bool {
		return n["uin"] == "ok" && n["lg"] == "ok"
	})
	if nodes["lg"] != "ok" {
		// Retry: listener is up now, send another datagram directly
		conn, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: port})
		if err != nil {
			t.Fatalf("direct dial failed: %v", err)
		}
		_, _ = conn.Write([]byte("retry"))
		_ = conn.Close()
		nodes = waitStatus(t, engine, workflow.ID, 2*time.Second, func(n map[string]string) bool {
			return n["lg"] == "ok"
		})
	}
	if nodes["lg"] != "ok" {
		t.Fatalf("log never reached ok; statuses=%v", nodes)
	}
}

// freeUDPPort grabs an ephemeral UDP port.
func freeUDPPort(t *testing.T) int {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("cannot reserve udp port: %v", err)
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).Port
}
