package noderunner_test

import (
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/dracory/nodebase/builtin"
	. "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// freePort reserves an ephemeral TCP port for the test listener
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// waitForListener polls until a TCP connection to port succeeds —
// the trigger/submit listener binds asynchronously during activation,
// so a fixed sleep after deploy races slow activations and flakes
// under load. Dialing is the only readiness signal that matters here.
func waitForListener(t *testing.T, port int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("listener on :%d not accepting within %v", port, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestHTTPTriggerFlow verifies the deploy → request → inject → httpResponse
// cycle: a real HTTP request to the trigger's listener must be answered by
// the flow's httpResponse node, not by the trigger itself.
func TestHTTPTriggerFlow(t *testing.T) {
	port := freePort(t)

	workflow := nodestore.NewWorkflow("HTTP Flow").SetID("test-http-flow").SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("trig", "httpTrigger").SetData(map[string]interface{}{
			"method": "GET",
			"path":   "/go",
			"port":   float64(port),
		}),
		nodestore.NewNode("resp", "httpResponse").SetData(map[string]interface{}{}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("e1", "trig", "resp"),
	})

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.GetID()) }()

	// Wait until the trigger listener is accepting connections
	url := fmt.Sprintf("http://127.0.0.1:%d/go?probe=1", port)
	var resp *http.Response
	var err error
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err = http.Get(url)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	resp = requireHTTPResponse(t, resp, err)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		t.Fatalf("response is not JSON: %v\nbody: %s", err, body)
	}

	// The body must be the injected GET payload (Node-RED's query
	// object), which proves the httpResponse node resolved the request.
	if data["probe"] != "1" {
		t.Fatalf("expected injected query payload in response, got: %s", body)
	}
}

// TestHTTPResponseStatusAndHeaders verifies the httpResponse node's
// statusCode/headers contract: config values are used, per-message
// overrides (Data.statusCode / Data.headers) take precedence, and a
// string body is sent raw rather than JSON-quoted.
func TestHTTPResponseStatusAndHeaders(t *testing.T) {
	port := freePort(t)

	// trig -> change (set payload to string + statusCode/headers) -> resp
	workflow := nodestore.NewWorkflow("HTTP Response Meta").SetID("test-http-resp-meta").SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("trig", "httpTrigger").SetData(map[string]interface{}{
			"method": "GET", "path": "/meta", "port": float64(port),
		}),
		nodestore.NewNode("chg", "change").SetData(map[string]interface{}{
			"rules": []interface{}{
				map[string]interface{}{"op": "set", "field": "payload", "value": `"plain text body"`},
				map[string]interface{}{"op": "set", "field": "msg.statusCode", "value": `418`},
				map[string]interface{}{"op": "set", "field": "msg.headers", "value": `{"X-Test":"yes"}`},
			},
		}),
		nodestore.NewNode("resp", "httpResponse").SetData(map[string]interface{}{
			"statusCode": float64(201), // config fallback — msg must win
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("e1", "trig", "chg"),
		nodestore.NewEdge("e2", "chg", "resp"),
	})

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.GetID()) }()

	url := fmt.Sprintf("http://127.0.0.1:%d/meta", port)
	var resp *http.Response
	var err error
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err = http.Get(url)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	resp = requireHTTPResponse(t, resp, err)
	defer resp.Body.Close()

	// nodestore.MessageInterface-level statusCode must override the node's config (201)
	if resp.StatusCode != http.StatusTeapot {
		t.Fatalf("expected 418 from message statusCode, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Test"); got != "yes" {
		t.Fatalf("expected X-Test: yes header, got %q", got)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "plain text body" {
		t.Fatalf("expected raw string body, got %q", body)
	}
}

// TestHTTPResponseConfigFallback verifies config statusCode is used
// when the message carries no override.
func TestHTTPResponseConfigFallback(t *testing.T) {
	port := freePort(t)

	workflow := nodestore.NewWorkflow("HTTP Response Config").SetID("test-http-resp-cfg").SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("trig", "httpTrigger").SetData(map[string]interface{}{
			"method": "GET", "path": "/cfg", "port": float64(port),
		}),
		nodestore.NewNode("chg", "change").SetData(map[string]interface{}{
			"rules": []interface{}{
				map[string]interface{}{"op": "set", "field": "payload", "value": `{"ok":true}`},
			},
		}),
		nodestore.NewNode("resp", "httpResponse").SetData(map[string]interface{}{
			"statusCode": float64(202),
			"headers":    map[string]interface{}{"X-From": "config"},
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("e1", "trig", "chg"),
		nodestore.NewEdge("e2", "chg", "resp"),
	})

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.GetID()) }()

	url := fmt.Sprintf("http://127.0.0.1:%d/cfg", port)
	var resp *http.Response
	var err error
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err = http.Get(url)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	resp = requireHTTPResponse(t, resp, err)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 from config statusCode, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("X-From"); got != "config" {
		t.Fatalf("expected X-From: config header, got %q", got)
	}
	body, _ := io.ReadAll(resp.Body)
	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil || data["ok"] != true {
		t.Fatalf("expected JSON body {ok:true}, got %s", body)
	}
}
