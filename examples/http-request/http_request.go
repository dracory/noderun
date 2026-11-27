package main

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	_ "github.com/dracory/nodebase/debug"
	_ "github.com/dracory/nodebase/httprequest"
	_ "github.com/dracory/nodebase/httpresponse"
	_ "github.com/dracory/nodebase/httptrigger"
	_ "github.com/dracory/nodebase/inject"
	_ "github.com/dracory/nodebase/template"
	noderunner "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

const workflowID = "http-request"

func run() error {
	port, err := freePort()
	if err != nil {
		return err
	}

	workflow := httpRequestWorkflow(port)
	engine := noderunner.NewWorkflowEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		return fmt.Errorf("deploy workflow: %w", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	if err := waitForListener(port, 5*time.Second); err != nil {
		return err
	}
	if err := engine.FireInjectNode(workflowID, "inject-start"); err != nil {
		return fmt.Errorf("fire inject: %w", err)
	}

	return waitForDebugMessage(engine, workflowID, "Hello from the endpoint!", 5*time.Second)
}

// One workflow, two branches: the engine serves its own target endpoint
// and calls it — no external service needed.
func httpRequestWorkflow(port int) nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("HTTP Request").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		// Server side: GET /api → rendered greeting → 200 response.
		nodestore.NewNode("http-in", "httpTrigger").SetData(map[string]interface{}{
			"method":   "GET",
			"path":     "/api",
			"listener": "own",
			"port":     port,
		}),
		nodestore.NewNode("template-body", "template").SetData(map[string]interface{}{
			"syntax":   "mustache",
			"template": "Hello from the endpoint!",
			"field":    "payload",
		}),
		nodestore.NewNode("http-out", "httpResponse").SetData(map[string]interface{}{
			"statusCode": 200,
		}),

		// Client side: manual inject → httpRequest → debug. The response
		// payload is {statusCode, status, headers, body, ...}; JSON
		// bodies arrive already parsed.
		nodestore.NewNode("inject-start", "inject").SetData(map[string]interface{}{
			"payload":    "go",
			"repeatType": "none",
		}),
		nodestore.NewNode("http-call", "httpRequest").SetData(map[string]interface{}{
			"method": "GET",
			"url":    fmt.Sprintf("http://127.0.0.1:%d/api", port),
		}),
		nodestore.NewNode("debug-output", "debug").SetData(map[string]interface{}{
			"complete": "payload.body",
			"console":  true,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-in-template", "http-in", "template-body"),
		nodestore.NewEdge("edge-template-out", "template-body", "http-out"),
		nodestore.NewEdge("edge-inject-call", "inject-start", "http-call"),
		nodestore.NewEdge("edge-call-debug", "http-call", "debug-output"),
	})
}

// freePort reserves a port by binding and releasing :0.
func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("find free port: %w", err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// waitForListener waits until the trigger's own listener accepts TCP.
func waitForListener(port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("listener on port %d never came up", port)
}

// waitForDebugMessage waits until a debug event containing want appears.
func waitForDebugMessage(engine *noderunner.WorkflowEngine, workflowID, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, _, events, _ := engine.GetWorkflowSnapshot(workflowID, 0)
		for _, event := range events {
			if event.Level == "debug" && strings.Contains(event.Message, want) {
				return nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for debug message %q", want)
}
