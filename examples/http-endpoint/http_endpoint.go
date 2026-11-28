package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	_ "github.com/dracory/nodebase/httpresponse"
	_ "github.com/dracory/nodebase/httptrigger"
	_ "github.com/dracory/nodebase/template"
	noderunner "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

const workflowID = "http-endpoint"

func run() error {
	port, err := freePort()
	if err != nil {
		return err
	}

	workflow := httpEndpointWorkflow(port)
	engine := noderunner.NewWorkflowEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		return fmt.Errorf("deploy workflow: %w", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	body, err := getWithRetry(fmt.Sprintf("http://127.0.0.1:%d/hello?name=world", port), 5*time.Second)
	if err != nil {
		return err
	}
	fmt.Println(body)
	return nil
}

func httpEndpointWorkflow(port int) nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("HTTP Endpoint").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		// Own listener on a private port; GET /hello?name=world puts the
		// query object {"name": "world"} into msg.payload.
		nodestore.NewNode("http-in", "httpTrigger").SetData(map[string]interface{}{
			"method":   "GET",
			"path":     "/hello",
			"listener": "own",
			"port":     port,
		}),
		nodestore.NewNode("template-greeting", "template").SetData(map[string]interface{}{
			"syntax":   "mustache",
			"template": "Hello {{payload.name}}!",
			"field":    "payload",
		}),
		// Answers the pending request correlated by msgId in metadata.
		nodestore.NewNode("http-out", "httpResponse").SetData(map[string]interface{}{
			"statusCode": 200,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-in-template", "http-in", "template-greeting"),
		nodestore.NewEdge("edge-template-out", "template-greeting", "http-out"),
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

// getWithRetry polls the URL until the trigger listener is up.
func getWithRetry(url string, timeout time.Duration) (string, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			if resp == nil {
				lastErr = fmt.Errorf("empty HTTP response")
				time.Sleep(50 * time.Millisecond)
				continue
			}
			body, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return string(body), err
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	return "", fmt.Errorf("endpoint never came up: %w", lastErr)
}
