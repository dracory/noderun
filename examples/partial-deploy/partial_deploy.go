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

const workflowID = "partial-deploy"

func run() error {
	port, err := freePort()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/hello", port)

	engine := noderunner.NewWorkflowEngine()
	if _, err := engine.DeployWorkflowPartial(context.Background(), partialWorkflow(port, "Hello v1!"), nil); err != nil {
		return fmt.Errorf("deploy v1: %w", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	body, err := getWithRetry(url, 5*time.Second)
	if err != nil {
		return err
	}
	fmt.Println(body) // Hello v1!

	// Only the template text changed — the trigger node's config is
	// identical, so its listener survives the partial deploy and keeps
	// answering on the same port.
	if _, err := engine.DeployWorkflowPartial(context.Background(), partialWorkflow(port, "Hello v2!"), nil); err != nil {
		return fmt.Errorf("partial deploy v2: %w", err)
	}

	body, err = getWithRetry(url, 5*time.Second)
	if err != nil {
		return err
	}
	fmt.Println(body) // Hello v2!
	return nil
}

func partialWorkflow(port int, greeting string) nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("Partial Deploy").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("http-in", "httpTrigger").SetData(map[string]interface{}{
			"method":   "GET",
			"path":     "/hello",
			"listener": "own",
			"port":     port,
		}),
		nodestore.NewNode("template-body", "template").SetData(map[string]interface{}{
			"syntax":   "mustache",
			"template": greeting,
			"field":    "payload",
		}),
		nodestore.NewNode("http-out", "httpResponse").SetData(map[string]interface{}{
			"statusCode": 200,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-in-template", "http-in", "template-body"),
		nodestore.NewEdge("edge-template-out", "template-body", "http-out"),
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

// getWithRetry polls the URL until the listener answers.
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
