package main

import (
	"context"
	"fmt"
	"time"

	_ "github.com/dracory/nodebase/debug"
	_ "github.com/dracory/nodebase/inject"
	noderunner "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

const workflowID = "hello-world"

func run() error {
	workflow := helloWorldWorkflow()
	engine := noderunner.NewWorkflowEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		return fmt.Errorf("deploy workflow: %w", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	return waitForDebugMessage(engine, workflowID, "Hello world!", 3*time.Second)
}

func helloWorldWorkflow() nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("Hello World").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inject-hello", "inject").SetData(map[string]interface{}{
			"payload":     "Hello world!",
			"payloadType": "string",
			"once":        true,
			"repeatType":  "none",
		}),
		nodestore.NewNode("debug-output", "debug").SetData(map[string]interface{}{
			"complete": "payload",
			"console":  true,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-inject-debug", "inject-hello", "debug-output"),
	})
}

func waitForDebugMessage(engine *noderunner.WorkflowEngine, workflowID, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, _, events, _ := engine.GetWorkflowSnapshot(workflowID, 0)
		for _, event := range events {
			if event.Level == "debug" && event.Message == want {
				return nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for debug message %q", want)
}
