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

const (
	workflowID   = "manual-inject"
	injectNodeID = "inject-manual"
)

func run() error {
	workflow := manualInjectWorkflow()
	engine := noderunner.NewWorkflowEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		return fmt.Errorf("deploy workflow: %w", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	if err := engine.FireInjectNode(workflowID, injectNodeID); err != nil {
		return fmt.Errorf("fire inject node: %w", err)
	}

	return waitForDebugMessage(engine, workflowID, "Hello from a manual inject!", 3*time.Second)
}

func manualInjectWorkflow() nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("Manual Inject").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode(injectNodeID, "inject").SetData(map[string]interface{}{
			"payload":     "Hello from a manual inject!",
			"payloadType": "string",
			"once":        false,
			"repeatType":  "none",
		}),
		nodestore.NewNode("debug-output", "debug").SetData(map[string]interface{}{
			"complete": "payload",
			"console":  true,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-inject-debug", injectNodeID, "debug-output"),
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
