package main

import (
	"context"
	"fmt"
	"time"

	_ "github.com/dracory/nodebase/change"
	_ "github.com/dracory/nodebase/debug"
	_ "github.com/dracory/nodebase/inject"
	noderunner "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

const workflowID = "change-payload"

func run() error {
	workflow := changePayloadWorkflow()
	engine := noderunner.NewWorkflowEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		return fmt.Errorf("deploy workflow: %w", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	return waitForDebugMessage(engine, workflowID, "Hello change node!", 3*time.Second)
}

func changePayloadWorkflow() nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("Change Payload").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inject-hello", "inject").SetData(map[string]interface{}{
			"payload":     "Hello world!",
			"payloadType": "string",
			"once":        true,
			"repeatType":  "none",
		}),
		nodestore.NewNode("change-message", "change").SetData(map[string]interface{}{
			"rules": []interface{}{
				map[string]interface{}{
					"op":          "replace",
					"field":       "payload",
					"pattern":     "world",
					"replacement": "change node",
				},
			},
		}),
		nodestore.NewNode("debug-output", "debug").SetData(map[string]interface{}{
			"complete": "payload",
			"console":  true,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-inject-change", "inject-hello", "change-message"),
		nodestore.NewEdge("edge-change-debug", "change-message", "debug-output"),
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
