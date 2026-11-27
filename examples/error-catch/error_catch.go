package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	_ "github.com/dracory/nodebase/catch"
	_ "github.com/dracory/nodebase/debug"
	_ "github.com/dracory/nodebase/function"
	_ "github.com/dracory/nodebase/inject"
	noderunner "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

const workflowID = "error-catch"

func run() error {
	workflow := errorCatchWorkflow()
	engine := noderunner.NewWorkflowEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		return fmt.Errorf("deploy workflow: %w", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	return waitForDebugMessage(engine, workflowID, "something went wrong", 3*time.Second)
}

func errorCatchWorkflow() nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("Error Catch").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inject-start", "inject").SetData(map[string]interface{}{
			"payload":     "input data",
			"payloadType": "string",
			"once":        true,
			"repeatType":  "none",
		}),
		nodestore.NewNode("function-fails", "function").SetData(map[string]interface{}{
			"func": "node.error('something went wrong'); return msg;",
		}),
		nodestore.NewNode("catch-errors", "catch").SetData(map[string]interface{}{
			"scope": "all",
		}),
		// The catch payload shape is {error: {message, source}, originalData}.
		// The debug node drills straight into it — no extraction node needed.
		nodestore.NewNode("debug-output", "debug").SetData(map[string]interface{}{
			"complete": "payload.error.message",
			"console":  true,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-inject-function", "inject-start", "function-fails"),
		nodestore.NewEdge("edge-catch-debug", "catch-errors", "debug-output"),
	})
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
	return fmt.Errorf("timed out waiting for debug message containing %q", want)
}
