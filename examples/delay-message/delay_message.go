package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	_ "github.com/dracory/nodebase/debug"
	_ "github.com/dracory/nodebase/delay"
	_ "github.com/dracory/nodebase/inject"
	noderunner "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

const workflowID = "delay-message"

const delayMilliseconds = 500

func run() error {
	workflow := delayMessageWorkflow()
	start := time.Now()

	engine := noderunner.NewWorkflowEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		return fmt.Errorf("deploy workflow: %w", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	if err := waitForDebugMessage(engine, workflowID, "Hello after a delay!", 3*time.Second); err != nil {
		return err
	}

	// The delay node must actually have held the message back: if the
	// print arrives almost instantly, the flow acted as a pass-through.
	elapsed := time.Since(start)
	if elapsed < (delayMilliseconds-100)*time.Millisecond {
		return fmt.Errorf("message arrived after %v — the delay node did not hold it back", elapsed)
	}
	return nil
}

func delayMessageWorkflow() nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("Delay Message").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inject-hello", "inject").SetData(map[string]interface{}{
			"payload":     "Hello after a delay!",
			"payloadType": "string",
			"once":        true,
			"repeatType":  "none",
		}),
		nodestore.NewNode("delay-half-second", "delay").SetData(map[string]interface{}{
			"pauseType":    "delay",
			"timeout":      float64(delayMilliseconds),
			"timeoutUnits": "milliseconds",
		}),
		nodestore.NewNode("debug-output", "debug").SetData(map[string]interface{}{
			"complete": "payload",
			"console":  true,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-inject-delay", "inject-hello", "delay-half-second"),
		nodestore.NewEdge("edge-delay-debug", "delay-half-second", "debug-output"),
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
