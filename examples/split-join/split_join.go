package main

import (
	"context"
	"fmt"
	"time"

	_ "github.com/dracory/nodebase/debug"
	_ "github.com/dracory/nodebase/inject"
	_ "github.com/dracory/nodebase/join"
	_ "github.com/dracory/nodebase/split"
	noderunner "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

const workflowID = "split-join"

func run() error {
	workflow := splitJoinWorkflow()
	engine := noderunner.NewWorkflowEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		return fmt.Errorf("deploy workflow: %w", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	// The split node emits three items (1, 2, 3) and the join node
	// reassembles them into the array [1,2,3].
	for _, want := range []string{"1", "2", "3", "[1,2,3]"} {
		if err := waitForDebugMessage(engine, workflowID, want, 3*time.Second); err != nil {
			return err
		}
	}
	return nil
}

func splitJoinWorkflow() nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("Split Join").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inject-array", "inject").SetData(map[string]interface{}{
			"payload":     "[1, 2, 3]",
			"payloadType": "json",
			"once":        true,
			"repeatType":  "none",
		}),
		nodestore.NewNode("split-items", "split").SetData(map[string]interface{}{}),
		nodestore.NewNode("debug-each", "debug").SetData(map[string]interface{}{
			"complete": "payload",
			"console":  true,
		}),
		nodestore.NewNode("join-items", "join").SetData(map[string]interface{}{
			"mode": "auto",
		}),
		nodestore.NewNode("debug-joined", "debug").SetData(map[string]interface{}{
			"complete": "payload",
			"console":  true,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-inject-split", "inject-array", "split-items"),
		nodestore.NewEdge("edge-split-each", "split-items", "debug-each"),
		nodestore.NewEdge("edge-split-join", "split-items", "join-items"),
		nodestore.NewEdge("edge-join-debug", "join-items", "debug-joined"),
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
