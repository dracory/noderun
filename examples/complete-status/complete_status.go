package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	_ "github.com/dracory/nodebase/change"
	_ "github.com/dracory/nodebase/complete"
	_ "github.com/dracory/nodebase/debug"
	_ "github.com/dracory/nodebase/inject"
	_ "github.com/dracory/nodebase/status"
	noderunner "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

const workflowID = "complete-status"

func run() error {
	workflow := completeStatusWorkflow()
	engine := noderunner.NewWorkflowEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		return fmt.Errorf("deploy workflow: %w", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	// Four lines are expected: the main branch payload, the change
	// node's two status transitions, and the complete branch's message.
	// The status/complete observers run in their own traversals, so
	// their relative order is not guaranteed — only presence matters.
	for _, want := range []string{
		"doing work",
		`"status":"running"`,
		`"status":"ok"`,
		"main branch finished",
	} {
		if err := waitForDebugMessage(engine, workflowID, want, 3*time.Second); err != nil {
			return err
		}
	}
	return nil
}

func completeStatusWorkflow() nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("Complete Status").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inject-start", "inject").SetData(map[string]interface{}{
			"payload":     "work",
			"payloadType": "string",
			"once":        true,
			"repeatType":  "none",
		}),
		nodestore.NewNode("change-main", "change").SetData(map[string]interface{}{
			"rules": []interface{}{
				map[string]interface{}{
					"op":    "set",
					"field": "payload",
					"value": "doing work",
				},
			},
		}),
		nodestore.NewNode("debug-main", "debug").SetData(map[string]interface{}{
			"complete": "payload",
			"console":  true,
		}),

		// Status observer: scoped to the change node only, so the
		// observer branches' own transitions do not add noise. Prints
		// the whole {nodeId, status} object per transition.
		nodestore.NewNode("status-watch", "status").SetData(map[string]interface{}{
			"scope": "selected",
			"nodes": []interface{}{"change-main"},
		}),
		nodestore.NewNode("debug-status", "debug").SetData(map[string]interface{}{
			"complete": "payload.status",
			"console":  true,
		}),

		// Complete observer: fires once the injected message's main
		// traversal has drained. Its payload is a clone of the
		// completed message, so a change node writes the human-readable
		// line before printing.
		nodestore.NewNode("complete-watch", "complete").SetData(map[string]interface{}{
			"scope": "selected",
			"nodes": []interface{}{"inject-start"},
		}),
		nodestore.NewNode("change-complete", "change").SetData(map[string]interface{}{
			"rules": []interface{}{
				map[string]interface{}{
					"op":    "set",
					"field": "payload",
					"value": "main branch finished",
				},
			},
		}),
		nodestore.NewNode("debug-complete", "debug").SetData(map[string]interface{}{
			"complete": "payload",
			"console":  true,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-inject-change", "inject-start", "change-main"),
		nodestore.NewEdge("edge-change-debug", "change-main", "debug-main"),
		nodestore.NewEdge("edge-status-debug", "status-watch", "debug-status"),
		nodestore.NewEdge("edge-complete-change", "complete-watch", "change-complete"),
		nodestore.NewEdge("edge-change-complete-debug", "change-complete", "debug-complete"),
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
