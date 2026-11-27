package main

import (
	"context"
	"fmt"
	"time"

	_ "github.com/dracory/nodebase/change"
	_ "github.com/dracory/nodebase/debug"
	_ "github.com/dracory/nodebase/inject"
	_ "github.com/dracory/nodebase/switch"
	noderunner "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

const workflowID = "switch-routing"

func run() error {
	workflow := switchRoutingWorkflow()
	engine := noderunner.NewWorkflowEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		return fmt.Errorf("deploy workflow: %w", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	if err := waitForDebugMessage(engine, workflowID, "42 is hot (above 30)", 3*time.Second); err != nil {
		return err
	}
	return waitForDebugMessage(engine, workflowID, "10 is cold (30 or below)", 3*time.Second)
}

func switchRoutingWorkflow() nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("Switch Routing").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inject-hot", "inject").SetData(map[string]interface{}{
			"payload":     "42",
			"payloadType": "num",
			"once":        true,
			"repeatType":  "none",
		}),
		nodestore.NewNode("inject-cold", "inject").SetData(map[string]interface{}{
			"payload":     "10",
			"payloadType": "num",
			"once":        true,
			"repeatType":  "none",
		}),
		nodestore.NewNode("temperature-switch", "switch").SetData(map[string]interface{}{
			"mode":      "first",
			"otherwise": true,
			"rules": []interface{}{
				map[string]interface{}{
					"property":  "",
					"op":        "gt",
					"value":     "30",
					"valueType": "num",
				},
			},
		}),
		nodestore.NewNode("format-hot", "change").SetData(map[string]interface{}{
			"rules": []interface{}{
				map[string]interface{}{
					"op":        "set",
					"field":     "payload",
					"value":     `$string(payload) & " is hot (above 30)"`,
					"valueType": "jsonata",
				},
			},
		}),
		nodestore.NewNode("format-cold", "change").SetData(map[string]interface{}{
			"rules": []interface{}{
				map[string]interface{}{
					"op":        "set",
					"field":     "payload",
					"value":     `$string(payload) & " is cold (30 or below)"`,
					"valueType": "jsonata",
				},
			},
		}),
		nodestore.NewNode("debug-output", "debug").SetData(map[string]interface{}{
			"complete": "payload",
			"console":  true,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-hot-switch", "inject-hot", "temperature-switch"),
		nodestore.NewEdge("edge-cold-switch", "inject-cold", "temperature-switch"),
		nodestore.NewEdge("edge-switch-hot", "temperature-switch", "format-hot").SetSourceKey("output_1"),
		nodestore.NewEdge("edge-switch-cold", "temperature-switch", "format-cold").SetSourceKey("output_2"),
		nodestore.NewEdge("edge-hot-debug", "format-hot", "debug-output"),
		nodestore.NewEdge("edge-cold-debug", "format-cold", "debug-output"),
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
