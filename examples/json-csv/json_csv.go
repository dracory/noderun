package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	_ "github.com/dracory/nodebase/csv"
	_ "github.com/dracory/nodebase/debug"
	_ "github.com/dracory/nodebase/inject"
	_ "github.com/dracory/nodebase/json"
	noderunner "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

const workflowID = "json-csv"

func run() error {
	workflow := jsonCSVWorkflow()
	engine := noderunner.NewWorkflowEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		return fmt.Errorf("deploy workflow: %w", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	if err := waitForDebugMessage(engine, workflowID, `{"count":2,"name":"ada"}`, 3*time.Second); err != nil {
		return err
	}
	return waitForDebugMessage(engine, workflowID, `[{"age":30,"name":"ada"},{"age":36,"name":"grace"}]`, 3*time.Second)
}

func jsonCSVWorkflow() nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("JSON CSV").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		// Branch 1: a JSON string arrives as the payload; the json node
		// auto-detects the string and parses it into an object.
		nodestore.NewNode("inject-json", "inject").SetData(map[string]interface{}{
			"payload":     `{"name": "ada", "count": 2}`,
			"payloadType": "string",
			"once":        true,
			"repeatType":  "none",
		}),
		nodestore.NewNode("json-parse", "json").SetData(map[string]interface{}{
			"action":   "", // auto: parse strings, stringify objects
			"property": "payload",
		}),
		nodestore.NewNode("debug-json", "debug").SetData(map[string]interface{}{
			"complete": "payload",
			"console":  true,
		}),

		// Branch 2: a CSV string with a header row arrives as the
		// payload; the csv node with multi:"mult" returns the whole
		// table as an array of row objects in one message.
		nodestore.NewNode("inject-csv", "inject").SetData(map[string]interface{}{
			"payload":     "name,age\nada,30\ngrace,36",
			"payloadType": "string",
			"once":        true,
			"repeatType":  "none",
		}),
		nodestore.NewNode("csv-parse", "csv").SetData(map[string]interface{}{
			"headers": true,
			"multi":   "mult",
		}),
		nodestore.NewNode("debug-csv", "debug").SetData(map[string]interface{}{
			"complete": "payload",
			"console":  true,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-json-inject", "inject-json", "json-parse"),
		nodestore.NewEdge("edge-json-debug", "json-parse", "debug-json"),
		nodestore.NewEdge("edge-csv-inject", "inject-csv", "csv-parse"),
		nodestore.NewEdge("edge-csv-debug", "csv-parse", "debug-csv"),
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
	return fmt.Errorf("timed out waiting for debug message %q", want)
}
