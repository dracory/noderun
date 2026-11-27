package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	_ "github.com/dracory/nodebase/debug"
	_ "github.com/dracory/nodebase/inject"
	_ "github.com/dracory/nodebase/template"
	noderunner "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

const workflowID = "template-message"

func run() error {
	workflow := templateMessageWorkflow()
	engine := noderunner.NewWorkflowEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		return fmt.Errorf("deploy workflow: %w", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	return waitForDebugMessage(engine, workflowID, "Hello world, from the template node!", 3*time.Second)
}

func templateMessageWorkflow() nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("Template Message").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inject-start", "inject").SetData(map[string]interface{}{
			"payload":     `{"name": "world", "source": "the template node"}`,
			"payloadType": "json",
			"once":        true,
			"repeatType":  "none",
		}),
		// Mustache is the default syntax (Node-RED parity); the whole
		// message is the render context, so {{payload.name}} drills into
		// the injected object. The rendered string replaces msg.payload.
		nodestore.NewNode("template-render", "template").SetData(map[string]interface{}{
			"syntax":    "mustache",
			"template":  "Hello {{payload.name}}, from {{payload.source}}!",
			"field":     "payload",
			"fieldType": "msg",
		}),
		nodestore.NewNode("debug-output", "debug").SetData(map[string]interface{}{
			"complete": "payload",
			"console":  true,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-inject-template", "inject-start", "template-render"),
		nodestore.NewEdge("edge-template-debug", "template-render", "debug-output"),
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
