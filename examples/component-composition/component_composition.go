package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	_ "github.com/dracory/nodebase/debug"
	_ "github.com/dracory/nodebase/function"
	_ "github.com/dracory/nodebase/inject"
	noderunner "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

const workflowID = "component-composition"

func run() error {
	workflow := componentWorkflow()
	engine := noderunner.NewWorkflowEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		return fmt.Errorf("deploy workflow: %w", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	if err := waitForDebugMessage(engine, workflowID, "[alpha]", 3*time.Second); err != nil {
		return err
	}
	return waitForDebugMessage(engine, workflowID, "[beta]", 3*time.Second)
}

// One "component" definition node holds a reusable inner graph; two
// "component:<defId>" instance nodes get expanded at deploy time into
// namespaced copies (instA/wrap, instB/wrap).
func componentWorkflow() nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("Component Composition").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		// The definition: one input port wired to the inner function,
		// one output port fed by it. Definitions are templates — they
		// are removed from the running graph by expansion.
		nodestore.NewNode("def-wrap", "component").SetData(map[string]interface{}{
			"name": "Wrap",
			"in": []interface{}{
				map[string]interface{}{"wires": []interface{}{map[string]interface{}{"id": "wrap"}}},
			},
			"out": []interface{}{
				map[string]interface{}{"wires": []interface{}{map[string]interface{}{"id": "wrap", "port": 0.0}}},
			},
			"nodes": []interface{}{
				map[string]interface{}{
					"id":   "wrap",
					"type": "function",
					"data": map[string]interface{}{"func": `msg.payload = "[" + msg.payload + "]"; return msg;`},
				},
			},
		}),

		nodestore.NewNode("inject-a", "inject").SetData(map[string]interface{}{
			"payload": "alpha", "payloadType": "string", "once": true, "repeatType": "none",
		}),
		nodestore.NewNode("inst-a", "component:def-wrap").SetData(map[string]interface{}{}),
		nodestore.NewNode("debug-a", "debug").SetData(map[string]interface{}{
			"complete": "payload", "console": true,
		}),

		nodestore.NewNode("inject-b", "inject").SetData(map[string]interface{}{
			"payload": "beta", "payloadType": "string", "once": true, "repeatType": "none",
		}),
		nodestore.NewNode("inst-b", "component:def-wrap").SetData(map[string]interface{}{}),
		nodestore.NewNode("debug-b", "debug").SetData(map[string]interface{}{
			"complete": "payload", "console": true,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-a-in", "inject-a", "inst-a"),
		nodestore.NewEdge("edge-a-out", "inst-a", "debug-a"),
		nodestore.NewEdge("edge-b-in", "inject-b", "inst-b"),
		nodestore.NewEdge("edge-b-out", "inst-b", "debug-b"),
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
