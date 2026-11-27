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

const workflowID = "context-scopes"

func run() error {
	workflow := contextScopesWorkflow()
	engine := noderunner.NewWorkflowEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		return fmt.Errorf("deploy workflow: %w", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	return waitForDebugMessage(engine, workflowID, `{"flowSees":"flow value","globalSees":"GoFlow","nodeSees":null}`, 3*time.Second)
}

func contextScopesWorkflow() nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("Context Scopes").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inject-start", "inject").SetData(map[string]interface{}{
			"payload": "go", "payloadType": "string", "once": true, "repeatType": "none",
		}),
		// Writer: one key per scope. The node-scope write is namespaced
		// to THIS node — the reader's own context cannot see it.
		nodestore.NewNode("function-write", "function").SetData(map[string]interface{}{
			"func": `context.set("marker", "writer only");
flow.set("shared", "flow value");
global.set("appName", "GoFlow");
return msg;`,
		}),
		// Reader: same flow and global scope, but a different node
		// context — "marker" comes back null, proving node isolation.
		nodestore.NewNode("function-read", "function").SetData(map[string]interface{}{
			"func": `msg.payload = JSON.stringify({
  nodeSees: context.get("marker") || null,
  flowSees: flow.get("shared"),
  globalSees: global.get("appName")
});
return msg;`,
		}),
		nodestore.NewNode("debug-output", "debug").SetData(map[string]interface{}{
			"complete": "payload",
			"console":  true,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-inject-write", "inject-start", "function-write"),
		nodestore.NewEdge("edge-write-read", "function-write", "function-read"),
		nodestore.NewEdge("edge-read-debug", "function-read", "debug-output"),
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
