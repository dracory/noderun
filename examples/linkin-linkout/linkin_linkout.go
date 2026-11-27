package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	_ "github.com/dracory/nodebase/change"
	_ "github.com/dracory/nodebase/debug"
	_ "github.com/dracory/nodebase/inject"
	_ "github.com/dracory/nodebase/linkin"
	_ "github.com/dracory/nodebase/linkout"
	noderunner "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

const workflowID = "linkin-linkout"

func run() error {
	workflow := linkWorkflow()
	engine := noderunner.NewWorkflowEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		return fmt.Errorf("deploy workflow: %w", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	if err := waitForDebugMessage(engine, workflowID, "Hello direct!", 3*time.Second); err != nil {
		return err
	}
	return waitForDebugMessage(engine, workflowID, "arrived via link", 3*time.Second)
}

// The linkIn node has no edge into it — linkOut pushes a message clone
// into it by node id, and the clone continues from linkIn's own wiring.
func linkWorkflow() nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("Link In Link Out").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inject-start", "inject").SetData(map[string]interface{}{
			"payload": "Hello direct!", "payloadType": "string", "once": true, "repeatType": "none",
		}),
		// mode "link" (default): push a clone to every linked linkIn
		// AND continue downstream normally.
		nodestore.NewNode("link-send", "linkOut").SetData(map[string]interface{}{
			"links": []interface{}{"link-receive"},
		}),
		nodestore.NewNode("debug-direct", "debug").SetData(map[string]interface{}{
			"complete": "payload", "console": true,
		}),

		// The virtual wire's landing point: receives the pushed clone
		// (with metadata["linkSource"] set) and fans out from here.
		nodestore.NewNode("link-receive", "linkIn").SetData(map[string]interface{}{}),
		nodestore.NewNode("change-via", "change").SetData(map[string]interface{}{
			"rules": []interface{}{
				map[string]interface{}{"op": "set", "field": "payload", "value": "arrived via link"},
			},
		}),
		nodestore.NewNode("debug-via", "debug").SetData(map[string]interface{}{
			"complete": "payload", "console": true,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-inject-out", "inject-start", "link-send"),
		nodestore.NewEdge("edge-out-debug", "link-send", "debug-direct"),
		nodestore.NewEdge("edge-in-change", "link-receive", "change-via"),
		nodestore.NewEdge("edge-change-debug", "change-via", "debug-via"),
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
