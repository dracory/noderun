package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/dracory/nodebase/debug"
	_ "github.com/dracory/nodebase/file"
	_ "github.com/dracory/nodebase/filein"
	_ "github.com/dracory/nodebase/inject"
	noderunner "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

const workflowID = "file-write-read"

func run() error {
	path := filepath.Join(os.TempDir(), "noderun-example-file-write-read", "greeting.txt")
	defer func() { _ = os.RemoveAll(filepath.Dir(path)) }()

	workflow := fileWriteReadWorkflow(path)
	engine := noderunner.NewWorkflowEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		return fmt.Errorf("deploy workflow: %w", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	return waitForDebugMessage(engine, workflowID, "Hello file!", 3*time.Second)
}

func fileWriteReadWorkflow(path string) nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("File Write Read").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inject-start", "inject").SetData(map[string]interface{}{
			"payload":     "Hello file!",
			"payloadType": "string",
			"once":        true,
			"repeatType":  "none",
		}),
		// Writes msg.payload to the file and forwards the message with
		// msg.filename set — which is what the fileIn node then reads.
		nodestore.NewNode("file-write", "file").SetData(map[string]interface{}{
			"filename":  path,
			"createDir": true,
		}),
		nodestore.NewNode("file-read", "fileIn").SetData(map[string]interface{}{
			"filenameType": "msg",
		}),
		nodestore.NewNode("debug-output", "debug").SetData(map[string]interface{}{
			"complete": "payload",
			"console":  true,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-inject-write", "inject-start", "file-write"),
		nodestore.NewEdge("edge-write-read", "file-write", "file-read"),
		nodestore.NewEdge("edge-read-debug", "file-read", "debug-output"),
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
