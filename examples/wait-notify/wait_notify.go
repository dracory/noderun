package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/dracory/nodebase/debug"
	_ "github.com/dracory/nodebase/inject"
	_ "github.com/dracory/nodebase/notify"
	_ "github.com/dracory/nodebase/wait"
	noderunner "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
	_ "modernc.org/sqlite"
)

const workflowID = "wait-notify"

func run() error {
	dir, err := os.MkdirTemp("", "noderun-wait-notify")
	if err != nil {
		return fmt.Errorf("temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	// Wait/notify coordinates through the durable coordination store —
	// there is no in-memory fallback, so the engine needs both stores.
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(dir, "coordination.db"))+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return fmt.Errorf("open coordination db: %w", err)
	}
	defer func() { _ = db.Close() }()

	engine := noderunner.NewWorkflowEngine()
	coordinationStore, err := noderunner.NewNeatCoordinationStore(db)
	if err != nil {
		return fmt.Errorf("coordination store: %w", err)
	}
	engine.CoordinationStore = coordinationStore
	inboxStore, err := noderunner.NewNeatInboxStore(db)
	if err != nil {
		return fmt.Errorf("inbox store: %w", err)
	}
	engine.DurableInboxStore = inboxStore

	workflow := waitNotifyWorkflow()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		return fmt.Errorf("deploy workflow: %w", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	// Let the waiter park, then release it with the notify branch.
	time.Sleep(200 * time.Millisecond)
	if err := engine.FireInjectNode(workflowID, "inject-notify"); err != nil {
		return fmt.Errorf("fire notify inject: %w", err)
	}

	return waitForDebugMessage(engine, workflowID, "the parked message is free!", 5*time.Second)
}

// Two branches sharing one global signal key: the waiter parks until
// the notify side increments "job-1" to the target count.
func waitNotifyWorkflow() nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("Wait Notify").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inject-wait", "inject").SetData(map[string]interface{}{
			"payload":     "the parked message is free!",
			"payloadType": "string",
			"once":        true,
			"repeatType":  "none",
		}),
		nodestore.NewNode("wait-gate", "wait").SetData(map[string]interface{}{
			"signal":         "job-1",
			"targetCount":    "1",
			"pollIntervalMs": 50,
		}),
		nodestore.NewNode("debug-output", "debug").SetData(map[string]interface{}{
			"complete": "payload",
			"console":  true,
		}),

		nodestore.NewNode("inject-notify", "inject").SetData(map[string]interface{}{
			"payload":     "release",
			"payloadType": "string",
			"repeatType":  "none",
		}),
		nodestore.NewNode("notify-signal", "notify").SetData(map[string]interface{}{
			"signal": "job-1",
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-inject-wait", "inject-wait", "wait-gate"),
		nodestore.NewEdge("edge-wait-debug", "wait-gate", "debug-output"),
		nodestore.NewEdge("edge-inject-notify", "inject-notify", "notify-signal"),
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
