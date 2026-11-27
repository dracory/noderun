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
	_ "github.com/dracory/nodebase/delay"
	_ "github.com/dracory/nodebase/inject"
	noderunner "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
	_ "modernc.org/sqlite"
)

const workflowID = "durable-inbox-resume"

func run() error {
	dir, err := os.MkdirTemp("", "noderun-durable-inbox")
	if err != nil {
		return fmt.Errorf("temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(dir, "inbox.db"))+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return fmt.Errorf("open inbox db: %w", err)
	}
	defer func() { _ = db.Close() }()

	engine := noderunner.NewWorkflowEngine()
	inboxStore, err := noderunner.NewNeatInboxStore(db)
	if err != nil {
		return fmt.Errorf("inbox store: %w", err)
	}
	engine.DurableInboxStore = inboxStore

	// Run 1: deploy and let the inject land mid-flight inside the delay
	// node's durable inbox — then stop before it can drain.
	if _, err := engine.DeployWorkflow(context.Background(), durableInboxWorkflow(), nil); err != nil {
		return fmt.Errorf("first deploy: %w", err)
	}
	time.Sleep(300 * time.Millisecond)
	if err := engine.StopWorkflow(workflowID); err != nil {
		return fmt.Errorf("stop: %w", err)
	}

	// Run 2: redeploying adopts the surviving inbox row — the delay
	// fires and the message reaches debug. At-least-once delivery.
	if _, err := engine.DeployWorkflow(context.Background(), durableInboxWorkflow(), nil); err != nil {
		return fmt.Errorf("second deploy: %w", err)
	}

	return waitForDebugMessage(engine, workflowID, "Hello after restart!", 5*time.Second)
}

func durableInboxWorkflow() nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("Durable Inbox Resume").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inject-start", "inject").SetData(map[string]interface{}{
			"payload":     "Hello after restart!",
			"payloadType": "string",
			"once":        true,
			"repeatType":  "none",
		}),
		// A long hold guarantees the message is still parked in the
		// node's durable inbox when the workflow stops.
		nodestore.NewNode("delay-hold", "delay").SetData(map[string]interface{}{
			"pauseType":    "delay",
			"timeout":      "1",
			"timeoutUnits": "seconds",
		}),
		nodestore.NewNode("debug-output", "debug").SetData(map[string]interface{}{
			"complete": "payload",
			"console":  true,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-inject-delay", "inject-start", "delay-hold"),
		nodestore.NewEdge("edge-delay-debug", "delay-hold", "debug-output"),
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
