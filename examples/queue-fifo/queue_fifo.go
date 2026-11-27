package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/dracory/nodebase/debug"
	_ "github.com/dracory/nodebase/inject"
	_ "github.com/dracory/nodebase/queue"
	_ "github.com/dracory/nodebase/split"
	noderunner "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

const workflowID = "queue-fifo"

func run() error {
	dir, err := os.MkdirTemp("", "noderun-queue-fifo")
	if err != nil {
		return fmt.Errorf("temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	workflow := queueFIFOWorkflow(dir)
	engine := noderunner.NewWorkflowEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		return fmt.Errorf("deploy workflow: %w", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	// The queue is held: items stage but nothing drains. Wait until all
	// three items are enqueued, then release the gate.
	if err := waitForQueueDepth(engine, workflowID, "queue-stage", 3, 3*time.Second); err != nil {
		return err
	}
	q, ok := engine.QueueHandle(workflowID, "queue-stage")
	if !ok {
		return fmt.Errorf("queue handle unavailable")
	}
	q.SetHold(false)

	got, err := collectDebugMessages(engine, workflowID, 3, 3*time.Second)
	if err != nil {
		return err
	}
	fmt.Println(strings.Join(got, ","))
	return nil
}

func queueFIFOWorkflow(dir string) nodestore.WorkflowInterface {
	return nodestore.NewWorkflow("Queue FIFO").SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inject-start", "inject").SetData(map[string]interface{}{
			"payload":     "[1, 2, 3]",
			"payloadType": "json",
			"once":        true,
			"repeatType":  "none",
		}),
		nodestore.NewNode("split-items", "split").SetData(map[string]interface{}{}),
		// hold: true — deliveries enqueue but nothing drains until the
		// gate is released. The dir backend stages each item as an
		// inspectable {seq}-{msgid}.json file.
		nodestore.NewNode("queue-stage", "queue").SetData(map[string]interface{}{
			"hold":    true,
			"backend": "dir",
			"dir":     dir,
		}),
		nodestore.NewNode("debug-output", "debug").SetData(map[string]interface{}{
			"complete": "payload",
			"console":  true,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("edge-inject-split", "inject-start", "split-items"),
		nodestore.NewEdge("edge-split-queue", "split-items", "queue-stage"),
		nodestore.NewEdge("edge-queue-debug", "queue-stage", "debug-output"),
	})
}

// waitForQueueDepth waits until the named queue holds want items.
func waitForQueueDepth(engine *noderunner.WorkflowEngine, workflowID, nodeID string, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if q, ok := engine.QueueHandle(workflowID, nodeID); ok && q.Count() >= want {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for queue %s to reach depth %d", nodeID, want)
}

// collectDebugMessages gathers the first count debug messages in
// event order.
func collectDebugMessages(engine *noderunner.WorkflowEngine, workflowID string, count int, timeout time.Duration) ([]string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, _, events, _ := engine.GetWorkflowSnapshot(workflowID, 0)
		var got []string
		for _, event := range events {
			if event.Level == "debug" {
				got = append(got, event.Message)
			}
		}
		if len(got) >= count {
			return got[:count], nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil, fmt.Errorf("timed out waiting for %d debug messages", count)
}
