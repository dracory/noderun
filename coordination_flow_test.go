package noderunner_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/dracory/nodebase/builtin"
	. "github.com/dracory/noderun"
	"github.com/dracory/nodestore"
	_ "modernc.org/sqlite"
)

func coordinationWaitData(timeoutSeconds interface{}) map[string]interface{} {
	return map[string]interface{}{
		"signal":         "job-42",
		"targetCount":    "2",
		"timeoutSeconds": timeoutSeconds,
		"mergeMode":      "wait-wins",
		"pollIntervalMs": 20,
	}
}

func coordinationFlow(id string, timeoutSeconds interface{}) nodestore.WorkflowInterface {
	return nodestore.NewWorkflow(id).SetID(id).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("injWait", "junction").SetData(map[string]interface{}{}),
		nodestore.NewNode("injNotify", "junction").SetData(map[string]interface{}{}),
		nodestore.NewNode("wait", "wait").SetData(coordinationWaitData(timeoutSeconds)),
		nodestore.NewNode("notify", "notify").SetData(map[string]interface{}{
			"signal": "job-42", "cacheKeys": "worker", "signalTTLSeconds": 0,
		}),
		nodestore.NewNode("dbg", "debug").SetData(map[string]interface{}{}),
		nodestore.NewNode("dbgExpired", "debug").SetData(map[string]interface{}{}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("e1", "injWait", "wait"),
		nodestore.NewEdge("e2", "wait", "dbg"),
		nodestore.NewEdge("e3", "wait", "dbgExpired").SetSourceKey("output_2"),
		nodestore.NewEdge("e4", "injNotify", "notify"),
	})
}

func coordinationNotifyOnlyFlow(id string) nodestore.WorkflowInterface {
	return nodestore.NewWorkflow(id).SetID(id).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("injNotify", "junction").SetData(map[string]interface{}{}),
		nodestore.NewNode("notify", "notify").SetData(map[string]interface{}{
			"signal": "job-42", "cacheKeys": "worker", "signalTTLSeconds": 0,
		}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("e1", "injNotify", "notify"),
	})
}

func coordinationWaitOnlyFlow(id string) nodestore.WorkflowInterface {
	return nodestore.NewWorkflow(id).SetID(id).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("injWait", "junction").SetData(map[string]interface{}{}),
		nodestore.NewNode("wait", "wait").SetData(coordinationWaitData(0)),
		nodestore.NewNode("dbg", "debug").SetData(map[string]interface{}{}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("e1", "injWait", "wait"),
		nodestore.NewEdge("e2", "wait", "dbg"),
	})
}

func injectCoordinationMessage(t *testing.T, engine *WorkflowEngine, workflowID, nodeID string, msg nodestore.MessageInterface) {
	t.Helper()
	msgID, _, err := engine.InjectMessage(workflowID, nodeID, msg)
	if err != nil {
		t.Fatalf("inject into %s failed: %v", nodeID, err)
	}
	engine.DiscardPending(msgID)
}

func injectText(t *testing.T, engine *WorkflowEngine, workflowID, nodeID, text string) {
	t.Helper()
	injectCoordinationMessage(t, engine, workflowID, nodeID, nodestore.NewMessage().SetPayload(text))
}

func injectNotify(t *testing.T, engine *WorkflowEngine, workflowID, worker string) {
	t.Helper()
	injectCoordinationMessage(t, engine, workflowID, "injNotify", nodestore.NewMessage().SetPayload("done").SetMetadata(map[string]interface{}{"worker": worker}))
}

func debugNodeValues(engine *WorkflowEngine, workflowID, nodeID string) []string {
	_, _, events, _ := engine.GetWorkflowSnapshot(workflowID, 0)
	var out []string
	for _, ev := range events {
		if ev.NodeID == nodeID && ev.Level == "debug" {
			out = append(out, ev.Message)
		}
	}
	return out
}

func waitForDebugNodeValues(engine *WorkflowEngine, workflowID, nodeID string, n int, timeout time.Duration) []string {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := debugNodeValues(engine, workflowID, nodeID); len(got) >= n {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	return debugNodeValues(engine, workflowID, nodeID)
}

func TestWaitNotifyFlowReleasesOnlyAtTargetCount(t *testing.T) {
	store, _ := neatCoordinationTestStore(t, true)
	workflow := coordinationFlow("wf-wait-notify", 0)
	engine := NewWorkflowEngine()
	engine.CoordinationStore = store
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.GetID()) })

	injectText(t, engine, workflow.GetID(), "injWait", "parked")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		waiters, _ := store.Waiters(context.Background(), workflow.GetID(), "wait")
		if len(waiters) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	waiters, _ := store.Waiters(context.Background(), workflow.GetID(), "wait")
	if len(waiters) != 1 {
		t.Fatalf("message was not durably parked: %+v", waiters)
	}

	injectNotify(t, engine, workflow.GetID(), "alpha")
	time.Sleep(150 * time.Millisecond)
	if got := debugNodeValues(engine, workflow.GetID(), "dbg"); len(got) != 0 {
		t.Fatalf("first notify below target must not release, got %v", got)
	}
	signal, ok, err := store.Signal(context.Background(), "job-42")
	if err != nil || !ok || signal.Count != 1 {
		t.Fatalf("first signal count = %+v ok=%v err=%v, want 1", signal, ok, err)
	}

	injectNotify(t, engine, workflow.GetID(), "beta")
	got := waitForDebugNodeValues(engine, workflow.GetID(), "dbg", 1, 2*time.Second)
	if len(got) != 1 || got[0] != "parked" {
		t.Fatalf("released payload wrong: %v", got)
	}
	if _, ok, _ := store.Signal(context.Background(), "job-42"); ok {
		t.Fatal("target-sized consumption should remove the zero-count signal")
	}
	waiters, _ = store.Waiters(context.Background(), workflow.GetID(), "wait")
	if len(waiters) != 0 {
		t.Fatalf("released waiter still parked: %+v", waiters)
	}
}

func TestWaitExpirationRoutesDistinctOutput(t *testing.T) {
	store, _ := neatCoordinationTestStore(t, true)
	workflow := coordinationFlow("wf-wait-expiry", 1)
	engine := NewWorkflowEngine()
	engine.CoordinationStore = store
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.GetID()) })

	injectText(t, engine, workflow.GetID(), "injWait", "expires")
	got := waitForDebugNodeValues(engine, workflow.GetID(), "dbgExpired", 1, 3*time.Second)
	if len(got) != 1 || got[0] != "expires" {
		t.Fatalf("expired payload wrong: %v", got)
	}
	if got := debugNodeValues(engine, workflow.GetID(), "dbg"); len(got) != 0 {
		t.Fatalf("expired message must not emit on success output: %v", got)
	}
}

func openCoordinationTestDB(t *testing.T, path string) (*sql.DB, *NeatCoordinationStore) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewNeatCoordinationStore(db)
	if err != nil {
		db.Close()
		t.Fatalf("NewNeatCoordinationStore: %v", err)
	}
	return db, store
}

func TestWaitNotifyRestartPreservesWaiterAndSignalCount(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "restart-coordination.db")
	workflow := coordinationFlow("wf-wait-restart", 0)

	db1, store1 := openCoordinationTestDB(t, dbPath)
	engine1 := NewWorkflowEngine()
	engine1.CoordinationStore = store1
	if _, err := engine1.DeployWorkflow(ctx, workflow, nil); err != nil {
		t.Fatalf("first deploy failed: %v", err)
	}
	injectText(t, engine1, workflow.GetID(), "injWait", "restart-safe")
	injectNotify(t, engine1, workflow.GetID(), "before-restart")
	time.Sleep(150 * time.Millisecond)
	signal, ok, err := store1.Signal(ctx, "job-42")
	if err != nil || !ok || signal.Count != 1 {
		t.Fatalf("pre-restart signal = %+v ok=%v err=%v", signal, ok, err)
	}
	waiters, err := store1.Waiters(ctx, workflow.GetID(), "wait")
	if err != nil || len(waiters) != 1 {
		t.Fatalf("pre-restart waiters = %+v err=%v", waiters, err)
	}
	if err := engine1.StopWorkflow(workflow.GetID()); err != nil {
		t.Fatalf("stop first engine: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close first db: %v", err)
	}

	db2, store2 := openCoordinationTestDB(t, dbPath)
	defer db2.Close()
	engine2 := NewWorkflowEngine()
	engine2.CoordinationStore = store2
	if _, err := engine2.DeployWorkflow(ctx, workflow, nil); err != nil {
		t.Fatalf("restart deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine2.StopWorkflow(workflow.GetID()) })
	injectNotify(t, engine2, workflow.GetID(), "after-restart")
	got := waitForDebugNodeValues(engine2, workflow.GetID(), "dbg", 1, 3*time.Second)
	if len(got) != 1 || got[0] != "restart-safe" {
		t.Fatalf("restart release wrong: %v", got)
	}
}

func TestWaitNotifyTwoEnginesShareSQLStore(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "shared-coordination.db")

	dbA, storeA := openCoordinationTestDB(t, dbPath)
	defer dbA.Close()
	dbB, storeB := openCoordinationTestDB(t, dbPath)
	defer dbB.Close()

	waitWorkflow := coordinationWaitOnlyFlow("wf-wait-shared")
	engineA := NewWorkflowEngine()
	engineA.CoordinationStore = storeA
	if _, err := engineA.DeployWorkflow(ctx, waitWorkflow, nil); err != nil {
		t.Fatalf("deploy wait engine: %v", err)
	}
	t.Cleanup(func() { _ = engineA.StopWorkflow(waitWorkflow.GetID()) })

	notifyWorkflow := coordinationNotifyOnlyFlow("wf-notify-shared")
	engineB := NewWorkflowEngine()
	engineB.CoordinationStore = storeB
	if _, err := engineB.DeployWorkflow(ctx, notifyWorkflow, nil); err != nil {
		t.Fatalf("deploy notify engine: %v", err)
	}
	t.Cleanup(func() { _ = engineB.StopWorkflow(notifyWorkflow.GetID()) })

	injectText(t, engineA, waitWorkflow.GetID(), "injWait", "shared")
	injectNotify(t, engineB, notifyWorkflow.GetID(), "runner-b-one")
	time.Sleep(150 * time.Millisecond)
	if got := debugNodeValues(engineA, waitWorkflow.GetID(), "dbg"); len(got) != 0 {
		t.Fatalf("shared store released before target: %v", got)
	}
	injectNotify(t, engineB, notifyWorkflow.GetID(), "runner-b-two")
	got := waitForDebugNodeValues(engineA, waitWorkflow.GetID(), "dbg", 1, 3*time.Second)
	if len(got) != 1 || got[0] != "shared" {
		t.Fatalf("shared-store release wrong: %v", got)
	}
}
