package noderunner_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
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

func configureCoordinationEngine(t *testing.T, engine *WorkflowEngine, db *sql.DB, store *NeatCoordinationStore) {
	t.Helper()
	engine.CoordinationStore = store
	inbox, err := NewNeatInboxStore(db)
	if err != nil {
		t.Fatalf("NewNeatInboxStore: %v", err)
	}
	engine.DurableInboxStore = inbox
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
	store, db := neatCoordinationTestStore(t, true)
	workflow := coordinationFlow("wf-wait-notify", 0)
	engine := NewWorkflowEngine()
	configureCoordinationEngine(t, engine, db, store)
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
	store, db := neatCoordinationTestStore(t, true)
	workflow := coordinationFlow("wf-wait-expiry", 1)
	engine := NewWorkflowEngine()
	configureCoordinationEngine(t, engine, db, store)
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
	configureCoordinationEngine(t, engine1, db1, store1)
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
	configureCoordinationEngine(t, engine2, db2, store2)
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
	configureCoordinationEngine(t, engineA, dbA, storeA)
	if _, err := engineA.DeployWorkflow(ctx, waitWorkflow, nil); err != nil {
		t.Fatalf("deploy wait engine: %v", err)
	}
	t.Cleanup(func() { _ = engineA.StopWorkflow(waitWorkflow.GetID()) })

	notifyWorkflow := coordinationNotifyOnlyFlow("wf-notify-shared")
	engineB := NewWorkflowEngine()
	configureCoordinationEngine(t, engineB, dbB, storeB)
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

type failingSaveDurableStore struct {
	*FakeDurableStore
	err  error
	fail atomic.Bool
}

func (s *failingSaveDurableStore) SaveInboxItem(item *DurableInboxItem) error {
	if s.fail.Load() {
		return s.err
	}
	return s.FakeDurableStore.SaveInboxItem(item)
}

func TestEmitDurableReportsInboxPersistenceFailure(t *testing.T) {
	coordinationStore, _ := neatCoordinationTestStore(t, true)
	workflow := coordinationFlow("wf-durable-emit-error", 0)
	engine := NewWorkflowEngine()
	engine.CoordinationStore = coordinationStore
	durable := &failingSaveDurableStore{FakeDurableStore: NewFakeDurableStore(), err: errors.New("forced inbox write failure")}
	durable.fail.Store(true)
	engine.DurableInboxStore = durable
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.GetID()) })

	err := engine.EmitDurable(workflow.GetID(), "wait", nodestore.NewMessage().SetPayload("parked"), []string{"output_1"})
	if err == nil || !strings.Contains(err.Error(), "forced inbox write failure") {
		t.Fatalf("EmitDurable must surface inbox persistence failure, got %v", err)
	}
}

func TestWaitEmissionFailureRequeuesAndLaterReleases(t *testing.T) {
	coordinationStore, _ := neatCoordinationTestStore(t, true)
	workflow := coordinationFlow("wf-wait-emit-retry", 0)
	durable := &failingSaveDurableStore{FakeDurableStore: NewFakeDurableStore(), err: errors.New("forced downstream write failure")}
	engine := NewWorkflowEngine()
	engine.CoordinationStore = coordinationStore
	engine.DurableInboxStore = durable
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}

	injectText(t, engine, workflow.GetID(), "injWait", "retry-me")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		waiters, _ := coordinationStore.Waiters(context.Background(), workflow.GetID(), "wait")
		if len(waiters) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	waiters, err := coordinationStore.Waiters(context.Background(), workflow.GetID(), "wait")
	if err != nil || len(waiters) != 1 {
		t.Fatalf("parked waiter missing before forced failure: len=%d err=%v", len(waiters), err)
	}

	// Once the message is parked, make every downstream durable inbox write
	// fail and satisfy the signal directly. The wait row must be nacked and
	// retained rather than vanish with its consumed count.
	durable.fail.Store(true)
	if err := coordinationStore.IncrementSignal(context.Background(), "job-42", nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := coordinationStore.IncrementSignal(context.Background(), "job-42", nil, 0); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(2 * time.Second)
	observedFailure := false
	for time.Now().Before(deadline) {
		_, _, events, _ := engine.GetWorkflowSnapshot(workflow.GetID(), 0)
		for _, ev := range events {
			if ev.NodeID == "wait" && ev.Level == "error" && strings.Contains(ev.Message, "waiter requeued for retry") {
				observedFailure = true
				break
			}
		}
		if observedFailure {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !observedFailure {
		t.Fatal("forced downstream failure was not exercised by the wait poller")
	}
	waiters, err = coordinationStore.Waiters(context.Background(), workflow.GetID(), "wait")
	if err != nil || len(waiters) != 1 {
		t.Fatalf("forced emission failure lost the waiter: len=%d err=%v", len(waiters), err)
	}
	if got := debugNodeValues(engine, workflow.GetID(), "dbg"); len(got) != 0 {
		t.Fatalf("failed durable emission must not reach debug: %v", got)
	}

	if err := engine.StopWorkflow(workflow.GetID()); err != nil {
		t.Fatalf("stop failed-deployment engine: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	engine.DurableInboxStore = NewFakeDurableStore()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("recovery deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.GetID()) })
	got := waitForDebugNodeValues(engine, workflow.GetID(), "dbg", 1, 3*time.Second)
	if len(got) != 1 || got[0] != "retry-me" {
		t.Fatalf("requeued waiter did not release after recovery: %v", got)
	}
}
