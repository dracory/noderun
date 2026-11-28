package noderunner_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dracory/nodebase"
	_ "github.com/dracory/nodebase/builtin"
	. "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

// pauseTestFlow builds inj -> dbg. inj is a plain junction so tests
// control every admission through InjectMessage.
func pauseTestFlow(id string) nodestore.WorkflowInterface {
	return nodestore.NewWorkflow(id).SetID(id).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inj", "inject").SetData(map[string]interface{}{"interval": 3600}),
		nodestore.NewNode("dbg", "debug").SetData(map[string]interface{}{}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("e1", "inj", "dbg"),
	})
}

func pauseInject(t *testing.T, engine *Engine, workflowID, data string) {
	t.Helper()
	msgID, _, err := engine.InjectMessage(workflowID, "inj", nodestore.NewMessage().SetPayload(data))
	if err != nil {
		t.Fatalf("inject %q failed: %v", data, err)
	}
	engine.DiscardPending(msgID)
}

func requirePauseError(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("expected error containing %q, got %v", want, err)
	}
}

func assertNoNewDebugValues(t *testing.T, engine *Engine, workflowID string, before int, wait time.Duration) {
	t.Helper()
	time.Sleep(wait)
	if got := len(debugValues(engine, workflowID)); got != before {
		t.Fatalf("messages were delivered while paused: before=%d after=%d values=%v", before, got, debugValues(engine, workflowID))
	}
}

func assertDebugPayloads(t *testing.T, values []string, payloads ...string) {
	t.Helper()
	for _, payload := range payloads {
		found := false
		for _, value := range values {
			if strings.Contains(value, payload) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("payload %q missing from debug values %v", payload, values)
		}
	}
}

func TestPauseResumeInvalidTransitions(t *testing.T) {
	ctx := context.Background()
	engine := NewEngine()
	workflowID := "pause-invalid"

	requirePauseError(t, engine.PauseWorkflow(workflowID), "not running")
	requirePauseError(t, engine.ResumeWorkflow(workflowID), "not running")

	if _, err := engine.ExecuteWorkflow(ctx, pauseTestFlow(workflowID), nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	if err := engine.PauseWorkflow(workflowID); err != nil {
		t.Fatalf("pause failed: %v", err)
	}
	if status, _ := engine.GetWorkflowStatus(workflowID); status != "paused" {
		t.Fatalf("status after pause = %q, want paused", status)
	}
	requirePauseError(t, engine.PauseWorkflow(workflowID), "already paused")

	if err := engine.ResumeWorkflow(workflowID); err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	if status, _ := engine.GetWorkflowStatus(workflowID); status != "running" {
		t.Fatalf("status after resume = %q, want running", status)
	}
	requirePauseError(t, engine.ResumeWorkflow(workflowID), "not paused")

	if err := engine.StopWorkflow(workflowID); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
	if status, _ := engine.GetWorkflowStatus(workflowID); status != "stopped" {
		t.Fatalf("status after stop = %q, want stopped", status)
	}
	requirePauseError(t, engine.PauseWorkflow(workflowID), "not running")
	requirePauseError(t, engine.ResumeWorkflow(workflowID), "not running")
}

func TestPauseQueuesMemoryBacklogAndResumeDrainsIt(t *testing.T) {
	ctx := context.Background()
	engine := NewEngine()
	workflowID := "pause-memory"

	if _, err := engine.ExecuteWorkflow(ctx, pauseTestFlow(workflowID), nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()
	if err := engine.PauseWorkflow(workflowID); err != nil {
		t.Fatalf("pause failed: %v", err)
	}

	pauseInject(t, engine, workflowID, "mem-a")
	pauseInject(t, engine, workflowID, "mem-b")
	pauseInject(t, engine, workflowID, "mem-c")
	assertNoNewDebugValues(t, engine, workflowID, 0, 150*time.Millisecond)

	if err := engine.ResumeWorkflow(workflowID); err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	values := waitForDebugValues(engine, workflowID, 3, 3*time.Second)
	if len(values) != 3 {
		t.Fatalf("resume delivered %d messages, want 3: %v", len(values), values)
	}
	assertDebugPayloads(t, values, "mem-a", "mem-b", "mem-c")
}

func TestPausePersistsDurableBacklogAndResumeAcksIt(t *testing.T) {
	ctx := context.Background()
	engine := NewEngine()
	durable := newMemDurableStore()
	engine.DurableInboxStore = durable
	workflowID := "pause-durable"

	if _, err := engine.ExecuteWorkflow(ctx, pauseTestFlow(workflowID), nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()
	if err := engine.PauseWorkflow(workflowID); err != nil {
		t.Fatalf("pause failed: %v", err)
	}

	pauseInject(t, engine, workflowID, "dur-a")
	pauseInject(t, engine, workflowID, "dur-b")
	if got := durable.count(); got != 2 {
		t.Fatalf("durable backlog rows = %d, want 2", got)
	}
	assertNoNewDebugValues(t, engine, workflowID, 0, 150*time.Millisecond)

	if err := engine.ResumeWorkflow(workflowID); err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	values := waitForDebugValues(engine, workflowID, 2, 3*time.Second)
	if len(values) != 2 {
		t.Fatalf("resume delivered %d messages, want 2: %v", len(values), values)
	}
	assertDebugPayloads(t, values, "dur-a", "dur-b")
	if got := durable.count(); got != 0 {
		t.Fatalf("durable rows after drain = %d, want 0", got)
	}
}

// pauseBlockingNode blocks inside Execute until released. It lets the
// test pause a workflow while one admitted traversal is unambiguously
// still in flight.
type pauseBlockingNode struct {
	*nodebase.BaseNode
	started     chan struct{}
	release     chan struct{}
	startedOnce *sync.Once
}

func (n *pauseBlockingNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	n.startedOnce.Do(func() { close(n.started) })
	select {
	case <-n.release:
		return input, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestPauseAllowsInFlightTraversalToComplete(t *testing.T) {
	ctx := context.Background()
	engine := NewEngine()
	workflowID := "pause-inflight"
	started := make(chan struct{})
	release := make(chan struct{})
	startedOnce := &sync.Once{}

	if err := engine.WorkflowEngine.RegisterNodeType("pause-block-test", func(node nodestore.NodeInterface, host nodebase.NodeHost) (nodebase.NodeExecutor, error) {
		return &pauseBlockingNode{
			BaseNode:    nodebase.NewBaseNode(node),
			started:     started,
			release:     release,
			startedOnce: startedOnce,
		}, nil
	}); err != nil {
		t.Fatalf("register blocking node: %v", err)
	}

	flow := nodestore.NewWorkflow(workflowID).SetID(workflowID).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inj", "junction").SetData(map[string]interface{}{}),
		nodestore.NewNode("block", "pause-block-test").SetData(map[string]interface{}{}),
		nodestore.NewNode("dbg", "debug").SetData(map[string]interface{}{}),
		nodestore.NewNode("keepalive", "inject").SetData(map[string]interface{}{"interval": 3600}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("e1", "inj", "block"),
		nodestore.NewEdge("e2", "block", "dbg"),
	})
	if _, err := engine.ExecuteWorkflow(ctx, flow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()

	pauseInject(t, engine, workflowID, "in-flight")
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("blocking node never started")
	}

	if err := engine.PauseWorkflow(workflowID); err != nil {
		t.Fatalf("pause failed: %v", err)
	}
	pauseInject(t, engine, workflowID, "paused-new")
	close(release)

	values := waitForDebugValues(engine, workflowID, 1, 3*time.Second)
	if len(values) != 1 || !strings.Contains(values[0], "in-flight") {
		t.Fatalf("in-flight message did not complete first: %v", values)
	}
	assertNoNewDebugValues(t, engine, workflowID, 1, 150*time.Millisecond)

	if err := engine.ResumeWorkflow(workflowID); err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	values = waitForDebugValues(engine, workflowID, 2, 3*time.Second)
	if len(values) != 2 {
		t.Fatalf("resume delivered %d messages, want 2: %v", len(values), values)
	}
	assertDebugPayloads(t, values, "in-flight", "paused-new")
}

func TestPauseStagesQueueInputsWithoutDrainingThem(t *testing.T) {
	ctx := context.Background()
	engine := NewEngine()
	durable := newMemDurableStore()
	engine.DurableInboxStore = durable
	workflowID := "pause-queue"

	flow := queueTestFlow(workflowID, map[string]interface{}{})
	flow.AddNode(nodestore.NewNode("keepalive", "inject").SetData(map[string]interface{}{"interval": 3600}))
	if _, err := engine.ExecuteWorkflow(ctx, flow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()
	if err := engine.PauseWorkflow(workflowID); err != nil {
		t.Fatalf("pause failed: %v", err)
	}

	pauseInject(t, engine, workflowID, "queued-while-paused")
	assertNoNewDebugValues(t, engine, workflowID, 0, 150*time.Millisecond)
	if got := durable.count(); got != 1 {
		t.Fatalf("queue-backed paused row count = %d, want 1", got)
	}

	if err := engine.ResumeWorkflow(workflowID); err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	values := waitForDebugValues(engine, workflowID, 1, 3*time.Second)
	if len(values) != 1 || !strings.Contains(values[0], "queued-while-paused") {
		t.Fatalf("queued message was not delivered after resume: %v", values)
	}
	if got := durable.count(); got != 0 {
		t.Fatalf("durable rows after queue drain = %d, want 0", got)
	}
}

func TestDeployingUnpausedWorkflowDrainsPausedMemoryBacklog(t *testing.T) {
	ctx := context.Background()
	engine := NewEngine()
	workflowID := "pause-deploy-unpaused"
	flow := pauseTestFlow(workflowID)

	if _, err := engine.ExecuteWorkflow(ctx, flow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflowID) }()
	if err := engine.PauseWorkflow(workflowID); err != nil {
		t.Fatalf("pause failed: %v", err)
	}
	pauseInject(t, engine, workflowID, "redeployed")
	assertNoNewDebugValues(t, engine, workflowID, 0, 100*time.Millisecond)

	// A deploy carrying paused=false is an explicit unpause: activation
	// completes in running state and the memory-only backlog is not
	// discarded just because the operator chose redeploy instead of resume.
	flow.SetPaused(false)
	if _, err := engine.DeployWorkflow(ctx, flow, nil); err != nil {
		t.Fatalf("unpaused redeploy failed: %v", err)
	}
	if status, _ := engine.GetWorkflowStatus(workflowID); status != "running" {
		t.Fatalf("status after unpaused redeploy = %q, want running", status)
	}
	values := waitForDebugValues(engine, workflowID, 1, 3*time.Second)
	if len(values) != 1 || !strings.Contains(values[0], "redeployed") {
		t.Fatalf("memory backlog was not drained by unpaused redeploy: %v", values)
	}
}

func TestResumeDeployedKeepsPausedWorkflowPaused(t *testing.T) {
	ctx := context.Background()
	store := resumeTestStore(t)
	durable := newMemDurableStore()
	engine := NewEngine()
	engine.DurableInboxStore = durable
	workflowID := "resume-paused"

	flow := nodestore.NewWorkflow(workflowID).
		SetID(workflowID).
		SetDeployed(true).
		SetPaused(true).
		SetNodes([]nodestore.NodeInterface{
			nodestore.NewNode("src", "junction").SetData(map[string]interface{}{}),
			nodestore.NewNode("mid", "junction").SetData(map[string]interface{}{}),
			nodestore.NewNode("dbg", "debug").SetData(map[string]interface{}{}),
			nodestore.NewNode("keepalive", "inject").SetData(map[string]interface{}{"interval": 3600}),
		}).
		SetEdges([]nodestore.EdgeInterface{
			nodestore.NewEdge("e1", "src", "mid"),
			nodestore.NewEdge("e2", "mid", "dbg"),
		})
	if err := store.WorkflowCreate(ctx, flow); err != nil {
		t.Fatalf("create flow: %v", err)
	}

	// Simulate output admitted by the previous process while paused but
	// never delivered because that process stopped before resume.
	if err := durable.SaveInboxItem(&DurableInboxItem{
		WorkflowID: workflowID,
		NodeID:     "mid",
		Payload:    nodestore.NewMessage().SetPayload("restart-survivor"),
		EnqueuedAt: time.Now(),
	}); err != nil {
		t.Fatalf("seed durable row: %v", err)
	}

	if errs := engine.ResumeDeployed(ctx, store); len(errs) != 0 {
		t.Fatalf("ResumeDeployed errors: %v", errs)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, _ := engine.GetWorkflowStatus(workflowID)
		if status == "paused" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("resumed workflow status = %q, want paused", status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	assertNoNewDebugValues(t, engine, workflowID, 0, 150*time.Millisecond)
	if got := durable.count(); got != 1 {
		t.Fatalf("paused deploy drained survivor rows: count=%d, want 1", got)
	}

	// New admissions are also held while the restart-paused gate is closed.
	msgID, _, err := engine.InjectMessage(workflowID, "src", nodestore.NewMessage().SetPayload("post-restart"))
	if err != nil {
		t.Fatalf("inject while restart-paused failed: %v", err)
	}
	engine.DiscardPending(msgID)
	if got := durable.count(); got != 2 {
		t.Fatalf("paused backlog rows = %d, want 2", got)
	}

	if err := engine.ResumeWorkflow(workflowID); err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	values := waitForDebugValues(engine, workflowID, 2, 3*time.Second)
	if len(values) != 2 {
		t.Fatalf("resume delivered %d messages, want 2: %v", len(values), values)
	}
	assertDebugPayloads(t, values, "restart-survivor", "post-restart")
	if got := durable.count(); got != 0 {
		t.Fatalf("durable rows after explicit resume = %d, want 0", got)
	}
}
