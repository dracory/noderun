package noderunner

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// freePort is defined in engine_test.go.

func TestDeployEventContract(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-deploy-contract",
		Name: "Deploy Contract",
		Nodes: []Node{
			{ID: "inj", Type: "inject", Data: map[string]interface{}{"payload": `1`}},
			{ID: "trig", Type: "httpTrigger", Data: map[string]interface{}{
				"method": "GET", "path": "/x", "port": freePort(t),
			}},
			{ID: "lg", Type: "log", Data: map[string]interface{}{"message": "hi"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "lg"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	events := waitForEvents(engine, workflow.ID, 2*time.Second, func(ev NodeEvent) bool {
		return ev.Message == "workflow deployed"
	})
	if len(events) == 0 {
		t.Fatal("deployed event never emitted")
	}

	if n := countEvents(events, func(ev NodeEvent) bool { return ev.Message == "workflow deployed" }); n != 1 {
		t.Fatalf("expected exactly 1 'workflow deployed', got %d; events=%v", n, events)
	}
	for _, noise := range []string{"node started", "node completed"} {
		if n := countEvents(events, func(ev NodeEvent) bool { return ev.Message == noise }); n != 0 {
			t.Fatalf("expected no %q events, got %d; events=%v", noise, n, events)
		}
	}
	for _, nodeID := range []string{"inj", "trig"} {
		if n := countEvents(events, func(ev NodeEvent) bool { return ev.NodeID == nodeID && ev.Message == "activated" }); n != 1 {
			t.Fatalf("expected 1 'activated' for %s, got %d; events=%v", nodeID, n, events)
		}
	}
}

// TestDeployRunsOnlyStoppers asserts the activation pass does NOT
// execute ordinary processing nodes — they must wait for injected
// messages. A deploy must never produce log/debug output.
func TestDeployRunsOnlyStoppers(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-deploy-stoppers",
		Name: "Deploy Stoppers",
		Nodes: []Node{
			{ID: "inj", Type: "inject", Data: map[string]interface{}{"payload": `1`}},
			{ID: "lg", Type: "log", Data: map[string]interface{}{"message": "should not appear"}},
			{ID: "resp", Type: "httpResponse", Data: map[string]interface{}{}},
		},
		Edges: []Edge{{ID: "e1", Source: "inj", Target: "lg"}},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()

	waitForEvents(engine, workflow.ID, 2*time.Second, func(ev NodeEvent) bool {
		return ev.Message == "workflow deployed"
	})
	time.Sleep(50 * time.Millisecond) // let any stray executions land

	_, nodes, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
	if s := nodes["lg"]; s == "running" || s == "ok" {
		t.Fatalf("log node executed on deploy (status %q) — deploy must only activate triggers", s)
	}
	if s := nodes["resp"]; s == "running" || s == "ok" {
		t.Fatalf("httpResponse executed on deploy (status %q)", s)
	}
	if n := countEvents(events, func(ev NodeEvent) bool { return ev.Message == "should not appear" }); n != 0 {
		t.Fatal("log node produced output during deploy")
	}
}

// TestStopperKeepsWorkflowRunning asserts a workflow owning a live
// listener stays "running" after activation, and that StopWorkflow
// actually frees the bound port (the Stopper contract).
func TestStopperKeepsWorkflowRunning(t *testing.T) {
	port := freePort(t)
	workflow := &Workflow{
		ID:   "test-stopper-running",
		Name: "Stopper Running",
		Nodes: []Node{
			{ID: "trig", Type: "httpTrigger", Data: map[string]interface{}{
				"method": "GET", "path": "/ping", "port": port,
			}},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}

	waitForEvents(engine, workflow.ID, 2*time.Second, func(ev NodeEvent) bool {
		return ev.Message == "workflow deployed"
	})
	time.Sleep(50 * time.Millisecond)

	status, err := engine.GetWorkflowStatus(workflow.ID)
	if err != nil || status != "running" {
		t.Fatalf("workflow with live trigger should stay running, got %q (err=%v)", status, err)
	}

	// The port must actually be held while running
	if l, err := net.Listen("tcp", fmt.Sprintf(":%d", port)); err == nil {
		_ = l.Close()
		t.Fatalf("port %d was not bound by the trigger listener", port)
	}

	if err := engine.StopWorkflow(workflow.ID); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
	status, _ = engine.GetWorkflowStatus(workflow.ID)
	if status != "stopped" {
		t.Fatalf("expected stopped after StopWorkflow, got %q", status)
	}

	// The listener must be released — rebinding must succeed
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err == nil {
			_ = l.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("port %d still bound after StopWorkflow — listener leaked", port)
}

// TestInjectTraversesOnlySourceDownstream asserts a message injected at
// a node reaches ONLY that node's downstream subgraph — sibling chains
// must not execute. Locks in the engine-side semantics behind the
// "inject fired the wrong node" editor bug.
func TestInjectTraversesOnlySourceDownstream(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-inject-isolation",
		Name: "Inject Isolation",
		Nodes: []Node{
			{ID: "inj1", Type: "inject", Data: map[string]interface{}{"payload": `1`}},
			{ID: "lg1", Type: "log", Data: map[string]interface{}{"message": "chain-one"}},
			{ID: "inj2", Type: "inject", Data: map[string]interface{}{"payload": `2`}},
			{ID: "lg2", Type: "log", Data: map[string]interface{}{"message": "chain-two"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj1", Target: "lg1"},
			{ID: "e2", Source: "inj2", Target: "lg2"},
		},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()
	waitForEvents(engine, workflow.ID, 2*time.Second, func(ev NodeEvent) bool {
		return ev.Message == "workflow deployed"
	})

	msgID, _, err := engine.InjectMessage(workflow.ID, "inj1", &Message{
		Payload:   "x",
		Metadata:  map[string]interface{}{},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	engine.DiscardPending(msgID)

	events := waitForEvents(engine, workflow.ID, 2*time.Second, func(ev NodeEvent) bool {
		return ev.NodeID == "lg1" && ev.Message == "chain-one"
	})
	if n := countEvents(events, func(ev NodeEvent) bool { return ev.Message == "chain-one" }); n != 1 {
		t.Fatalf("expected 1 message at lg1, got %d; events=%v", n, events)
	}
	if n := countEvents(events, func(ev NodeEvent) bool { return ev.NodeID == "lg2" }); n != 0 {
		t.Fatalf("sibling chain lg2 executed %d times — injection must not cross subgraphs; events=%v", n, events)
	}
}

// TestManualInjectToResponseNode asserts the fire-and-forget contract:
// an injected message that reaches an httpResponse with no pending
// request produces a clear error event (not a hang or a panic). This
// is what the user sees when a manual inject lands on a response node.
func TestManualInjectToResponseNode(t *testing.T) {
	workflow := &Workflow{
		ID:   "test-inject-nopending",
		Name: "Inject NoPending",
		Nodes: []Node{
			{ID: "inj", Type: "inject", Data: map[string]interface{}{"payload": `1`}},
			{ID: "resp", Type: "httpResponse", Data: map[string]interface{}{}},
		},
		Edges: []Edge{{ID: "e1", Source: "inj", Target: "resp"}},
	}

	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(workflow.ID) }()
	waitForEvents(engine, workflow.ID, 2*time.Second, func(ev NodeEvent) bool {
		return ev.Message == "workflow deployed"
	})

	msgID, _, err := engine.InjectMessage(workflow.ID, "inj", &Message{
		Payload:   "x",
		Metadata:  map[string]interface{}{},
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	engine.DiscardPending(msgID) // fire-and-forget, same as the editor button

	events := waitForEvents(engine, workflow.ID, 2*time.Second, func(ev NodeEvent) bool {
		return ev.NodeID == "resp" && ev.Level == "error"
	})
	if n := countEvents(events, func(ev NodeEvent) bool {
		return ev.NodeID == "resp" && ev.Level == "error" &&
			strings.Contains(ev.Message, "no pending request")
	}); n != 1 {
		t.Fatalf("expected 1 'no pending request' error from httpResponse; events=%v", events)
	}
}

// TestStopWorkflowDoesNotBlockRunMu — BUG-4: StopWorkflow must not hold
// runMu while stoppers shut down. An httpTrigger's Shutdown can take up
// to 5s; GetWorkflowSnapshot must stay responsive through it.
func TestStopWorkflowDoesNotBlockRunMu(t *testing.T) {
	engine := NewEngine()
	port := freePort(t)
	wf := &Workflow{
		ID:   "test-stop-runmu",
		Name: "Stop RunMu",
		Nodes: []Node{
			{ID: "trig", Type: "httpTrigger", Data: map[string]interface{}{
				"method": "GET", "path": "/hold", "port": port,
			}},
			// No httpResponse: the request parks until the timeout or
			// the server shuts down — keeping Shutdown busy for seconds.
		},
	}
	if _, err := engine.ExecuteWorkflow(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	time.Sleep(150 * time.Millisecond)

	// Fire a request that will hang waiting for a response.
	respDone := make(chan struct{})
	go func() {
		defer close(respDone)
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/hold", port))
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	time.Sleep(150 * time.Millisecond) // let the request reach the handler

	// Stop in the background — Shutdown blocks on the parked request.
	stopDone := make(chan struct{})
	go func() {
		_ = engine.StopWorkflow(wf.ID)
		close(stopDone)
	}()
	time.Sleep(100 * time.Millisecond)

	// runMu must be free: snapshot polling returns well under 1s.
	start := time.Now()
	engine.GetWorkflowSnapshot(wf.ID, 0)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("GetWorkflowSnapshot blocked %v behind stopper shutdown", elapsed)
	}

	<-stopDone
	<-respDone
}

// TestStopWorkflowCancelsTraversal — BUG-5: a traversal mid-flight
// (blocked in a delay node) must abort when the workflow stops, and
// the downstream node must not execute.
func TestStopWorkflowCancelsTraversal(t *testing.T) {
	engine := NewEngine()
	wf := &Workflow{
		ID:   "test-stop-traversal",
		Name: "Stop Traversal",
		Nodes: []Node{
			{ID: "inj", Type: "inject", Data: map[string]interface{}{}},
			{ID: "dl", Type: "delay", Data: map[string]interface{}{"duration": 5000}},
			{ID: "lg", Type: "log", Data: map[string]interface{}{"message": "reached"}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "dl"},
			{ID: "e2", Source: "dl", Target: "lg"},
		},
	}
	if _, err := engine.ExecuteWorkflow(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()

	if _, _, err := engine.InjectMessage(wf.ID, "inj", &Message{Timestamp: time.Now()}); err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	time.Sleep(150 * time.Millisecond) // traversal is now parked in delay

	if err := engine.StopWorkflow(wf.ID); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
	time.Sleep(150 * time.Millisecond)

	_, nodes, _, _ := engine.GetWorkflowSnapshot(wf.ID, 0)
	if status := nodes["lg"]; status == "ok" || status == "running" {
		t.Fatalf("downstream node executed after stop: status %q", status)
	}
}

// TestRedeployCancelsPreviousRun — N-5: redeploying a running workflow
// must cancel the previous run's context (killing in-flight
// traversals) and leave the new run intact for StopWorkflow.
func TestRedeployCancelsPreviousRun(t *testing.T) {
	engine := NewEngine()
	mk := func() *Workflow {
		return &Workflow{
			ID:   "test-redeploy",
			Name: "Redeploy",
			Nodes: []Node{
				{ID: "trig", Type: "httpTrigger", Data: map[string]interface{}{
					"method": "GET", "path": "/x", "port": freePort(t),
				}},
			},
		}
	}

	if _, err := engine.ExecuteWorkflow(context.Background(), mk(), nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	waitForEvents(engine, "test-redeploy", 2*time.Second, func(ev NodeEvent) bool {
		return ev.Message == "workflow deployed"
	})

	engine.runMu.Lock()
	oldCtx := engine.runCtx["test-redeploy"]
	engine.runMu.Unlock()
	if oldCtx == nil {
		t.Fatal("no run context recorded for first deploy")
	}

	if _, err := engine.ExecuteWorkflow(context.Background(), mk(), nil); err != nil {
		t.Fatalf("redeploy failed: %v", err)
	}
	waitForEvents(engine, "test-redeploy", 2*time.Second, func(ev NodeEvent) bool {
		return ev.Message == "workflow deployed"
	})

	engine.runMu.Lock()
	newCtx := engine.runCtx["test-redeploy"]
	engine.runMu.Unlock()
	if newCtx == nil || newCtx == oldCtx {
		t.Fatal("redeploy did not install a fresh run context")
	}
	select {
	case <-oldCtx.Done():
	default:
		t.Fatal("previous run context was not cancelled on redeploy")
	}
	if err := newCtx.Err(); err != nil {
		t.Fatalf("new run context was cancelled: %v", err)
	}

	// The second run must still be stoppable.
	if err := engine.StopWorkflow("test-redeploy"); err != nil {
		t.Fatalf("stop of second run failed: %v", err)
	}
	status, _ := engine.GetWorkflowStatus("test-redeploy")
	if status != "stopped" {
		t.Fatalf("expected stopped, got %q", status)
	}
}

// TestDeployValidatesNonStoppers — R2-2: node config validation runs at
// deploy time for EVERY node, not only when a message arrives. A
// workflow whose only node has an invalid config must not deploy.
func TestDeployValidatesNonStoppers(t *testing.T) {
	engine := NewEngine()
	wf := &Workflow{
		ID:   "test-deploy-validate",
		Name: "Deploy Validate",
		Nodes: []Node{
			{ID: "sw", Type: "switch", Data: map[string]interface{}{
				"rules": []interface{}{
					map[string]interface{}{"property": "v", "op": "bogus", "value": "1"},
				},
			}},
		},
	}
	if _, err := engine.ExecuteWorkflow(context.Background(), wf, nil); err != nil {
		t.Fatalf("ExecuteWorkflow returned error: %v", err)
	}

	events := waitForEvents(engine, wf.ID, 2*time.Second, func(ev NodeEvent) bool {
		return ev.Message == "workflow deploy failed"
	})
	if n := countEvents(events, func(ev NodeEvent) bool {
		return ev.Message == "workflow deploy failed"
	}); n != 1 {
		t.Fatalf("expected 1 'workflow deploy failed', got %d; events=%v", n, events)
	}
	if n := countEvents(events, func(ev NodeEvent) bool {
		return ev.NodeID == "sw" && ev.Level == "error"
	}); n == 0 {
		t.Fatalf("expected a node error event for sw; events=%v", events)
	}
	if status, _ := engine.GetWorkflowStatus(wf.ID); status == "running" {
		t.Fatal("invalid workflow must not stay running")
	}
}
