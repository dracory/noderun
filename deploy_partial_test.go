package noderunner

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

// stoppersOf returns a snapshot of the workflow's registered stoppers.
func stoppersOf(e *Engine, workflowID string) []Stopper {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	return append([]Stopper(nil), e.stoppers[workflowID]...)
}

// stopperFor returns the registered stopper belonging to nodeID (the
// stopper is an executor, so ID() is the node ID).
func stopperFor(e *Engine, workflowID, nodeID string) Stopper {
	for _, s := range stoppersOf(e, workflowID) {
		if ex, ok := s.(NodeExecutor); ok && ex.ID() == nodeID {
			return s
		}
	}
	return nil
}

// partialFlow builds a tcpIn -> debug workflow for partial-deploy tests.
func partialFlow(id string, port int, debugMsg string) *Workflow {
	return &Workflow{
		ID:   id,
		Name: "Partial Deploy Flow",
		Nodes: []Node{
			{ID: "tcp", Type: "tcpIn", Data: map[string]interface{}{
				"mode": "listen", "port": float64(port),
			}},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{
				"message": debugMsg,
			}},
		},
		Edges: []Edge{{ID: "e1", Source: "tcp", Target: "dbg"}},
	}
}

// TestPartialDeployKeepsUnchangedStopper is the core acceptance case:
// editing one node must not restart an unchanged tcpIn listener — the
// same executor instance stays registered and the socket stays bound.
func TestPartialDeployKeepsUnchangedStopper(t *testing.T) {
	port := freePort(t)
	engine := NewEngine()
	wf := partialFlow("test-partial-keep", port, "v1")

	if _, err := engine.DeployWorkflowPartial(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()

	before := stopperFor(engine, wf.ID, "tcp")
	if before == nil {
		t.Fatal("tcpIn stopper not registered")
	}

	// Redeploy with only the debug node changed.
	wf2 := partialFlow(wf.ID, port, "v2")
	if _, err := engine.DeployWorkflowPartial(context.Background(), wf2, nil); err != nil {
		t.Fatalf("partial redeploy failed: %v", err)
	}

	after := stopperFor(engine, wf.ID, "tcp")
	if after != before {
		t.Fatal("unchanged tcpIn stopper was restarted — expected same instance")
	}

	// The listener must still accept connections (socket survived).
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 2*time.Second)
	if err != nil {
		t.Fatalf("tcpIn listener not reachable after partial deploy: %v", err)
	}
	_ = conn.Close()

	// The kept node must not have logged a second "activated" event.
	_, _, events, _ := engine.GetWorkflowSnapshot(wf.ID, 0)
	if n := countEvents(events, func(ev NodeEvent) bool {
		return ev.NodeID == "tcp" && ev.Message == "activated"
	}); n != 1 {
		t.Fatalf("expected exactly 1 'activated' for kept tcpIn, got %d", n)
	}
}

// TestPartialDeployRestartsChangedStopper verifies a reconfigured
// stopper is replaced: the old listener is torn down and the new one
// binds the new port.
func TestPartialDeployRestartsChangedStopper(t *testing.T) {
	port1 := freePort(t)
	port2 := freePort(t)
	engine := NewEngine()
	wf := partialFlow("test-partial-change", port1, "v1")

	if _, err := engine.DeployWorkflowPartial(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()

	before := stopperFor(engine, wf.ID, "tcp")

	// Redeploy with the tcpIn node moved to a different port.
	wf2 := partialFlow(wf.ID, port2, "v1")
	if _, err := engine.DeployWorkflowPartial(context.Background(), wf2, nil); err != nil {
		t.Fatalf("partial redeploy failed: %v", err)
	}

	after := stopperFor(engine, wf.ID, "tcp")
	if after == nil || after == before {
		t.Fatal("changed tcpIn stopper was not replaced")
	}

	// Old port released, new port bound.
	if c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port1), time.Second); err == nil {
		_ = c.Close()
		t.Fatal("old tcpIn port still accepting — old listener not torn down")
	}
	if c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port2), 2*time.Second); err != nil {
		t.Fatalf("new tcpIn port not bound: %v", err)
	} else {
		_ = c.Close()
	}
}

// TestPartialDeployAddedAndRemovedNodes covers the add/remove cases:
// an added stopper activates, a removed stopper is torn down, and the
// untouched one survives.
func TestPartialDeployAddedAndRemovedNodes(t *testing.T) {
	port := freePort(t)
	engine := NewEngine()
	wf := partialFlow("test-partial-addremove", port, "v1")
	// Add an inject node to the first deploy.
	wf.Nodes = append(wf.Nodes, Node{
		ID: "inj", Type: "inject",
		Data: map[string]interface{}{"payload": "1", "interval": float64(60)},
	})

	if _, err := engine.DeployWorkflowPartial(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()

	tcpBefore := stopperFor(engine, wf.ID, "tcp")
	injBefore := stopperFor(engine, wf.ID, "inj")
	if tcpBefore == nil || injBefore == nil {
		t.Fatalf("expected tcpIn and inject stoppers, got %v", stoppersOf(engine, wf.ID))
	}

	// Redeploy: remove the inject node, keep everything else.
	wf2 := partialFlow(wf.ID, port, "v1")
	if _, err := engine.DeployWorkflowPartial(context.Background(), wf2, nil); err != nil {
		t.Fatalf("partial redeploy failed: %v", err)
	}

	if got := stopperFor(engine, wf.ID, "inj"); got != nil {
		t.Fatal("removed inject node still has a registered stopper")
	}
	if got := stopperFor(engine, wf.ID, "tcp"); got != tcpBefore {
		t.Fatal("unchanged tcpIn stopper was restarted")
	}

	// Redeploy again: add a fresh inject node — it must activate.
	wf3 := partialFlow(wf.ID, port, "v1")
	wf3.Nodes = append(wf3.Nodes, Node{
		ID: "inj2", Type: "inject",
		Data: map[string]interface{}{"payload": "1", "interval": float64(60)},
	})
	if _, err := engine.DeployWorkflowPartial(context.Background(), wf3, nil); err != nil {
		t.Fatalf("partial redeploy failed: %v", err)
	}
	if got := stopperFor(engine, wf.ID, "inj2"); got == nil {
		t.Fatal("added inject node was not activated")
	}
	if got := stopperFor(engine, wf.ID, "tcp"); got != tcpBefore {
		t.Fatal("unchanged tcpIn stopper was restarted")
	}
}

// TestPartialDeployWiringChangeRestartsBothEnds verifies that rewiring
// marks both ends of the changed edge dirty — even though the inject
// node's own config is unchanged, its stopper is restarted.
func TestPartialDeployWiringChangeRestartsBothEnds(t *testing.T) {
	port := freePort(t)
	engine := NewEngine()

	mkFlow := func(target string) *Workflow {
		return &Workflow{
			ID:   "test-partial-wiring",
			Name: "Wiring",
			Nodes: []Node{
				{ID: "tcp", Type: "tcpIn", Data: map[string]interface{}{
					"mode": "listen", "port": float64(port),
				}},
				{ID: "dbg1", Type: "debug", Data: map[string]interface{}{"message": "a"}},
				{ID: "dbg2", Type: "debug", Data: map[string]interface{}{"message": "b"}},
			},
			Edges: []Edge{{ID: "e1", Source: "tcp", Target: target}},
		}
	}

	wf := mkFlow("dbg1")
	if _, err := engine.DeployWorkflowPartial(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()

	before := stopperFor(engine, wf.ID, "tcp")

	// Rewire tcp -> dbg2. tcp's config is identical; the wiring change
	// alone must restart it.
	wf2 := mkFlow("dbg2")
	if _, err := engine.DeployWorkflowPartial(context.Background(), wf2, nil); err != nil {
		t.Fatalf("partial redeploy failed: %v", err)
	}

	if got := stopperFor(engine, wf.ID, "tcp"); got == before {
		t.Fatal("wiring change did not restart the edge's source node")
	}
}

// TestPartialDeployIdenticalFlowIsNoop redeploys an unchanged flow and
// asserts nothing restarts.
func TestPartialDeployIdenticalFlowIsNoop(t *testing.T) {
	port := freePort(t)
	engine := NewEngine()
	wf := partialFlow("test-partial-noop", port, "v1")

	if _, err := engine.DeployWorkflowPartial(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()

	before := stoppersOf(engine, wf.ID)

	wf2 := partialFlow(wf.ID, port, "v1")
	if _, err := engine.DeployWorkflowPartial(context.Background(), wf2, nil); err != nil {
		t.Fatalf("partial redeploy failed: %v", err)
	}

	after := stoppersOf(engine, wf.ID)
	if len(after) != len(before) {
		t.Fatalf("stopper count changed: %d -> %d", len(before), len(after))
	}
	for i := range before {
		if after[i] != before[i] {
			t.Fatal("unchanged flow restarted a stopper")
		}
	}
}

// TestPartialDeploySubflowReExpansion verifies the diff runs on the
// subflowDefNodeShim adapts subflowDefNode (subflow_test.go) to the
// shim Node type used by this file's Workflow literals.
func subflowDefNodeShim(id, name string, in, out []interface{}, env []interface{}, nodes, edges []interface{}) Node {
	return Node{ID: id, Type: "subflow", Data: map[string]interface{}{
		"name": name, "in": in, "out": out, "env": env, "nodes": nodes, "edges": edges,
	}}
}

// expanded graph: editing a subflow definition changes the namespaced
// internal nodes, while an untouched inject trigger stays running.
func TestPartialDeploySubflowReExpansion(t *testing.T) {
	mkFlow := func(innerFunc string) *Workflow {
		return &Workflow{
			ID:   "test-partial-subflow",
			Name: "Subflow Partial",
			Nodes: []Node{
				subflowDefNodeShim(
					"def1", "Wrap",
					[]interface{}{map[string]interface{}{"wires": []interface{}{map[string]interface{}{"id": "fn"}}}},
					[]interface{}{map[string]interface{}{"wires": []interface{}{map[string]interface{}{"id": "fn", "port": 0.0}}}},
					nil,
					[]interface{}{map[string]interface{}{
						"id":   "fn",
						"type": "function",
						"data": map[string]interface{}{"func": innerFunc},
					}},
					nil,
				),
				{ID: "inj", Type: "inject", Data: map[string]interface{}{
					"payload": "hi", "payloadType": "str",
				}},
				{ID: "inst", Type: "subflow:def1", Data: map[string]interface{}{}},
				{ID: "dbg", Type: "debug", Data: map[string]interface{}{}},
			},
			Edges: []Edge{
				{ID: "e1", Source: "inj", Target: "inst"},
				{ID: "e2", Source: "inst", Target: "dbg"},
			},
		}
	}

	engine := NewEngine()
	wf := mkFlow(`return msg;`)
	if _, err := engine.DeployWorkflowPartial(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()

	before := stopperFor(engine, wf.ID, "inj")
	if before == nil {
		t.Fatal("inject stopper not registered")
	}

	// Edit the subflow definition's internal node. The inject node is
	// unchanged and must keep running.
	wf2 := mkFlow(`msg.payload = "inner"; return msg;`)
	if _, err := engine.DeployWorkflowPartial(context.Background(), wf2, nil); err != nil {
		t.Fatalf("partial redeploy failed: %v", err)
	}

	if got := stopperFor(engine, wf.ID, "inj"); got != before {
		t.Fatal("subflow def edit restarted an unrelated stopper")
	}
}

// TestFullDeployStillRestartsEverything guards the default path:
// DeployWorkflow (full mode) replaces every stopper even when nothing
// changed.
func TestFullDeployStillRestartsEverything(t *testing.T) {
	port := freePort(t)
	engine := NewEngine()
	wf := partialFlow("test-full-restart", port, "v1")

	if _, err := engine.DeployWorkflow(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	defer func() { _ = engine.StopWorkflow(wf.ID) }()

	before := stopperFor(engine, wf.ID, "tcp")

	wf2 := partialFlow(wf.ID, port, "v1")
	if _, err := engine.DeployWorkflow(context.Background(), wf2, nil); err != nil {
		t.Fatalf("full redeploy failed: %v", err)
	}

	if got := stopperFor(engine, wf.ID, "tcp"); got == before {
		t.Fatal("full deploy kept an old stopper — expected restart")
	}
}
