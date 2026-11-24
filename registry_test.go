package noderunner

import (
	"context"
	nodestore "github.com/dracory/nodestore"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// testHookNode is a minimal custom node: implements the required
// NodeExecutor contract and reports executions through a counter.
type testHookNode struct {
	*BaseNode
	ran *atomic.Int32
}

func (n *testHookNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	n.ran.Add(1)
	return input, nil
}

// waitFor polls cond until it holds or the timeout expires.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestRegisterNodeTypeDeployAndExecute: a third-party type registered
// through the public API (the node-pack init() path) deploys and
// executes like a built-in.
func TestRegisterNodeTypeDeployAndExecute(t *testing.T) {
	var ran atomic.Int32
	if err := RegisterNodeType("testhook", func(node Node, e *Engine) (NodeExecutor, error) {
		return &testHookNode{BaseNode: NewBaseNode(node.ID, node.Type), ran: &ran}, nil
	}); err != nil {
		t.Fatalf("RegisterNodeType: %v", err)
	}

	engine := NewEngine()
	wf := &Workflow{
		ID:   "wf-testhook",
		Name: "registry test",
		Nodes: []Node{
			{ID: "j1", Type: "junction", Data: map[string]interface{}{}},
			{ID: "h1", Type: "testhook", Data: map[string]interface{}{}},
		},
		Edges: []Edge{{ID: "e1", Source: "j1", Target: "h1"}},
	}
	if _, err := engine.DeployWorkflow(context.Background(), wf, &Message{Timestamp: time.Now()}); err != nil {
		t.Fatalf("DeployWorkflow: %v", err)
	}

	if _, _, err := engine.InjectMessage("wf-testhook", "j1", &Message{
		Payload:   map[string]interface{}{"payload": "hi"},
		Metadata:  map[string]interface{}{},
		Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("InjectMessage: %v", err)
	}
	waitFor(t, "testhook execution", func() bool { return ran.Load() == 1 })
}

// TestRegisterCredentialKeysCustomType: credential keys registered for
// a custom type flow through the same mask/strip machinery as
// built-ins — nodestore.IsCredentialKey reports them and the Node-RED export
// strips them entirely.
func TestRegisterCredentialKeysCustomType(t *testing.T) {
	nodestore.RegisterCredentialKeys("testhook", []string{"apiKey"})

	if !nodestore.IsCredentialKey("testhook", "apiKey") {
		t.Fatal("testhook.apiKey should be a credential key after nodestore.RegisterCredentialKeys")
	}
	if nodestore.IsCredentialKey("testhook", "endpoint") {
		t.Fatal("testhook.endpoint is not a credential key")
	}

	w := &Workflow{
		ID:   "wf-cred-export",
		Name: "cred export",
		Nodes: []Node{
			{
				ID:   "c1",
				Type: "testhook",
				Data: map[string]interface{}{
					"endpoint": "https://example.org",
					"apiKey":   nodestore.CredentialSentinel,
				},
			},
			{
				ID:   "c2",
				Type: "testhook",
				Data: map[string]interface{}{"apiKey": "pl4intext-secret"},
			},
		},
	}
	raw, err := nodestore.ExportNodeRED(w)
	if err != nil {
		t.Fatalf("ExportNodeRED: %v", err)
	}
	out := string(raw)
	for _, leak := range []string{"apiKey", "pl4intext-secret", nodestore.CredentialSentinel} {
		if strings.Contains(out, leak) {
			t.Fatalf("export leaks %q: %s", leak, out)
		}
	}
	if !strings.Contains(out, "endpoint") {
		t.Fatalf("export should keep non-credential fields: %s", out)
	}
}

// testStopperNode is a custom long-lived node: implements Stopper and
// the stopKey dedupe contract (like queue/mailIn), sharing state
// across executor rebuilds via the shared struct.
type testStopperNode struct {
	*BaseNode
	shared *testStopperShared
}

type testStopperShared struct {
	ran   atomic.Int32
	stops atomic.Int32
}

func (n *testStopperNode) Execute(ctx context.Context, input nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	n.shared.ran.Add(1)
	return input, nil
}

func (n *testStopperNode) Stop() error {
	n.shared.stops.Add(1)
	return nil
}

// Executors are rebuilt per message; one logical resource survives
// via a stable StopKey.
func (n *testStopperNode) StopKey() string { return "teststopper:" + n.ID() }

// TestCustomStopperDedupesAcrossDeploys: an engine-scoped registered
// Stopper node registers once (stopKey dedupe covers executor
// rebuilds), survives an unchanged partial deploy, and is stopped and
// replaced when its config changes.
func TestCustomStopperDedupesAcrossDeploys(t *testing.T) {
	shared := &testStopperShared{}
	engine := NewEngine()
	if err := engine.RegisterNodeType("teststopper", func(node Node, e *Engine) (NodeExecutor, error) {
		return &testStopperNode{BaseNode: NewBaseNode(node.ID, node.Type), shared: shared}, nil
	}); err != nil {
		t.Fatalf("RegisterNodeType: %v", err)
	}

	build := func(data map[string]interface{}) *Workflow {
		return &Workflow{
			ID:   "wf-stopper",
			Name: "stopper test",
			Nodes: []Node{
				{ID: "j1", Type: "junction", Data: map[string]interface{}{}},
				{ID: "s1", Type: "teststopper", Data: data},
			},
			Edges: []Edge{{ID: "e1", Source: "j1", Target: "s1"}},
		}
	}
	stopperCount := func() int {
		engine.runMu.Lock()
		defer engine.runMu.Unlock()
		return len(engine.stoppers["wf-stopper"])
	}

	if _, err := engine.DeployWorkflow(context.Background(), build(map[string]interface{}{"endpoint": "a"}), &Message{Timestamp: time.Now()}); err != nil {
		t.Fatalf("DeployWorkflow: %v", err)
	}
	if got := stopperCount(); got != 1 {
		t.Fatalf("expected 1 stopper after deploy, got %d", got)
	}

	// A message through the node rebuilds its executor; stopKey
	// dedupe must keep the registration at one.
	if _, _, err := engine.InjectMessage("wf-stopper", "j1", &Message{
		Payload:   map[string]interface{}{"payload": "x"},
		Metadata:  map[string]interface{}{},
		Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("InjectMessage: %v", err)
	}
	waitFor(t, "stopper message execution", func() bool { return shared.ran.Load() >= 2 })
	if got := stopperCount(); got != 1 {
		t.Fatalf("stopper registered twice across executor rebuilds: got %d", got)
	}

	// Unchanged partial deploy: the running stopper is kept — not
	// stopped, not duplicated.
	if _, err := engine.DeployWorkflowPartial(context.Background(), build(map[string]interface{}{"endpoint": "a"}), &Message{Timestamp: time.Now()}); err != nil {
		t.Fatalf("DeployWorkflowPartial (unchanged): %v", err)
	}
	if got := stopperCount(); got != 1 {
		t.Fatalf("expected 1 kept stopper after unchanged partial deploy, got %d", got)
	}
	if got := shared.stops.Load(); got != 0 {
		t.Fatalf("unchanged stopper was stopped: %d", got)
	}

	// Changed partial deploy: the dirty node is stopped and a fresh
	// executor registered — still exactly one live stopper.
	if _, err := engine.DeployWorkflowPartial(context.Background(), build(map[string]interface{}{"endpoint": "b"}), &Message{Timestamp: time.Now()}); err != nil {
		t.Fatalf("DeployWorkflowPartial (changed): %v", err)
	}
	if got := stopperCount(); got != 1 {
		t.Fatalf("expected 1 stopper after changed partial deploy, got %d", got)
	}
	if got := shared.stops.Load(); got != 1 {
		t.Fatalf("expected the replaced stopper to be stopped once, got %d", got)
	}
}
