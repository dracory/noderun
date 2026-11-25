package noderunner_test

import (
	"context"
	"testing"

	_ "github.com/dracory/nodebase/builtin"
	. "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

// TestCredentialsProviderInjection covers the deploy-time credential
// seam: the engine asks CredentialsProvider for the node's secrets and
// merges them into the deployed graph — the stored workflow keeps only
// sentinel markers (flowadmin injectCredentials parity).
func TestCredentialsProviderInjection(t *testing.T) {
	nodestore.RegisterCredentialKeys("junction", []string{"password"})
	t.Cleanup(func() { nodestore.RegisterCredentialKeys("junction", nil) })

	engine := NewEngine()
	var captured map[string]map[string]string
	engine.CredentialsProvider = func(ctx context.Context, workflowID string) (map[string]map[string]string, error) {
		captured = map[string]map[string]string{}
		return map[string]map[string]string{
			"n1": {"password": "hunter2"},
		}, nil
	}

	wf := nodestore.NewWorkflow("w").SetID("wf-creds")
	node := nodestore.NewNode("n1", "junction").SetData(map[string]any{
		"password": nodestore.CredentialSentinel,
	})
	wf.SetNodes([]nodestore.NodeInterface{node})

	if _, err := engine.DeployWorkflow(context.Background(), wf, nil); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow("wf-creds") })

	if captured == nil {
		t.Fatal("credentials provider was not consulted")
	}
	running, ok := engine.RunningFlow("wf-creds")
	if !ok {
		t.Fatal("workflow not running")
	}
	rn := running.FindNodeByID("n1")
	if rn == nil {
		t.Fatal("node n1 missing from running flow")
	}
	// The running-flow view re-masks credentials to the sentinel —
	// injected secrets are visible only to the executing node.
	if rn.GetData()["password"] != nodestore.CredentialSentinel {
		t.Fatalf("running flow leaked injected secret: %v", rn.GetData()["password"])
	}
	// The caller's stored document must keep the sentinel — deploy
	// works on a clone.
	if node.GetData()["password"] != nodestore.CredentialSentinel {
		t.Fatalf("stored node tainted with secret: %v", node.GetData()["password"])
	}
}
