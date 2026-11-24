package noderunner

import (
	"crypto/tls"
	"encoding/json"
	"fmt"

	"github.com/dracory/nodebase"
)

// LoadTLSConfig resolves a tlsConfig node reference (node id within
// the workflow) into a ready *tls.Config for a client dial. It returns
// an error when the referenced node is missing, is not a tlsConfig
// node, or its files cannot be loaded.
func (e *WorkflowEngine) LoadTLSConfig(workflowID, ref string) (*tls.Config, error) {
	node, ok := e.LookupNode(workflowID, ref)
	if !ok {
		return nil, fmt.Errorf("tls config %q not found in workflow", ref)
	}
	if node.GetType() != "tlsConfig" {
		return nil, fmt.Errorf("node %q is type %q, not tlsConfig", ref, node.GetType())
	}
	raw, err := json.Marshal(node.GetData())
	if err != nil {
		return nil, fmt.Errorf("invalid tlsConfig data: %w", err)
	}
	cfg := &nodebase.TLSConfig{VerifyServerCert: true}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("invalid tlsConfig data: %w", err)
	}
	return cfg.Build()
}
