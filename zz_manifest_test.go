package noderunner_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestSpecManifest dumps the registry of ported upstream titles to
// the file named by GOFLOW_SPEC_MANIFEST. The coverage generator
// (cmd/speccoverage) sets that variable when it runs this suite.
// File order puts this test last (zz_) so every it() has registered.
func TestSpecManifest(t *testing.T) {
	out := os.Getenv("GOFLOW_SPEC_MANIFEST")
	if out == "" {
		t.Skip("GOFLOW_SPEC_MANIFEST not set")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if dir := filepath.Dir(out); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("create manifest dir: %v", err)
		}
	}
	data, err := json.MarshalIndent(registry, "", "  ")
	if err != nil {
		t.Fatalf("marshal registry: %v", err)
	}
	if err := os.WriteFile(out, data, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	t.Logf("wrote %d ported titles to %s", len(registry), out)
}
