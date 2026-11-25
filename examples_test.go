package noderunner_test

// Node-RED example flows smoke harness.
//
// The upstream node-red project ships example flows for its core nodes
// under packages/node_modules/@node-red/nodes/examples/ — the same flows
// the Import → Examples menu offers. This suite fetches them at the
// pinned tag (examplesTag below), imports each via
// nodestore.ImportNodeRED, deploys on a fresh engine, fires every inject
// node, and asserts no panic / no unexplained failure.
//
// Hermetic by default: with no examples present the suite skips.
// Resolve the examples dir in this order:
//  1. NODERUN_EXAMPLES_DIR — a local path containing the .json files
//  2. testdata/nodered-examples/ — previously fetched files
//  3. NODERUN_EXAMPLES_FETCH=1 — fetch from GitHub into testdata/ and run
//
// Honesty rule: every tolerated failure is a NAMED entry in
// exampleExpectations below with a written reason — silent xfails are
// banned. A tolerated file that starts passing fails the suite so the
// map gets tightened as parity improves.
//
// The aggregated node-type usage report is written next to the
// examples dir as nodered-examples-usage.json whenever the suite runs
// (both files are gitignored — upstream examples are fetched, not
// committed).

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dracory/nodebase"
	. "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

// examplesTag is the upstream Node-RED release the examples are
// fetched from — keep in sync with cmd/speccoverage's pinnedTag.
const examplesTag = "5.0.10"

// examplesUpstreamPrefix is the repo path inside node-red/node-red
// holding the example flow JSON files.
const examplesUpstreamPrefix = "packages/node_modules/@node-red/nodes/examples/"

// exampleExpectation records a tolerated outcome for one example file
// (keyed by its path relative to the examples dir). Empty fields mean
// "must succeed". Every entry needs Note — a written reason.
type exampleExpectation struct {
	ImportFails bool   // import is expected to fail
	DeployFails bool   // ExecuteWorkflow is expected to error
	Note        string // why this is tolerated — required
}

// exampleExpectations is the named-tolerance list. Add entries ONLY
// with an investigated reason; a passing tolerated file fails the
// suite until its entry is removed.
var exampleExpectations = map[string]exampleExpectation{
	// Filled in as gaps are confirmed — see docs/example-flows.md.
}

// exampleUsageEntry is one row of the emitted usage report.
type exampleUsageEntry struct {
	File       string   `json:"file"`
	Types      []string `json:"types"`    // node type ids used by the flow
	Unmapped   []string `json:"unmapped"` // types with no registered executor
	Deploys    bool     `json:"deploys"`
	ExpectedTo string   `json:"expected_to,omitempty"` // tolerated outcome, if any
	Note       string   `json:"note,omitempty"`
}

func TestNodeREDExampleFlows(t *testing.T) {
	dir := examplesDir(t)
	files := exampleFiles(t, dir)
	if len(files) == 0 {
		t.Skip("no example .json files found — set NODERUN_EXAMPLES_DIR or NODERUN_EXAMPLES_FETCH=1")
	}
	t.Logf("running %d example flows from %s", len(files), dir)

	var usage []exampleUsageEntry
	for _, f := range files {
		rel, _ := filepath.Rel(dir, f)
		rel = filepath.ToSlash(rel)
		t.Run("examples/"+sanitizeName(rel), func(t *testing.T) {
			usage = append(usage, runExample(t, f, rel))
		})
	}

	writeUsageReport(t, dir, usage)
}

// examplesDir resolves the examples directory per the env contract.
func examplesDir(t *testing.T) string {
	t.Helper()
	dir := os.Getenv("NODERUN_EXAMPLES_DIR")
	if dir == "" {
		dir = filepath.Join("testdata", "nodered-examples")
	}
	if _, err := os.Stat(dir); err == nil {
		return dir
	}
	if os.Getenv("NODERUN_EXAMPLES_FETCH") == "1" {
		if err := fetchExampleFiles(dir); err != nil {
			t.Fatalf("fetch examples: %v", err)
		}
		return dir
	}
	t.Skip("example flows not fetched — set NODERUN_EXAMPLES_DIR or NODERUN_EXAMPLES_FETCH=1")
	return ""
}

// exampleFiles walks dir for *.json files.
func exampleFiles(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(p, ".json") && !strings.HasSuffix(p, "-usage.json") {
			files = append(files, p)
		}
		return nil
	})
	sort.Strings(files)
	return files
}

// runExample drives one example file and returns its usage row. Fail-
// loud discipline: unexplained failures error the subtest; expected
// failures are checked against exampleExpectations both ways.
func runExample(t *testing.T, path, rel string) (u exampleUsageEntry) {
	t.Helper()
	u.File = rel
	exp, tolerated := exampleExpectations[rel]

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	w, err := nodestore.ImportNodeRED(data)
	if err != nil {
		if !(tolerated && exp.ImportFails) {
			t.Fatalf("import failed (no expectation recorded): %v — investigate, then fix or add a named expectation", err)
		}
		u.ExpectedTo = "import-fail"
		u.Note = exp.Note
		return u
	}
	if tolerated && exp.ImportFails {
		t.Fatalf("import now succeeds — remove the expectation for %s (was: %s)", rel, exp.Note)
	}

	types := map[string]bool{}
	var injectIDs []string
	for _, n := range w.GetNodes() {
		types[n.GetType()] = true
		if n.GetType() == "inject" {
			injectIDs = append(injectIDs, n.GetID())
		}
	}
	for ty := range types {
		u.Types = append(u.Types, ty)
		if _, ok := nodebase.RegisteredNode(ty); !ok {
			u.Unmapped = append(u.Unmapped, ty)
		}
	}
	sort.Strings(u.Types)
	sort.Strings(u.Unmapped)

	// Deploy with a bounded context; a panic escapes as a test crash
	// which is correct — a panic is never a tolerated outcome.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	engine := NewWorkflowEngine()
	deployErr := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("deploy panicked: %v", r)
			}
		}()
		_, err = engine.ExecuteWorkflow(ctx, w, nil)
		return err
	}()
	defer func() { _ = engine.StopWorkflow(w.GetID()) }()

	if deployErr != nil {
		if !(tolerated && exp.DeployFails) {
			t.Fatalf("deploy failed (no expectation recorded): %v — investigate, then fix or add a named expectation", deployErr)
		}
		u.ExpectedTo = "deploy-fail"
		u.Note = exp.Note
		return u
	}
	if tolerated && exp.DeployFails {
		t.Fatalf("deploy now succeeds — remove the expectation for %s (was: %s)", rel, exp.Note)
	}

	// Fire every inject node (button press), then let traversal settle.
	for _, id := range injectIDs {
		if _, _, err := engine.InjectMessage(w.GetID(), id, nodestore.NewMessage().SetTimestamp(time.Now())); err != nil {
			t.Fatalf("inject into %s failed: %v", id, err)
		}
	}
	time.Sleep(500 * time.Millisecond)

	u.Deploys = true
	if tolerated {
		t.Fatalf("example %s passes but has a tolerated expectation — remove it (was: %s)", rel, exp.Note)
	}
	return u
}

// writeUsageReport emits the aggregated per-file usage report beside
// the examples dir (gitignored — regenerated, not committed).
func writeUsageReport(t *testing.T, dir string, usage []exampleUsageEntry) {
	t.Helper()
	out := map[string]interface{}{
		"generated": time.Now().UTC().Format(time.RFC3339),
		"tag":       examplesTag,
		"files":     usage,
	}
	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		t.Fatalf("marshal usage: %v", err)
	}
	p := filepath.Join(dir, "nodered-examples-usage.json")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatalf("write usage report: %v", err)
	}
	t.Logf("usage report: %s", p)
}

// -------------------------------------------------------------------
// Fetching the upstream examples (GitHub tree API + raw files — the
// speccoverage pattern; examples are Apache-2.0, fetched not committed)
// -------------------------------------------------------------------

func fetchExampleFiles(dir string) error {
	treeURL := fmt.Sprintf("https://api.github.com/repos/node-red/node-red/git/trees/%s?recursive=1", examplesTag)
	req, err := http.NewRequest("GET", treeURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "noderun-example-flows")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("GET %s: %s", treeURL, resp.Status)
	}
	var tree struct {
		Tree []struct {
			Path string `json:"path"`
		} `json:"tree"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tree); err != nil {
		return err
	}
	var paths []string
	for _, e := range tree.Tree {
		if strings.HasPrefix(e.Path, examplesUpstreamPrefix) && strings.HasSuffix(e.Path, ".json") {
			paths = append(paths, e.Path)
		}
	}
	if len(paths) == 0 {
		return fmt.Errorf("no example files found under %s @%s", examplesUpstreamPrefix, examplesTag)
	}
	for _, p := range paths {
		rel := strings.TrimPrefix(p, examplesUpstreamPrefix)
		raw, err := httpGet(fmt.Sprintf("https://raw.githubusercontent.com/node-red/node-red/%s/%s", examplesTag, p))
		if err != nil {
			return err
		}
		dst := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(dst, raw, 0o644); err != nil {
			return err
		}
	}
	fmt.Printf("fetched %d example flows @%s into %s\n", len(paths), examplesTag, dir)
	return nil
}

func httpGet(url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "noderun-example-flows")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return io.ReadAll(resp.Body)
}
