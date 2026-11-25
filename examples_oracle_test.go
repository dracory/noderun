package noderunner_test

// Node-RED example flows oracle comparison.
//
// An oracle.json recorded from a REAL Node-RED instance (produced by
// whatever recorder the consuming application uses) captures debug
// output for every example flow. This suite replays the same flows on
// this engine — import, deploy, fire the recorded injects — and
// compares the debug events (node id + formatted message, as a
// multiset) against the oracle.
//
// Opt-in: runs only when NODERUN_EXAMPLES_ORACLE points at an
// oracle.json file; otherwise the suite skips. Example files resolve
// via the same contract as TestNodeREDExampleFlows
// (NODERUN_EXAMPLES_DIR / testdata / NODERUN_EXAMPLES_FETCH).
//
// Honesty rule: every accepted difference is a NAMED entry in
// oracleDifferences below, with a reason keyed per event
// (file + node id + message). An accepted difference that stops
// occurring fails the suite so the map tightens as parity improves.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

// oracleDebugEvent mirrors the recorder's debug event shape.
type oracleDebugEvent struct {
	Node   string `json:"node"`
	Format string `json:"format"`
	Msg    string `json:"msg"`
	Path   string `json:"path,omitempty"`
	Topic  string `json:"topic,omitempty"`
}

// oracleFlow mirrors one recorded flow entry in oracle.json.
type oracleFlow struct {
	File    string             `json:"file"`
	Injects []string           `json:"injects"`
	Debug   []oracleDebugEvent `json:"debug"`
}

type oracleJSON struct {
	Tag   string       `json:"tag"`
	Flows []oracleFlow `json:"flows"`
}

// oracleDifference accepts one event-level difference: file + node +
// message must all match (exact strings). Note is required.
type oracleDifference struct {
	File string
	Node string // empty = any node
	Msg  string // empty = any message (with Node set, accepts all events of that node)
	Note string
}

// oracleDifferences is the named-difference list — the only place
// divergence between this engine and real Node-RED may be recorded.
var oracleDifferences = []oracleDifference{
	// Filled in as real divergences are confirmed — see
	// docs/example-flows.md.
}

func TestNodeREDExampleOracle(t *testing.T) {
	oraclePath := os.Getenv("NODERUN_EXAMPLES_ORACLE")
	if oraclePath == "" {
		t.Skip("no oracle — set NODERUN_EXAMPLES_ORACLE=/path/to/oracle.json (see docs/example-flows.md)")
	}
	data, err := os.ReadFile(oraclePath)
	if err != nil {
		t.Fatalf("read oracle %s: %v", oraclePath, err)
	}
	var orc oracleJSON
	if err := json.Unmarshal(data, &orc); err != nil {
		t.Fatalf("parse oracle %s: %v", oraclePath, err)
	}
	if orc.Tag != examplesTag {
		t.Fatalf("oracle tag %q does not match pinned tag %q — regenerate", orc.Tag, examplesTag)
	}

	dir := examplesDir(t)
	files := map[string]string{}
	for _, f := range exampleFiles(t, dir) {
		rel, _ := filepath.Rel(dir, f)
		files[filepath.ToSlash(rel)] = f
	}

	accepted := map[int]bool{} // oracleDifferences indexes actually used
	var failures int
	for _, of := range orc.Flows {
		path, ok := files[of.File]
		if !ok {
			t.Errorf("oracle flow %s not present in examples dir", of.File)
			continue
		}
		of := of
		t.Run("oracle/"+sanitizeName(of.File), func(t *testing.T) {
			got := runOracleFlow(t, path, of)
			missing, extra := diffOracle(t, of, got, accepted)
			for _, m := range missing {
				t.Errorf("missing debug event (real Node-RED emitted): node=%s msg=%s", m.Node, trunc(m.Msg, 120))
			}
			for _, e := range extra {
				t.Errorf("extra debug event (real Node-RED did not emit): node=%s msg=%s", e.Node, trunc(e.Msg, 120))
			}
			if len(missing)+len(extra) > 0 {
				failures++
			}
		})
	}
	for i, d := range oracleDifferences {
		if !accepted[i] {
			t.Errorf("accepted difference never occurred — remove it: %s node=%s msg=%s (%s)",
				d.File, d.Node, d.Msg, d.Note)
		}
	}
	t.Logf("%d/%d oracle flows matched", len(orc.Flows)-failures, len(orc.Flows))
}

// runOracleFlow deploys one example and returns its debug events in
// recorder shape (node id + formatted message).
func runOracleFlow(t *testing.T, path string, of oracleFlow) []oracleDebugEvent {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", of.File, err)
	}
	w, err := nodestore.ImportNodeRED(raw)
	if err != nil {
		t.Fatalf("import %s: %v", of.File, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	engine := NewWorkflowEngine()
	if _, err := engine.ExecuteWorkflow(ctx, w, nil); err != nil {
		t.Fatalf("deploy %s: %v", of.File, err)
	}
	defer func() { _ = engine.StopWorkflow(w.GetID()) }()

	for _, id := range of.Injects {
		if _, _, err := engine.InjectMessage(w.GetID(), id, nodestore.NewMessage().SetTimestamp(time.Now())); err != nil {
			t.Fatalf("inject %s in %s: %v", id, of.File, err)
		}
	}
	time.Sleep(500 * time.Millisecond)

	_, _, events, _ := engine.GetWorkflowSnapshot(w.GetID(), 0)
	var got []oracleDebugEvent
	for _, ev := range events {
		if ev.Level == "debug" && ev.NodeID != "" {
			got = append(got, oracleDebugEvent{Node: ev.NodeID, Msg: ev.Message})
		}
	}
	return got
}

// diffOracle compares want/got as multisets over (node, msg). Events
// matching a named oracleDifferences entry are skipped (and the entry
// is marked as used).
func diffOracle(t *testing.T, of oracleFlow, got []oracleDebugEvent, accepted map[int]bool) (missing, extra []oracleDebugEvent) {
	t.Helper()

	skip := func(ev oracleDebugEvent) bool {
		for i, d := range oracleDifferences {
			if d.File != of.File {
				continue
			}
			if d.Node != "" && d.Node != ev.Node {
				continue
			}
			if d.Msg != "" && d.Msg != ev.Msg {
				continue
			}
			accepted[i] = true
			return true
		}
		return false
	}

	type key struct{ node, msg string }
	want := map[key]int{}
	for _, ev := range of.Debug {
		if skip(ev) {
			continue
		}
		want[key{ev.Node, ev.Msg}]++
	}
	gots := map[key]int{}
	for _, ev := range got {
		if skip(ev) {
			continue
		}
		gots[key{ev.Node, ev.Msg}]++
	}
	for k, n := range want {
		if gots[k] < n {
			for i := 0; i < n-gots[k]; i++ {
				missing = append(missing, oracleDebugEvent{Node: k.node, Msg: k.msg})
			}
		}
	}
	for k, n := range gots {
		if want[k] < n {
			for i := 0; i < n-want[k]; i++ {
				extra = append(extra, oracleDebugEvent{Node: k.node, Msg: k.msg})
			}
		}
	}
	return missing, extra
}

func trunc(s string, n int) string {
	if len(s) > n {
		return fmt.Sprintf("%s…(%d bytes)", s[:n], len(s))
	}
	return s
}
