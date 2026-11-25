package noderunner_test

import (
	"context"
	"encoding/json"
	"github.com/dracory/nodebase"
	_ "github.com/dracory/nodebase/builtin"
	. "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
	"strings"
	"testing"
	"time"
)

// journalFlow builds inj -> a -> b (junction pass-throughs).
func journalFlow(id string) nodestore.WorkflowInterface {
	return nodestore.NewWorkflow(id).SetID(id).SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inj", "junction").SetData(map[string]interface{}{}),
		nodestore.NewNode("a", "junction").SetData(map[string]interface{}{}),
		nodestore.NewNode("b", "junction").SetData(map[string]interface{}{}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("e1", "inj", "a"),
		nodestore.NewEdge("e2", "a", "b"),
	})
}

// waitForJournal polls the node's journal until it holds n entries.
func waitForJournal(t *testing.T, engine *Engine, workflowID, nodeID string, n int) []nodebase.RunJournalEntry {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		entries, ok := engine.RunJournal(workflowID, nodeID)
		if ok && len(entries) >= n {
			return entries
		}
		time.Sleep(10 * time.Millisecond)
	}
	entries, _ := engine.RunJournal(workflowID, nodeID)
	t.Fatalf("journal for %q never reached %d entries (has %d)", nodeID, n, len(entries))
	return nil
}

// journalData extracts summary["data"] from a journaled payload.
func journalData(t *testing.T, v interface{}) interface{} {
	t.Helper()
	m, ok := v.(map[string]interface{})
	if !ok {
		t.Fatalf("expected summary map, got %T", v)
	}
	return m["data"]
}

// TestRunJournalRecordsExecutions verifies a deploy+inject records a
// journal entry per executed node with time, duration and summarized
// input/output.
func TestRunJournalRecordsExecutions(t *testing.T) {
	engine := NewEngine()
	workflow := journalFlow("wf-journal-basic")
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.GetID()) })

	injectInto(t, engine, workflow.GetID(), "hello")

	entries := waitForJournal(t, engine, workflow.GetID(), "a", 1)
	entry := entries[0]
	if entry.Time.IsZero() {
		t.Fatal("entry has no timestamp")
	}
	if entry.DurationMs < 0 {
		t.Fatalf("negative duration: %v", entry.DurationMs)
	}
	if entry.Error != "" {
		t.Fatalf("unexpected error: %q", entry.Error)
	}
	if data := journalData(t, entry.Input); data != "hello" {
		t.Fatalf("input data = %v, want hello", data)
	}
	// Junctions pass through: output must mirror input.
	if data := journalData(t, entry.Output); data != "hello" {
		t.Fatalf("output data = %v, want hello", data)
	}
	// The downstream node journals its own execution too.
	waitForJournal(t, engine, workflow.GetID(), "b", 1)
}

// TestRunJournalRecordsErrors verifies a failing node journals the
// error text and no output.
func TestRunJournalRecordsErrors(t *testing.T) {
	engine := NewEngine()
	workflow := nodestore.NewWorkflow("wf-journal-error").SetID("wf-journal-error").SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inj", "junction").SetData(map[string]interface{}{}),
		nodestore.NewNode("f", "function").SetData(map[string]interface{}{
			"func": "throw new Error('boom');",
		}),
	}).SetEdges([]nodestore.EdgeInterface{nodestore.NewEdge("e1", "inj", "f")})
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.GetID()) })

	injectInto(t, engine, workflow.GetID(), "x")

	entries := waitForJournal(t, engine, workflow.GetID(), "f", 1)
	entry := entries[0]
	if !strings.Contains(entry.Error, "boom") {
		t.Fatalf("expected error text, got %q", entry.Error)
	}
	if entry.Output != nil {
		t.Fatalf("failed run must not journal an output, got %v", entry.Output)
	}
}

// TestRunJournalRingBounds verifies the journal keeps only the last
// RunJournalSize executions.
func TestRunJournalRingBounds(t *testing.T) {
	engine := NewEngine()
	workflow := journalFlow("wf-journal-ring")
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.GetID()) })

	// Each InjectMessage runs its own traversal goroutine, so
	// concurrent injections interleave the journal nondeterministically.
	// Inject sequentially — wait for each message's entry to land
	// before sending the next — so the ring's eviction order is
	// deterministic.
	total := RunJournalSize + 5
	for i := 0; i < total; i++ {
		msgID, _, err := engine.InjectMessage(workflow.GetID(), "inj", nodestore.NewMessage().SetPayload(i))
		if err != nil {
			t.Fatalf("inject %d failed: %v", i, err)
		}
		engine.DiscardPending(msgID)
		// The ring saturates at RunJournalSize, so count alone cannot
		// signal completion past it — wait for the newest entry to
		// carry THIS message's data instead.
		deadline := time.Now().Add(5 * time.Second)
		for {
			entries, _ := engine.RunJournal(workflow.GetID(), "a")
			if len(entries) > 0 {
				last := entries[len(entries)-1]
				if data, _ := journalData(t, last.Input).(float64); data == float64(i) {
					break
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("message %d never journaled", i)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	entries, ok := engine.RunJournal(workflow.GetID(), "a")
	if !ok || len(entries) != RunJournalSize {
		t.Fatalf("ring holds %d entries, want %d", len(entries), RunJournalSize)
	}
	// Oldest five must have been evicted: first survivor is m5.
	if data := journalData(t, entries[0].Input); data != float64(5) {
		t.Fatalf("oldest surviving input = %v, want 5", data)
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].Seq <= entries[i-1].Seq {
			t.Fatalf("entry seqs not increasing: %d then %d", entries[i-1].Seq, entries[i].Seq)
		}
	}
}

// TestRunJournalMasksCredentials verifies credential-keyed payload
// fields (nodestore.IsCredentialKey) and sentinel values never reach the
// journal — for the executing node's type and at any nesting depth.
func TestRunJournalMasksCredentials(t *testing.T) {
	// junction has no credential keys of its own; lend it one for the
	// duration of this test so a payload field named "password" is
	// treated as secret for nodes of that type.
	nodestore.RegisterCredentialKeys("junction", []string{"password"})
	t.Cleanup(func() { nodestore.RegisterCredentialKeys("junction", nil) })

	engine := NewEngine()
	workflow := journalFlow("wf-journal-creds")
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.GetID()) })

	msgID, _, err := engine.InjectMessage(workflow.GetID(), "inj", nodestore.NewMessage().SetPayload(map[string]interface{}{
		"password": "s3cret",
		"nested":   map[string]interface{}{"password": "inner", "keep": 1},
		"sentinel": nodestore.CredentialSentinel,
		"visible":  "ok",
	}))
	if err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	engine.DiscardPending(msgID)

	entries := waitForJournal(t, engine, workflow.GetID(), "a", 1)
	data, ok := journalData(t, entries[0].Input).(map[string]interface{})
	if !ok {
		t.Fatalf("input data is not a map: %v", entries[0].Input)
	}
	if data["password"] != JournalMasked {
		t.Fatalf("password not masked: %v", data["password"])
	}
	if data["sentinel"] != JournalMasked {
		t.Fatalf("sentinel value not masked: %v", data["sentinel"])
	}
	if data["visible"] != "ok" {
		t.Fatalf("visible field lost: %v", data["visible"])
	}
	nested, ok := data["nested"].(map[string]interface{})
	if !ok || nested["password"] != JournalMasked || nested["keep"] != float64(1) {
		t.Fatalf("nested scrub wrong: %v", data["nested"])
	}
	// Belt and braces: the serialized summary must not contain the
	// secret anywhere.
	if raw, _ := json.Marshal(entries[0].Input); strings.Contains(string(raw), "s3cret") {
		t.Fatalf("secret leaked into journal: %s", raw)
	}
}

// TestRunJournalTruncatesLargePayloads verifies the stored summary is
// size-bounded and degrades to a truncation marker.
func TestRunJournalTruncatesLargePayloads(t *testing.T) {
	engine := NewEngine()
	workflow := journalFlow("wf-journal-trunc")
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.GetID()) })

	msgID, _, err := engine.InjectMessage(workflow.GetID(), "inj", nodestore.NewMessage().SetPayload(strings.Repeat("x", MaxJournalPayloadBytes*2)))
	if err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	engine.DiscardPending(msgID)

	entries := waitForJournal(t, engine, workflow.GetID(), "a", 1)
	marker, ok := entries[0].Input.(map[string]interface{})
	if !ok || marker["truncated"] != true {
		t.Fatalf("expected truncation marker, got %v", entries[0].Input)
	}
	if len(marker["preview"].(string)) != MaxJournalPayloadBytes {
		t.Fatalf("preview not bounded to %d bytes", MaxJournalPayloadBytes)
	}
}

// TestRunJournalNotRunning verifies the accessor reports ok=false for
// a workflow that was never deployed.
func TestRunJournalNotRunning(t *testing.T) {
	engine := NewEngine()
	if _, ok := engine.RunJournal("no-such-wf", "n"); ok {
		t.Fatal("expected ok=false for a workflow that is not running")
	}
}
