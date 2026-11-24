package noderunner_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	nodestore "github.com/dracory/nodestore"
	. "github.com/dracory/noderun"
	_ "github.com/dracory/nodebase/builtin"
)

// neatInboxTestStore builds a NeatInboxStore on a private in-memory
// SQLite DB — the same *sql.DB an entitystore would use in production.
func neatInboxTestStore(t *testing.T) *NeatInboxStore {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	// A pooled sqlite :memory: opens a fresh empty database per
	// connection — pin to one so every call sees the same tables.
	db.SetMaxOpenConns(1)
	store, err := NewNeatInboxStore(db)
	if err != nil {
		t.Fatalf("NewNeatInboxStore: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return store
}

// TestNeatInboxStoreRoundtrip covers save/load ordering (seq defines
// per-node FIFO at persist time), payload roundtrip with lineage, and
// the delete ack.
func TestNeatInboxStoreRoundtrip(t *testing.T) {
	store := neatInboxTestStore(t)

	for _, p := range []string{"one", "two", "three"} {
		err := store.SaveInboxItem(&DurableInboxItem{
			WorkflowID: "wf",
			NodeID:     "n1",
			Payload: nodestore.NewMessage().
				SetID("msg-" + p).
				SetPayload(p).
				SetMetadata(map[string]interface{}{"parentMsgId": "parent-" + p}),
		})
		if err != nil {
			t.Fatalf("save %q: %v", p, err)
		}
	}
	// A row for a different workflow/node must not leak into the load.
	_ = store.SaveInboxItem(&DurableInboxItem{
		WorkflowID: "other", NodeID: "n1",
		Payload: nodestore.NewMessage().SetPayload("stray"),
	})

	rows, err := store.LoadInbox("wf")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	for i, want := range []string{"one", "two", "three"} {
		if got := rows[i].Payload.GetPayload(); got != want {
			t.Fatalf("row %d payload = %v, want %q", i, got, want)
		}
		if rows[i].Payload.GetID() != "msg-"+want {
			t.Fatalf("row %d lost envelope id: %q", i, rows[i].Payload.GetID())
		}
		if rows[i].Payload.GetMetadata()["parentMsgId"] != "parent-"+want {
			t.Fatalf("row %d lost lineage metadata", i)
		}
		if rows[i].Seq <= 0 {
			t.Fatalf("row %d has no seq", i)
		}
	}

	if err := store.DeleteInboxItem("wf", rows[0].Seq); err != nil {
		t.Fatalf("delete: %v", err)
	}
	rows, _ = store.LoadInbox("wf")
	if len(rows) != 2 || rows[0].Payload.GetPayload() != "two" {
		t.Fatalf("delete ack left wrong rows: %+v", rows)
	}
}

// TestNeatInboxStoreClaimSemantics is the two-runner contract at store
// level: a live claim excludes other runners, the claimant still sees
// its own rows (LoadInboxFor), and an expired claim becomes claimable
// again — at-least-once, no double delivery.
func TestNeatInboxStoreClaimSemantics(t *testing.T) {
	store := neatInboxTestStore(t)
	item := &DurableInboxItem{
		WorkflowID: "wf", NodeID: "n1",
		Payload: nodestore.NewMessage().SetPayload("work"),
	}
	if err := store.SaveInboxItem(item); err != nil {
		t.Fatal(err)
	}

	// Runner A claims with a long TTL — runner B must not see or take it.
	claimed, err := store.ClaimInboxItem("wf", item.Seq, "runner-A", time.Hour)
	if err != nil || !claimed {
		t.Fatalf("runner A claim failed: claimed=%v err=%v", claimed, err)
	}
	if claimed, _ := store.ClaimInboxItem("wf", item.Seq, "runner-B", time.Hour); claimed {
		t.Fatal("runner B claimed a live row owned by runner A")
	}
	if rows, _ := store.LoadInbox("wf"); len(rows) != 0 {
		t.Fatalf("live-claimed row visible to agnostic load: %d", len(rows))
	}
	if rows, _ := store.LoadInboxFor("wf", "runner-B"); len(rows) != 0 {
		t.Fatalf("live-claimed row visible to runner B: %d", len(rows))
	}
	if rows, _ := store.LoadInboxFor("wf", "runner-A"); len(rows) != 1 {
		t.Fatalf("runner A cannot see its own claimed row: %d", len(rows))
	}
	// Re-claim by the owner succeeds (idempotent drain).
	if claimed, _ := store.ClaimInboxItem("wf", item.Seq, "runner-A", time.Hour); !claimed {
		t.Fatal("owner re-claim failed")
	}
}

// TestNeatInboxStoreClaimExpiry simulates runner A dying mid-process:
// its short-TTL claim expires and runner B takes the row over.
func TestNeatInboxStoreClaimExpiry(t *testing.T) {
	store := neatInboxTestStore(t)
	item := &DurableInboxItem{
		WorkflowID: "wf", NodeID: "n1",
		Payload: nodestore.NewMessage().SetPayload("orphan"),
	}
	if err := store.SaveInboxItem(item); err != nil {
		t.Fatal(err)
	}
	// A claims with a 1s TTL, then "dies" (no ack).
	claimed, err := store.ClaimInboxItem("wf", item.Seq, "runner-A", time.Second)
	if err != nil || !claimed {
		t.Fatalf("runner A claim failed: %v", err)
	}
	// Before expiry the row is invisible to B.
	if rows, _ := store.LoadInboxFor("wf", "runner-B"); len(rows) != 0 {
		t.Fatal("unexpired claim visible to runner B")
	}
	time.Sleep(2200 * time.Millisecond) // let CURRENT_TIMESTAMP pass the TTL
	// After expiry B sees it and the conditional UPDATE hands it over.
	if rows, _ := store.LoadInboxFor("wf", "runner-B"); len(rows) != 1 {
		t.Fatal("expired claim not visible to runner B")
	}
	if claimed, _ := store.ClaimInboxItem("wf", item.Seq, "runner-B", time.Hour); !claimed {
		t.Fatal("runner B could not take over expired claim")
	}
	if claimed, _ := store.ClaimInboxItem("wf", item.Seq, "runner-A", time.Hour); claimed {
		t.Fatal("dead runner A reclaimed an item now owned by B")
	}
}

// TestAlwaysPersistedHop verifies durable-by-default: with a
// DurableInboxStore configured, a hop between nodes with NO
// inboxDurable flags is persisted and acked — the memory inbox is only
// a cache.
func TestAlwaysPersistedHop(t *testing.T) {
	ctx := context.Background()
	store := neatInboxTestStore(t)
	engine := NewEngine()
	engine.DurableInboxStore = store

	workflow := nodestore.NewWorkflow("always-persist").
		SetID("always-persist").
		SetNodes([]nodestore.NodeInterface{
			nodestore.NewNode("src", "junction").SetData(map[string]interface{}{}),
			nodestore.NewNode("dbg", "debug").SetData(map[string]interface{}{}), // no inboxDurable flag
		}).
		SetEdges([]nodestore.EdgeInterface{
			nodestore.NewEdge("e1", "src", "dbg"),
		})
	if _, err := engine.DeployWorkflow(ctx, workflow, nil); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	engine.ExecuteDownstream(ctx, "always-persist", "src", workflow,
		nodestore.NewMessage().SetPayload("hop"), nil)

	values := waitForDebugValues(engine, "always-persist", 1, 3*time.Second)
	if len(values) == 0 {
		t.Fatal("message not delivered")
	}
	// The hop row was persisted and acked — nothing survives in the
	// store of record after successful delivery.
	if rows, _ := store.LoadInbox("always-persist"); len(rows) != 0 {
		t.Fatalf("%d unacked inbox row(s) after delivery", len(rows))
	}
}

// TestRestartDeliversDeadRunnersClaimedRow is the kill-and-restart /
// two-runner acceptance test in one: a row claimed by dead runner A
// (claim expired) is drained by runner B's fresh engine over the same
// DB — delivered exactly once and acked.
func TestRestartDeliversDeadRunnersClaimedRow(t *testing.T) {
	ctx := context.Background()
	store := neatInboxTestStore(t)
	nodeStore := resumeTestStore(t) // entitystore on its own DB

	workflow := nodestore.NewWorkflow("handoff").
		SetID("handoff").
		SetDeployed(true).
		SetNodes([]nodestore.NodeInterface{
			nodestore.NewNode("mid", "junction").SetData(map[string]interface{}{}),
			nodestore.NewNode("dbg", "debug").SetData(map[string]interface{}{}),
		}).
		SetEdges([]nodestore.EdgeInterface{
			nodestore.NewEdge("e1", "mid", "dbg"),
		})
	if err := nodeStore.WorkflowCreate(ctx, workflow); err != nil {
		t.Fatal(err)
	}

	// The crash leftover: runner A persisted the hop AND claimed it,
	// then died mid-process — the claim expires quickly.
	item := &DurableInboxItem{
		WorkflowID: "handoff", NodeID: "mid",
		Payload:    nodestore.NewMessage().SetPayload("deliver-me"),
		EnqueuedAt: time.Now(),
	}
	if err := store.SaveInboxItem(item); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimInboxItem("handoff", item.Seq, "runner-A-dead", time.Second)
	if err != nil || !claimed {
		t.Fatalf("seed claim failed: %v", err)
	}
	time.Sleep(2200 * time.Millisecond) // claim expires per the DB clock
	if rows, err := store.LoadInboxFor("handoff", "runner-B"); err != nil || len(rows) != 1 {
		t.Fatalf("pre-resume load: rows=%d err=%v", len(rows), err)
	}

	// Runner B mounts: fresh engine on the same inbox DB.
	engineB := NewEngine()
	engineB.RunnerID = "runner-B"
	engineB.DurableInboxStore = store
	if errs := engineB.ResumeDeployed(ctx, nodeStore); len(errs) != 0 {
		t.Fatalf("resume: %v", errs)
	}

	values := waitForDebugValues(engineB, "handoff", 1, 4*time.Second)
	if len(values) == 0 {
		_, _, events, _ := engineB.GetWorkflowSnapshot("handoff", 0)
		for _, ev := range events {
			t.Logf("event %s %s %s", ev.Level, ev.NodeID, ev.Message)
		}
		t.Fatal("dead runner's claimed row was not delivered after takeover")
	}
	if !strings.Contains(values[0], "deliver-me") {
		t.Fatalf("unexpected payload: %q", values[0])
	}
	if rows, _ := store.LoadInbox("handoff"); len(rows) != 0 {
		t.Fatalf("row not acked after delivery: %d left", len(rows))
	}
	// Delivered exactly once — no duplicate after the drain settles.
	if got := waitForDebugValues(engineB, "handoff", 2, 300*time.Millisecond); len(got) != 1 {
		t.Fatalf("double delivery detected: %v", got)
	}
}
