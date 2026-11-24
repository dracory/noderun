package noderunner

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestDurableBackendWriteThrough verifies enqueue persists a row for
// durable nodes only, and dequeue/evict delete exactly that row.
func TestDurableBackendWriteThrough(t *testing.T) {
	store := NewFakeDurableStore()
	backend := NewDurableInboxBackend("wf", store)
	backend.(interface{ SetDurableNodes(map[string]bool) }).SetDurableNodes(map[string]bool{"d": true})

	// Non-durable node: pure memory, no row.
	if err := backend.Enqueue(item("m")); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if got := store.RowCount("wf"); got != 0 {
		t.Fatalf("non-durable node must not persist, got %d rows", got)
	}

	// Durable node: row written before the entry is queued.
	if err := backend.Enqueue(item("d")); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if got := store.RowCount("wf"); got != 1 {
		t.Fatalf("expected 1 persisted row, got %d", got)
	}
	if n, _ := backend.Pending("d"); n != 1 {
		t.Fatalf("expected pending 1, got %d", n)
	}

	// FIFO order across memory + durable entries is preserved.
	first, ok, err := backend.Dequeue()
	if err != nil || !ok || first.NodeID != "m" {
		t.Fatalf("expected memory item first, got %+v ok=%v err=%v", first, ok, err)
	}
	second, ok, err := backend.Dequeue()
	if err != nil || !ok || second.NodeID != "d" {
		t.Fatalf("expected durable item next, got %+v ok=%v err=%v", second, ok, err)
	}
	if got := store.RowCount("wf"); got != 0 {
		t.Fatalf("dequeue must ack the row, %d left", got)
	}
}

// TestDurableBackendAdmitStored verifies a surviving row re-enters the
// queue by seq and is deleted — not re-inserted — on dequeue.
func TestDurableBackendAdmitStored(t *testing.T) {
	store := NewFakeDurableStore()
	store.Seed("wf", "d", "old")
	backend := NewDurableInboxBackend("wf", store).(storedItemAdmitter)

	rows, err := store.LoadInbox("wf")
	if err != nil || len(rows) != 1 {
		t.Fatalf("load failed: %v (%d rows)", err, len(rows))
	}
	backend.AdmitStored(InboxItem{NodeID: rows[0].NodeID, Payload: rows[0].Payload}, rows[0].Seq)

	got, ok, err := backend.(InboxBackend).Dequeue()
	if err != nil || !ok || got.Payload.GetPayload() != "old" {
		t.Fatalf("expected the stored item, got %+v ok=%v err=%v", got, ok, err)
	}
	if n := store.RowCount("wf"); n != 0 {
		t.Fatalf("stored row must be deleted on dequeue, %d left", n)
	}
	if store.saves != 1 {
		t.Fatalf("admit must not re-insert, saves=%d", store.saves)
	}
}

// TestNewInboxBackendSelection verifies the backend picked per flow:
// memory default, durable only when a store exists AND a node opts in,
// custom factory always wins.
func TestNewInboxBackendSelection(t *testing.T) {
	durableFlow := &Workflow{ID: "w1", Nodes: []Node{
		{ID: "n", Type: "junction", Data: map[string]interface{}{"inboxDurable": true}},
	}}
	plainFlow := &Workflow{ID: "w2", Nodes: []Node{
		{ID: "n", Type: "junction", Data: map[string]interface{}{}},
	}}

	engine := NewEngine()
	// No store: memory everywhere, flag or not.
	if ib := engine.newInbox("w1", durableFlow); ib.backend != nil {
		if _, ok := ib.backend.(*memInboxBackend); !ok {
			t.Fatalf("no store must yield memory backend, got %T", ib.backend)
		}
	}

	engine.DurableInboxStore = NewFakeDurableStore()
	// Durable-by-default (GOD ruling 2026-11-24): once a store is
	// configured every inbox persists — the inboxDurable flags are
	// superseded and ignored.
	if _, ok := engine.newInbox("w1", durableFlow).backend.(*durableInboxBackend); !ok {
		t.Fatal("store configured must yield the durable backend")
	}
	if _, ok := engine.newInbox("w2", plainFlow).backend.(*durableInboxBackend); !ok {
		t.Fatal("store configured must persist even without flags")
	}

	// Custom factory wins over the built-in selection.
	engine.InboxBackendFactory = func(workflowID string) InboxBackend {
		return newMemInboxBackend()
	}
	if _, ok := engine.newInbox("w1", durableFlow).backend.(*memInboxBackend); !ok {
		t.Fatal("custom factory must override durable selection")
	}
}

// durableTestFlow builds inj -> jn -> dbg where jn is the durable
// (or plain) inbox under test and dbg is a debug node whose event log
// records delivery order.
func durableTestFlow(id string, durable bool) *Workflow {
	data := map[string]interface{}{}
	if durable {
		data["inboxDurable"] = true
	}
	return &Workflow{
		ID:   id,
		Name: id,
		Nodes: []Node{
			{ID: "inj", Type: "junction", Data: map[string]interface{}{}},
			{ID: "jn", Type: "junction", Data: data},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "jn"},
			{ID: "e2", Source: "jn", Target: "dbg"},
		},
	}
}

func TestDurableRequeueOnRestart(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := durableTestFlow("wf-restart", true)

	// "Previous run" leftovers, enqueued before this engine existed.
	store.Seed(workflow.ID, "jn", "m1")
	store.Seed(workflow.ID, "jn", "m2")
	store.Seed(workflow.ID, "jn", "m3")

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	got := waitForDebugValues(engine, workflow.ID, 3, 3*time.Second)
	want := []string{`"m1"`, `"m2"`, `"m3"`}
	if len(got) != 3 {
		t.Fatalf("expected 3 re-queued deliveries, got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order broken at %d: want %v, got %v", i, want, got)
		}
	}
	if n := store.RowCount(workflow.ID); n != 0 {
		t.Fatalf("delivered rows must be acked, %d left", n)
	}
}

// TestDurableEnqueueDrains verifies a live traversal writes durable
// rows through and acks them — the store ends empty and the message
// is delivered.
func TestDurableEnqueueDrains(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := durableTestFlow("wf-live", true)

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	msgID, _, err := engine.InjectMessage(workflow.ID, "inj", &Message{Payload: "hello"})
	if err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	engine.DiscardPending(msgID)

	got := waitForDebugValues(engine, workflow.ID, 1, 3*time.Second)
	if len(got) != 1 || got[0] != `"hello"` {
		t.Fatalf("expected delivery of hello, got %v", got)
	}
	if store.saves == 0 {
		t.Fatal("durable node must write-through on enqueue")
	}
	if n := store.RowCount(workflow.ID); n != 0 {
		t.Fatalf("queue must drain to zero rows, %d left", n)
	}
}

// TestDurableByDefault verifies the durable-by-default ruling: with
// a store configured even a flow without opt-in flags write-throughs
// to the store (and drains cleanly).
func TestDurableByDefault(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := durableTestFlow("wf-plain", false)

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	msgID, _, err := engine.InjectMessage(workflow.ID, "inj", &Message{Payload: "hi"})
	if err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	engine.DiscardPending(msgID)

	got := waitForDebugValues(engine, workflow.ID, 1, 3*time.Second)
	if len(got) != 1 {
		t.Fatalf("expected delivery, got %v", got)
	}
	if store.saves == 0 {
		t.Fatal("durable-by-default flow must write-through on enqueue")
	}
	if n := store.RowCount(workflow.ID); n != 0 {
		t.Fatalf("queue must drain to zero rows, %d left", n)
	}
}

// TestParseDurableFlag covers accepted flag values.
func TestParseDurableFlag(t *testing.T) {
	for value, want := range map[interface{}]bool{
		true: true, false: false,
		"true": true, "yes": true, "1": true, "on": true,
		"false": false, "no": false, "0": false, "off": false,
		float64(1): true, float64(0): false,
	} {
		got, ok := ParseDurableFlag(value)
		if !ok || got != want {
			t.Fatalf("ParseDurableFlag(%v) = %v,%v; want %v", value, got, ok, want)
		}
	}
	if _, ok := ParseDurableFlag("maybe"); ok {
		t.Fatal("unknown flag value must be rejected")
	}
}

// TestRequeueUsesStoredSeq covers the non-durable-backend fallback in
// requeueDurable: leftover rows still drain (transiently) and are
// removed from the store even when the flow no longer opts in.
func TestRequeueUsesStoredSeq(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := durableTestFlow("wf-undurable", false) // flag removed post-crash

	store.Seed(workflow.ID, "jn", "stuck")

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.ExecuteWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })

	got := waitForDebugValues(engine, workflow.ID, 1, 3*time.Second)
	if len(got) != 1 || !strings.Contains(got[0], "stuck") {
		t.Fatalf("expected the stuck row to drain, got %v", got)
	}
	if n := store.RowCount(workflow.ID); n != 0 {
		t.Fatalf("drained row must be removed, %d left", n)
	}
}
