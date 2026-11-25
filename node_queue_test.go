package noderunner_test

import (
	"context"
	. "github.com/dracory/noderun"
	"testing"
	"time"
)

func TestQueueEnqueueDrains(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := queueTestFlow("wf-queue", nil)

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.GetID()) })

	injectInto(t, engine, workflow.GetID(), "hello")

	got := waitForDebugValues(engine, workflow.GetID(), 1, 3*time.Second)
	// nodebase debug node renders string payloads bare (no JSON
	// quotes) for Node-RED parity.
	if len(got) != 1 || got[0] != `hello` {
		t.Fatalf("expected delivery of hello, got %v", got)
	}
	if n := store.RowCount(workflow.GetID()); n != 0 {
		t.Fatalf("queue row must be acked after drain, %d left", n)
	}
	if q := waitForQueueCount(t, engine, workflow.GetID(), 0, time.Second); q == nil {
		t.Fatal("queue must be reachable via QueueHandle")
	}
}

// TestQueueHoldRelease covers the hold gate: held queues accumulate
// without delivering; releasing drains in FIFO order.
func TestQueueHoldRelease(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := queueTestFlow("wf-hold", map[string]interface{}{"hold": true})

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.GetID()) })

	injectSequentially(t, engine, workflow.GetID(), 0, "m1", "m2", "m3")

	q := waitForQueueCount(t, engine, workflow.GetID(), 3, 3*time.Second)
	if q == nil || q.Count() != 3 {
		t.Fatalf("expected 3 held items, got %v", q)
	}
	if !q.Held() {
		t.Fatal("queue must report held")
	}
	// Give the drainer a moment to prove it is not draining.
	time.Sleep(150 * time.Millisecond)
	if got := debugValues(engine, workflow.GetID()); len(got) != 0 {
		t.Fatalf("held queue must not drain, got %v", got)
	}
	if n := store.RowCount(workflow.GetID()); n != 3 {
		t.Fatalf("held items must stay persisted, got %d rows", n)
	}

	q.SetHold(false)
	got := waitForDebugValues(engine, workflow.GetID(), 3, 3*time.Second)
	// Bare strings: nodebase debug renders payloads without JSON quotes.
	want := []string{`m1`, `m2`, `m3`}
	if len(got) != 3 {
		t.Fatalf("expected 3 drained messages, got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("release order broken at %d: want %v, got %v", i, want, got)
		}
	}
	if n := store.RowCount(workflow.GetID()); n != 0 {
		t.Fatalf("drained rows must be acked, %d left", n)
	}
}

// TestQueueRestartRequeue simulates a crash: rows seeded under the
// queue node id are recovered by the queue's own activation path and
// drained at-least-once — requeueDurable must not double-deliver them.
func TestQueueRestartRequeue(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := queueTestFlow("wf-queue-restart", nil)

	// "Previous run" leftovers, written before this engine existed.
	store.Seed(workflow.GetID(), "f", "r1")
	store.Seed(workflow.GetID(), "f", "r2")
	store.Seed(workflow.GetID(), "f", "r3")

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.GetID()) })

	got := waitForDebugValues(engine, workflow.GetID(), 3, 3*time.Second)
	// Bare strings: nodebase debug renders payloads without JSON quotes.
	want := []string{`r1`, `r2`, `r3`}
	if len(got) != 3 {
		t.Fatalf("expected exactly 3 recovered deliveries (no duplicates), got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("recovery order broken at %d: want %v, got %v", i, want, got)
		}
	}
	if n := store.RowCount(workflow.GetID()); n != 0 {
		t.Fatalf("recovered rows must be acked, %d left", n)
	}
}

// TestQueueRestartHeldRecoversWithoutDraining verifies a held queue
// still recovers its rows on restart but keeps them staged.
func TestQueueRestartHeldRecoversWithoutDraining(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := queueTestFlow("wf-queue-held", map[string]interface{}{"hold": true})

	store.Seed(workflow.GetID(), "f", "stuck1")
	store.Seed(workflow.GetID(), "f", "stuck2")

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.GetID()) })

	q := waitForQueueCount(t, engine, workflow.GetID(), 2, 3*time.Second)
	if q == nil || q.Count() != 2 {
		t.Fatalf("held queue must recover 2 items, got %v", q)
	}
	if got := debugValues(engine, workflow.GetID()); len(got) != 0 {
		t.Fatalf("held queue must not drain on restart, got %v", got)
	}
}

// TestQueueDeleteAndRequeue covers the inspection operations against a
// held queue: delete drops a row, requeue releases one item now.
func TestQueueDeleteAndRequeue(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := queueTestFlow("wf-queue-ops", map[string]interface{}{"hold": true})

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.GetID()) })

	injectSequentially(t, engine, workflow.GetID(), 0, "a", "b")

	q := waitForQueueCount(t, engine, workflow.GetID(), 2, 3*time.Second)
	if q == nil || q.Count() != 2 {
		t.Fatalf("expected 2 queued items, got %v", q)
	}
	items := q.Items()
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}

	// Delete the first: row removed, depth drops.
	if err := q.DeleteItem(items[0].Seq); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if q.Count() != 1 || store.RowCount(workflow.GetID()) != 1 {
		t.Fatalf("delete must drop one row: count=%d rows=%d", q.Count(), store.RowCount(workflow.GetID()))
	}

	// Requeue the survivor: it emits downstream immediately despite hold.
	if err := q.RequeueItem(items[1].Seq); err != nil {
		t.Fatalf("requeue failed: %v", err)
	}
	got := waitForDebugValues(engine, workflow.GetID(), 1, 3*time.Second)
	// Bare string: nodebase debug renders payloads without JSON quotes.
	if len(got) != 1 || got[0] != `b` {
		t.Fatalf("expected requeued b, got %v", got)
	}
	if q.Count() != 0 || store.RowCount(workflow.GetID()) != 0 {
		t.Fatalf("requeue must drain the row: count=%d rows=%d", q.Count(), store.RowCount(workflow.GetID()))
	}
}

// TestQueueDropOldest covers the capacity overflow policy: a full
// drop-oldest queue evicts the oldest staged item.
func TestQueueDropOldest(t *testing.T) {
	store := NewFakeDurableStore()
	workflow := queueTestFlow("wf-queue-cap", map[string]interface{}{
		"hold":     true,
		"capacity": float64(2),
		"policy":   "drop-oldest",
	})

	engine := NewEngine()
	engine.DurableInboxStore = store
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.GetID()) })

	injectSequentially(t, engine, workflow.GetID(), 2, "old", "mid", "new")

	q := waitForQueueCount(t, engine, workflow.GetID(), 2, 3*time.Second)
	if q == nil || q.Count() != 2 {
		t.Fatalf("expected capacity-bounded depth 2, got %v", q)
	}
	q.SetHold(false)
	got := waitForDebugValues(engine, workflow.GetID(), 2, 3*time.Second)
	// Bare strings: nodebase debug renders payloads without JSON quotes.
	if len(got) != 2 || got[0] != `mid` || got[1] != `new` {
		t.Fatalf("drop-oldest must evict the oldest item, got %v", got)
	}
}
