package noderunner_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dracory/nodebase"
	. "github.com/dracory/noderun"
	"github.com/dracory/nodestore"
	_ "modernc.org/sqlite"
)

func neatCoordinationTestStore(t *testing.T, memory bool) (*NeatCoordinationStore, *sql.DB) {
	t.Helper()
	dsn := ":memory:"
	if !memory {
		dsn = "file:" + filepath.Join(t.TempDir(), "coordination.db") + "?_pragma=busy_timeout(5000)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if memory {
		db.SetMaxOpenConns(1)
	}
	store, err := NewNeatCoordinationStore(db)
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewNeatCoordinationStore: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return store, db
}

func parkTestWaiter(t *testing.T, store *NeatCoordinationStore, id, key string, target int64) nodebase.CoordinationWaiter {
	t.Helper()
	waiter := nodebase.CoordinationWaiter{
		ID: id, SignalKey: key, WorkflowID: "wf", NodeID: "wait", TargetCount: target,
		Message:   nodestore.NewMessage().SetID("msg-" + id).SetPayload("payload-" + id).SetMetadata(map[string]interface{}{"origin": id}),
		CreatedAt: time.Now().UTC(),
	}
	if err := store.ParkWaiter(context.Background(), waiter); err != nil {
		t.Fatalf("park waiter: %v", err)
	}
	return waiter
}

func TestNeatCoordinationStoreIncrementReleaseAndRemainder(t *testing.T) {
	store, _ := neatCoordinationTestStore(t, true)
	ctx := context.Background()
	if err := store.IncrementSignal(ctx, "job", map[string]interface{}{"worker": "a"}, 0); err != nil {
		t.Fatalf("increment 1: %v", err)
	}
	if err := store.IncrementSignal(ctx, "job", map[string]interface{}{"attempt": 2}, 0); err != nil {
		t.Fatalf("increment 2: %v", err)
	}
	if err := store.IncrementSignal(ctx, "job", nil, 0); err != nil {
		t.Fatalf("increment 3: %v", err)
	}
	signal, ok, err := store.Signal(ctx, "job")
	if err != nil || !ok || signal.Count != 3 {
		t.Fatalf("signal = %+v ok=%v err=%v, want count 3", signal, ok, err)
	}
	if signal.Metadata["worker"] != "a" || signal.Metadata["attempt"] != float64(2) {
		t.Fatalf("cached metadata did not merge: %#v", signal.Metadata)
	}

	parkTestWaiter(t, store, "w1", "job", 2)
	released, expired, err := store.PollWaiters(ctx, "wf", "wait")
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(expired) != 0 || len(released) != 1 {
		t.Fatalf("poll released=%d expired=%d, want 1/0", len(released), len(expired))
	}
	waiter := released[0]
	if waiter.Message.GetPayload() != "payload-w1" || waiter.Message.GetMetadata()["origin"] != "w1" {
		t.Fatalf("message envelope did not round-trip: %#v", waiter.Message)
	}
	if waiter.CachedMetadata["worker"] != "a" {
		t.Fatalf("release did not carry cached metadata: %#v", waiter.CachedMetadata)
	}
	signal, ok, err = store.Signal(ctx, "job")
	if err != nil || !ok || signal.Count != 1 {
		t.Fatalf("remainder = %+v ok=%v err=%v, want count 1", signal, ok, err)
	}

	// The final remainder releases a later target-1 waiter and removes the
	// zero-count signal row.
	parkTestWaiter(t, store, "w2", "job", 1)
	released, _, err = store.PollWaiters(ctx, "wf", "wait")
	if err != nil || len(released) != 1 {
		t.Fatalf("second release=%d err=%v", len(released), err)
	}
	if _, ok, _ = store.Signal(ctx, "job"); ok {
		t.Fatal("zero-count signal row should be removed")
	}
}

func TestNeatCoordinationStoreConcurrentIncrementAndSingleClaim(t *testing.T) {
	store, _ := neatCoordinationTestStore(t, false)
	ctx := context.Background()
	const increments = 40
	var wg sync.WaitGroup
	errs := make(chan error, increments)
	for i := 0; i < increments; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- store.IncrementSignal(ctx, "race", nil, 0)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent increment: %v unwrap=%+v", err, unwrapTestErrors(err))
		}
	}
	signal, ok, err := store.Signal(ctx, "race")
	if err != nil || !ok || signal.Count != increments {
		t.Fatalf("count = %+v ok=%v err=%v, want %d", signal, ok, err, increments)
	}

	parkTestWaiter(t, store, "winner", "race", increments)
	const pollers = 16
	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup
	results := make(chan int, pollers)
	for i := 0; i < pollers; i++ {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			released, _, err := store.PollWaiters(ctx, "wf", "wait")
			if err != nil {
				t.Errorf("concurrent poll: %v", err)
				results <- -1
				return
			}
			results <- len(released)
		}()
	}
	start.Done()
	done.Wait()
	close(results)
	total := 0
	for n := range results {
		if n < 0 {
			t.Fatal("concurrent poll errored")
		}
		total += n
	}
	if total != 1 {
		t.Fatalf("one waiter must be claimed exactly once across concurrent pollers, got %d", total)
	}
	if _, ok, _ := store.Signal(ctx, "race"); ok {
		t.Fatal("race signal should be fully consumed")
	}
}

func TestNeatCoordinationStoreExpiration(t *testing.T) {
	store, _ := neatCoordinationTestStore(t, true)
	ctx := context.Background()
	waiter := parkTestWaiter(t, store, "expired", "job", 1)
	waiter.ExpiresAt = time.Now().UTC().Add(-time.Second)
	// Replace the non-expiring row written by the helper with an already
	// expired row. The store has no update API because production writes
	// only insert/delete/claim.
	_ = store.ParkWaiter(ctx, nodebase.CoordinationWaiter{
		ID: "expired-row", SignalKey: "job", WorkflowID: "wf", NodeID: "wait", TargetCount: 1,
		Message: waiter.Message, ExpiresAt: waiter.ExpiresAt, CreatedAt: time.Now().UTC().Add(-2 * time.Second),
	})
	released, expired, err := store.PollWaiters(ctx, "wf", "wait")
	if err != nil {
		t.Fatal(err)
	}
	if len(released) != 0 || len(expired) != 1 || expired[0].ID != "expired-row" {
		t.Fatalf("expiration claim wrong: released=%d expired=%+v", len(released), expired)
	}

	parkTestWaiter(t, store, "ttlwait", "ttl", 1)
	if err := store.IncrementSignal(ctx, "ttl", nil, time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2100 * time.Millisecond)
	if _, ok, err := store.Signal(ctx, "ttl"); err != nil || ok {
		t.Fatalf("expired signal should be absent: ok=%v err=%v", ok, err)
	}
	released, _, err = store.PollWaiters(ctx, "wf", "wait")
	if err != nil {
		t.Fatal(err)
	}
	for _, waiter := range released {
		if waiter.ID == "ttlwait" {
			t.Fatal("expired signal count must never release a waiter")
		}
	}
	if err := store.IncrementSignal(ctx, "ttl", nil, 0); err != nil {
		t.Fatal(err)
	}
	released, _, err = store.PollWaiters(ctx, "wf", "wait")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, waiter := range released {
		if waiter.ID == "ttlwait" {
			found = true
		}
	}
	if !found {
		t.Fatalf("fresh signal after expiry should release ttlwait, released=%+v", released)
	}
}

func unwrapTestErrors(err error) []string {
	var out []string
	for current := err; current != nil; current = errors.Unwrap(current) {
		out = append(out, current.Error())
	}
	return out
}

func TestNeatCoordinationStoreConcurrentExpiredSingleClaim(t *testing.T) {
	store, _ := neatCoordinationTestStore(t, false)
	ctx := context.Background()
	expiredAt := time.Now().UTC().Add(-time.Second)
	if err := store.ParkWaiter(ctx, nodebase.CoordinationWaiter{
		ID: "expired-race", SignalKey: "expiry-race", WorkflowID: "wf", NodeID: "wait", TargetCount: 1,
		Message: nodestore.NewMessage().SetPayload("late"), ExpiresAt: expiredAt, CreatedAt: expiredAt.Add(-time.Second),
	}); err != nil {
		t.Fatalf("park expired waiter: %v", err)
	}

	const pollers = 12
	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup
	results := make(chan int, pollers)
	for i := 0; i < pollers; i++ {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			_, expired, err := store.PollWaiters(ctx, "wf", "wait")
			if err != nil {
				t.Errorf("concurrent expired poll: %v", err)
				results <- -1
				return
			}
			results <- len(expired)
		}()
	}
	start.Done()
	done.Wait()
	close(results)
	total := 0
	for n := range results {
		if n < 0 {
			t.Fatal("concurrent expired poll errored")
		}
		total += n
	}
	if total != 1 {
		t.Fatalf("expired waiter must be claimed exactly once across concurrent pollers, got %d", total)
	}
}

func TestNeatCoordinationStoreIncrementReleaseRaceKeepsNotifyCount(t *testing.T) {
	store, _ := neatCoordinationTestStore(t, false)
	ctx := context.Background()
	for round := 0; round < 12; round++ {
		key := fmt.Sprintf("overlap-%d", round)
		if err := store.IncrementSignal(ctx, key, nil, 0); err != nil {
			t.Fatalf("seed increment round %d: %v", round, err)
		}
		parkTestWaiter(t, store, fmt.Sprintf("waiter-%d", round), key, 1)

		var wg sync.WaitGroup
		wg.Add(2)
		incrementErr := make(chan error, 1)
		pollErr := make(chan error, 1)
		releasedCount := make(chan int, 1)
		go func() {
			defer wg.Done()
			incrementErr <- store.IncrementSignal(ctx, key, nil, 0)
		}()
		go func() {
			defer wg.Done()
			released, _, err := store.PollWaiters(ctx, "wf", "wait")
			pollErr <- err
			releasedCount <- len(released)
		}()
		wg.Wait()
		if err := <-incrementErr; err != nil {
			t.Fatalf("overlapping increment round %d failed: %v", round, err)
		}
		if err := <-pollErr; err != nil {
			t.Fatalf("overlapping poll round %d failed: %v", round, err)
		}
		if got := <-releasedCount; got != 1 {
			t.Fatalf("round %d released %d waiters, want 1", round, got)
		}
		signal, ok, err := store.Signal(ctx, key)
		if err != nil || !ok || signal.Count != 1 {
			t.Fatalf("round %d final signal = %+v ok=%v err=%v, want unconsumed count 1", round, signal, ok, err)
		}
	}
}

func TestNeatCoordinationStoreExpiredClaimReusesOutcomeWithoutConsumingAgain(t *testing.T) {
	store, _ := neatCoordinationTestStore(t, true)
	store.SetCoordinationClaimTTLForTest(30 * time.Millisecond)
	ctx := context.Background()
	if err := store.IncrementSignal(ctx, "lease", map[string]interface{}{"worker": "owner"}, 0); err != nil {
		t.Fatal(err)
	}
	if err := store.IncrementSignal(ctx, "lease", nil, 0); err != nil {
		t.Fatal(err)
	}
	parkTestWaiter(t, store, "lease-crash", "lease", 1)

	released, _, err := store.PollWaiters(ctx, "wf", "wait")
	if err != nil || len(released) != 1 {
		t.Fatalf("initial claim released=%d err=%v", len(released), err)
	}
	first := released[0]
	if first.ClaimToken == "" || first.ClaimExpiresAt.IsZero() {
		t.Fatalf("claim lease missing: %+v", first)
	}

	released, _, err = store.PollWaiters(ctx, "wf", "wait")
	if err != nil || len(released) != 0 {
		t.Fatalf("live claim must not be handed to a second owner: released=%d err=%v", len(released), err)
	}
	time.Sleep(45 * time.Millisecond)
	released, _, err = store.PollWaiters(ctx, "wf", "wait")
	if err != nil || len(released) != 1 {
		t.Fatalf("expired claim was not reclaimed: released=%d err=%v", len(released), err)
	}
	second := released[0]
	if second.ID != first.ID || second.ClaimToken == "" || second.ClaimToken == first.ClaimToken {
		t.Fatalf("reclaim ownership wrong: first=%+v second=%+v", first, second)
	}
	if second.CachedMetadata["worker"] != "owner" {
		t.Fatalf("stored release outcome lost cached metadata: %#v", second.CachedMetadata)
	}
	if err := store.AckWaiter(ctx, first.ID, first.ClaimToken); err == nil {
		t.Fatal("stale claim token must not acknowledge a reclaimed waiter")
	}
	if err := store.AckWaiter(ctx, second.ID, second.ClaimToken); err != nil {
		t.Fatalf("current owner ack: %v", err)
	}
	signal, ok, err := store.Signal(ctx, "lease")
	if err != nil || !ok || signal.Count != 1 {
		t.Fatalf("reclaim consumed count again: signal=%+v ok=%v err=%v, want remainder 1", signal, ok, err)
	}
	released, _, err = store.PollWaiters(ctx, "wf", "wait")
	if err != nil || len(released) != 0 {
		t.Fatalf("acknowledged waiter reappeared: released=%d err=%v", len(released), err)
	}
}

func TestNeatCoordinationStoreNackRetriesWithoutConsumingAgain(t *testing.T) {
	store, _ := neatCoordinationTestStore(t, true)
	ctx := context.Background()
	if err := store.IncrementSignal(ctx, "nack", nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := store.IncrementSignal(ctx, "nack", nil, 0); err != nil {
		t.Fatal(err)
	}
	parkTestWaiter(t, store, "nack-waiter", "nack", 1)
	released, _, err := store.PollWaiters(ctx, "wf", "wait")
	if err != nil || len(released) != 1 {
		t.Fatalf("initial release=%d err=%v", len(released), err)
	}
	first := released[0]
	if err := store.NackWaiter(ctx, first.ID, first.ClaimToken); err != nil {
		t.Fatalf("nack: %v", err)
	}
	released, _, err = store.PollWaiters(ctx, "wf", "wait")
	if err != nil || len(released) != 1 {
		t.Fatalf("nacked waiter was not retried immediately: released=%d err=%v", len(released), err)
	}
	if err := store.AckWaiter(ctx, released[0].ID, released[0].ClaimToken); err != nil {
		t.Fatalf("ack retry: %v", err)
	}
	signal, ok, err := store.Signal(ctx, "nack")
	if err != nil || !ok || signal.Count != 1 {
		t.Fatalf("nack retry consumed count again: signal=%+v ok=%v err=%v, want remainder 1", signal, ok, err)
	}
}

func TestNeatCoordinationStoreConcurrentConstructors(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "constructors.db")
	dsn := "file:" + filepath.ToSlash(dbPath) + "?_pragma=busy_timeout(5000)"
	const constructors = 12
	var wg sync.WaitGroup
	errs := make(chan error, constructors)
	for i := 0; i < constructors; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db, err := sql.Open("sqlite", dsn)
			if err != nil {
				errs <- err
				return
			}
			defer func() { _ = db.Close() }()
			store, err := NewNeatCoordinationStore(db)
			if err != nil {
				errs <- err
				return
			}
			if err := store.IncrementSignal(context.Background(), "constructed", nil, 0); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent constructor/increment: %v unwrap=%+v", err, unwrapTestErrors(err))
		}
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store, err := NewNeatCoordinationStore(db)
	if err != nil {
		t.Fatal(err)
	}
	signal, ok, err := store.Signal(context.Background(), "constructed")
	if err != nil || !ok || signal.Count != constructors {
		t.Fatalf("constructed signal=%+v ok=%v err=%v, want %d", signal, ok, err, constructors)
	}
}

func TestNeatCoordinationStoreMigratesLegacyNullClaimColumns(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	_, err = db.Exec("CREATE TABLE node_coordination_signals (signal_key TEXT PRIMARY KEY, signal_count INTEGER, cached_metadata TEXT, expires_at TEXT, updated_at TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("CREATE TABLE node_coordination_waiters (id TEXT PRIMARY KEY, signal_key TEXT, workflow_id TEXT, node_id TEXT, target_count INTEGER, payload TEXT, expires_at TEXT, created_at TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	payload := "{\"id\":\"legacy-msg\",\"payload\":\"legacy\",\"metadata\":{}}"
	_, err = db.Exec("INSERT INTO node_coordination_waiters (id, signal_key, workflow_id, node_id, target_count, payload, expires_at, created_at) VALUES (?,?,?,?,?,?,?,?)",
		"legacy-waiter", "legacy-key", "wf", "wait", 1, payload, "", time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}

	store, err := NewNeatCoordinationStore(db)
	if err != nil {
		t.Fatalf("migrated constructor: %v", err)
	}
	if err := store.IncrementSignal(context.Background(), "legacy-key", nil, 0); err != nil {
		t.Fatal(err)
	}
	released, expired, err := store.PollWaiters(context.Background(), "wf", "wait")
	if err != nil || len(expired) != 0 || len(released) != 1 {
		t.Fatalf("legacy waiter did not release after claim-column migration: released=%d expired=%d err=%v", len(released), len(expired), err)
	}
	if err := store.AckWaiter(context.Background(), released[0].ID, released[0].ClaimToken); err != nil {
		t.Fatalf("ack migrated legacy waiter: %v", err)
	}
}

func TestNeatCoordinationStoreExpiredOutcomeSurvivesNackAndRetry(t *testing.T) {
	store, _ := neatCoordinationTestStore(t, true)
	ctx := context.Background()
	expiredAt := time.Now().UTC().Add(-time.Second)
	if err := store.ParkWaiter(ctx, nodebase.CoordinationWaiter{
		ID: "expired-nack", SignalKey: "expired-key", WorkflowID: "wf", NodeID: "wait", TargetCount: 1,
		Message: nodestore.NewMessage().SetPayload("late"), CreatedAt: expiredAt.Add(-time.Second), ExpiresAt: expiredAt,
	}); err != nil {
		t.Fatal(err)
	}
	_, expired, err := store.PollWaiters(ctx, "wf", "wait")
	if err != nil || len(expired) != 1 {
		t.Fatalf("initial expired claim=%d err=%v", len(expired), err)
	}
	first := expired[0]
	if first.ClaimToken == "" {
		t.Fatalf("expired claim missing token: %+v", first)
	}
	if err := store.NackWaiter(ctx, first.ID, first.ClaimToken); err != nil {
		t.Fatalf("nack expired claim: %v", err)
	}
	_, expired, err = store.PollWaiters(ctx, "wf", "wait")
	if err != nil || len(expired) != 1 || expired[0].ID != first.ID {
		t.Fatalf("expired outcome did not survive nack/retry: expired=%+v err=%v", expired, err)
	}
	if err := store.AckWaiter(ctx, expired[0].ID, expired[0].ClaimToken); err != nil {
		t.Fatalf("ack retried expired waiter: %v", err)
	}
	waiters, err := store.Waiters(ctx, "wf", "wait")
	if err != nil || len(waiters) != 0 {
		t.Fatalf("acknowledged expired waiter remains: %+v err=%v", waiters, err)
	}
}
