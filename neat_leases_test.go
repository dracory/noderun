package noderunner_test

import (
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	. "github.com/dracory/noderun"
)

// neatLeaseTestStore builds a NeatLeaseStore on a private in-memory
// SQLite DB — the same *sql.DB the entitystore uses in production.
func neatLeaseTestStore(t *testing.T) *NeatLeaseStore {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	// A pooled sqlite :memory: opens a fresh empty database per
	// connection — pin to one so every call sees the same tables.
	db.SetMaxOpenConns(1)
	store, err := NewNeatLeaseStore(db)
	if err != nil {
		t.Fatalf("NewNeatLeaseStore: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return store
}

// TestNeatLeaseStoreClaimRenewRelease covers the lease lifecycle:
// first claim wins, a live lease blocks others, renew keeps ownership,
// release frees it for the next runner.
func TestNeatLeaseStoreClaimRenewRelease(t *testing.T) {
	store := neatLeaseTestStore(t)

	held, holder, err := store.ClaimLease("wf1", "runner-a", time.Minute)
	if err != nil || !held || holder != "" {
		t.Fatalf("first claim: held=%v holder=%q err=%v", held, holder, err)
	}
	held, holder, err = store.ClaimLease("wf1", "runner-b", time.Minute)
	if err != nil || held || holder != "runner-a" {
		t.Fatalf("rival claim: held=%v holder=%q err=%v", held, holder, err)
	}
	ok, err := store.RenewLease("wf1", "runner-a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("renew own: ok=%v err=%v", ok, err)
	}
	if ok, _ := store.RenewLease("wf1", "runner-b", time.Minute); ok {
		t.Fatal("renew by non-owner must fail")
	}
	if err := store.ReleaseLease("wf1", "runner-a"); err != nil {
		t.Fatalf("release: %v", err)
	}
	held, _, err = store.ClaimLease("wf1", "runner-b", time.Minute)
	if err != nil || !held {
		t.Fatalf("claim after release: held=%v err=%v", held, err)
	}
}

// TestNeatLeaseStoreExpiry covers takeover of an expired lease per the
// DB clock.
func TestNeatLeaseStoreExpiry(t *testing.T) {
	store := neatLeaseTestStore(t)
	held, _, err := store.ClaimLease("wf1", "runner-a", -time.Minute)
	if err != nil || !held {
		t.Fatalf("claim: %v %v", held, err)
	}
	// The lease is already expired (negative TTL) — another runner
	// takes over immediately.
	held, holder, err := store.ClaimLease("wf1", "runner-b", time.Minute)
	if err != nil || !held || holder != "" {
		t.Fatalf("expired claim: held=%v holder=%q err=%v", held, holder, err)
	}
}

// TestNeatLeaseStoreRunners covers the runner heartbeat table.
func TestNeatLeaseStoreRunners(t *testing.T) {
	store := neatLeaseTestStore(t)
	if err := store.UpsertRunner(Runner{RunnerID: "r1", Hostname: "h1", RunningCount: 2}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := store.UpsertRunner(Runner{RunnerID: "r1", Hostname: "h1", RunningCount: 5}); err != nil {
		t.Fatalf("re-upsert: %v", err)
	}
	runners, err := store.ListRunners()
	if err != nil || len(runners) != 1 || runners[0].RunningCount != 5 {
		t.Fatalf("runners: %v %v", err, runners)
	}
	leases, err := store.ListLeases()
	if err != nil || len(leases) != 0 {
		t.Fatalf("leases: %v %v", err, leases)
	}
}
