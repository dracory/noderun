package noderunner_test

import (
	"context"
	. "github.com/dracory/noderun"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeLeaseStore is an in-memory LeaseStore for engine-level tests.
type fakeLeaseStore struct {
	mu       sync.Mutex
	leases   map[string]Lease
	runners  map[string]Runner
	renewErr error
	renewOK  map[string]bool // force renew results per workflow (optional)
}

func newFakeLeaseStore() *fakeLeaseStore {
	return &fakeLeaseStore{leases: map[string]Lease{}, runners: map[string]Runner{}}
}

func (f *fakeLeaseStore) ClaimLease(workflowID, runnerID string, ttl time.Duration) (bool, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	l, ok := f.leases[workflowID]
	if !ok || l.RunnerID == runnerID || time.Now().After(l.ExpiresAt) {
		l.RunnerID = runnerID
		l.WorkflowID = workflowID
		l.ExpiresAt = time.Now().Add(ttl)
		l.Generation++
		f.leases[workflowID] = l
		return true, "", nil
	}
	return false, l.RunnerID, nil
}

func (f *fakeLeaseStore) RenewLease(workflowID, runnerID string, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.renewErr != nil {
		return false, f.renewErr
	}
	if f.renewOK != nil {
		if v, ok := f.renewOK[workflowID]; ok {
			return v, nil
		}
	}
	l, ok := f.leases[workflowID]
	if !ok || l.RunnerID != runnerID {
		return false, nil
	}
	l.ExpiresAt = time.Now().Add(ttl)
	f.leases[workflowID] = l
	return true, nil
}

func (f *fakeLeaseStore) ReleaseLease(workflowID, runnerID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if l, ok := f.leases[workflowID]; ok && l.RunnerID == runnerID {
		delete(f.leases, workflowID)
	}
	return nil
}

func (f *fakeLeaseStore) ListLeases() ([]Lease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Lease, 0, len(f.leases))
	for _, l := range f.leases {
		out = append(out, l)
	}
	return out, nil
}

func (f *fakeLeaseStore) UpsertRunner(r Runner) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runners[r.RunnerID] = r
	return nil
}

func (f *fakeLeaseStore) ListRunners() ([]Runner, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Runner, 0, len(f.runners))
	for _, r := range f.runners {
		out = append(out, r)
	}
	return out, nil
}

func leaseTestWorkflow(id string) *Workflow {
	return &Workflow{ID: id, Name: "lease test", Nodes: []Node{}, Edges: []Edge{}}
}

func TestExecuteWorkflowClaimsLease(t *testing.T) {
	store := newFakeLeaseStore()
	engine := NewEngine()
	engine.RunnerID = "runner-a"
	engine.LeaseStore = store

	_, err := engine.ExecuteWorkflow(context.Background(), leaseTestWorkflow("wf-1"), &Message{})
	if err != nil {
		t.Fatalf("ExecuteWorkflow failed: %v", err)
	}
	if got := engine.LeaseRunner("wf-1"); got != "runner-a" {
		t.Fatalf("expected lease holder runner-a, got %q", got)
	}
}

func TestExecuteWorkflowLeaseHeldByOther(t *testing.T) {
	store := newFakeLeaseStore()

	engineA := NewEngine()
	engineA.RunnerID = "runner-a"
	engineA.LeaseStore = store
	if _, err := engineA.ExecuteWorkflow(context.Background(), leaseTestWorkflow("wf-1"), &Message{}); err != nil {
		t.Fatalf("runner-a ExecuteWorkflow failed: %v", err)
	}

	engineB := NewEngine()
	engineB.RunnerID = "runner-b"
	engineB.LeaseStore = store
	_, err := engineB.ExecuteWorkflow(context.Background(), leaseTestWorkflow("wf-1"), &Message{})
	if err == nil || !strings.Contains(err.Error(), "leased to runner runner-a") {
		t.Fatalf("expected lease-conflict error, got %v", err)
	}
}

func TestStopWorkflowReleasesLease(t *testing.T) {
	store := newFakeLeaseStore()
	engine := NewEngine()
	engine.RunnerID = "runner-a"
	engine.LeaseStore = store

	if _, err := engine.ExecuteWorkflow(context.Background(), leaseTestWorkflow("wf-1"), &Message{}); err != nil {
		t.Fatalf("ExecuteWorkflow failed: %v", err)
	}
	if err := engine.StopWorkflow("wf-1"); err != nil {
		t.Fatalf("StopWorkflow failed: %v", err)
	}
	if got := engine.LeaseRunner("wf-1"); got != "" {
		t.Fatalf("expected lease released, still held by %q", got)
	}
}

func TestNoLeaseStoreUnchanged(t *testing.T) {
	// No LeaseStore: single-runner behaviour — always allowed, no
	// lease bookkeeping.
	engine := NewEngine()
	if _, err := engine.ExecuteWorkflow(context.Background(), leaseTestWorkflow("wf-1"), &Message{}); err != nil {
		t.Fatalf("ExecuteWorkflow failed: %v", err)
	}
	if got := engine.LeaseRunner("wf-1"); got != "" {
		t.Fatalf("expected no runner, got %q", got)
	}
}
