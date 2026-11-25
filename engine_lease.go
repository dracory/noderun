package noderunner

import (
	"fmt"
	"os"
	"time"
)

// Lease represents a workflow execution lease held by a runner.
// A workflow may run on at most one runner at a time; the lease is
// renewed by heartbeat and can be taken over once it expires.
type Lease struct {
	WorkflowID string    `json:"workflow_id"`
	RunnerID   string    `json:"runner_id"`
	ExpiresAt  time.Time `json:"expires_at"`
	Generation int64     `json:"generation"`
}

// Runner describes an engine process registered in the shared store.
type Runner struct {
	RunnerID     string    `json:"runner_id"`
	Hostname     string    `json:"hostname"`
	Version      string    `json:"version"`
	LastSeen     time.Time `json:"last_seen"`
	RunningCount int       `json:"running_count"`
}

// LeaseStore is the persistence contract the engine uses for
// multi-runner lease coordination. It lives in flowrunner (not
// flowadmin/storage) because the import direction is
// flowadmin/storage -> flowrunner. Implementations must compare
// expiry against the database clock (CURRENT_TIMESTAMP), not the
// caller's clock, so hosts with skewed clocks still agree.
type LeaseStore interface {
	// ClaimLease attempts to acquire (or re-acquire) the lease for
	// workflowID on behalf of runnerID. held=false means another live
	// runner owns it; holder then carries that runner's ID.
	ClaimLease(workflowID, runnerID string, ttl time.Duration) (held bool, holder string, err error)
	// RenewLease extends the lease expiry. ok=false means the lease
	// was lost (expired and claimed by another runner, or released).
	RenewLease(workflowID, runnerID string, ttl time.Duration) (ok bool, err error)
	// ReleaseLease drops the lease if (and only if) runnerID owns it.
	ReleaseLease(workflowID, runnerID string) error
	// ListLeases returns all workflow leases (expired ones included).
	ListLeases() ([]Lease, error)
	// UpsertRunner registers/refreshes a runner heartbeat row.
	UpsertRunner(r Runner) error
	// ListRunners returns all known runners.
	ListRunners() ([]Runner, error)
}

// Default lease timing: heartbeat every 5s, leases live for 15s —
// a dead runner's leases are claimable after ~3 missed heartbeats.
const (
	DefaultLeaseTTL               = 15 * time.Second
	DefaultLeaseHeartbeatInterval = 5 * time.Second
)

// runnerID returns the configured RunnerID, defaulting to the OS
// hostname (pod name under K8s, hostname elsewhere).
func (e *WorkflowEngine) runnerID() string {
	if e.RunnerID != "" {
		return e.RunnerID
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "runner-unknown"
	}
	return host
}

// leaseTTL returns the configured lease TTL or the default.
func (e *WorkflowEngine) leaseTTL() time.Duration {
	if e.LeaseTTL > 0 {
		return e.LeaseTTL
	}
	return DefaultLeaseTTL
}

// heartbeatInterval returns the configured heartbeat tick or default.
func (e *WorkflowEngine) heartbeatInterval() time.Duration {
	if e.LeaseHeartbeatInterval > 0 {
		return e.LeaseHeartbeatInterval
	}
	return DefaultLeaseHeartbeatInterval
}

// claimLease acquires the workflow lease before execution. It is a
// no-op when no LeaseStore is configured (single-runner installs keep
// working unchanged).
func (e *WorkflowEngine) claimLease(workflowID string) error {
	if e.LeaseStore == nil {
		return nil
	}
	held, holder, err := e.LeaseStore.ClaimLease(workflowID, e.runnerID(), e.leaseTTL())
	if err != nil {
		return fmt.Errorf("failed to claim workflow lease: %w", err)
	}
	if !held {
		return fmt.Errorf("workflow is leased to runner %s", holder)
	}
	return nil
}

// releaseLease drops the workflow lease if this runner owns it.
func (e *WorkflowEngine) releaseLease(workflowID string) {
	if e.LeaseStore == nil {
		return
	}
	_ = e.LeaseStore.ReleaseLease(workflowID, e.runnerID())
}

// LeaseRunner returns the runner ID currently holding the workflow's
// lease, or "" when unleased / leases are not configured. Used by the
// API to surface runner_id on workflow-status.
func (e *WorkflowEngine) LeaseRunner(workflowID string) string {
	if e.LeaseStore == nil {
		return ""
	}
	leases, err := e.LeaseStore.ListLeases()
	if err != nil {
		return ""
	}
	for _, l := range leases {
		if l.WorkflowID == workflowID {
			return l.RunnerID
		}
	}
	return ""
}

// startLeaseHeartbeat launches the heartbeat goroutine on first use.
// Each tick renews this runner's leases, upserts its runners row and
// runs the janitor pass (stop local workflows whose lease was lost).
//
// DEFERRED (see task 35 / spike): autostart — workflows marked running
// in the DB whose lease has expired should be claimed and started here.
// The engine has no access to stored workflow definitions (storage
// imports flowrunner, not vice versa), so loading the definition is a
// storage-layer concern; wire it once a definition-provider callback
// is added to the engine.
func (e *WorkflowEngine) startLeaseHeartbeat() {
	e.leaseOnce.Do(func() {
		go e.leaseHeartbeat()
	})
}

// leaseHeartbeat is the periodic renew/janitor loop.
func (e *WorkflowEngine) leaseHeartbeat() {
	ticker := time.NewTicker(e.heartbeatInterval())
	defer ticker.Stop()
	for range ticker.C {
		e.heartbeatTick()
	}
}

// heartbeatTick performs one renew + janitor + runner-upsert pass.
func (e *WorkflowEngine) heartbeatTick() {
	if e.LeaseStore == nil {
		return
	}
	runnerID := e.runnerID()

	e.runMu.Lock()
	ids := make([]string, 0, len(e.running))
	for id := range e.running {
		ids = append(ids, id)
	}
	e.runMu.Unlock()

	lost := 0
	for _, id := range ids {
		ok, err := e.LeaseStore.RenewLease(id, runnerID, e.leaseTTL())
		if err != nil {
			e.EmitEvent(id, "", "", "warn", fmt.Sprintf("lease renewal failed: %v", err))
			continue
		}
		if !ok {
			// Janitor: the lease was taken over (or lost) — stop the
			// local copy so two runners never execute the same flow.
			lost++
			e.EmitEvent(id, "", "", "warn", "workflow lease lost — stopping local execution")
			_ = e.StopWorkflow(id)
		}
	}

	host, _ := os.Hostname()
	if err := e.LeaseStore.UpsertRunner(Runner{
		RunnerID:     runnerID,
		Hostname:     host,
		Version:      e.RunnerVersion,
		LastSeen:     time.Now().UTC(),
		RunningCount: len(ids) - lost,
	}); err != nil {
		// Runner table is advisory — never kill the heartbeat over it.
		e.EmitEvent("", "", "", "debug", fmt.Sprintf("runner heartbeat upsert failed: %v", err))
	}
}
