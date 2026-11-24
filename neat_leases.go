package noderunner

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dracory/neat"
	neatschema "github.com/dracory/neat/contracts/database/schema"
)

// neat_leases.go ports flowadmin/storage/neat_leases.go onto the new
// stack: a neat-backed LeaseStore sharing the nodestore entity DB
// (constructed on the same *sql.DB as the NeatInboxStore — see
// nodeadmin wiring). Leases decide which runner drains a workflow's
// durable inbox and executes its graph when several runners share one
// database.
//
// Expiry is stored as UTC 'YYYY-MM-DD HH:MM:SS' text and compared
// against the DB clock (CURRENT_TIMESTAMP) in claim predicates, so
// hosts with skewed clocks still agree — the same contract as the
// flowadmin original.
//
// Table names are distinct from flowadmin's (node_workflow_leases /
// node_runners) so the two engines never fight over the same lease
// rows while the cutover runs both stacks side by side.

const (
	leaseTableName  = "node_workflow_leases"
	runnerTableName = "node_runners"
)

// runnerRow is the database row for the node_runners table
type runnerRow struct {
	RunnerID     string `db:"runner_id"`
	Hostname     string `db:"hostname"`
	Version      string `db:"version"`
	LastSeen     string `db:"last_seen"`
	RunningCount int64  `db:"running_count"`
}

// TableName specifies the table name
func (runnerRow) TableName() string {
	return runnerTableName
}

// leaseRow is the database row for the node_workflow_leases table.
// ExpiresAt is stored as UTC 'YYYY-MM-DD HH:MM:SS' text so that the
// portable CURRENT_TIMESTAMP comparison in the claim predicate works
// lexically on SQLite and as timestamps on client-server dialects.
type leaseRow struct {
	WorkflowID string `db:"workflow_id"`
	RunnerID   string `db:"runner_id"`
	ExpiresAt  string `db:"expires_at"`
	Generation int64  `db:"generation"`
}

// TableName specifies the table name
func (leaseRow) TableName() string {
	return leaseTableName
}

// leaseTimeFormat matches CURRENT_TIMESTAMP's text layout so expiry
// comparisons hold on every neat dialect.
const leaseTimeFormat = "2006-01-02 15:04:05"

// leaseExpiry converts a Go TTL into the stored expiry timestamp,
// computed on the caller's clock but always compared against the DB
// clock (CURRENT_TIMESTAMP) in predicates — clock skew between hosts
// therefore shifts the effective TTL rather than breaking correctness.
func leaseExpiry(ttl time.Duration) string {
	return time.Now().UTC().Add(ttl).Format(leaseTimeFormat)
}

// parseLeaseTime reads a stored expiry timestamp (accepts fractional
// seconds that some dialects append).
func parseLeaseTime(s string) time.Time {
	for _, f := range []string{"2006-01-02 15:04:05.999999999", leaseTimeFormat, time.RFC3339} {
		if t, err := time.Parse(f, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

// NeatLeaseStore is the neat-backed LeaseStore for the new stack.
// Construct it on the same *sql.DB the entitystore uses (see
// nodeadmin) so leases coordinate every runner on that database.
type NeatLeaseStore struct {
	db *neat.Database
}

// NewNeatLeaseStore wraps an open *sql.DB in a neat Database (driver
// auto-detected) and migrates the lease/runner tables. The caller
// keeps ownership of sqlDB — Close only releases neat's bookkeeping.
func NewNeatLeaseStore(sqlDB *sql.DB) (*NeatLeaseStore, error) {
	if sqlDB == nil {
		return nil, fmt.Errorf("neat lease store: sqlDB is nil")
	}
	db, err := neat.NewFromSQLDB(sqlDB)
	if err != nil {
		return nil, fmt.Errorf("neat lease store: %w", err)
	}
	s := &NeatLeaseStore{db: db}
	if err := s.migrate(); err != nil {
		return nil, fmt.Errorf("neat lease store migrate: %w", err)
	}
	return s, nil
}

// migrate creates node_workflow_leases and node_runners on first use.
func (s *NeatLeaseStore) migrate() error {
	schema := s.db.Schema()
	if !schema.HasTable(runnerTableName) {
		err := schema.Create(runnerTableName, func(t neatschema.Blueprint) {
			t.String("runner_id")
			t.String("hostname").Nullable()
			t.String("version").Nullable()
			t.String("last_seen").Nullable()
			t.Integer("running_count").Nullable()
			t.Primary("runner_id")
		})
		if err != nil {
			return err
		}
	}
	if !schema.HasTable(leaseTableName) {
		err := schema.Create(leaseTableName, func(t neatschema.Blueprint) {
			t.String("workflow_id")
			t.String("runner_id")
			// String-typed (not DateTime) so the 'YYYY-MM-DD HH:MM:SS'
			// value passes through verbatim and the lexical
			// CURRENT_TIMESTAMP comparison holds on every dialect —
			// same pattern as the inbox claim columns.
			t.String("expires_at")
			t.Integer("generation")
			t.Primary("workflow_id")
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// ClaimLease implements LeaseStore. The conditional UPDATE is atomic
// because SQL writers serialize; it succeeds only when the lease is
// free, expired (per the DB clock) or already ours. When no row exists
// yet we insert one, then fall back to the UPDATE — which resolves the
// concurrent-insert race the same way (one side loses the PK insert,
// retries the UPDATE, sees a live holder).
func (s *NeatLeaseStore) ClaimLease(workflowID, runnerID string, ttl time.Duration) (bool, string, error) {
	if workflowID == "" || runnerID == "" {
		return false, "", fmt.Errorf("workflowID and runnerID are required")
	}
	expiry := leaseExpiry(ttl)

	claim := func() (int64, error) {
		res, err := s.db.Query().Table(leaseTableName).
			Where("workflow_id = ? AND (expires_at < CURRENT_TIMESTAMP OR runner_id = ?)", workflowID, runnerID).
			// generation is left untouched — the neat Update builder
			// cannot express "generation + 1" and the counter is
			// advisory anyway (lease decisions ride on expiry).
			Update(map[string]interface{}{
				"runner_id":  runnerID,
				"expires_at": expiry,
			})
		if err != nil {
			return 0, err
		}
		return res.RowsAffected, nil
	}

	updated, err := claim()
	if err != nil {
		return false, "", fmt.Errorf("failed to claim lease: %w", err)
	}
	if updated > 0 {
		return true, "", nil
	}

	// UPDATE matched nothing: either no row exists or a live runner
	// holds it.
	var row leaseRow
	err = s.db.Query().Model(&leaseRow{}).Where("workflow_id = ?", workflowID).First(&row)
	if err == nil {
		return false, row.RunnerID, nil
	}
	if !isLeaseNotFound(err) {
		return false, "", fmt.Errorf("failed to read lease: %w", err)
	}

	// No row — insert generation 1. If another runner inserted first,
	// the PK violation surfaces as an error; retry the guarded UPDATE
	// to learn who won.
	_, insErr := s.db.Query().Table(leaseTableName).InsertGetId(map[string]interface{}{
		"workflow_id": workflowID,
		"runner_id":   runnerID,
		"expires_at":  expiry,
		"generation":  1,
	})
	if insErr == nil {
		return true, "", nil
	}
	updated, err = claim()
	if err != nil {
		return false, "", fmt.Errorf("failed to claim lease: %w", err)
	}
	if updated > 0 {
		return true, "", nil
	}
	if err := s.db.Query().Model(&leaseRow{}).Where("workflow_id = ?", workflowID).First(&row); err != nil {
		return false, "", fmt.Errorf("failed to read lease: %w", err)
	}
	return false, row.RunnerID, nil
}

// RenewLease implements LeaseStore. Zero rows affected means the lease
// was lost (taken over or released).
func (s *NeatLeaseStore) RenewLease(workflowID, runnerID string, ttl time.Duration) (bool, error) {
	res, err := s.db.Query().Table(leaseTableName).
		Where("workflow_id = ? AND runner_id = ?", workflowID, runnerID).
		Update(map[string]interface{}{"expires_at": leaseExpiry(ttl)})
	if err != nil {
		return false, fmt.Errorf("failed to renew lease: %w", err)
	}
	return res.RowsAffected > 0, nil
}

// ReleaseLease implements LeaseStore.
func (s *NeatLeaseStore) ReleaseLease(workflowID, runnerID string) error {
	if _, err := s.db.Query().Model(&leaseRow{}).
		Where("workflow_id = ?", workflowID).
		Where("runner_id = ?", runnerID).
		Delete(); err != nil {
		return fmt.Errorf("failed to release lease: %w", err)
	}
	return nil
}

// ListLeases implements LeaseStore.
func (s *NeatLeaseStore) ListLeases() ([]Lease, error) {
	var rows []leaseRow
	if err := s.db.Query().Model(&leaseRow{}).Find(&rows); err != nil {
		return nil, fmt.Errorf("failed to list leases: %w", err)
	}
	leases := make([]Lease, 0, len(rows))
	for _, r := range rows {
		leases = append(leases, Lease{
			WorkflowID: r.WorkflowID,
			RunnerID:   r.RunnerID,
			ExpiresAt:  parseLeaseTime(r.ExpiresAt),
			Generation: r.Generation,
		})
	}
	return leases, nil
}

// UpsertRunner implements LeaseStore.
func (s *NeatLeaseStore) UpsertRunner(r Runner) error {
	if r.RunnerID == "" {
		return fmt.Errorf("runner ID is required")
	}
	now := time.Now().UTC().Format(leaseTimeFormat)
	res, err := s.db.Query().Table(runnerTableName).
		Where("runner_id = ?", r.RunnerID).
		Update(map[string]interface{}{
			"hostname":      r.Hostname,
			"version":       r.Version,
			"last_seen":     now,
			"running_count": r.RunningCount,
		})
	if err != nil {
		return fmt.Errorf("failed to upsert runner: %w", err)
	}
	if res.RowsAffected > 0 {
		return nil
	}
	_, err = s.db.Query().Table(runnerTableName).InsertGetId(map[string]interface{}{
		"runner_id":     r.RunnerID,
		"hostname":      r.Hostname,
		"version":       r.Version,
		"last_seen":     now,
		"running_count": r.RunningCount,
	})
	if err != nil {
		return fmt.Errorf("failed to insert runner: %w", err)
	}
	return nil
}

// ListRunners implements LeaseStore.
func (s *NeatLeaseStore) ListRunners() ([]Runner, error) {
	var rows []runnerRow
	if err := s.db.Query().Model(&runnerRow{}).Find(&rows); err != nil {
		return nil, fmt.Errorf("failed to list runners: %w", err)
	}
	runners := make([]Runner, 0, len(rows))
	for _, r := range rows {
		runners = append(runners, Runner{
			RunnerID:     r.RunnerID,
			Hostname:     r.Hostname,
			Version:      r.Version,
			LastSeen:     parseLeaseTime(r.LastSeen),
			RunningCount: int(r.RunningCount),
		})
	}
	return runners, nil
}

// isLeaseNotFound reports whether err represents a missing record —
// mirrors flowadmin's isNotFound helper.
func isLeaseNotFound(err error) bool {
	return errors.Is(err, sql.ErrNoRows) || strings.Contains(err.Error(), "record not found")
}

// Compile-time check that NeatLeaseStore satisfies the engine contract.
var _ LeaseStore = (*NeatLeaseStore)(nil)
