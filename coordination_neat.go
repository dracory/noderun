package noderunner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dracory/neat"
	"github.com/dracory/neat/contracts/database/orm"
	neatschema "github.com/dracory/neat/contracts/database/schema"
	"github.com/dracory/nodebase"
	"github.com/dracory/nodestore"
)

// coordination_neat.go is the production CoordinationStore: dedicated SQL
// tables shared by every runner on the entity database. Count increments
// use atomic SQL increment expressions. Release claims lease the waiter row
// and consume the count in one transaction; the row is deleted only by
// AckWaiter after the caller proves the downstream handoff is durable. A
// crashed owner's lease expires and the stored outcome is reclaimed without
// consuming the count again. The SQL uses portable predicates and neat's
// dialect-aware query builder — no SQLite-only upsert or locking syntax.

const (
	coordinationSignalsTable = "node_coordination_signals"
	coordinationWaitersTable = "node_coordination_waiters"

	coordinationClaimPending  = 0
	coordinationClaimReleased = 1
	coordinationClaimExpired  = 2

	coordinationDefaultClaimTTL = 30 * time.Second
)

// coordinationTimeFormat stores UTC expiry/creation timestamps in a
// canonical RFC3339 form. Expiry comparisons happen in Go rather than
// database-specific datetime arithmetic: SQL datetime types and
// CURRENT_TIMESTAMP representations differ across SQLite, Postgres and
// MySQL, while the conditional row claims that matter for correctness are
// plain portable SQL.
const coordinationTimeFormat = time.RFC3339Nano

type coordinationSignalRow struct {
	SignalKey      string `db:"signal_key"`
	Count          int64  `db:"signal_count"`
	CachedMetadata string `db:"cached_metadata"`
	ExpiresAt      string `db:"expires_at"`
	UpdatedAt      string `db:"updated_at"`
}

func (coordinationSignalRow) TableName() string { return coordinationSignalsTable }

type coordinationWaiterRow struct {
	ID              string `db:"id"`
	SignalKey       string `db:"signal_key"`
	WorkflowID      string `db:"workflow_id"`
	NodeID          string `db:"node_id"`
	TargetCount     int64  `db:"target_count"`
	Payload         string `db:"payload"`
	ExpiresAt       string `db:"expires_at"`
	CreatedAt       string `db:"created_at"`
	ClaimState      int    `db:"claim_state"`
	ClaimActive     int    `db:"claim_active"`
	ClaimToken      string `db:"claim_token"`
	ClaimExpiresAt  string `db:"claim_expires_at"`
	ReleaseMetadata string `db:"release_metadata"`
}

func (coordinationWaiterRow) TableName() string { return coordinationWaitersTable }

// NeatCoordinationStore is the neat-backed distributed coordination store.
// Construct it on the same *sql.DB as the entity store, durable inbox and
// lease store so every runner sharing that database observes the same
// signal counts and parked messages.
type NeatCoordinationStore struct {
	db       *neat.Database
	claimTTL time.Duration

	// lastSignalCleanupUnixNano throttles opportunistic cleanup because
	// PollWaiters can run many times per second for every waiting node.
	lastSignalCleanupUnixNano atomic.Int64
}

// NewNeatCoordinationStore wraps an open *sql.DB and migrates the
// coordination tables. The caller keeps ownership of sqlDB.
func NewNeatCoordinationStore(sqlDB *sql.DB) (*NeatCoordinationStore, error) {
	if sqlDB == nil {
		return nil, fmt.Errorf("neat coordination store: sqlDB is nil")
	}
	db, err := neat.NewFromSQLDB(sqlDB)
	if err != nil {
		return nil, fmt.Errorf("neat coordination store: %w", err)
	}
	store := &NeatCoordinationStore{db: db, claimTTL: coordinationDefaultClaimTTL}
	if err := store.migrate(); err != nil {
		return nil, fmt.Errorf("neat coordination store migrate: %w", err)
	}
	return store, nil
}

// SetCoordinationClaimTTLForTest shortens the ownership lease in focused
// crash/reclaim tests. Production uses the conservative default above; the
// method is exported only because noderun's integration tests are external.
func (s *NeatCoordinationStore) SetCoordinationClaimTTLForTest(ttl time.Duration) {
	s.claimTTL = ttl
}

func (s *NeatCoordinationStore) migrate() error {
	schema := s.db.Schema()
	err := createCoordinationTableIfMissing(schema, coordinationSignalsTable, func(t neatschema.Blueprint) {
		t.String("signal_key")
		t.BigInteger("signal_count")
		t.Text("cached_metadata").Nullable()
		t.String("expires_at").Nullable()
		t.String("updated_at").Nullable()
		t.Primary("signal_key")
		t.Index("expires_at")
	})
	if err != nil {
		return err
	}
	err = createCoordinationTableIfMissing(schema, coordinationWaitersTable, func(t neatschema.Blueprint) {
		t.String("id")
		t.String("signal_key")
		t.String("workflow_id")
		t.String("node_id")
		t.BigInteger("target_count")
		t.Text("payload")
		t.String("expires_at").Nullable()
		t.String("created_at")
		t.Integer("claim_state").Nullable()
		t.Integer("claim_active").Nullable()
		t.String("claim_token").Nullable()
		t.String("claim_expires_at").Nullable()
		t.Text("release_metadata").Nullable()
		t.Primary("id")
		t.Index("signal_key")
		t.Index("workflow_id")
	})
	if err != nil {
		return err
	}
	// Upgrade path for tables created before ownership leasing existed. The
	// feature is new, but keeping this idempotent also makes concurrent
	// constructors safe when one runner is mid-migration.
	for _, column := range []string{"claim_state", "claim_active", "claim_token", "claim_expires_at", "release_metadata"} {
		if err := addCoordinationColumnIfMissing(schema, coordinationWaitersTable, column); err != nil {
			return err
		}
	}
	return backfillCoordinationWaiterClaimColumns(s.db)
}

// backfillCoordinationWaiterClaimColumns normalizes rows that existed before
// the leasing columns were added. SQL NULL is not equal to zero/empty in the
// conditional claim predicates, so without this migration step legacy parked
// rows would scan as pending in Go but never win their SQL claim update.
// The updates are idempotent and safe for concurrent constructors.
func backfillCoordinationWaiterClaimColumns(db *neat.Database) error {
	updates := []struct {
		column string
		value  interface{}
	}{
		{column: "claim_state", value: coordinationClaimPending},
		{column: "claim_active", value: 0},
		{column: "claim_token", value: ""},
		{column: "claim_expires_at", value: ""},
		{column: "release_metadata", value: ""},
	}
	for _, update := range updates {
		var lastErr error
		for attempt := 0; attempt < 30; attempt++ {
			_, lastErr = db.Query().Table(coordinationWaitersTable).
				Where(update.column + " IS NULL").
				Update(map[string]interface{}{update.column: update.value})
			if lastErr == nil {
				break
			}
			if !isCoordinationBusy(lastErr) {
				return fmt.Errorf("backfill coordination waiter %s: %w", update.column, lastErr)
			}
			time.Sleep(time.Duration((attempt+1)*5+(attempt*7)%13) * time.Millisecond)
		}
		if lastErr != nil {
			return fmt.Errorf("backfill coordination waiter %s: %w", update.column, lastErr)
		}
	}
	return nil
}

func addCoordinationColumnIfMissing(schema neatschema.Schema, table, column string) error {
	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		if schema.HasColumn(table, column) {
			return nil
		}
		lastErr = schema.Table(table, func(t neatschema.Blueprint) {
			switch column {
			case "claim_state", "claim_active":
				t.Integer(column).Nullable()
			case "release_metadata":
				t.Text(column).Nullable()
			default:
				t.String(column).Nullable()
			}
		})
		if lastErr == nil || schema.HasColumn(table, column) {
			return nil
		}
		if !isCoordinationBusy(lastErr) && !isCoordinationAlreadyExists(lastErr) {
			return lastErr
		}
		time.Sleep(time.Duration((attempt+1)*5+(attempt*7)%13) * time.Millisecond)
	}
	if schema.HasColumn(table, column) {
		return nil
	}
	return lastErr
}

// createCoordinationTableIfMissing makes first-use DDL safe for several
// runners constructing the store against the same empty database. neat
// grammars intentionally emit plain CREATE TABLE (no portable IF NOT
// EXISTS), so the losing runner re-checks existence after duplicate DDL;
// transient lock contention is retried briefly.
func createCoordinationTableIfMissing(schema neatschema.Schema, table string, build func(neatschema.Blueprint)) error {
	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		if schema.HasTable(table) {
			return nil
		}
		lastErr = schema.Create(table, build)
		if lastErr == nil || schema.HasTable(table) {
			return nil
		}
		if !isCoordinationBusy(lastErr) && !isCoordinationAlreadyExists(lastErr) {
			return lastErr
		}
		time.Sleep(time.Duration((attempt+1)*5+(attempt*7)%13) * time.Millisecond)
	}
	if schema.HasTable(table) {
		return nil
	}
	return lastErr
}

// IncrementSignal implements nodebase.CoordinationStore. The increment is
// an atomic SQL count=count+1 update. The first increment inserts the key;
// a concurrent first insert loses the primary-key race and retries through
// the atomic update path. Metadata cache conflicts are last-writer-wins per
// key inside one transaction; count correctness does not depend on that
// metadata merge.
func (s *NeatCoordinationStore) IncrementSignal(ctx context.Context, key string, metadata map[string]interface{}, ttl time.Duration) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("coordination signal key is required")
	}
	if metadata == nil {
		metadata = map[string]interface{}{}
	}
	now := time.Now().UTC()
	expires := ""
	if ttl > 0 {
		expires = now.Add(ttl).Format(coordinationTimeFormat)
	}
	updated := now.Format(coordinationTimeFormat)

	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		lastErr = s.db.Transaction(func(tx orm.Query) error {
			var row coordinationSignalRow
			err := tx.Model(&coordinationSignalRow{}).Where("signal_key = ?", key).First(&row)
			rowExists := true
			if isCoordinationNotFound(err) {
				rowExists = false
			} else if err != nil {
				return err
			} else if expiredCoordinationValue(row.ExpiresAt, now) {
				// Treat an expired row as absent. Deleting it inside the same
				// transaction keeps cleanup portable; if another runner wins the
				// race first, ordinary transaction/duplicate handling applies.
				if _, err := tx.Model(&coordinationSignalRow{}).
					Where("signal_key = ? AND expires_at = ?", key, row.ExpiresAt).
					Delete(); err != nil {
					return err
				}
				rowExists = false
			}

			if !rowExists {
				cached, err := nodestore.MarshalPayloadJSON(metadata)
				if err != nil {
					return err
				}
				// Create is used rather than InsertGetId: these tables use
				// natural primary keys and have no id column. InsertGetId is
				// auto-increment-oriented and is not portable to Postgres here.
				err = tx.Model(&coordinationSignalRow{}).Create(&coordinationSignalRow{
					SignalKey:      key,
					Count:          1,
					CachedMetadata: string(cached),
					ExpiresAt:      expires,
					UpdatedAt:      updated,
				})
				return err
			}

			merged, err := mergeCachedMetadataJSON(row.CachedMetadata, metadata)
			if err != nil {
				return err
			}
			res, err := tx.Table(coordinationSignalsTable).
				Where("signal_key = ?", key).
				Increment("signal_count")
			if err != nil {
				return err
			}
			if res == nil || res.RowsAffected != 1 {
				// The row existed when read but a concurrent release consumed its
				// final count and removed it before this increment. Retry the whole
				// operation; the next attempt inserts the post-release count.
				return errCoordinationSignalGone
			}
			_, err = tx.Table(coordinationSignalsTable).
				Where("signal_key = ?", key).
				Update(map[string]interface{}{
					"cached_metadata": merged,
					"expires_at":      expires,
					"updated_at":      updated,
				})
			return err
		})
		if lastErr == nil {
			return nil
		}
		if !errors.Is(lastErr, errCoordinationSignalGone) && !isCoordinationDuplicate(lastErr) && !isCoordinationBusy(lastErr) {
			break
		}
		time.Sleep(time.Duration((attempt+1)*5+(attempt*7)%13) * time.Millisecond)
	}
	return fmt.Errorf("increment coordination signal: %w", lastErr)
}

// ParkWaiter implements nodebase.CoordinationStore. The complete message
// envelope uses the same binary-safe serialization as the durable inbox.
func (s *NeatCoordinationStore) ParkWaiter(ctx context.Context, waiter nodebase.CoordinationWaiter) error {
	if strings.TrimSpace(waiter.ID) == "" || strings.TrimSpace(waiter.SignalKey) == "" || waiter.WorkflowID == "" || waiter.NodeID == "" {
		return fmt.Errorf("coordination waiter id, signal, workflow and node are required")
	}
	if waiter.TargetCount <= 0 {
		return fmt.Errorf("coordination waiter target count must be positive")
	}
	if waiter.Message == nil {
		return fmt.Errorf("coordination waiter message is required")
	}
	payload, err := marshalInboxPayload(waiter.Message)
	if err != nil {
		return fmt.Errorf("serialize coordination waiter: %w", err)
	}
	created := waiter.CreatedAt.UTC()
	if created.IsZero() {
		created = time.Now().UTC()
	}
	expires := ""
	if !waiter.ExpiresAt.IsZero() {
		expires = waiter.ExpiresAt.UTC().Format(coordinationTimeFormat)
	}
	err = s.db.Query().Model(&coordinationWaiterRow{}).Create(&coordinationWaiterRow{
		ID:          waiter.ID,
		SignalKey:   waiter.SignalKey,
		WorkflowID:  waiter.WorkflowID,
		NodeID:      waiter.NodeID,
		TargetCount: waiter.TargetCount,
		Payload:     payload,
		ExpiresAt:   expires,
		CreatedAt:   created.Format(coordinationTimeFormat),
		ClaimState:  coordinationClaimPending,
		ClaimActive: 0,
	})
	if err != nil {
		return fmt.Errorf("park coordination waiter: %w", err)
	}
	return nil
}

var (
	errCoordinationWaiterGone = errors.New("coordination waiter already claimed")
	errCoordinationSignalGone = errors.New("coordination signal disappeared during increment")
	errCoordinationShort      = errors.New("coordination signal count below target")
)

// PollWaiters implements nodebase.CoordinationStore. Rows are leased, not
// deleted: the winner owns ClaimToken until AckWaiter/NackWaiter or until
// ClaimExpiresAt passes. The first release claim consumes the signal count
// and stores the outcome on the waiter row in the same transaction. A retry
// after nack or crash therefore reuses that stored outcome and never consumes
// the count a second time.
func (s *NeatCoordinationStore) PollWaiters(ctx context.Context, workflowID, nodeID string) ([]nodebase.CoordinationWaiter, []nodebase.CoordinationWaiter, error) {
	if workflowID == "" || nodeID == "" {
		return nil, nil, fmt.Errorf("workflowID and nodeID are required")
	}
	now := time.Now().UTC()
	if err := s.cleanupExpiredSignals(now); err != nil {
		log.Printf("warning: coordination signal cleanup failed: %v", err)
	}

	var rows []coordinationWaiterRow
	if err := s.db.Query().Model(&coordinationWaiterRow{}).
		Where("workflow_id = ? AND node_id = ?", workflowID, nodeID).
		OrderBy("created_at").
		Find(&rows); err != nil {
		return nil, nil, fmt.Errorf("load coordination waiters: %w", err)
	}

	released := make([]nodebase.CoordinationWaiter, 0, len(rows))
	expired := make([]nodebase.CoordinationWaiter, 0)
	for _, row := range rows {
		msg, err := unmarshalInboxPayload(row.Payload)
		if err != nil {
			log.Printf("warning: dropping corrupt coordination waiter id=%s: %v", row.ID, err)
			_, _ = s.db.Query().Model(&coordinationWaiterRow{}).Where("id = ?", row.ID).Delete()
			continue
		}

		// A live claim belongs to another runner (or this owner's in-flight
		// delivery). Expired claims become reclaimable below.
		if row.ClaimActive == 1 && !expiredCoordinationValue(row.ClaimExpiresAt, now) {
			continue
		}

		// A decided outcome survives nack/crash. Reclaim it without touching
		// the signal count again.
		if row.ClaimState != coordinationClaimPending {
			waiter, ok, err := s.claimStoredOutcome(row, msg, now)
			if err != nil {
				return released, expired, err
			}
			if !ok {
				continue
			}
			if waiterState(row) == coordinationClaimExpired {
				expired = append(expired, waiter)
			} else {
				released = append(released, waiter)
			}
			continue
		}

		// A live-claim/expired-claim pending row should not exist in this
		// implementation because outcome and first ownership are written in one
		// transaction. Leave it leased rather than guessing whether its count
		// was consumed; operators can inspect the row and the lease will not
		// resurrect an ambiguous count state.
		if row.ClaimActive == 1 {
			continue
		}

		if expiredCoordinationValue(row.ExpiresAt, now) {
			waiter, ok, err := s.claimExpired(row, msg, now)
			if err != nil {
				return released, expired, err
			}
			if ok {
				expired = append(expired, waiter)
			}
			continue
		}

		waiter, ok, err := s.tryRelease(row, msg, now)
		if err != nil {
			return released, expired, err
		}
		if ok {
			released = append(released, waiter)
		}
	}
	return released, expired, nil
}

func waiterState(row coordinationWaiterRow) int {
	if row.ClaimState == coordinationClaimExpired {
		return coordinationClaimExpired
	}
	return coordinationClaimReleased
}

func (s *NeatCoordinationStore) claimLease(now time.Time) (token, expires string) {
	ttl := s.claimTTL
	if ttl <= 0 {
		ttl = coordinationDefaultClaimTTL
	}
	return nodebase.GenerateID(), now.Add(ttl).UTC().Format(coordinationTimeFormat)
}

func ownedWaiter(row coordinationWaiterRow, msg nodestore.MessageInterface, cached map[string]interface{}, token, claimExpires string) nodebase.CoordinationWaiter {
	row.ClaimToken = token
	row.ClaimExpiresAt = claimExpires
	return waiterFromRow(row, msg, cached)
}

// claimStoredOutcome re-leases a row whose release/expired decision is already
// durable. It handles both nacked rows (claim_active=0) and crash leftovers
// whose prior lease expired (claim_active=1 with the exact old token/expiry).
func (s *NeatCoordinationStore) claimStoredOutcome(row coordinationWaiterRow, msg nodestore.MessageInterface, now time.Time) (nodebase.CoordinationWaiter, bool, error) {
	token, claimExpires := s.claimLease(now)
	updates := map[string]interface{}{
		"claim_active":     1,
		"claim_token":      token,
		"claim_expires_at": claimExpires,
	}
	var res *orm.Result
	var err error
	if row.ClaimActive == 1 {
		res, err = s.db.Query().Table(coordinationWaitersTable).
			Where("id = ? AND claim_active = 1 AND claim_token = ? AND claim_expires_at = ?", row.ID, row.ClaimToken, row.ClaimExpiresAt).
			Update(updates)
	} else {
		res, err = s.db.Query().Table(coordinationWaitersTable).
			Where("id = ? AND claim_active = 0 AND claim_state = ?", row.ID, row.ClaimState).
			Update(updates)
	}
	if err != nil {
		return nodebase.CoordinationWaiter{}, false, fmt.Errorf("reclaim decided coordination waiter %q: %w", row.ID, err)
	}
	if res == nil || res.RowsAffected != 1 {
		return nodebase.CoordinationWaiter{}, false, nil
	}
	cached := map[string]interface{}{}
	if row.ClaimState == coordinationClaimReleased {
		cached, err = unmarshalCachedMetadataJSON(row.ReleaseMetadata)
		if err != nil {
			return nodebase.CoordinationWaiter{}, false, fmt.Errorf("decode stored release metadata for waiter %q: %w", row.ID, err)
		}
	}
	return ownedWaiter(row, msg, cached, token, claimExpires), true, nil
}

func (s *NeatCoordinationStore) claimExpired(row coordinationWaiterRow, msg nodestore.MessageInterface, now time.Time) (nodebase.CoordinationWaiter, bool, error) {
	token, claimExpires := s.claimLease(now)
	res, err := s.db.Query().Table(coordinationWaitersTable).
		Where("id = ? AND claim_active = 0 AND claim_state = ?", row.ID, coordinationClaimPending).
		Update(map[string]interface{}{
			"claim_state":      coordinationClaimExpired,
			"claim_active":     1,
			"claim_token":      token,
			"claim_expires_at": claimExpires,
		})
	if err != nil {
		return nodebase.CoordinationWaiter{}, false, fmt.Errorf("claim expired coordination waiter %q: %w", row.ID, err)
	}
	if res == nil || res.RowsAffected != 1 {
		return nodebase.CoordinationWaiter{}, false, nil
	}
	return ownedWaiter(row, msg, nil, token, claimExpires), true, nil
}

func (s *NeatCoordinationStore) tryRelease(row coordinationWaiterRow, msg nodestore.MessageInterface, now time.Time) (nodebase.CoordinationWaiter, bool, error) {
	cached := map[string]interface{}{}
	token, claimExpires := s.claimLease(now)
	var err error
	for attempt := 0; attempt < 20; attempt++ {
		err = s.db.Transaction(func(tx orm.Query) error {
			var signal coordinationSignalRow
			err := tx.Model(&coordinationSignalRow{}).Where("signal_key = ?", row.SignalKey).First(&signal)
			if isCoordinationNotFound(err) {
				return errCoordinationShort
			}
			if err != nil {
				return err
			}
			if expiredCoordinationValue(signal.ExpiresAt, time.Now().UTC()) {
				return errCoordinationShort
			}
			cached, err = unmarshalCachedMetadataJSON(signal.CachedMetadata)
			if err != nil {
				return err
			}

			// Ownership and the release decision are written before count
			// consumption inside this transaction. If either the waiter claim or
			// the conditional decrement loses its race, the transaction rolls
			// back as one atomic unit.
			res, err := tx.Table(coordinationWaitersTable).
				Where("id = ? AND claim_active = 0 AND claim_state = ?", row.ID, coordinationClaimPending).
				Update(map[string]interface{}{
					"claim_state":      coordinationClaimReleased,
					"claim_active":     1,
					"claim_token":      token,
					"claim_expires_at": claimExpires,
					"release_metadata": signal.CachedMetadata,
				})
			if err != nil {
				return err
			}
			if res == nil || res.RowsAffected != 1 {
				return errCoordinationWaiterGone
			}

			res, err = tx.Table(coordinationSignalsTable).
				Where("signal_key = ? AND signal_count >= ?", row.SignalKey, row.TargetCount).
				Decrement("signal_count", row.TargetCount)
			if err != nil {
				return err
			}
			if res == nil || res.RowsAffected != 1 {
				return errCoordinationShort
			}
			if _, err := tx.Model(&coordinationSignalRow{}).
				Where("signal_key = ?", row.SignalKey).Where("signal_count <= 0").
				Delete(); err != nil {
				return err
			}
			return nil
		})
		if !isCoordinationBusy(err) {
			break
		}
		time.Sleep(time.Duration((attempt+1)*5+(attempt*7)%13) * time.Millisecond)
	}
	if errors.Is(err, errCoordinationWaiterGone) || errors.Is(err, errCoordinationShort) {
		return nodebase.CoordinationWaiter{}, false, nil
	}
	if err != nil {
		return nodebase.CoordinationWaiter{}, false, fmt.Errorf("release coordination waiter %q: %w", row.ID, err)
	}
	return ownedWaiter(row, msg, cached, token, claimExpires), true, nil
}

// AckWaiter implements nodebase.CoordinationStore. It is the delete step and
// is valid only for the current claim owner.
func (s *NeatCoordinationStore) AckWaiter(ctx context.Context, waiterID, claimToken string) error {
	if waiterID == "" || claimToken == "" {
		return fmt.Errorf("coordination waiter id and claim token are required")
	}
	res, err := s.db.Query().Model(&coordinationWaiterRow{}).
		Where("id = ? AND claim_active = 1 AND claim_token = ?", waiterID, claimToken).
		Delete()
	if err != nil {
		return fmt.Errorf("ack coordination waiter: %w", err)
	}
	if res == nil || res.RowsAffected != 1 {
		return fmt.Errorf("ack coordination waiter %q: claim is not owned by this token", waiterID)
	}
	return nil
}

// NackWaiter implements nodebase.CoordinationStore. It preserves the message
// and decided outcome, but clears active ownership so the next poll retries
// immediately. Counts are deliberately not restored: restoring them would
// duplicate signal consumption and break the target-count contract.
func (s *NeatCoordinationStore) NackWaiter(ctx context.Context, waiterID, claimToken string) error {
	if waiterID == "" || claimToken == "" {
		return fmt.Errorf("coordination waiter id and claim token are required")
	}
	res, err := s.db.Query().Table(coordinationWaitersTable).
		Where("id = ? AND claim_active = 1 AND claim_token = ?", waiterID, claimToken).
		Update(map[string]interface{}{
			"claim_active":     0,
			"claim_token":      "",
			"claim_expires_at": "",
		})
	if err != nil {
		return fmt.Errorf("nack coordination waiter: %w", err)
	}
	if res == nil || res.RowsAffected != 1 {
		return fmt.Errorf("nack coordination waiter %q: claim is not owned by this token", waiterID)
	}
	return nil
}

// Signal implements nodebase.CoordinationStore for observability and tests.
// Expired rows are treated as absent and removed opportunistically.
func (s *NeatCoordinationStore) Signal(ctx context.Context, key string) (nodebase.CoordinationSignal, bool, error) {
	var row coordinationSignalRow
	err := s.db.Query().Model(&coordinationSignalRow{}).Where("signal_key = ?", key).First(&row)
	if isCoordinationNotFound(err) {
		return nodebase.CoordinationSignal{}, false, nil
	}
	if err != nil {
		return nodebase.CoordinationSignal{}, false, fmt.Errorf("read coordination signal: %w", err)
	}
	if expiredCoordinationValue(row.ExpiresAt, time.Now().UTC()) {
		// Hide expired rows immediately. Removal is deliberately left to the
		// throttled cleanup/increment paths so a stale reader cannot erase a
		// concurrently renewed row with an unconditional delete.
		return nodebase.CoordinationSignal{}, false, nil
	}
	metadata, err := unmarshalCachedMetadataJSON(row.CachedMetadata)
	if err != nil {
		return nodebase.CoordinationSignal{}, false, fmt.Errorf("decode coordination signal metadata: %w", err)
	}
	return nodebase.CoordinationSignal{Key: row.SignalKey, Count: row.Count, Metadata: metadata}, true, nil
}

// Waiters implements nodebase.CoordinationStore for observability and
// tests. Corrupt rows are dropped loudly rather than wedging inspection.
func (s *NeatCoordinationStore) Waiters(ctx context.Context, workflowID, nodeID string) ([]nodebase.CoordinationWaiter, error) {
	var rows []coordinationWaiterRow
	query := s.db.Query().Model(&coordinationWaiterRow{}).OrderBy("created_at")
	if workflowID != "" {
		query = query.Where("workflow_id = ?", workflowID)
	}
	if nodeID != "" {
		query = query.Where("node_id = ?", nodeID)
	}
	if err := query.Find(&rows); err != nil {
		return nil, fmt.Errorf("list coordination waiters: %w", err)
	}
	out := make([]nodebase.CoordinationWaiter, 0, len(rows))
	for _, row := range rows {
		msg, err := unmarshalInboxPayload(row.Payload)
		if err != nil {
			log.Printf("warning: dropping corrupt coordination waiter id=%s: %v", row.ID, err)
			_, _ = s.db.Query().Model(&coordinationWaiterRow{}).Where("id = ?", row.ID).Delete()
			continue
		}
		out = append(out, waiterFromRow(row, msg, nil))
	}
	return out, nil
}

func expiredCoordinationValue(value string, now time.Time) bool {
	if strings.TrimSpace(value) == "" {
		return false
	}
	expires, err := time.ParseInLocation(coordinationTimeFormat, value, time.UTC)
	if err != nil {
		// Corrupt timestamps fail closed for release coordination: treat them
		// as expired so they cannot release a parked message and the cleanup
		// path can remove the invalid row.
		return true
	}
	return !expires.After(now)
}

// cleanupExpiredSignals is intentionally throttled and is not a correctness
// dependency. Every reader treats expired rows as absent even before this
// maintenance pass physically removes them.
func (s *NeatCoordinationStore) cleanupExpiredSignals(now time.Time) error {
	lastUnixNano := s.lastSignalCleanupUnixNano.Load()
	if lastUnixNano != 0 && now.Sub(time.Unix(0, lastUnixNano)) < 30*time.Second {
		return nil
	}
	if !s.lastSignalCleanupUnixNano.CompareAndSwap(lastUnixNano, now.UnixNano()) {
		return nil
	}
	var rows []coordinationSignalRow
	if err := s.db.Query().Model(&coordinationSignalRow{}).
		Where("expires_at <> ''").
		Find(&rows); err != nil {
		return fmt.Errorf("scan expired coordination signals: %w", err)
	}
	for _, row := range rows {
		if !expiredCoordinationValue(row.ExpiresAt, now) {
			continue
		}
		// Match the exact expiry observed in the scan. If a concurrent notify
		// renewed the row, this delete affects no row and the renewal survives.
		if _, err := s.db.Query().Model(&coordinationSignalRow{}).
			Where("signal_key = ? AND expires_at = ?", row.SignalKey, row.ExpiresAt).
			Delete(); err != nil {
			return fmt.Errorf("delete expired coordination signal: %w", err)
		}
	}
	return nil
}

func waiterFromRow(row coordinationWaiterRow, msg nodestore.MessageInterface, cached map[string]interface{}) nodebase.CoordinationWaiter {
	created, _ := time.ParseInLocation(coordinationTimeFormat, row.CreatedAt, time.UTC)
	var expires time.Time
	if row.ExpiresAt != "" {
		expires, _ = time.ParseInLocation(coordinationTimeFormat, row.ExpiresAt, time.UTC)
	}
	var claimExpires time.Time
	if row.ClaimExpiresAt != "" {
		claimExpires, _ = time.ParseInLocation(coordinationTimeFormat, row.ClaimExpiresAt, time.UTC)
	}
	return nodebase.CoordinationWaiter{
		ID:             row.ID,
		SignalKey:      row.SignalKey,
		WorkflowID:     row.WorkflowID,
		NodeID:         row.NodeID,
		TargetCount:    row.TargetCount,
		Message:        msg,
		ExpiresAt:      expires,
		CreatedAt:      created,
		CachedMetadata: cached,
		ClaimToken:     row.ClaimToken,
		ClaimExpiresAt: claimExpires,
	}
}

func mergeCachedMetadataJSON(existing string, metadata map[string]interface{}) (string, error) {
	merged, err := unmarshalCachedMetadataJSON(existing)
	if err != nil {
		return "", err
	}
	for key, value := range metadata {
		merged[key] = value
	}
	raw, err := nodestore.MarshalPayloadJSON(merged)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func unmarshalCachedMetadataJSON(raw string) (map[string]interface{}, error) {
	if strings.TrimSpace(raw) == "" {
		return map[string]interface{}{}, nil
	}
	decoded, err := nodestore.UnmarshalPayloadJSON([]byte(raw))
	if err != nil {
		return nil, err
	}
	metadata, ok := decoded.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("cached coordination metadata is not a JSON object")
	}
	return metadata, nil
}

func isCoordinationNotFound(err error) bool {
	return errors.Is(err, sql.ErrNoRows) || err != nil && strings.Contains(err.Error(), "record not found")
}

func coordinationErrorText(err error) string {
	var parts []string
	for current := err; current != nil; current = errors.Unwrap(current) {
		parts = append(parts, current.Error())
	}
	return strings.ToLower(strings.Join(parts, " "))
}

func isCoordinationDuplicate(err error) bool {
	if err == nil {
		return false
	}
	text := coordinationErrorText(err)
	return strings.Contains(text, "duplicate") ||
		strings.Contains(text, "unique constraint") ||
		strings.Contains(text, "constraint failed") ||
		strings.Contains(text, "primary key constraint") ||
		strings.Contains(text, "constraint violation")
}

func isCoordinationBusy(err error) bool {
	if err == nil {
		return false
	}
	text := coordinationErrorText(err)
	return strings.Contains(text, "database is locked") ||
		strings.Contains(text, "sqlite_busy") ||
		strings.Contains(text, "deadlock") ||
		strings.Contains(text, "could not serialize") ||
		strings.Contains(text, "serialization failure") ||
		strings.Contains(text, "40001")
}

func isCoordinationAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	text := coordinationErrorText(err)
	return strings.Contains(text, "already exists") ||
		strings.Contains(text, "duplicate table") ||
		strings.Contains(text, "duplicate column") ||
		strings.Contains(text, "42p07") ||
		strings.Contains(text, "42701")
}

// Compile-time check: the SQL store satisfies the runner-neutral contract.
var _ nodebase.CoordinationStore = (*NeatCoordinationStore)(nil)
