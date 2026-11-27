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
// use atomic SQL increment expressions; release consumes counts inside a
// transaction that first atomically deletes the waiter row. A competing
// runner therefore either wins both the waiter and the count or wins
// neither. The SQL uses portable predicates and neat's dialect-aware query
// builder — no SQLite-only upsert or locking syntax.

const (
	coordinationSignalsTable = "node_coordination_signals"
	coordinationWaitersTable = "node_coordination_waiters"
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
	ID          string `db:"id"`
	SignalKey   string `db:"signal_key"`
	WorkflowID  string `db:"workflow_id"`
	NodeID      string `db:"node_id"`
	TargetCount int64  `db:"target_count"`
	Payload     string `db:"payload"`
	ExpiresAt   string `db:"expires_at"`
	CreatedAt   string `db:"created_at"`
}

func (coordinationWaiterRow) TableName() string { return coordinationWaitersTable }

// NeatCoordinationStore is the neat-backed distributed coordination store.
// Construct it on the same *sql.DB as the entity store, durable inbox and
// lease store so every runner sharing that database observes the same
// signal counts and parked messages.
type NeatCoordinationStore struct {
	db *neat.Database

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
	store := &NeatCoordinationStore{db: db}
	if err := store.migrate(); err != nil {
		return nil, fmt.Errorf("neat coordination store migrate: %w", err)
	}
	return store, nil
}

func (s *NeatCoordinationStore) migrate() error {
	schema := s.db.Schema()
	if !schema.HasTable(coordinationSignalsTable) {
		err := schema.Create(coordinationSignalsTable, func(t neatschema.Blueprint) {
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
	}
	if !schema.HasTable(coordinationWaitersTable) {
		err := schema.Create(coordinationWaitersTable, func(t neatschema.Blueprint) {
			t.String("id")
			t.String("signal_key")
			t.String("workflow_id")
			t.String("node_id")
			t.BigInteger("target_count")
			t.Text("payload")
			t.String("expires_at").Nullable()
			t.String("created_at")
			t.Primary("id")
			t.Index("signal_key")
			t.Index("workflow_id")
		})
		if err != nil {
			return err
		}
	}
	return nil
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

// PollWaiters implements nodebase.CoordinationStore. Expiry claims are a
// single conditional DELETE. Release claims are transactional: the waiter
// row is deleted first and the target-sized signal decrement must also
// affect one row, otherwise the transaction rolls back. This coupling is
// what prevents two runners from consuming the same count for one waiter.
func (s *NeatCoordinationStore) PollWaiters(ctx context.Context, workflowID, nodeID string) ([]nodebase.CoordinationWaiter, []nodebase.CoordinationWaiter, error) {
	if workflowID == "" || nodeID == "" {
		return nil, nil, fmt.Errorf("workflowID and nodeID are required")
	}
	now := time.Now().UTC()
	if err := s.cleanupExpiredSignals(now); err != nil {
		log.Printf("warning: coordination signal cleanup failed: %v", err)
	}

	var expiringRows []coordinationWaiterRow
	if err := s.db.Query().Model(&coordinationWaiterRow{}).
		Where("workflow_id = ? AND node_id = ? AND expires_at <> ''", workflowID, nodeID).
		OrderBy("created_at").
		Find(&expiringRows); err != nil {
		return nil, nil, fmt.Errorf("load expired coordination waiters: %w", err)
	}
	expired := make([]nodebase.CoordinationWaiter, 0, len(expiringRows))
	for _, row := range expiringRows {
		if !expiredCoordinationValue(row.ExpiresAt, now) {
			continue
		}
		msg, err := unmarshalInboxPayload(row.Payload)
		if err != nil {
			log.Printf("warning: dropping corrupt coordination waiter id=%s: %v", row.ID, err)
			_, _ = s.db.Query().Model(&coordinationWaiterRow{}).Where("id = ?", row.ID).Delete()
			continue
		}
		res, err := s.db.Query().Model(&coordinationWaiterRow{}).
			Where("id = ? AND workflow_id = ? AND node_id = ?", row.ID, workflowID, nodeID).
			Delete()
		if err != nil {
			return nil, nil, fmt.Errorf("claim expired coordination waiter: %w", err)
		}
		if res == nil || res.RowsAffected != 1 {
			continue // another runner claimed it first
		}
		expired = append(expired, waiterFromRow(row, msg, nil))
	}

	var rows []coordinationWaiterRow
	if err := s.db.Query().Model(&coordinationWaiterRow{}).
		Where("workflow_id = ? AND node_id = ?", workflowID, nodeID).
		OrderBy("created_at").
		Find(&rows); err != nil {
		return nil, expired, fmt.Errorf("load coordination waiters: %w", err)
	}
	released := make([]nodebase.CoordinationWaiter, 0, len(rows))
	for _, row := range rows {
		waiter, ok, err := s.tryRelease(row)
		if err != nil {
			return released, expired, err
		}
		if ok {
			released = append(released, waiter)
		}
	}
	return released, expired, nil
}

func (s *NeatCoordinationStore) tryRelease(row coordinationWaiterRow) (nodebase.CoordinationWaiter, bool, error) {
	msg, err := unmarshalInboxPayload(row.Payload)
	if err != nil {
		log.Printf("warning: dropping corrupt coordination waiter id=%s: %v", row.ID, err)
		_, _ = s.db.Query().Model(&coordinationWaiterRow{}).Where("id = ?", row.ID).Delete()
		return nodebase.CoordinationWaiter{}, false, nil
	}
	cached := map[string]interface{}{}
	for attempt := 0; attempt < 20; attempt++ {
		err = s.db.Transaction(func(tx orm.Query) error {
			res, err := tx.Model(&coordinationWaiterRow{}).Where("id = ?", row.ID).Delete()
			if err != nil {
				return err
			}
			if res == nil || res.RowsAffected != 1 {
				return errCoordinationWaiterGone
			}
			var signal coordinationSignalRow
			err = tx.Model(&coordinationSignalRow{}).Where("signal_key = ?", row.SignalKey).First(&signal)
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
	return waiterFromRow(row, msg, cached), true, nil
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

// Compile-time check: the SQL store satisfies the runner-neutral contract.
var _ nodebase.CoordinationStore = (*NeatCoordinationStore)(nil)
