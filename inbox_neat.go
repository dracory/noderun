package noderunner

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/dracory/neat"
	neatschema "github.com/dracory/neat/contracts/database/schema"

	"github.com/dracory/nodestore"
)

// This file implements the production DurableInboxStore for the
// nodestore-backed engine (task always_persisted_messages):
// every message handoff between nodes leaves a row in the shared
// node_inbox_messages table BEFORE the downstream node may process it.
// Multiple runners sharing one DSN (SQLite file, Postgres, MySQL) all
// see the same rows; the workflow lease decides which runner drains.
//
// Lifecycle per row: pending -> claimed -> deleted.
//   - SaveInboxItem inserts status='pending' — the hop record.
//   - ClaimInboxItem is the atomic conditional UPDATE a draining runner
//     performs at dequeue time; the claim is a lease with TTL
//     (claim_expires_at), never a bare delete, so a dead claimant's
//     rows become claimable again — at-least-once delivery, msgId is
//     the dedup key downstream.
//   - DeleteInboxItem is the ack, issued after the item was dispatched
//     to the node and its downstream offers were persisted. A row left
//     'claimed' means "picked up but crashed mid-process" and is
//     requeued on the next drain, distinct from never-touched 'pending'.
//
// Ordering: seq is the auto-increment id assigned at persist time, so
// concurrent emit calls cannot reorder rows; loads order by seq and the
// drain is per-node FIFO.
//
// Portability: claim expiry is compared against the DB clock
// (CURRENT_TIMESTAMP) — the same pattern as neat_leases.go — and all
// access goes through neat's query builder / schema, so the store works
// unchanged on SQLite, Postgres and MySQL. The table is separate from
// flowadmin's inbox_messages because the serialized payload envelope
// differs (flowrunner.Message vs nodestore.MessageInterface); sharing
// it would let the two engines corrupt each other's rows.

// inboxMessagesTable is the durable handoff table name.
const inboxMessagesTable = "node_inbox_messages"

// inboxRowStatus* enumerate the row lifecycle.
const (
	inboxRowPending = "pending"
	inboxRowClaimed = "claimed"
)

// neatInboxRow is the database row for node_inbox_messages. The
// auto-increment seq defines dequeue order per workflow.
type neatInboxRow struct {
	Seq            int64     `db:"seq"`
	WorkflowID     string    `db:"workflow_id"`
	NodeID         string    `db:"node_id"`
	Payload        string    `db:"payload"`
	Status         string    `db:"status"`
	ClaimedBy      string    `db:"claimed_by"`
	ClaimedAt      string    `db:"claimed_at"`
	ClaimExpiresAt string    `db:"claim_expires_at"`
	EnqueuedAt     time.Time `db:"enqueued_at"`
}

// TableName specifies the table name
func (neatInboxRow) TableName() string {
	return inboxMessagesTable
}

// inboxPayload is the serialized form of a queued message envelope:
// enough of nodestore.MessageInterface to rebuild it with lineage
// (msgId, parentMsgId via Metadata) and error state intact. Payload and
// metadata use the shared durable payload JSON codec so []byte values
// keep their binary type across the database round-trip.
type inboxPayload struct {
	ID        string          `json:"id,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
	Error     *string         `json:"error,omitempty"`
	CreatedAt time.Time       `json:"created_at,omitempty"`
}

func marshalInboxPayload(msg nodestore.MessageInterface) (string, error) {
	p := inboxPayload{}
	if msg != nil {
		p.ID = msg.GetID()
		if msg.GetPayload() != nil {
			payload, err := nodestore.MarshalPayloadJSON(msg.GetPayload())
			if err != nil {
				return "", err
			}
			p.Payload = json.RawMessage(payload)
		}
		if len(msg.GetMetadata()) > 0 {
			metadata, err := nodestore.MarshalPayloadJSON(msg.GetMetadata())
			if err != nil {
				return "", err
			}
			p.Metadata = json.RawMessage(metadata)
		}
		p.Error = msg.GetError()
		p.CreatedAt = msg.GetCreatedAt()
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func unmarshalInboxPayload(raw string) (nodestore.MessageInterface, error) {
	var p inboxPayload
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return nil, err
	}
	var payload any
	if len(p.Payload) > 0 {
		decoded, err := nodestore.UnmarshalPayloadJSON(p.Payload)
		if err != nil {
			return nil, err
		}
		payload = decoded
	}
	metadata := map[string]interface{}{}
	if len(p.Metadata) > 0 {
		decoded, err := nodestore.UnmarshalPayloadJSON(p.Metadata)
		if err != nil {
			return nil, err
		}
		metadataMap, ok := decoded.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("inbox metadata is not a JSON object")
		}
		metadata = metadataMap
	}
	msg := nodestore.NewMessage().SetID(p.ID).SetPayload(payload).SetMetadata(metadata)
	if p.Error != nil {
		msg.SetError(*p.Error)
	}
	if !p.CreatedAt.IsZero() {
		msg.SetCreatedAt(p.CreatedAt)
	}
	return msg, nil
}

// NeatInboxStore is the neat-backed DurableInboxStore. Construct it on
// the same *sql.DB the entitystore uses (see nodeadmin) so workflows
// and their queued messages share one database — that sharing is what
// makes handoff visible to every runner.
type NeatInboxStore struct {
	db *neat.Database
}

// NewNeatInboxStore wraps an open *sql.DB in a neat Database (driver
// auto-detected) and migrates the node_inbox_messages table. The caller
// keeps ownership of sqlDB — Close only releases neat's bookkeeping.
func NewNeatInboxStore(sqlDB *sql.DB) (*NeatInboxStore, error) {
	if sqlDB == nil {
		return nil, fmt.Errorf("neat inbox store: sqlDB is nil")
	}
	db, err := neat.NewFromSQLDB(sqlDB)
	if err != nil {
		return nil, fmt.Errorf("neat inbox store: %w", err)
	}
	s := &NeatInboxStore{db: db}
	if err := s.migrate(); err != nil {
		return nil, fmt.Errorf("neat inbox store migrate: %w", err)
	}
	return s, nil
}

// migrate creates node_inbox_messages on first use and adds the claim
// columns to tables created before they existed.
func (s *NeatInboxStore) migrate() error {
	schema := s.db.Schema()
	if !schema.HasTable(inboxMessagesTable) {
		err := schema.Create(inboxMessagesTable, func(t neatschema.Blueprint) {
			t.ID("seq")
			t.String("workflow_id")
			t.String("node_id")
			t.Text("payload")
			t.String("status")
			t.String("claimed_by").Nullable()
			// Claim timestamps are TEXT columns storing the
			// 'YYYY-MM-DD HH:MM:SS' layout: string-typed columns pass
			// the value through verbatim (datetime columns would rebind
			// to RFC3339), so the lexical CURRENT_TIMESTAMP comparison
			// holds on every neat dialect — same pattern as leaseRow.
			t.String("claimed_at").Nullable()
			t.String("claim_expires_at").Nullable()
			t.DateTime("enqueued_at").Nullable()
			t.Index("workflow_id")
		})
		if err != nil {
			return err
		}
	}
	// Upgrade path: tables created by an earlier build without the
	// claim columns gain them here (all string-typed; see above).
	for _, col := range []string{"status", "claimed_by", "claimed_at", "claim_expires_at"} {
		if schema.HasColumn(inboxMessagesTable, col) {
			continue
		}
		err := schema.Table(inboxMessagesTable, func(t neatschema.Blueprint) {
			t.String(col).Nullable()
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// inboxTimeFormat matches CURRENT_TIMESTAMP's text layout so claim
// expiry comparisons hold lexically on SQLite and as datetimes
// elsewhere (same pattern as leaseTimeFormat in flowadmin).
const inboxTimeFormat = "2006-01-02 15:04:05"

// claimablePredicate is the WHERE fragment selecting rows a drain may
// pick up: never claimed, claim expired (per the DB clock), or already
// claimed by runnerID (same-runner restart drains its own crash
// leftovers without waiting out the TTL).
const claimablePredicate = "(status = '" + inboxRowPending + "' OR status IS NULL OR claim_expires_at < CURRENT_TIMESTAMP OR claimed_by = ?)"

// loadInbox is the shared query behind LoadInbox / LoadInboxFor.
func (s *NeatInboxStore) loadInbox(workflowID, runnerID string) ([]DurableInboxItem, error) {
	var rows []neatInboxRow
	q := s.db.Query().Model(&neatInboxRow{}).
		Where("workflow_id = ? AND "+claimablePredicate, workflowID, runnerID).
		OrderBy("seq")
	if err := q.Find(&rows); err != nil {
		return nil, fmt.Errorf("failed to load inbox: %w", err)
	}
	items := make([]DurableInboxItem, 0, len(rows))
	for _, row := range rows {
		payload, err := unmarshalInboxPayload(row.Payload)
		if err != nil {
			// A corrupt row must not wedge the queue — drop it, but
			// surface the loss instead of deleting silently.
			log.Printf("warning: dropping corrupt durable inbox row seq=%d for workflow %s: %v", row.Seq, workflowID, err)
			_ = s.DeleteInboxItem(workflowID, row.Seq)
			continue
		}
		items = append(items, DurableInboxItem{
			WorkflowID: row.WorkflowID,
			NodeID:     row.NodeID,
			Seq:        row.Seq,
			Payload:    payload,
			EnqueuedAt: row.EnqueuedAt,
		})
	}
	return items, nil
}

// LoadInbox implements DurableInboxStore: surviving pending rows and
// expired claims for the workflow in seq order. A live claim held by
// another runner is excluded — that runner owns the row.
func (s *NeatInboxStore) LoadInbox(workflowID string) ([]DurableInboxItem, error) {
	return s.loadInbox(workflowID, "")
}

// LoadInboxFor implements workflowClaimLoader: additionally returns
// rows already claimed by runnerID, so a restarting runner requeues its
// own mid-process crash leftovers immediately instead of waiting out
// the claim TTL.
func (s *NeatInboxStore) LoadInboxFor(workflowID, runnerID string) ([]DurableInboxItem, error) {
	return s.loadInbox(workflowID, runnerID)
}

// SaveInboxItem implements DurableInboxStore: serializes the envelope
// and inserts a 'pending' row; item.Seq is assigned from the
// auto-increment id — that IS the seq-at-persist ordering key.
func (s *NeatInboxStore) SaveInboxItem(item *DurableInboxItem) error {
	raw, err := marshalInboxPayload(item.Payload)
	if err != nil {
		return fmt.Errorf("failed to serialize inbox payload: %w", err)
	}
	enqueuedAt := item.EnqueuedAt
	if enqueuedAt.IsZero() {
		enqueuedAt = time.Now()
	}
	id, err := s.db.Query().Table(inboxMessagesTable).InsertGetId(map[string]interface{}{
		"workflow_id": item.WorkflowID,
		"node_id":     item.NodeID,
		"payload":     raw,
		"status":      inboxRowPending,
		"enqueued_at": enqueuedAt,
	})
	if err != nil {
		return fmt.Errorf("failed to persist inbox message: %w", err)
	}
	item.Seq = int64(id)
	return nil
}

// ClaimInboxItem implements InboxClaimer: one atomic conditional UPDATE
// transitions the row to 'claimed' with this runner's id and a fresh
// expiry. RowsAffected==0 means a live claim by another runner (or no
// such row) — the item is skipped, never double-delivered.
func (s *NeatInboxStore) ClaimInboxItem(workflowID string, seq int64, runnerID string, ttl time.Duration) (bool, error) {
	expires := time.Now().UTC().Add(ttl).Format(inboxTimeFormat)
	now := time.Now().UTC().Format(inboxTimeFormat)
	res, err := s.db.Query().Table(inboxMessagesTable).
		Where("seq = ? AND workflow_id = ? AND "+claimablePredicate, seq, workflowID, runnerID).
		Update(map[string]interface{}{
			"status":           inboxRowClaimed,
			"claimed_by":       runnerID,
			"claimed_at":       now,
			"claim_expires_at": expires,
		})
	if err != nil {
		return false, fmt.Errorf("failed to claim inbox message: %w", err)
	}
	return res.RowsAffected > 0, nil
}

// DeleteInboxItem implements DurableInboxStore (the delivery ack).
func (s *NeatInboxStore) DeleteInboxItem(workflowID string, seq int64) error {
	if _, err := s.db.Query().Model(&neatInboxRow{}).
		Where("workflow_id = ?", workflowID).
		Where("seq = ?", seq).
		Delete(); err != nil {
		return fmt.Errorf("failed to delete inbox message: %w", err)
	}
	return nil
}

// DeleteWorkflowInbox implements DurableInboxStore: drops every queued
// row for the workflow.
func (s *NeatInboxStore) DeleteWorkflowInbox(workflowID string) error {
	if _, err := s.db.Query().Model(&neatInboxRow{}).
		Where("workflow_id = ?", workflowID).
		Delete(); err != nil {
		return fmt.Errorf("failed to delete workflow inbox: %w", err)
	}
	return nil
}

// Compile-time checks: the store satisfies the durable contract plus
// the claim and runner-aware load extensions the engine probes for.
var _ DurableInboxStore = (*NeatInboxStore)(nil)
var _ InboxClaimer = (*NeatInboxStore)(nil)
var _ workflowClaimLoader = (*NeatInboxStore)(nil)
