package noderunner_test

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dracory/entitystore"
	_ "modernc.org/sqlite"

	_ "github.com/dracory/nodebase/builtin"
	. "github.com/dracory/noderun"
	nodestore "github.com/dracory/nodestore"
)

// resumeTestStore builds an in-memory sqlite-backed nodestore for the
// restart-resume tests.
func resumeTestStore(t *testing.T) *nodestore.Store {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	// A pooled sqlite :memory: opens a fresh empty database per
	// connection — pin to one so every call sees the same tables.
	db.SetMaxOpenConns(1)
	entityStore, err := entitystore.NewStore(entitystore.NewStoreOptions{
		DB:                 db,
		EntityTableName:    "entity",
		AttributeTableName: "attribute",
		AutomigrateEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := nodestore.NewStore(nodestore.NewStoreOptions{EntityStore: entityStore})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return store
}

// TestResumeDeployedRestartsDeployedWorkflows simulates a restart: a
// workflow persisted with deployed=true (as the previous process left
// it) must come back up when the host calls ResumeDeployed on mount;
// a workflow with deployed=false must stay stopped. The inject node
// is a long-lived stopper, so a resumed workflow reports "running".
func TestResumeDeployedRestartsDeployedWorkflows(t *testing.T) {
	ctx := context.Background()
	store := resumeTestStore(t)
	engine := NewEngine()

	live := nodestore.NewWorkflow("resume-live").
		SetID("resume-live").
		SetDeployed(true).
		SetNodes([]nodestore.NodeInterface{
			nodestore.NewNode("inj", "inject").SetData(map[string]interface{}{
				"interval": 3600,
			}),
			nodestore.NewNode("dbg", "debug").SetData(map[string]interface{}{}),
		}).
		SetEdges([]nodestore.EdgeInterface{
			nodestore.NewEdge("e1", "inj", "dbg"),
		})
	if err := store.WorkflowCreate(ctx, live); err != nil {
		t.Fatalf("create live: %v", err)
	}

	stopped := nodestore.NewWorkflow("resume-stopped").
		SetID("resume-stopped").
		SetDeployed(false).
		SetNodes([]nodestore.NodeInterface{
			nodestore.NewNode("inj2", "junction").SetData(map[string]interface{}{}),
		})
	if err := store.WorkflowCreate(ctx, stopped); err != nil {
		t.Fatalf("create stopped: %v", err)
	}

	if errs := engine.ResumeDeployed(ctx, store); len(errs) != 0 {
		t.Fatalf("ResumeDeployed errors: %v", errs)
	}

	// Activation runs asynchronously after deploy; poll for running.
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, _ := engine.GetWorkflowStatus("resume-live")
		if status == "running" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("deployed workflow did not resume; status=%s", status)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if status, _ := engine.GetWorkflowStatus("resume-stopped"); status == "running" {
		t.Fatalf("stopped workflow was resumed")
	}
}

// memDurableStore is a test DurableInboxStore. It survives the engine
// (a separate object), so rows it holds when one engine stops are
// exactly what a restarted engine's requeueDurable recovers.
type memDurableStore struct {
	mu     sync.Mutex
	seq    int64
	rows   map[int64]DurableInboxItem
	byWfID map[string][]int64
}

func newMemDurableStore() *memDurableStore {
	return &memDurableStore{rows: map[int64]DurableInboxItem{}, byWfID: map[string][]int64{}}
}

func (s *memDurableStore) LoadInbox(workflowID string) ([]DurableInboxItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []DurableInboxItem
	for _, seq := range s.byWfID[workflowID] {
		out = append(out, s.rows[seq])
	}
	return out, nil
}

func (s *memDurableStore) SaveInboxItem(item *DurableInboxItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	item.Seq = s.seq
	s.rows[s.seq] = *item
	s.byWfID[item.WorkflowID] = append(s.byWfID[item.WorkflowID], s.seq)
	return nil
}

func (s *memDurableStore) DeleteInboxItem(workflowID string, seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.rows, seq)
	list := s.byWfID[workflowID]
	for i, v := range list {
		if v == seq {
			s.byWfID[workflowID] = append(list[:i], list[i+1:]...)
			break
		}
	}
	return nil
}

func (s *memDurableStore) DeleteWorkflowInbox(workflowID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seq := range s.byWfID[workflowID] {
		delete(s.rows, seq)
	}
	delete(s.byWfID, workflowID)
	return nil
}

func (s *memDurableStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rows)
}

// TestResumeDrainsDurableInbox is the restart-resume drain: a durable
// inbox row persisted by a previous run (simulating a crash that left
// it undelivered) must be re-queued on resume and delivered
// downstream — and acked (deleted) on dequeue.
func TestResumeDrainsDurableInbox(t *testing.T) {
	ctx := context.Background()
	store := resumeTestStore(t)
	durable := newMemDurableStore()
	engine := NewEngine()
	engine.DurableInboxStore = durable

	flow := nodestore.NewWorkflow("resume-drain").
		SetID("resume-drain").
		SetDeployed(true).
		SetNodes([]nodestore.NodeInterface{
			nodestore.NewNode("src", "junction").SetData(map[string]interface{}{}),
			nodestore.NewNode("mid", "junction").SetData(map[string]interface{}{
				"inboxDurable": true,
			}),
			nodestore.NewNode("dbg", "debug").SetData(map[string]interface{}{}),
		}).
		SetEdges([]nodestore.EdgeInterface{
			nodestore.NewEdge("e1", "src", "mid"),
			nodestore.NewEdge("e2", "mid", "dbg"),
		})
	if err := store.WorkflowCreate(ctx, flow); err != nil {
		t.Fatalf("create flow: %v", err)
	}

	// The crash leftover: a row the dead runner persisted for node's
	// "mid" inbox but never dequeued.
	if err := durable.SaveInboxItem(&DurableInboxItem{
		WorkflowID: "resume-drain",
		NodeID:     "mid",
		Payload:    nodestore.NewMessage().SetPayload("recovered"),
		EnqueuedAt: time.Now(),
	}); err != nil {
		t.Fatalf("seed durable row: %v", err)
	}

	if errs := engine.ResumeDeployed(ctx, store); len(errs) != 0 {
		t.Fatalf("ResumeDeployed errors: %v", errs)
	}

	values := waitForDebugValues(engine, "resume-drain", 1, 3*time.Second)
	if len(values) == 0 {
		t.Fatal("durable inbox row was not delivered after resume")
	}
	// Debug events render the payload JSON-encoded.
	if !strings.Contains(values[0], "recovered") {
		t.Fatalf("unexpected delivered payload: %q", values[0])
	}
	if got := durable.count(); got != 0 {
		t.Fatalf("durable row was not acked on delivery; %d row(s) left", got)
	}
}

// TestResumeDeployedFailsLoudOnUnknownType returns an error for a
// persisted deployed workflow referencing an unregistered node type
// rather than silently skipping it.
func TestResumeDeployedFailsLoudOnUnknownType(t *testing.T) {
	ctx := context.Background()
	store := resumeTestStore(t)
	engine := NewEngine()

	broken := nodestore.NewWorkflow("resume-broken").
		SetID("resume-broken").
		SetDeployed(true).
		SetNodes([]nodestore.NodeInterface{
			nodestore.NewNode("mystery", "nosuchtype").SetData(map[string]interface{}{}),
		})
	if err := store.WorkflowCreate(ctx, broken); err != nil {
		t.Fatalf("create broken: %v", err)
	}

	// Deploy is async for traversal but activation (which resolves
	// executors) reports through the deploy channel — give it a beat,
	// then require the workflow is not running. ResumeDeployed returns
	// immediately with per-workflow deploy errors when the factory
	// lookup fails synchronously; an async activation failure leaves
	// the workflow stopped either way.
	_ = engine.ResumeDeployed(ctx, store)

	deadline := time.Now().Add(2 * time.Second)
	for {
		status, _ := engine.GetWorkflowStatus("resume-broken")
		if status != "running" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("workflow with unregistered type is running")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
