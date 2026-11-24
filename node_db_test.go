package noderunner_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	. "github.com/dracory/noderun"
	"testing"
	"time"
)

// dbTestFlow builds inj -> db -> dbg. The db node carries the given
// Data config; dbg is a debug node whose event log records deliveries.
func dbTestFlow(id string, dbData map[string]interface{}) *Workflow {
	return &Workflow{
		ID:   id,
		Name: id,
		Nodes: []Node{
			{ID: "inj", Type: "junction", Data: map[string]interface{}{}},
			{ID: "db", Type: "db", Data: dbData},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "db"},
			{ID: "e2", Source: "db", Target: "dbg"},
		},
	}
}

func deployDBFlow(t *testing.T, workflow *Workflow) *Engine {
	t.Helper()
	engine := NewEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.ID) })
	return engine
}

// TestDBExecAndQuery covers the sqlite happy path: one flow where an
// exec node creates and populates a table feeding a query node whose
// payload is the row array.
func TestDBExecAndQuery(t *testing.T) {
	dsn := "sqlite://" + filepath.Join(t.TempDir(), "test.db")
	workflow := &Workflow{
		ID:   "wf-db",
		Name: "wf-db",
		Nodes: []Node{
			{ID: "inj", Type: "junction", Data: map[string]interface{}{}},
			{ID: "w", Type: "db", Data: map[string]interface{}{
				"dsn":       dsn,
				"operation": "exec",
				"sql":       "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT); INSERT INTO t (name) VALUES ('a'), ('b');",
			}},
			{ID: "r", Type: "db", Data: map[string]interface{}{
				"dsn":       dsn,
				"operation": "query",
				"sql":       "SELECT id, name FROM t ORDER BY id",
			}},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "w"},
			{ID: "e2", Source: "w", Target: "r"},
			{ID: "e3", Source: "r", Target: "dbg"},
		},
	}
	engine := deployDBFlow(t, workflow)

	injectInto(t, engine, workflow.ID, "go")

	got := waitForDebugValues(engine, workflow.ID, 1, 3*time.Second)
	if len(got) != 1 {
		_, statuses, events, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		t.Fatalf("expected query rows at debug, got %v; statuses: %v; events: %+v", got, statuses, events)
	}
	var rows []map[string]interface{}
	if err := json.Unmarshal([]byte(got[0]), &rows); err != nil {
		t.Fatalf("query payload not a row array: %v (%s)", err, got[0])
	}
	if len(rows) != 2 || rows[0]["name"] != "a" || rows[1]["name"] != "b" {
		t.Fatalf("unexpected rows: %v", rows)
	}
}

// TestDBExecResultShape: exec emits payload {affected, lastId}.
func TestDBExecResultShape(t *testing.T) {
	dsn := "sqlite://" + filepath.Join(t.TempDir(), "exec.db")
	workflow := dbTestFlow("wf-db-exec", map[string]interface{}{
		"dsn":       dsn,
		"operation": "exec",
		"sql":       "CREATE TABLE t (n INTEGER); INSERT INTO t VALUES (1);",
	})
	engine := deployDBFlow(t, workflow)

	injectInto(t, engine, workflow.ID, "go")

	got := waitForDebugValues(engine, workflow.ID, 1, 3*time.Second)
	if len(got) != 1 {
		t.Fatalf("expected exec result at debug, got %v", got)
	}
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(got[0]), &result); err != nil {
		t.Fatalf("exec payload not JSON: %v (%s)", err, got[0])
	}
	if result["affected"] == nil {
		t.Fatalf("exec payload missing affected: %v", result)
	}
}

// TestDBSplitEmitsPerRow: split mode turns the row array into one
// message per row.
func TestDBSplitEmitsPerRow(t *testing.T) {
	dsn := "sqlite://" + filepath.Join(t.TempDir(), "split.db")
	workflow := &Workflow{
		ID:   "wf-db-split",
		Name: "wf-db-split",
		Nodes: []Node{
			{ID: "inj", Type: "junction", Data: map[string]interface{}{}},
			{ID: "w", Type: "db", Data: map[string]interface{}{
				"dsn":       dsn,
				"operation": "exec",
				"sql":       "CREATE TABLE t (n INTEGER); INSERT INTO t VALUES (1),(2),(3);",
			}},
			{ID: "r", Type: "db", Data: map[string]interface{}{
				"dsn":       dsn,
				"operation": "query",
				"sql":       "SELECT n FROM t ORDER BY n",
				"split":     true,
			}},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "w"},
			{ID: "e2", Source: "w", Target: "r"},
			{ID: "e3", Source: "r", Target: "dbg"},
		},
	}
	engine := deployDBFlow(t, workflow)

	injectInto(t, engine, workflow.ID, "go")

	got := waitForDebugValues(engine, workflow.ID, 3, 3*time.Second)
	if len(got) != 3 {
		t.Fatalf("expected 3 row messages, got %v", got)
	}
	for i, want := range []string{"1", "2", "3"} {
		var row map[string]interface{}
		if err := json.Unmarshal([]byte(got[i]), &row); err != nil {
			t.Fatalf("row %d not a JSON object: %v (%s)", i, err, got[i+1])
		}
		if got := fmt.Sprint(row["n"]); got != want {
			t.Fatalf("row %d n=%s want %s", i, got, want)
		}
	}
}

// TestDBParamsBoundNotInterpolated: an injection-shaped param binds as
// a literal value — it lands in the row, not in the SQL.
func TestDBParamsBoundNotInterpolated(t *testing.T) {
	dsn := "sqlite://" + filepath.Join(t.TempDir(), "params.db")
	evil := "x'); DROP TABLE t; --"
	workflow := &Workflow{
		ID:   "wf-db-params",
		Name: "wf-db-params",
		Nodes: []Node{
			{ID: "inj", Type: "junction", Data: map[string]interface{}{}},
			{ID: "w", Type: "db", Data: map[string]interface{}{
				"dsn":       dsn,
				"operation": "exec",
				"sql":       "CREATE TABLE t (name TEXT); INSERT INTO t (name) VALUES (?);",
				"params":    "params",
			}},
			{ID: "r", Type: "db", Data: map[string]interface{}{
				"dsn":       dsn,
				"operation": "query",
				"sql":       "SELECT name FROM t",
			}},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "w"},
			{ID: "e2", Source: "w", Target: "r"},
			{ID: "e3", Source: "r", Target: "dbg"},
		},
	}
	engine := deployDBFlow(t, workflow)

	msgID, _, err := engine.InjectMessage(workflow.ID, "inj", &Message{
		Payload: map[string]interface{}{"params": []interface{}{evil}},
	})
	if err != nil {
		t.Fatalf("inject failed: %v", err)
	}
	engine.DiscardPending(msgID)

	got := waitForDebugValues(engine, workflow.ID, 1, 3*time.Second)
	if len(got) != 1 {
		t.Fatalf("expected query rows at debug, got %v", got)
	}
	var rows []map[string]interface{}
	if err := json.Unmarshal([]byte(got[0]), &rows); err != nil {
		t.Fatalf("query payload not a row array: %v (%s)", err, got[0])
	}
	// The injection-shaped input is stored verbatim — proof it bound
	// as a value. The table still exists (the SELECT itself succeeded).
	if len(rows) != 1 || rows[0]["name"] != evil {
		t.Fatalf("injection-shaped input was not stored literally: %v", rows)
	}
}

// TestDBBadDSNFailsDeploy: a DSN neat cannot parse/driver-resolve
// surfaces as a deploy error via the standard activation path.
func TestDBBadDSNFailsDeploy(t *testing.T) {
	workflow := dbTestFlow("wf-db-bad", map[string]interface{}{
		"dsn":       "bogus://nothing",
		"operation": "query",
		"sql":       "SELECT 1",
	})
	engine := NewEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err == nil {
		_ = engine.StopWorkflow(workflow.ID)
		t.Fatal("expected deploy to fail for a bogus DSN")
	}

	// Unresolved env reference fails deploy too.
	workflow2 := dbTestFlow("wf-db-env", map[string]interface{}{
		"dsn": "env:GOFLOW_TEST_UNSET_DB_DSN",
		"sql": "SELECT 1",
	})
	engine2 := NewEngine()
	if _, err := engine2.DeployWorkflow(context.Background(), workflow2, nil); err == nil {
		_ = engine2.StopWorkflow(workflow2.ID)
		t.Fatal("expected deploy to fail for an unset env DSN")
	}
}

// TestDBQueryErrorStandardPath: a statement failure at message time
// produces a normal node error (status + catch routing).
func TestDBQueryErrorStandardPath(t *testing.T) {
	dsn := "sqlite://" + filepath.Join(t.TempDir(), "err.db")
	workflow := &Workflow{
		ID:   "wf-db-err",
		Name: "wf-db-err",
		Nodes: []Node{
			{ID: "inj", Type: "junction", Data: map[string]interface{}{}},
			{ID: "r", Type: "db", Data: map[string]interface{}{
				"dsn":       dsn,
				"operation": "query",
				"sql":       "SELECT * FROM missing_table",
			}},
			{ID: "catch", Type: "catch", Data: map[string]interface{}{}},
			{ID: "dbg", Type: "debug", Data: map[string]interface{}{}},
		},
		Edges: []Edge{
			{ID: "e1", Source: "inj", Target: "r"},
			{ID: "e2", Source: "catch", Target: "dbg"},
		},
	}
	engine := deployDBFlow(t, workflow)

	injectInto(t, engine, workflow.ID, "go")

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, statuses, _, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
		if statuses["r"] == "error" {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	_, statuses, _, _ := engine.GetWorkflowSnapshot(workflow.ID, 0)
	if statuses["r"] != "error" {
		t.Fatalf("expected db node status error, got %v", statuses)
	}
	got := waitForDebugValues(engine, workflow.ID, 1, 3*time.Second)
	if len(got) == 0 {
		t.Fatal("catch node never received the db error")
	}
}

// TestDBReadOnlyRejectsExec: readOnly + exec fails validation at deploy.
func TestDBReadOnlyRejectsExec(t *testing.T) {
	workflow := dbTestFlow("wf-db-ro", map[string]interface{}{
		"dsn":       "sqlite://:memory:",
		"operation": "exec",
		"sql":       "CREATE TABLE t (x)",
		"readOnly":  true,
	})
	engine := NewEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err == nil {
		_ = engine.StopWorkflow(workflow.ID)
		t.Fatal("expected deploy to fail for readOnly exec")
	}
}
