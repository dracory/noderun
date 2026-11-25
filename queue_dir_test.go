package noderunner_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	nodestore "github.com/dracory/nodestore"
	. "github.com/dracory/noderun"
	"github.com/dracory/nodebase"
	_ "github.com/dracory/nodebase/builtin"
	"regexp"
	"strings"
	"testing"
	"time"
)

// queueDirData builds node.Data for a dir-backed queue node.
func queueDirData(dir string, extra map[string]interface{}) map[string]interface{} {
	data := map[string]interface{}{"backend": "dir", "dir": dir}
	for k, v := range extra {
		data[k] = v
	}
	return data
}

// dirItemFiles returns the .json item files in dir (never .tmp).
func dirItemFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir failed: %v", err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			out = append(out, e.Name())
		}
	}
	return out
}

// TestQueueDirBackendMaterializes covers the core acceptance item: a
// dir-backed held queue writes one {seq}-{msgid}.json file per item.
func TestQueueDirBackendMaterializes(t *testing.T) {
	dir := t.TempDir()
	workflow := queueTestFlow("wf-qdir", queueDirData(dir, map[string]interface{}{"hold": true}))

	engine := NewEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.GetID()) })

	injectSequentially(t, engine, workflow.GetID(), 0, "a", "b")

	q := waitForQueueCount(t, engine, workflow.GetID(), 2, 3*time.Second)
	if q == nil || q.Count() != 2 {
		t.Fatalf("expected 2 queued items, got %v", q)
	}

	files := dirItemFiles(t, dir)
	if len(files) != 2 {
		t.Fatalf("expected 2 item files, got %v", files)
	}
	nameRe := regexp.MustCompile(`^\d+-[^/\\]+\.json$`)
	seen := map[int64]bool{}
	for _, name := range files {
		if !nameRe.MatchString(name) {
			t.Fatalf("item file %q does not match {seq}-{msgid}.json", name)
		}
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read %q failed: %v", name, err)
		}
		var body map[string]interface{}
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatalf("item file %q is not valid JSON: %v", name, err)
		}
		// seq is authoritative in the body.
		seq, ok := body["seq"].(float64)
		if !ok || seq < 1 {
			t.Fatalf("item file %q missing body seq: %v", name, body)
		}
		seen[int64(seq)] = true
		if body["nodeId"] != "f" {
			t.Fatalf("item file %q has wrong nodeId: %v", name, body["nodeId"])
		}
	}
	// No .tmp fragments may remain after commits.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("leftover tmp file: %s", e.Name())
		}
	}
	if len(seen) != 2 {
		t.Fatalf("expected 2 distinct seqs, got %v", seen)
	}
}

// TestQueueDirExternalDeleteReconciles: deleting an item file behind
// the engine's back removes the item on the next arm — no index is
// kept, the files are advisory.
func TestQueueDirExternalDeleteReconciles(t *testing.T) {
	dir := t.TempDir()
	workflow := queueTestFlow("wf-qdir-del", queueDirData(dir, map[string]interface{}{"hold": true}))

	engine := NewEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	injectSequentially(t, engine, workflow.GetID(), 0, "keep", "gone")
	q := waitForQueueCount(t, engine, workflow.GetID(), 2, 3*time.Second)
	if q == nil || q.Count() != 2 {
		t.Fatalf("expected 2 queued items, got %v", q)
	}
	if err := engine.StopWorkflow(workflow.GetID()); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	// Delete the file holding "gone".
	for _, name := range dirItemFiles(t, dir) {
		raw, _ := os.ReadFile(filepath.Join(dir, name))
		if strings.Contains(string(raw), `"gone"`) {
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				t.Fatalf("external delete failed: %v", err)
			}
		}
	}

	// A fresh engine re-arms: only the surviving file loads.
	engine2 := NewEngine()
	if _, err := engine2.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("redeploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine2.StopWorkflow(workflow.GetID()) })

	q2 := waitForQueueCount(t, engine2, workflow.GetID(), 1, 3*time.Second)
	if q2 == nil || q2.Count() != 1 {
		t.Fatalf("expected 1 recovered item after external delete, got %v", q2)
	}
	items := q2.Items()
	if len(items) != 1 || fmt.Sprint(items[0].Payload.GetPayload()) != "keep" {
		t.Fatalf("wrong item recovered: %v", items)
	}
}

// TestQueueDirEditedPayloadDelivers covers fix-and-reinject: the dir
// store re-reads the item file at release time, so edits between stage
// and drain change the delivered payload.
func TestQueueDirEditedPayloadDelivers(t *testing.T) {
	dir := t.TempDir()
	workflow := queueTestFlow("wf-qdir-edit", queueDirData(dir, map[string]interface{}{"hold": true}))

	engine := NewEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.GetID()) })

	injectSequentially(t, engine, workflow.GetID(), 0, "orig")
	q := waitForQueueCount(t, engine, workflow.GetID(), 1, 3*time.Second)
	if q == nil || q.Count() != 1 {
		t.Fatalf("expected 1 queued item, got %v", q)
	}

	files := dirItemFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("expected 1 item file, got %v", files)
	}
	path := filepath.Join(dir, files[0])
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read item file failed: %v", err)
	}
	edited := strings.Replace(string(raw), `"orig"`, `"fixed"`, 1)
	if edited == string(raw) {
		t.Fatalf("item file does not contain payload: %s", raw)
	}
	if err := os.WriteFile(path, []byte(edited), 0644); err != nil {
		t.Fatalf("edit item file failed: %v", err)
	}

	q.SetHold(false)
	got := waitForDebugValues(engine, workflow.GetID(), 1, 3*time.Second)
	if len(got) != 1 || got[0] != `"fixed"` {
		t.Fatalf("expected edited payload to be delivered, got %v", got)
	}
	if files := dirItemFiles(t, dir); len(files) != 0 {
		t.Fatalf("drained item file must be acked, left %v", files)
	}
}

// TestQueueDirInspectOps: the inspection API (hold, delete, requeue)
// works identically on the dir backend.
func TestQueueDirInspectOps(t *testing.T) {
	dir := t.TempDir()
	workflow := queueTestFlow("wf-qdir-ops", queueDirData(dir, map[string]interface{}{"hold": true}))

	engine := NewEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.GetID()) })

	injectSequentially(t, engine, workflow.GetID(), 0, "a", "b")
	q := waitForQueueCount(t, engine, workflow.GetID(), 2, 3*time.Second)
	if q == nil || q.Count() != 2 || !q.Held() {
		t.Fatalf("expected 2 held items, got %v", q)
	}
	items := q.Items()
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}

	if err := q.DeleteItem(items[0].Seq); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if q.Count() != 1 || len(dirItemFiles(t, dir)) != 1 {
		t.Fatalf("delete must drop one file: count=%d files=%v", q.Count(), dirItemFiles(t, dir))
	}

	if err := q.RequeueItem(items[1].Seq); err != nil {
		t.Fatalf("requeue failed: %v", err)
	}
	got := waitForDebugValues(engine, workflow.GetID(), 1, 3*time.Second)
	if len(got) != 1 || got[0] != `"b"` {
		t.Fatalf("expected requeued b, got %v", got)
	}
	if q.Count() != 0 || len(dirItemFiles(t, dir)) != 0 {
		t.Fatalf("requeue must drain the file: count=%d files=%v", q.Count(), dirItemFiles(t, dir))
	}
}

// TestQueueDirCorruptAndTmpSkipped: corrupt .json files warn + stay on
// disk, and .tmp fragments from a crashed write never load — no
// partial .json is ever readable as an item.
func TestQueueDirCorruptAndTmpSkipped(t *testing.T) {
	dir := t.TempDir()

	store, err := NewDirQueueStore(dir)
	if err != nil {
		t.Fatalf("store create failed: %v", err)
	}
	good := &DurableInboxItem{WorkflowID: "wf", NodeID: "f", Payload: nodestore.NewMessage().SetPayload("ok")}
	if err := store.SaveInboxItem(good); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// Simulate a crashed write (tmp fragment) and a corrupt item file.
	if err := os.WriteFile(filepath.Join(dir, "99-bad.json.tmp"), []byte(`{"seq":99,"payl`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "50-corrupt.json"), []byte(`{not json`), 0644); err != nil {
		t.Fatal(err)
	}

	items, err := store.LoadInbox("wf")
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if len(items) != 1 || fmt.Sprint(items[0].Payload.GetPayload()) != "ok" {
		t.Fatalf("corrupt/tmp files must be skipped, got %v", items)
	}
	// Corrupt file stays on disk for the operator.
	if _, err := os.Stat(filepath.Join(dir, "50-corrupt.json")); err != nil {
		t.Fatal("corrupt file must stay on disk")
	}

	// Deleting the real item is by body seq, not filename: renaming the
	// file must not confuse the store.
	files := dirItemFiles(t, dir)
	if len(files) != 2 { // good item + corrupt file
		t.Fatalf("expected item + corrupt file, got %v", files)
	}
	var goodName string
	for _, n := range files {
		if n != "50-corrupt.json" {
			goodName = n
		}
	}
	if err := os.Rename(filepath.Join(dir, goodName), filepath.Join(dir, "renamed.json")); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteInboxItem("wf", good.Seq); err != nil {
		t.Fatalf("delete by body seq failed: %v", err)
	}
	items, err = store.LoadInbox("wf")
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("renamed file must still resolve by body seq, got %v", items)
	}
}

// TestQueueDirRequiresDir is the deploy-time validation: backend=dir
// without a path fails fast instead of at first enqueue.
func TestQueueDirRequiresDir(t *testing.T) {
	workflow := queueTestFlow("wf-qdir-nodir", map[string]interface{}{"backend": "dir"})
	engine := NewEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err == nil {
		t.Fatal("backend=dir without dir must fail deploy")
	}
}

// TestQueueDirUnknownBackend rejects backends that do not exist yet.
func TestQueueDirUnknownBackend(t *testing.T) {
	workflow := queueTestFlow("wf-qdir-bad", map[string]interface{}{"backend": "s3"})
	engine := NewEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err == nil {
		t.Fatal("unknown backend must fail deploy")
	}
}

// TestQueueDirSharedDirStoreIsolation covers the shared-dir hazard at
// store level: two scoped stores on one directory assign colliding
// seqs, yet neither can load, re-read or delete the other's files
// (task queue_dir_shared).
func TestQueueDirSharedDirStoreIsolation(t *testing.T) {
	dir := t.TempDir()

	qa, err := NewScopedDirQueueStore(dir, "qa")
	if err != nil {
		t.Fatalf("store A failed: %v", err)
	}
	qb, err := NewScopedDirQueueStore(dir, "qb")
	if err != nil {
		t.Fatalf("store B failed: %v", err)
	}

	itemA := &DurableInboxItem{WorkflowID: "wf", NodeID: "qa", Payload: nodestore.NewMessage().SetPayload("for-a")}
	itemB := &DurableInboxItem{WorkflowID: "wf", NodeID: "qb", Payload: nodestore.NewMessage().SetPayload("for-b")}
	if err := qa.SaveInboxItem(itemA); err != nil {
		t.Fatalf("save A failed: %v", err)
	}
	if err := qb.SaveInboxItem(itemB); err != nil {
		t.Fatalf("save B failed: %v", err)
	}
	// Seqs collide — each store only counts its own files.
	if itemA.Seq != itemB.Seq {
		t.Fatalf("expected colliding seqs (each store scans only its own files), got %d and %d", itemA.Seq, itemB.Seq)
	}
	if len(dirItemFiles(t, dir)) != 2 {
		t.Fatalf("expected 2 item files, got %v", dirItemFiles(t, dir))
	}

	// B sees only its own file; A's file is invisible to it.
	itemsB, err := qb.LoadInbox("wf")
	if err != nil {
		t.Fatalf("load B failed: %v", err)
	}
	if len(itemsB) != 1 || fmt.Sprint(itemsB[0].Payload.GetPayload()) != "for-b" {
		t.Fatalf("B must see only its own item, got %v", itemsB)
	}

	// The seqs collide, so B's re-read of "seq 1" must resolve to B's
	// own file — never to A's file with the same seq.
	gotB, err := qb.LoadInboxItem("wf", itemB.Seq)
	if err != nil || fmt.Sprint(gotB.Payload.GetPayload()) != "for-b" {
		t.Fatalf("B must re-read its own file, got %v err %v", gotB, err)
	}
	if gotB.NodeID != "qb" {
		t.Fatalf("reloaded item must belong to qb, got %v", gotB)
	}

	// A deleting by the colliding seq removes only its own file.
	if err := qa.DeleteInboxItem("wf", itemA.Seq); err != nil {
		t.Fatalf("delete A failed: %v", err)
	}
	files := dirItemFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("A's delete must leave B's file, got %v", files)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, files[0]))
	if !strings.Contains(string(raw), `"for-b"`) {
		t.Fatalf("surviving file must be B's: %s", raw)
	}
}

// TestQueueDirSharedDirTwoQueues is the engine-level half: two queue
// nodes in one workflow point at the same dir; each stages, recovers,
// releases and acks only its own files.
func TestQueueDirSharedDirTwoQueues(t *testing.T) {
	dir := t.TempDir()
	workflow := nodestore.NewWorkflow("wf-qdir-shared").SetID("wf-qdir-shared").SetNodes([]nodestore.NodeInterface{
		nodestore.NewNode("inj", "junction").SetData(map[string]interface{}{}),
		nodestore.NewNode("q1", "queue").SetData(queueDirData(dir, map[string]interface{}{"hold": true, "name": "one"})),
		nodestore.NewNode("q2", "queue").SetData(queueDirData(dir, map[string]interface{}{"hold": true, "name": "two"})),
		nodestore.NewNode("dbg", "debug").SetData(map[string]interface{}{}),
	}).SetEdges([]nodestore.EdgeInterface{
		nodestore.NewEdge("e1", "inj", "q1"),
		nodestore.NewEdge("e2", "inj", "q2"),
		nodestore.NewEdge("e3", "q1", "dbg"),
		nodestore.NewEdge("e4", "q2", "dbg"),
	})

	waitNode := func(engine *Engine, nodeID string, n int) nodebase.QueueInspector {
		deadline := time.Now().Add(3 * time.Second)
		var q nodebase.QueueInspector
		for time.Now().Before(deadline) {
			if h, ok := engine.QueueHandle(workflow.GetID(), nodeID); ok {
				q = h
				if q.Count() == n {
					return q
				}
			}
			time.Sleep(15 * time.Millisecond)
		}
		return q
	}

	engine := NewEngine()
	if _, err := engine.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine.StopWorkflow(workflow.GetID()) })

	injectInto(t, engine, workflow.GetID(), "job")
	q1 := waitNode(engine, "q1", 1)
	q2 := waitNode(engine, "q2", 1)
	if q1 == nil || q2 == nil || q1.Count() != 1 || q2.Count() != 1 {
		t.Fatalf("both queues must stage one item, got %v / %v", q1, q2)
	}
	if len(dirItemFiles(t, dir)) != 2 {
		t.Fatalf("expected 2 item files, got %v", dirItemFiles(t, dir))
	}

	// Releasing q1 must not touch q2's file — before the fix both
	// items shared seq 1 and q1's ack could delete q2's file.
	if err := q1.RequeueItem(q1.Items()[0].Seq); err != nil {
		t.Fatalf("release q1 failed: %v", err)
	}
	if files := dirItemFiles(t, dir); len(files) != 1 {
		t.Fatalf("q1 ack must leave q2's file, got %v", files)
	}
	if q2.Count() != 1 {
		t.Fatalf("q2 must still hold its item, got %d", q2.Count())
	}

	// A fresh engine re-arms: each queue recovers only its own file.
	if err := engine.StopWorkflow(workflow.GetID()); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
	engine2 := NewEngine()
	if _, err := engine2.DeployWorkflow(context.Background(), workflow, nil); err != nil {
		t.Fatalf("redeploy failed: %v", err)
	}
	t.Cleanup(func() { _ = engine2.StopWorkflow(workflow.GetID()) })
	if q := waitNode(engine2, "q2", 1); q == nil || q.Count() != 1 {
		t.Fatalf("q2 must recover its own file only, got %v", q)
	}
	if q := waitNode(engine2, "q1", 1); q != nil && q.Count() != 0 {
		t.Fatalf("q1 must not resurrect its acked item, got %v", q.Items())
	}
}
