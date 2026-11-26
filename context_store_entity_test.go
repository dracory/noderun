package noderunner

import (
	"context"
	"database/sql"
	"reflect"
	"sync"
	"testing"

	"github.com/dracory/entitystore"
	"github.com/dracory/nodebase"
	"github.com/dracory/nodestore"
	_ "modernc.org/sqlite"
)

type fakeContextValueBackend struct {
	mu     sync.Mutex
	values map[string]string
	owners map[string]string
}

func newFakeContextValueBackend() *fakeContextValueBackend {
	return &fakeContextValueBackend{
		values: map[string]string{},
		owners: map[string]string{},
	}
}

func contextValueFakeKey(scope, scopeID, key string) string {
	return scope + "\x00" + scopeID + "\x00" + key
}

func (f *fakeContextValueBackend) ContextValueGet(_ context.Context, scope, scopeID, key string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	value, ok := f.values[contextValueFakeKey(scope, scopeID, key)]
	return value, ok, nil
}

func (f *fakeContextValueBackend) ContextValueSet(_ context.Context, workflowID, scope, scopeID, key, value string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := contextValueFakeKey(scope, scopeID, key)
	f.values[k] = value
	f.owners[k] = workflowID
	return nil
}

func (f *fakeContextValueBackend) ContextValueDelete(_ context.Context, scope, scopeID, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := contextValueFakeKey(scope, scopeID, key)
	delete(f.values, k)
	delete(f.owners, k)
	return nil
}

func (f *fakeContextValueBackend) ContextValueDeleteScope(_ context.Context, scope, scopeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := scope + "\x00" + scopeID + "\x00"
	for k := range f.values {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			delete(f.values, k)
			delete(f.owners, k)
		}
	}
	return nil
}

func (f *fakeContextValueBackend) ContextValueKeys(_ context.Context, scope, scopeID string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := scope + "\x00" + scopeID + "\x00"
	keys := []string{}
	for k := range f.values {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k[len(prefix):])
		}
	}
	return keys, nil
}

func TestEntityContextStoreJSONNormalizationAndKeys(t *testing.T) {
	backend := newFakeContextValueBackend()
	store := newEntityContextStore(backend)

	input := map[string]interface{}{
		"n":    2,
		"text": "hello",
		"list": []interface{}{1, "x", true},
	}
	if err := store.SetContext(nodebase.ContextScopeFlow, "wf-a", "obj", input); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, ok, err := store.GetContext(nodebase.ContextScopeFlow, "wf-a", "obj")
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	want := map[string]interface{}{
		"n":    float64(2),
		"text": "hello",
		"list": []interface{}{float64(1), "x", true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("JSON normalization mismatch:\n got %#v\nwant %#v", got, want)
	}

	if err := store.SetContext(nodebase.ContextScopeFlow, "wf-a", "z", nil); err != nil {
		t.Fatalf("set nil: %v", err)
	}
	got, ok, err = store.GetContext(nodebase.ContextScopeFlow, "wf-a", "z")
	if err != nil || !ok || got != nil {
		t.Fatalf("nil round trip: got=%#v ok=%v err=%v", got, ok, err)
	}

	if err := store.SetContext(nodebase.ContextScopeFlow, "wf-a", "a", "x"); err != nil {
		t.Fatalf("set a: %v", err)
	}
	keys, err := store.ContextKeys(nodebase.ContextScopeFlow, "wf-a")
	if err != nil {
		t.Fatalf("keys: %v", err)
	}
	if !reflect.DeepEqual(keys, []string{"a", "obj", "z"}) {
		t.Fatalf("keys not sorted: %v", keys)
	}

	if err := store.DeleteContext(nodebase.ContextScopeFlow, "wf-a", "obj"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok, err = store.GetContext(nodebase.ContextScopeFlow, "wf-a", "obj"); err != nil || ok {
		t.Fatalf("deleted key readable: ok=%v err=%v", ok, err)
	}

	if err := store.SetContext(nodebase.ContextScopeFlow, "wf-a", "bad", func() {}); err == nil {
		t.Fatal("expected JSON encoding error for function value")
	}
}

func TestEntityContextStoreWorkflowOwnershipMapping(t *testing.T) {
	backend := newFakeContextValueBackend()
	store := newEntityContextStore(backend)

	if err := store.SetContext(nodebase.ContextScopeFlow, "wf-a", "f", 1); err != nil {
		t.Fatal(err)
	}
	if err := store.SetContext(nodebase.ContextScopeNode, "wf-a/n1", "n", 1); err != nil {
		t.Fatal(err)
	}
	if err := store.SetContext(nodebase.ContextScopeGlobal, "", "g", 1); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		scope   string
		scopeID string
		key     string
		owner   string
	}{
		{nodebase.ContextScopeFlow, "wf-a", "f", "wf-a"},
		{nodebase.ContextScopeNode, "wf-a/n1", "n", "wf-a"},
		{nodebase.ContextScopeGlobal, "", "g", ""},
	}
	for _, tc := range cases {
		k := contextValueFakeKey(tc.scope, tc.scopeID, tc.key)
		if backend.owners[k] != tc.owner {
			t.Fatalf("%s/%s owner: got %q want %q", tc.scope, tc.scopeID, backend.owners[k], tc.owner)
		}
	}
}

func entityContextTestNodeStore(t *testing.T) (*nodestore.Store, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
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
	return store, db
}

func TestEntityContextStoreNodestoreRoundTrip(t *testing.T) {
	nodeStore, db := entityContextTestNodeStore(t)
	defer func() { _ = db.Close() }()

	store, err := NewEntityContextStore(nodeStore)
	if err != nil {
		t.Skipf("nodestore dependency does not expose context-value storage yet: %v", err)
	}
	if err := store.SetContext(nodebase.ContextScopeNode, "wf-a/n1", "answer", 41); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, ok, err := store.GetContext(nodebase.ContextScopeNode, "wf-a/n1", "answer")
	if err != nil || !ok || got != float64(41) {
		t.Fatalf("get: got=%#v ok=%v err=%v", got, ok, err)
	}
}
