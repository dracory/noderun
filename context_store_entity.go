package noderunner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/dracory/nodebase"
	"github.com/dracory/nodestore"
)

// contextValueBackend is the narrow serialized-storage surface the
// persistent context adapter needs. nodestore.Store implements it
// structurally; keeping the interface local means noderun depends on
// the storage capability, not on a concrete storage revision.
type contextValueBackend interface {
	ContextValueGet(ctx context.Context, scope, scopeID, key string) (value string, ok bool, err error)
	ContextValueSet(ctx context.Context, workflowID, scope, scopeID, key, value string) error
	ContextValueDelete(ctx context.Context, scope, scopeID, key string) error
	ContextValueDeleteScope(ctx context.Context, scope, scopeID string) error
	ContextValueKeys(ctx context.Context, scope, scopeID string) ([]string, error)
}

// EntityContextStore adapts nodestore's serialized context-value
// entities to nodebase.ContextStore. The adapter owns the runtime
// contract: Node-RED scope-to-workflow ownership mapping and JSON
// normalization of arbitrary script values. Storage owns durability and
// workflow cascade deletion.
//
// The nodebase.ContextStore contract has no context.Context parameter,
// so storage calls use context.Background(). Cancellation remains the
// caller's database/driver concern, same as other engine-host storage
// seams.
type EntityContextStore struct {
	backend contextValueBackend
}

// NewEntityContextStore creates a persistent context store when the
// supplied nodestore implements the context-value storage capability.
// Older or custom stores do not implement it structurally; in that case
// the caller keeps the engine's explicit in-memory fallback.
func NewEntityContextStore(store nodestore.StoreInterface) (*EntityContextStore, error) {
	if store == nil {
		return nil, errors.New("entity context store: nodestore is nil")
	}
	backend, ok := store.(contextValueBackend)
	if !ok {
		return nil, errors.New("entity context store: nodestore does not implement context-value storage")
	}
	return newEntityContextStore(backend), nil
}

func newEntityContextStore(backend contextValueBackend) *EntityContextStore {
	return &EntityContextStore{backend: backend}
}

// ConfigureEntityContextStore resolves store as a persistent context
// backend and installs it on the engine. It is a method so composition
// roots can discover the capability structurally: a runner version
// without persistent-context support simply does not satisfy the
// optional interface, and the caller keeps the in-memory fallback.
func (e *WorkflowEngine) ConfigureEntityContextStore(store nodestore.StoreInterface) error {
	contextStore, err := NewEntityContextStore(store)
	if err != nil {
		return err
	}
	e.ContextStore = contextStore
	return nil
}

// contextWorkflowID maps Node-RED scope IDs to their owning workflow:
// flow scope is the workflow ID itself, node scope is
// "<workflowID>/<nodeID>", and global scope is unowned (empty). The
// storage layer treats this as ownership metadata only; the scope tuple
// remains the addressing key.
func contextWorkflowID(scope, scopeID string) string {
	switch scope {
	case nodebase.ContextScopeFlow:
		return scopeID
	case nodebase.ContextScopeNode:
		if slash := strings.Index(scopeID, "/"); slash > 0 {
			return scopeID[:slash]
		}
	}
	return ""
}

// GetContext implements nodebase.ContextStore. Stored JSON is decoded
// into ordinary Go JSON types so the persistent store exposes the same
// shapes as the JSON-normalizing in-memory store (objects as
// map[string]interface{}, arrays as []interface{}, numbers as float64).
func (s *EntityContextStore) GetContext(scope, scopeID, key string) (interface{}, bool, error) {
	raw, ok, err := s.backend.ContextValueGet(context.Background(), scope, scopeID, key)
	if err != nil || !ok {
		return nil, ok, err
	}
	var value interface{}
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return nil, false, fmt.Errorf("entity context store: decode %s/%s key %q: %w", scope, scopeID, key, err)
	}
	return value, true, nil
}

// SetContext implements nodebase.ContextStore. Values must be JSON
// representable; this is what makes restart persistence and the memory
// store's normalization semantics equivalent.
func (s *EntityContextStore) SetContext(scope, scopeID, key string, value interface{}) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("entity context store: encode %s/%s key %q: %w", scope, scopeID, key, err)
	}
	return s.backend.ContextValueSet(context.Background(), contextWorkflowID(scope, scopeID), scope, scopeID, key, string(raw))
}

// DeleteContext implements nodebase.ContextStore.
func (s *EntityContextStore) DeleteContext(scope, scopeID, key string) error {
	return s.backend.ContextValueDelete(context.Background(), scope, scopeID, key)
}

// DeleteContextScope implements nodebase.ContextStore.
func (s *EntityContextStore) DeleteContextScope(scope, scopeID string) error {
	return s.backend.ContextValueDeleteScope(context.Background(), scope, scopeID)
}

// ContextKeys implements nodebase.ContextStore.
func (s *EntityContextStore) ContextKeys(scope, scopeID string) ([]string, error) {
	keys, err := s.backend.ContextValueKeys(context.Background(), scope, scopeID)
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	return keys, nil
}

var _ nodebase.ContextStore = (*EntityContextStore)(nil)
