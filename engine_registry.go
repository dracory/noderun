package noderunner

import (
	"errors"
	"fmt"

	"github.com/dracory/nodebase"
)

// This file holds the engine-scoped slice of the node registry. The
// process-wide registry (type → descriptor + nodebase.NodeFactory) lives in
// package nodes — it is part of the pack contract, not the engine.
// An engine additionally accepts registrations scoped to itself
// (tests, hosts that want a type on one engine instance only);
// engine-scoped registrations win over the global registry at deploy
// time.

// RegisterNodeType registers factory under typeID for this engine
// only — useful for tests or hosts that want a type scoped to one
// engine instance rather than process-wide. Engine-scoped
// registrations win over the global registry; registering the same
// typeID twice on one engine is an error.
func (e *WorkflowEngine) RegisterNodeType(typeID string, factory nodebase.NodeFactory) error {
	if typeID == "" {
		return errors.New("node type id cannot be empty")
	}
	if factory == nil {
		return fmt.Errorf("node factory for %q cannot be nil", typeID)
	}
	e.factoriesMu.Lock()
	defer e.factoriesMu.Unlock()
	if _, exists := e.nodeFactories[typeID]; exists {
		return fmt.Errorf("node type %q is already registered on this engine", typeID)
	}
	e.nodeFactories[typeID] = nodebase.NodeRegistration{
		Descriptor: nodebase.NodeDescriptor{Type: typeID, Name: typeID},
		Factory:    factory,
	}
	return nil
}

// nodeFactoryFor resolves the factory registered for typeID:
// engine-scoped registrations win over the global registry.
func (e *WorkflowEngine) nodeFactoryFor(typeID string) (nodebase.NodeFactory, bool) {
	e.factoriesMu.RLock()
	reg, ok := e.nodeFactories[typeID]
	e.factoriesMu.RUnlock()
	if ok {
		return reg.Factory, reg.Factory != nil
	}
	reg, ok = nodebase.RegisteredNode(typeID)
	if !ok || reg.Factory == nil {
		return nil, false
	}
	return reg.Factory, true
}
