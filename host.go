package noderunner

import (
	"fmt"
	"io"
	"net/http"

	"github.com/dracory/nodebase"

	nodestore "github.com/dracory/nodestore"
)

// This file adapts WorkflowEngine to the nodebase.NodeHost contract —
// the narrow runtime seam executors are written against. The
// capability methods the executor-facing interface needs but the
// engine's public config fields cannot express (an interface cannot
// surface a field) live here; the rest of NodeHost is satisfied by
// the engine's existing methods (Emit, EmitEvent, InjectMessage,
// ResolveResponse, DiscardPending, LookupNode, RunningFlow,
// RunContext, GetOrCreateNodeState, ExecuteDownstream, NodeEnv,
// ResolvedContextStore, LoadTLSConfig).

// FileRootPath implements nodebase.NodeHost: the root directory file
// nodes are restricted to (empty = unrestricted).
func (e *WorkflowEngine) FileRootPath() string {
	return e.FileRoot
}

// ExecAllowed implements nodebase.NodeHost: whether the exec node may
// run arbitrary system commands.
func (e *WorkflowEngine) ExecAllowed() bool {
	return e.AllowExec
}

// OSEnvAllowed reports whether flows may read process environment
// variables — consulted by the function node's env.get OS fallback
// (and the engine's own "env"-typed resolution) via an optional
// interface, so hosts without the method keep Node-RED behaviour.
func (e *WorkflowEngine) OSEnvAllowed() bool {
	return e.AllowOSEnv
}

// SharedHTTPRouter implements nodebase.NodeHost: the optional shared
// listener HTTP-serving nodes mount routes on.
func (e *WorkflowEngine) SharedHTTPRouter() nodebase.HTTPRouter {
	return e.HTTPRouter
}

// HTTPAuthFunc implements nodebase.NodeHost: authenticates requests at
// node-served HTTP listeners. Nil means no credentials configured.
func (e *WorkflowEngine) HTTPAuthFunc() func(r *http.Request) (username string, ok bool) {
	return e.HTTPAuthenticator
}

// ResolvedContextStore resolves the store backing function-node context: the
// configured persistent store, or a lazily created process-local
// in-memory store when none is wired (context then survives
// undeploy/redeploy but not a process restart).
func (e *WorkflowEngine) ResolvedContextStore() nodebase.ContextStore {
	if e.ContextStore != nil {
		return e.ContextStore
	}
	e.runMu.Lock()
	defer e.runMu.Unlock()
	if e.contextMem == nil {
		e.contextMem = nodebase.NewMemoryContextStore()
	}
	return e.contextMem
}

// QueueService implements nodebase.NodeHost: the engine is its own queue
// service — the shared Queue machinery lives here because the
// engine's auto problem queue reuses it.
func (e *WorkflowEngine) QueueService() nodebase.QueueService {
	return e
}

// HasDurableStore implements nodebase.QueueService.
func (e *WorkflowEngine) HasDurableStore() bool {
	return e.DurableInboxStore != nil
}

// EnsureQueue implements nodebase.QueueService: returns the shared Queue
// for node, creating and configuring it on first use (resolved
// capacity/policy/hold plus the persistence backend). checkpoint and
// routes mark the queue manual (a checkpoint never self-drains) and
// carry its named release routes.
func (e *WorkflowEngine) EnsureQueue(node nodestore.NodeInterface, checkpoint bool, routes []string) (nodebase.QueueContract, error) {
	e.runMu.Lock()
	workflow := e.flows[node.GetWorkflowID()]
	capacity, policy, hold, backend, dir := e.ResolveQueueConfig(node, workflow)
	e.runMu.Unlock()

	q := e.GetOrCreateNodeState(node.GetWorkflowID(), node.GetID(), func() interface{} {
		return &Queue{
			engine:     e,
			workflowID: node.GetWorkflowID(),
			node:       node,
			capacity:   capacity,
			policy:     policy,
			hold:       hold,
			notify:     make(chan struct{}, 1),
			stopCh:     make(chan struct{}),
		}
	}).(*Queue)
	// Refresh the resolved config on every call so a redeploy picking
	// up new node.GetData() wins even if a queue somehow survives. A
	// backend or dir change rebuilds the store.
	q.mu.Lock()
	q.capacity, q.policy = capacity, policy
	q.node = node
	q.manual = checkpoint
	q.routes = routes
	if q.store == nil && q.storeErr == nil || q.backend != backend || q.dir != dir {
		if closer, ok := q.store.(io.Closer); ok {
			_ = closer.Close() // release the old locator's connection
		}
		q.backend, q.dir = backend, dir
		q.store, q.storeErr = e.BuildQueueStore(backend, dir, node.GetID(), node.GetData())
	}
	q.mu.Unlock()
	return q, nil
}

// EmitDurable is the coordination-boundary emission used by wait: unlike
// fire-and-forget Emit, it requires the durable inbox backend, runs the
// downstream traversal synchronously, and returns infrastructure failures
// (enqueue/dequeue/cancellation) so the caller can retain ownership of a
// parked message. Ordinary node execution errors remain flow errors and
// are not surfaced here — the durability question is whether the handoff
// itself was persisted and drained.
func (e *WorkflowEngine) EmitDurable(workflowID, nodeID string, payload nodestore.MessageInterface, ports []string) error {
	if payload == nil {
		return fmt.Errorf("durable emission failed: payload is nil")
	}
	if e.DurableInboxStore == nil {
		return fmt.Errorf("durable emission unavailable: no durable inbox store configured")
	}
	return e.DeliverSync(workflowID, nodeID, payload, ports)
}

// compile-time assertion: the engine is the reference NodeHost.
var _ nodebase.NodeHost = (*WorkflowEngine)(nil)

// ResolvedCoordinationStore returns the configured durable coordination
// store backing wait/notify. Nil means the nodes must fail explicitly —
// there is deliberately no process-local fallback for distributed state.
func (e *WorkflowEngine) ResolvedCoordinationStore() nodebase.CoordinationStore {
	return e.CoordinationStore
}
