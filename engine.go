package noderunner

import (
	"context"
	"errors"
	"fmt"
	"github.com/dracory/nodebase"
	nodestore "github.com/dracory/nodestore"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// NodeEvent is a single timestamped entry in a workflow's execution
// log — an alias of the runner-agnostic nodebase.NodeEvent contract so
// admin surfaces (nodeadmin/api) can consume workflow events
// without importing this package.
type NodeEvent = nodebase.NodeEvent

// maxWorkflowEvents caps the retained event buffer per workflow
const maxWorkflowEvents = 500

// WorkflowEngine manages the execution of workflows
type WorkflowEngine struct {
	nodes   map[string]nodebase.NodeExecutor
	nodesMu sync.RWMutex
	running map[string]context.CancelFunc
	runMu   sync.Mutex
	// statuses holds per-node execution status (running|ok|error) per workflow ID
	statuses map[string]map[string]string
	// events holds a bounded event log per workflow ID
	events map[string][]NodeEvent
	// journal holds the bounded per-node run journal (last
	// RunJournalSize executions) per workflow ID — powers the
	// editor's "Run details" panel. Run-scoped like statuses: reset
	// on deploy, dropped on stop.
	journal map[string]map[string][]nodebase.RunJournalEntry
	// stoppers holds long-lived node resources (e.g. HTTP listeners)
	// per workflow ID
	stoppers map[string][]nodebase.Stopper
	// flows holds the definition of each running workflow, needed to
	// inject external messages (e.g. HTTP trigger requests) into the
	// downstream node graph
	flows map[string]nodestore.WorkflowInterface
	// paused holds the workflow-level delivery gate. A paused workflow
	// stays deployed (triggers remain live) but new downstream deliveries
	// are admitted into the durable backlog instead of dispatched.
	paused map[string]bool
	// pausedSeqs tracks the durable inbox rows admitted while paused.
	// Tracking the exact seqs lets resume drain the pause backlog without
	// duplicating rows still owned by pre-pause in-flight traversals.
	pausedSeqs map[string]map[int64]bool
	// pausedMemory is the no-durable-store fallback backlog. It is
	// process-local by necessity; durability requires DurableInboxStore.
	pausedMemory map[string][]InboxItem
	// pauseLocks serializes pause/resume transitions with paused
	// admissions, so a row cannot be written after resume's backlog snapshot.
	pauseLocks map[string]*sync.Mutex
	// pending correlates injected messages with their response channels,
	// keyed by message ID (e.g. an HTTP request awaiting an httpResponse)
	pending map[string]chan nodestore.MessageInterface
	// states holds per-node buffered state (join groups, sort/batch
	// buffers) keyed workflowID → nodeID. Executors are rebuilt per
	// message, so stateful nodes must keep their buffers here.
	states map[string]map[string]interface{}
	// runCtx holds the per-run context (cancelled by StopWorkflow) so
	// in-flight message traversals stop promptly instead of running to
	// completion after a stop.
	runCtx map[string]context.Context
	// runGen is a per-workflow generation counter incremented by every
	// deploy. A run whose generation is no longer current has been
	// superseded — needed because partial deploys may share the run
	// context with the previous run (kept stoppers stay bound to it),
	// so ctx cancellation alone cannot detect replacement.
	runGen map[string]int64
	// deployLocks serializes start-to-activation per workflow ID so a
	// redeploy cannot interleave with the previous run's activation —
	// with partial deploy the supersede check alone is not enough, as
	// an overlapping activation could register its stopper into the
	// new run's list.
	deployLocks map[string]*sync.Mutex
	// seq is the global event/message sequence counter
	seq int64
	// AllowExec gates the exec node's arbitrary command execution.
	// Default off; enabled by the consuming application (wired in main).
	AllowExec bool
	// FileRoot restricts file/fileIn node paths to a root directory.
	// Empty means unrestricted; set by the consuming application (wired in main).
	FileRoot string
	// HTTPAuthenticator authenticates requests arriving at node-served
	// HTTP listeners (submitIn auth "admin"). It returns the
	// authenticated username — stamped into msg.submit.submittedBy.
	// Nil means no credentials are configured; wired in main from the
	// same auth-file env var the consuming application's admin UI uses.
	HTTPAuthenticator func(r *http.Request) (username string, ok bool)
	// nodebase.HTTPRouter is the optional shared-listener seam (see
	// http_router.go): when installed, httpTrigger/submitIn nodes
	// configured with listener "main" mount their route on the host's
	// HTTP server — behind its middleware — instead of binding a
	// private port. Nil disables the mode: main-mode nodes fail
	// activation. Wired in main from the admin's router adapter.
	HTTPRouter nodebase.HTTPRouter
	// RunnerID identifies this process for lease coordination.
	// Defaults to the OS hostname; override via the consuming application's runner-id env var.
	RunnerID string
	// RunnerVersion is stamped into the runners table on heartbeat.
	RunnerVersion string
	// LeaseStore enables multi-runner lease coordination. Nil means
	// leases are disabled (single-runner behaviour, unchanged).
	LeaseStore LeaseStore
	// CredentialsProvider returns the deploy-time credential map
	// (nodeID → key → value) for a workflow — the entity-backed
	// replacement for flowadmin's injectCredentials. Nil means no
	// credential storage is configured: sentinel markers pass through
	// to executors (single-user dev installs keep working unchanged).
	// Wired by the admin surface from nodestore.CredentialMapForWorkflow.
	CredentialsProvider func(ctx context.Context, workflowID string) (map[string]map[string]string, error)
	// LeaseTTL is how long a claimed lease lives without renewal
	// (default DefaultLeaseTTL = 15s).
	LeaseTTL time.Duration
	// LeaseHeartbeatInterval is the renew/janitor tick
	// (default DefaultLeaseHeartbeatInterval = 5s).
	LeaseHeartbeatInterval time.Duration
	// leaseOnce starts the heartbeat goroutine lazily on the first
	// leased ExecuteWorkflow.
	leaseOnce sync.Once
	// InboxCapacity is the engine-wide default bound for per-node
	// inboxes; <=0 means unbounded (legacy sequential-BFS behaviour).
	// Per-node node.GetData()["inboxCapacity"] and flow-level
	// workflow.GetView()["inboxCapacity"] override it.
	InboxCapacity int
	// InboxPolicy is the engine-wide default overflow policy applied
	// when a bounded inbox is full (empty defaults to nodebase.OverflowBlock).
	InboxPolicy nodebase.OverflowPolicy
	// InboxBackendFactory creates the storage backend behind each
	// traversal's node inboxes. Nil falls back to DurableInboxStore
	// (durable-by-default: every hop persists) and then to the
	// in-memory backend.
	InboxBackendFactory func(workflowID string) InboxBackend
	// DurableInboxStore enables the write-through durable inbox
	// backend for EVERY node inbox (GOD ruling 2026-11-24: multiple
	// runners may share one DB, so every hop must leave a durable
	// record — the superseded "inboxDurable" opt-in flags are ignored).
	// Nil keeps every inbox in memory (standalone dev without a DB).
	// Wired by the admin surface — see NeatInboxStore.
	DurableInboxStore DurableInboxStore
	// InboxDurable is DEPRECATED (superseded by durable-by-default):
	// kept for config compatibility only, no longer consulted.
	InboxDurable bool
	// MaxSteps is the engine-wide step quota: the maximum number of
	// node executions a single message lineage may trigger before the
	// traversal halts (runaway-loop guard). 0 falls back to
	// DefaultMaxSteps; <0 disables the quota. Per-workflow
	// workflow.GetView()["maxSteps"] overrides it (same semantics).
	// Wired in main by the consuming application.
	MaxSteps int
	// MaxInflightTraversals bounds concurrently running message
	// traversals (inject/Emit/DeliverSync): each external injection
	// launches a goroutine and — with a durable inbox — one DB row
	// per hop, so an unbounded flood is a goroutine + write
	// amplification DoS. 0 falls back to
	// DefaultMaxInflightTraversals; <0 disables the bound.
	// Wired in main by the consuming application.
	MaxInflightTraversals int
	// inflight counts currently running traversals (runMu).
	inflight int
	// pendingWF indexes pending response channels by workflow so
	// StopWorkflow can drop a stopped run's correlations.
	pendingWF map[string]string
	// AllowOSEnv gates whether flows may read process environment
	// variables — "env"-typed vars in group/component/flow/global env
	// layers and the env.get OS fallback in function nodes. Default
	// off: a flow author should not be able to exfiltrate process
	// secrets (DB DSNs, cloud credentials) via env.get. Enable via
	// the consuming application's opt-in env var (wired in main) — mirrors AllowExec.
	AllowOSEnv bool
	// nodebase.ContextStore persists function-node context (node/flow/global
	// scopes) across engine restarts and undeploy/redeploy — wired in
	// main via a type assertion on the storage backend, like
	// DurableInboxStore. Nil falls back to a process-local in-memory
	// store (context survives redeploys but not restarts).
	ContextStore nodebase.ContextStore
	// contextMem backs ResolvedContextStore() when no persistent nodebase.ContextStore
	// is configured.
	contextMem nodebase.ContextStore
	// CoordinationStore is the durable distributed wait/notify coordination
	// store. Nil disables wait/notify nodes explicitly; there is no
	// process-local fallback because the contract is multi-runner state.
	// Wired by the admin surface via NeatCoordinationStore.
	CoordinationStore nodebase.CoordinationStore
	// globalEnv holds the resolved global-config env vars (Node-RED
	// parity: a deployment-wide env layer consulted by env.get after
	// group/component/flow and before the OS environment). Set via
	// SetGlobalEnv — wired in main/api from the storage backend.
	globalEnv map[string]interface{}
	// nodeFactories holds engine-scoped node type registrations (see
	// RegisterNodeType in registry.go); the global registry sits
	// behind it (engine-scoped wins, global second, else deploy fails).
	nodeFactories map[string]nodebase.NodeRegistration
	factoriesMu   sync.RWMutex
	// deployObserver is the optional run-lifecycle seam (see
	// DeployObserver in deploy_observer.go): a host-installed hook
	// notified after successful activation and on run teardown.
	// Nil — the default — means no observation. Wired via
	// SetDeployObserver, e.g. by pkg/flowdashboard.
	deployObserver DeployObserver
}

// NewWorkflowEngine creates a new WorkflowEngine
func NewWorkflowEngine() *WorkflowEngine {
	return &WorkflowEngine{
		nodes:         make(map[string]nodebase.NodeExecutor),
		running:       make(map[string]context.CancelFunc),
		statuses:      make(map[string]map[string]string),
		events:        make(map[string][]NodeEvent),
		journal:       make(map[string]map[string][]nodebase.RunJournalEntry),
		stoppers:      make(map[string][]nodebase.Stopper),
		flows:         make(map[string]nodestore.WorkflowInterface),
		paused:        make(map[string]bool),
		pausedSeqs:    make(map[string]map[int64]bool),
		pausedMemory:  make(map[string][]InboxItem),
		pauseLocks:    make(map[string]*sync.Mutex),
		pending:       make(map[string]chan nodestore.MessageInterface),
		states:        make(map[string]map[string]interface{}),
		runCtx:        make(map[string]context.Context),
		runGen:        make(map[string]int64),
		deployLocks:   make(map[string]*sync.Mutex),
		pendingWF:     make(map[string]string),
		nodeFactories: make(map[string]nodebase.NodeRegistration),
	}
}

// RegisterNode adds a node executor to the engine
func (e *WorkflowEngine) RegisterNode(node nodebase.NodeExecutor) error {
	e.nodesMu.Lock()
	defer e.nodesMu.Unlock()

	if _, exists := e.nodes[node.ID()]; exists {
		return errors.New("node with this ID already exists")
	}

	e.nodes[node.ID()] = node
	return nil
}

// RunningFlow returns the expanded in-memory workflow definition a
// run is executing — node executors consult it for graph context
// (e.g. link target lookups, config-node resolution). ok is false
// when the workflow is not running on this engine.
func (e *WorkflowEngine) RunningFlow(workflowID string) (nodestore.WorkflowInterface, bool) {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	workflow, ok := e.flows[workflowID]
	if !ok {
		return nil, false
	}
	// The stored running flow carries injected credentials — hand
	// callers a sentinel-masked clone so admin/debug surfaces and
	// executors doing graph lookups can never read live secrets.
	return scrubWorkflowCredentials(workflow), true
}

// RunContext returns the live run context for workflowID — bound to
// StopWorkflow so node-side emissions cancel with the run. Falls back
// to context.Background when the workflow is not running.
func (e *WorkflowEngine) RunContext(workflowID string) context.Context {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	if ctx := e.runCtx[workflowID]; ctx != nil {
		return ctx
	}
	return context.Background()
}

// GetNode returns a node executor by ID
func (e *WorkflowEngine) GetNode(id string) (nodebase.NodeExecutor, bool) {
	e.nodesMu.RLock()
	defer e.nodesMu.RUnlock()

	node, exists := e.nodes[id]
	return node, exists
}

// deployTimeout bounds how long DeployWorkflow waits for activation
// (e.g. an mqttIn subscribe).
const deployTimeout = 30 * time.Second

// ExecuteWorkflow starts a workflow; activation runs in the background.
// Use DeployWorkflow when the caller needs the activation outcome.
func (e *WorkflowEngine) ExecuteWorkflow(ctx context.Context, workflow nodestore.WorkflowInterface, input nodestore.MessageInterface) (string, error) {
	id, _, err := e.startRun(ctx, workflow, input, DeployFull)
	return id, err
}

// DeployWorkflow starts a workflow and waits for activation to finish.
// The workflow entity's paused flag is honored: true activates with the
// delivery gate closed; false is an explicit unpause and drains the pause
// backlog after activation. A failed activation (e.g. a trigger that cannot bind its port) is
// returned as an error, and no listener is left running. A redeploy of
// a running workflow is a FULL deploy: every node restarts.
func (e *WorkflowEngine) DeployWorkflow(ctx context.Context, workflow nodestore.WorkflowInterface, input nodestore.MessageInterface) (string, error) {
	return e.deployWorkflow(ctx, workflow, input, DeployFull)
}

// DeployWorkflowPartial is DeployWorkflow with DeployPartial semantics:
// the incoming flow is diffed against the running one and only changed
// nodes are stopped and re-activated (Node-RED "modified nodes" mode).
// Unchanged Stoppers — tcpIn/mqttIn sockets, inject timers, queue
// drainers — keep running. On a cold deploy (nothing running) it is
// identical to a full deploy.
func (e *WorkflowEngine) DeployWorkflowPartial(ctx context.Context, workflow nodestore.WorkflowInterface, input nodestore.MessageInterface) (string, error) {
	return e.deployWorkflow(ctx, workflow, input, DeployPartial)
}

// deployWorkflow implements DeployWorkflow for the chosen mode.
func (e *WorkflowEngine) deployWorkflow(ctx context.Context, workflow nodestore.WorkflowInterface, input nodestore.MessageInterface, mode DeployMode) (string, error) {
	id, activated, err := e.startRun(ctx, workflow, input, mode)
	if err != nil {
		return "", err
	}
	select {
	case aerr := <-activated:
		if aerr != nil {
			return "", aerr
		}
		return id, nil
	case <-time.After(deployTimeout):
		return "", fmt.Errorf("workflow %s activation timed out", id)
	}
}

// deployLock returns the per-workflow mutex serializing deploys
// (created lazily).
func (e *WorkflowEngine) deployLock(workflowID string) *sync.Mutex {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	mu := e.deployLocks[workflowID]
	if mu == nil {
		mu = &sync.Mutex{}
		e.deployLocks[workflowID] = mu
	}
	return mu
}

// pauseLock returns the per-workflow mutex serializing pause/resume
// transitions with paused admissions (created lazily).
func (e *WorkflowEngine) pauseLock(workflowID string) *sync.Mutex {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	mu := e.pauseLocks[workflowID]
	if mu == nil {
		mu = &sync.Mutex{}
		e.pauseLocks[workflowID] = mu
	}
	return mu
}

// startRun registers a new run and starts its activation in the
// background, returning a channel that receives the activation result.
// With mode == DeployPartial and a flow already running, the incoming
// expanded graph is diffed against the stored one: unchanged stoppers
// stay live, only dirty node IDs are activated, and per-node state and
// statuses survive for untouched nodebase.
func (e *WorkflowEngine) startRun(ctx context.Context, workflow nodestore.WorkflowInterface, input nodestore.MessageInterface, mode DeployMode) (string, <-chan error, error) {
	if workflow == nil {
		return "", nil, errors.New("cannot run a nil workflow")
	}
	// Expand component instances into namespaced internal node copies
	// before the run registers — the stored flow, statuses and node
	// lookups all operate on the expanded graph. The expansion runs
	// on a copy so callers holding the workflow (e.g. startWorkflow,
	// which persists the deployed flag afterwards) never write the
	// expanded graph back to storage.
	expanded := cloneWorkflow(workflow)
	if err := ExpandComponents(expanded, e.AllowOSEnv); err != nil {
		return "", nil, fmt.Errorf("component expansion: %w", err)
	}
	workflow = expanded

	// Deploy-time credential injection (flowadmin injectCredentials
	// parity): stored flows carry CredentialSentinel markers; the real
	// values come from the credential entities via the provider. This
	// runs on the expanded clone — stored documents and in-memory
	// copies held by callers are never tainted with secrets.
	if e.CredentialsProvider != nil {
		creds, err := e.CredentialsProvider(ctx, workflow.GetID())
		if err != nil {
			return "", nil, fmt.Errorf("credential injection: %w", err)
		}
		// Deep clone first — the shallow run clone shares node data
		// maps with the caller's stored document, and injection
		// writes real secrets into them.
		workflow = nodestore.DeepCloneWorkflow(workflow)
		nodestore.ApplyWorkflowCredentials(workflow, creds)
	}

	// Claim the DB lease first — when several engine runners share one
	// database, only the lease holder may run a workflow. No-op when
	// no LeaseStore is configured.
	if err := e.claimLease(workflow.GetID()); err != nil {
		return "", nil, err
	}
	e.startLeaseHeartbeat()

	// Serialize deploys of this workflow from registration through
	// activation. The activation goroutine releases the mutex. Without
	// it a partial redeploy could interleave with the previous run's
	// activation and end up sharing stoppers between two runs.
	deployMu := e.deployLock(workflow.GetID())
	deployMu.Lock()

	// nodestore.Store the cancel function for this workflow and reset its
	// per-run observability state (statuses + event buffer). A previous
	// run of the same workflow is cancelled and its stoppers stopped —
	// redeploy must not leave the old traversal running.
	e.runMu.Lock()
	e.runGen[workflow.GetID()]++
	gen := e.runGen[workflow.GetID()]
	oldCancel := e.running[workflow.GetID()]
	oldFlow := e.flows[workflow.GetID()]
	oldRunCtx := e.runCtx[workflow.GetID()]

	// activateOnly restricts which stopper nodes the activation pass
	// (re)starts — nil means all of them (full deploy / cold start).
	var activateOnly map[string]bool
	// toStop collects stoppers that must shut down: every leftover
	// stopper on a full deploy, only dirty/removed ones on a partial
	// deploy.
	var toStop []nodebase.Stopper
	// keepCtx is set when kept stoppers stay bound to the previous run
	// context — cancelling it would kill their goroutines (e.g. the
	// inject ticker selects on ctx.Done), so the run keeps using it.
	keepCtx := false

	if mode == DeployPartial && oldFlow != nil {
		dirty := flowDiff(oldFlow, workflow)
		activateOnly = dirty
		var kept []nodebase.Stopper
		for _, s := range e.stoppers[workflow.GetID()] {
			// Stoppers are registered executors — ID() is the node ID.
			if ex, ok := s.(nodebase.NodeExecutor); ok && !dirty[ex.ID()] {
				kept = append(kept, s)
			} else {
				toStop = append(toStop, s)
			}
		}
		e.stoppers[workflow.GetID()] = kept
		// Untouched nodes keep their status, buffered state and the
		// event log; dirty/removed entries are dropped.
		for nodeID := range dirty {
			delete(e.statuses[workflow.GetID()], nodeID)
			delete(e.states[workflow.GetID()], nodeID)
			delete(e.journal[workflow.GetID()], nodeID)
		}
		if len(kept) > 0 && oldRunCtx != nil {
			keepCtx = true
		}
	} else {
		e.statuses[workflow.GetID()] = make(map[string]string)
		e.events[workflow.GetID()] = nil
		e.journal[workflow.GetID()] = nil
		toStop = e.stoppers[workflow.GetID()]
		e.stoppers[workflow.GetID()] = nil
		e.states[workflow.GetID()] = nil
	}

	ctx, cancel := context.WithCancel(ctx)
	// Stamp the run identity so node code (e.g. dashboard widgets)
	// can attribute side effects to this workflow — the nodebase.NodeFactory
	// signature carries no workflow parameter.
	ctx = ContextWithWorkflowID(ctx, workflow.GetID())
	if keepCtx && oldRunCtx != nil {
		cancel() // discard the fresh context; the old one stays in use
		ctx = oldRunCtx
	} else {
		e.running[workflow.GetID()] = cancel
		e.runCtx[workflow.GetID()] = ctx
	}
	e.flows[workflow.GetID()] = workflow
	var pausedMemoryResume []InboxItem
	if workflow.GetPaused() {
		e.paused[workflow.GetID()] = true
	} else {
		delete(e.paused, workflow.GetID())
		// Deploying the unpaused form is an explicit lifecycle reset:
		// durable rows are drained by executeWorkflow's survivor pass;
		// the process-local fallback backlog has no survivor row, so it
		// is carried through the redeploy and drained after activation.
		pausedMemoryResume = append(pausedMemoryResume, e.pausedMemory[workflow.GetID()]...)
		delete(e.pausedMemory, workflow.GetID())
		delete(e.pausedSeqs, workflow.GetID())
	}
	// Stamp the run's workflow id on every node here — the single-
	// threaded point before activation and traversals fan out.
	// executorForNode also stamps defensively, but doing it there
	// alone would race: several goroutines can hit the same shared
	// node object concurrently.
	for _, n := range workflow.GetNodes() {
		n.SetWorkflowID(workflow.GetID())
	}
	e.runMu.Unlock()

	// Operator-visible warning for tlsConfig nodes that disable peer
	// verification — insecure by explicit opt-out (Node-RED parity)
	// but silent downgrade must be audible in the deploy log.
	for _, node := range workflow.GetNodes() {
		if node.GetType() != "tlsConfig" {
			continue
		}
		if v, ok := node.GetData()["verifyservercert"].(bool); ok && !v {
			e.EmitEvent(workflow.GetID(), node.GetID(), nodebase.NodeName(node), "warn",
				"tlsConfig disables server certificate verification (InsecureSkipVerify)")
		}
	}

	if !keepCtx && oldCancel != nil {
		oldCancel()
	}

	// Shut down listeners left over from the previous run (full deploy)
	// or replaced by changed nodes (partial deploy).
	for _, s := range toStop {
		_ = s.Stop()
	}

	// Start the workflow execution in a goroutine
	activated := make(chan error, 1)
	go e.executeWorkflow(ctx, workflow, input, activated, gen, activateOnly, deployMu, pausedMemoryResume)

	return workflow.GetID(), activated, nil
}

// StopWorkflow stops a running workflow. Stop is terminal for the pause
// lifecycle too: the runtime gate and memory-only pause backlog are cleared;
// durable inbox rows remain persisted for a future deploy.
func (e *WorkflowEngine) StopWorkflow(workflowID string) error {
	// Copy state and clear the maps under the lock, but stop stoppers
	// AFTER releasing it — a stopper's Shutdown can take seconds (e.g.
	// the httpTrigger's 5s drain), and holding runMu through that
	// stalls status, events, inject and DiscardPending.
	pauseMu := e.pauseLock(workflowID)
	pauseMu.Lock()
	e.runMu.Lock()
	cancel, exists := e.running[workflowID]
	stoppers := e.stoppers[workflowID]
	if flow := e.flows[workflowID]; flow != nil {
		flow.SetPaused(false)
	}
	delete(e.paused, workflowID)
	delete(e.pausedSeqs, workflowID)
	delete(e.pausedMemory, workflowID)
	delete(e.stoppers, workflowID)
	delete(e.flows, workflowID)
	delete(e.states, workflowID)
	delete(e.journal, workflowID)
	delete(e.runCtx, workflowID)
	// Drop this run's pending response correlations so unanswered
	// requests cannot outlive their workflow.
	for msgID, wf := range e.pendingWF {
		if wf == workflowID {
			delete(e.pendingWF, msgID)
			delete(e.pending, msgID)
		}
	}
	if exists {
		cancel() // abort in-flight traversals before releasing the lock
		delete(e.running, workflowID)
	}
	// Release the DB lease so another runner may take over
	// immediately (graceful stop). No-op when leases are disabled or
	// we don't own it.
	e.releaseLease(workflowID)
	e.runMu.Unlock()
	pauseMu.Unlock()

	// Notify the run-lifecycle observer (e.g. the dashboard hub) so
	// observer-held run state is dropped alongside the engine's.
	if e.deployObserver != nil {
		e.deployObserver.WorkflowStopped(workflowID)
	}

	if !exists {
		return errors.New("workflow not running")
	}

	// Shut down long-lived node resources (e.g. trigger listeners)
	for _, s := range stoppers {
		_ = s.Stop()
	}
	return nil
}

// setNodeStatus records the execution status of a single node
func (e *WorkflowEngine) setNodeStatus(workflowID, nodeID, status string) {
	e.runMu.Lock()
	defer e.runMu.Unlock()

	if e.statuses[workflowID] == nil {
		e.statuses[workflowID] = make(map[string]string)
	}
	e.statuses[workflowID][nodeID] = status
}

// EmitEvent appends an event to the workflow's bounded event log
func (e *WorkflowEngine) EmitEvent(workflowID, nodeID, nodeName, level, message string) {
	e.runMu.Lock()
	defer e.runMu.Unlock()

	e.seq++
	e.events[workflowID] = append(e.events[workflowID], NodeEvent{
		Seq:      e.seq,
		Time:     time.Now(),
		NodeID:   nodeID,
		NodeName: nodeName,
		Level:    level,
		Message:  maskURICredentials(message),
	})
	if len(e.events[workflowID]) > maxWorkflowEvents {
		e.events[workflowID] = e.events[workflowID][len(e.events[workflowID])-maxWorkflowEvents:]
	}
}

// executeWorkflow is the activation pass: it only executes nodes that
// hold long-lived resources (Stoppers — e.g. httpTrigger listeners).
// Every other node waits for injected messages via InjectMessage —
// deploying a flow does not run its processing nodebase.
// executeWorkflow runs the activation pass. gen is the run generation
// captured at startRun — a run is superseded when the counter moved on,
// which covers partial deploys that share the previous run's context.
// only restricts which stopper nodes activate (nil = all). deployMu is
// the per-workflow deploy lock taken in startRun, released here when
// activation finishes.
func (e *WorkflowEngine) executeWorkflow(ctx context.Context, workflow nodestore.WorkflowInterface, input nodestore.MessageInterface, activated chan<- error, gen int64, only map[string]bool, deployMu *sync.Mutex, pausedMemoryResume []InboxItem) {
	var activationErr error
	defer func() {
		if activationErr != nil {
			e.abandonRun(workflow.GetID(), ctx, gen)
		}
		e.runMu.Lock()
		// Activation is done, but a workflow with long-lived resources
		// (e.g. an httpTrigger listener) is still "running" — it stays
		// alive until StopWorkflow shuts the stoppers down. Only clear
		// the entries when they still belong to THIS run — a redeploy
		// may have replaced them while this activation was in flight.
		if len(e.stoppers[workflow.GetID()]) == 0 && e.runCtx[workflow.GetID()] == ctx && e.runGen[workflow.GetID()] == gen {
			delete(e.running, workflow.GetID())
			delete(e.runCtx, workflow.GetID())
			// A flow without long-lived stoppers reaches its terminal
			// stopped state when activation completes. Like an explicit
			// stop, that transition clears the pause gate; durable rows
			// themselves remain persisted for a future deploy.
			if flow := e.flows[workflow.GetID()]; flow != nil {
				flow.SetPaused(false)
			}
			delete(e.paused, workflow.GetID())
			delete(e.pausedSeqs, workflow.GetID())
			delete(e.pausedMemory, workflow.GetID())
		}
		e.runMu.Unlock()
		activated <- activationErr
		deployMu.Unlock()
	}()

	order := topoOrder(workflow)
	payload := input

	// Missing types fail loudly before anything builds (Node-RED
	// type-registered parity): a workflow referencing unregistered
	// types must not silently run — the deploy surfaces the full
	// missing list. UnknownNode stays a storage/doc fallback only.
	{
		var missing []string
		seen := map[string]bool{}
		for _, node := range order {
			if nodeDisabled(workflow, node) || seen[node.GetType()] {
				continue
			}
			seen[node.GetType()] = true
			if _, ok := e.nodeFactoryFor(node.GetType()); !ok {
				missing = append(missing, node.GetType())
				e.setNodeStatus(workflow.GetID(), node.GetID(), nodestore.NodeStatusError)
			}
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			err := fmt.Errorf("unregistered node types in workflow: %s", strings.Join(missing, ", "))
			e.EmitEvent(workflow.GetID(), "", "", "error", err.Error())
			activationErr = err
			return
		}
	}

	// First pass: build and validate every executor. A bad node config
	// must fail the deploy before anything activates — not surface at
	// message time when the traversal happens to reach the node.
	executors := make(map[string]nodebase.NodeExecutor, len(order))
	for _, node := range order {
		// Disabled nodes are inert: they are never instantiated, so a
		// disabled trigger neither binds nor fires and a bad disabled
		// config cannot fail the deploy (Node-RED "d" semantics).
		if nodeDisabled(workflow, node) {
			continue
		}
		executor, err := e.executorForNode(workflow.GetID(), node)
		if err != nil {
			e.setNodeStatus(workflow.GetID(), node.GetID(), nodestore.NodeStatusError)
			e.EmitEvent(workflow.GetID(), node.GetID(), nodebase.NodeName(node), "error", err.Error())
			e.EmitEvent(workflow.GetID(), "", "", "error", "workflow deploy failed")
			activationErr = err
			return
		}
		if verr := executor.Validate(); verr != nil {
			e.setNodeStatus(workflow.GetID(), node.GetID(), nodestore.NodeStatusError)
			e.EmitEvent(workflow.GetID(), node.GetID(), nodebase.NodeName(node), "error", verr.Error())
			e.EmitEvent(workflow.GetID(), "", "", "error", "workflow deploy failed")
			activationErr = verr
			return
		}
		executors[node.GetID()] = executor
	}

	// Snapshot restart/backlog rows before activation and before the deploy
	// call can return. Without this pre-activation snapshot the asynchronous
	// requeue loader can race the first new injection and admit the same new
	// row both in the live traversal and in the recovery traversal.
	survivingInboxRows := e.loadDurableInboxRows(workflow)

	for _, node := range order {
		executor := executors[node.GetID()]
		if executor == nil {
			continue
		}

		// Only activatable nodes run on deploy; the rest wait for
		// messages injected by triggers.
		if _, activatable := executor.(nodebase.Stopper); !activatable {
			continue
		}
		// Partial deploy: unchanged stoppers are already running —
		// only dirty nodes (re)activate.
		if only != nil && !only[node.GetID()] {
			continue
		}

		// Activation is lifecycle work, not a message run — emit a
		// single "activated" event instead of started/completed noise.
		e.setNodeStatus(workflow.GetID(), node.GetID(), nodestore.NodeStatusRunning)
		// Supersede check and stopper registration under the same lock:
		// a redeploy may already have replaced this run, in which case
		// its listener must not end up in the new run's stopper list.
		e.runMu.Lock()
		superseded := ctx.Err() != nil || e.runGen[workflow.GetID()] != gen
		if !superseded {
			e.stoppers[workflow.GetID()] = append(e.stoppers[workflow.GetID()], executor.(nodebase.Stopper))
		}
		e.runMu.Unlock()
		if superseded {
			e.EmitEvent(workflow.GetID(), "", "", "warn", "workflow execution stopped")
			activationErr = errSuperseded
			return
		}

		// Nodes that also receive wired messages activate through the
		// nodebase.Activator contract so the deploy call cannot be mistaken for
		// a message delivery (e.g. queue arms its drainer here).
		var execErr error
		if act, ok := executor.(nodebase.Activator); ok {
			execErr = safeActivate(act, ctx)
		} else {
			_, execErr = safeExecuteNode(executor, ctx, payload)
		}
		if execErr != nil {
			e.setNodeStatus(workflow.GetID(), node.GetID(), nodestore.NodeStatusError)
			e.EmitEvent(workflow.GetID(), node.GetID(), nodebase.NodeName(node), "error", execErr.Error())
			e.EmitEvent(workflow.GetID(), "", "", "error", "workflow deploy failed")
			activationErr = execErr
			return
		}
		// If a redeploy replaced this run while the listener was
		// binding, the stopper is already out of the live list — shut
		// it down here so the bound port does not leak.
		e.runMu.Lock()
		superseded = ctx.Err() != nil || e.runGen[workflow.GetID()] != gen
		e.runMu.Unlock()
		if superseded {
			_ = executor.(nodebase.Stopper).Stop()
			e.EmitEvent(workflow.GetID(), "", "", "warn", "workflow execution stopped")
			activationErr = errSuperseded
			return
		}
		e.setNodeStatus(workflow.GetID(), node.GetID(), nodestore.NodeStatusOK)
		e.EmitEvent(workflow.GetID(), node.GetID(), nodebase.NodeName(node), "info", "activated")
	}

	// Notify the run-lifecycle observer now that activation succeeded
	// (initial deploy, full and partial redeploy alike) — e.g. the
	// dashboard hub refreshes its per-workflow manifest here.
	if e.deployObserver != nil {
		// The running flow carries injected credentials — observers
		// (dashboard hub, admin surfaces) get a sentinel-masked view.
		e.deployObserver.WorkflowActivated(scrubWorkflowCredentials(workflow))
	}

	e.EmitEvent(workflow.GetID(), "", "", "info", "workflow deployed")

	// Drain the pre-activation survivor snapshot in the background. The
	// snapshot query itself is deliberately synchronous: loading it later
	// can confuse rows created by this run with crash leftovers. The lease
	// (held since startRun) guarantees only this runner drains survivors,
	// and in-order delivery is preserved inside the requeue traversal.
	// A paused-flagged deploy is the exception: survivors stay persisted
	// and are only recorded as the explicit resume backlog.
	if e.workflowPaused(workflow.GetID()) {
		e.notePausedDurableRows(workflow.GetID(), survivingInboxRows)
	} else {
		go e.requeueDurable(ctx, workflow, survivingInboxRows)
		if len(pausedMemoryResume) > 0 {
			go e.requeuePausedMemory(ctx, workflow, pausedMemoryResume)
		}
		e.signalQueues(workflow.GetID())
	}
}

// errSuperseded reports that a run was replaced by a redeploy before its
// activation finished.
var errSuperseded = errors.New("workflow redeployed during activation")

// abandonRun tears down a run whose activation failed: its stoppers are
// shut down and it is unregistered, so the flow is not reported running.
// It does nothing when a newer run has already replaced this one.
func (e *WorkflowEngine) abandonRun(workflowID string, ctx context.Context, gen int64) {
	e.runMu.Lock()
	var stoppers []nodebase.Stopper
	if e.runCtx[workflowID] == ctx && e.runGen[workflowID] == gen {
		stoppers = e.stoppers[workflowID]
		delete(e.stoppers, workflowID)
		if cancel := e.running[workflowID]; cancel != nil {
			cancel()
		}
		delete(e.running, workflowID)
		delete(e.runCtx, workflowID)
	}
	e.runMu.Unlock()
	for _, s := range stoppers {
		_ = s.Stop()
	}
	if e.deployObserver != nil {
		e.deployObserver.WorkflowStopped(workflowID)
	}
}

// runNode executes a single node and records its status/events. It returns
// the node's output payload (unchanged input for pass-through nodes).
func (e *WorkflowEngine) runNode(ctx context.Context, workflowID string, node nodestore.NodeInterface, executor nodebase.NodeExecutor, payload nodestore.MessageInterface) (nodestore.MessageInterface, error) {
	name := nodebase.NodeName(node)
	e.setNodeStatus(workflowID, node.GetID(), nodestore.NodeStatusRunning)
	e.notifyStatus(workflowID, node, nodestore.NodeStatusRunning, payload)

	// Track long-lived resources (e.g. an HTTP trigger listener) so
	// they can be shut down on stop/restart — they outlive this run
	if s, ok := executor.(nodebase.Stopper); ok {
		e.runMu.Lock()
		if !stopperRegistered(e.stoppers[workflowID], s) {
			e.stoppers[workflowID] = append(e.stoppers[workflowID], s)
		}
		e.runMu.Unlock()
	}

	start := time.Now()
	out, err := safeExecuteNode(executor, ctx, payload)
	dur := time.Since(start)
	if err != nil {
		e.recordRun(workflowID, node, payload, nil, dur, err)
		e.setNodeStatus(workflowID, node.GetID(), nodestore.NodeStatusError)
		e.notifyStatus(workflowID, node, nodestore.NodeStatusError, payload)
		e.EmitEvent(workflowID, node.GetID(), name, "error", err.Error())
		return nil, err
	}

	e.recordRun(workflowID, node, payload, out, dur, nil)

	e.setNodeStatus(workflowID, node.GetID(), nodestore.NodeStatusOK)
	e.notifyStatus(workflowID, node, nodestore.NodeStatusOK, payload)

	// Executors may emit extra events after a successful run — debug
	// and log nodes feed the editor's debug panel this way, without
	// the engine knowing their types.
	if re, ok := executor.(nodebase.RunEventer); ok {
		for _, ev := range re.PostRunEvents(node, out) {
			e.EmitEvent(workflowID, node.GetID(), name, ev.Level, ev.Message)
		}
	}

	return out, nil
}

// InjectMessage routes an externally-originated payload (e.g. an HTTP
// trigger request) through the nodes downstream of fromNodeID in the
// running workflow. It returns a message ID whose response can be awaited
// via ResolveResponse — used by httpResponse nodes to reply to the
// original request.
func (e *WorkflowEngine) InjectMessage(workflowID, fromNodeID string, payload nodestore.MessageInterface) (string, chan nodestore.MessageInterface, error) {
	return e.inject(workflowID, fromNodeID, payload, nil)
}

// FireInjectNode triggers the configured payload of a running inject
// node — the engine-side equivalent of clicking Node-RED's inject
// button. Returns an error when the workflow is not running or the
// node is not a live inject node.
func (e *WorkflowEngine) FireInjectNode(workflowID, nodeID string) error {
	e.runMu.Lock()
	var target injectFirer
	for _, s := range e.stoppers[workflowID] {
		if inj, ok := s.(injectFirer); ok && inj.ID() == nodeID {
			target = inj
			break
		}
	}
	e.runMu.Unlock()
	if target == nil {
		return fmt.Errorf("no running inject node %q in workflow %q", nodeID, workflowID)
	}
	// Fire() emits downstream through inject() which takes runMu —
	// must run outside the lock.
	target.Fire()
	return nil
}

// injectFirer is the stopper contract the engine's FireInjectNode
// looks for: a live executor that can re-fire its configured payload
// on demand (the inject node's Fire method). Kept as a minimal
// interface so the engine needs no concrete executor type.
type injectFirer interface {
	nodebase.NodeExecutor
	// Fire injects the node's configured payload downstream once.
	Fire()
}

// inject is the shared scheduling path for InjectMessage (all outgoing
// edges) and emit (port-scoped). It registers the pending response
// channel, stamps the message ID into Metadata, and launches the
// downstream traversal. ports filters which outgoing edges of
// fromNodeID seed the traversal — nil means every outgoing edge.
func (e *WorkflowEngine) inject(workflowID, fromNodeID string, payload nodestore.MessageInterface, ports []string) (string, chan nodestore.MessageInterface, error) {
	if payload == nil {
		return "", nil, fmt.Errorf("workflow %q: cannot inject a nil message", workflowID)
	}
	pauseMu := e.pauseLock(workflowID)
	pauseMu.Lock()
	e.runMu.Lock()
	workflow, ok := e.flows[workflowID]
	if !ok {
		e.runMu.Unlock()
		pauseMu.Unlock()
		return "", nil, fmt.Errorf("workflow %q is not running", workflowID)
	}
	paused := e.paused[workflowID]
	if !paused {
		if limit := e.inflightLimit(); limit >= 0 && e.inflight >= limit {
			e.runMu.Unlock()
			pauseMu.Unlock()
			return "", nil, fmt.Errorf("workflow %q: inflight traversal limit %d reached", workflowID, limit)
		}
		e.inflight++
	}
	e.seq++
	msgID := fmt.Sprintf("msg-%d", e.seq)
	ch := make(chan nodestore.MessageInterface, 1)
	e.pending[msgID] = ch
	e.pendingWF[msgID] = workflowID
	// Bind the traversal to the run context so StopWorkflow cancels
	// in-flight work; fall back to Background when none is recorded.
	ctx, ok := e.runCtx[workflowID]
	if !ok || ctx == nil {
		ctx = context.Background()
	}
	e.runMu.Unlock()

	if payload.GetMetadata() == nil {
		payload.SetMetadata(map[string]interface{}{})
	}
	// Event messages re-emitted by the engine (e.g. a catch node
	// answering the original HTTP request) carry preserveMsgId so the
	// original correlation id survives — the pending channel registered
	// here under the fresh msgID is fire-and-forget.
	preserve, _ := payload.GetMetadata()["preserveMsgId"].(bool)
	delete(payload.GetMetadata(), "preserveMsgId")
	if !preserve {
		payload.GetMetadata()["msgId"] = msgID
	}
	// Birth the per-instance envelope ID (UUIDv7): unique to this
	// message instance — edge clones get their own fresh IDs with
	// parentMsgId recorded (see nodebase.ClonePayload).
	if payload.GetID() == "" {
		payload.SetID(nodebase.GenerateID())
	}

	if paused {
		err := e.admitPausedDownstreamLocked(workflowID, fromNodeID, workflow, payload, ports)
		pauseMu.Unlock()
		if err != nil {
			e.DiscardPending(msgID)
			e.EmitEvent(workflowID, fromNodeID, "", "error", err.Error())
			return "", nil, err
		}
		return msgID, ch, nil
	}
	pauseMu.Unlock()

	// Execute the downstream subgraph in the background; an httpResponse
	// node (or the fallback timeout in the caller) resolves the request.
	go func() {
		defer e.inflightDone()
		e.ExecuteDownstream(ctx, workflowID, fromNodeID, workflow, payload, ports)
	}()

	return msgID, ch, nil
}

// DefaultMaxInflightTraversals is the fallback bound on concurrently
// running traversals — see WorkflowEngine.MaxInflightTraversals.
const DefaultMaxInflightTraversals = 512

// inflightLimit resolves the effective inflight-traversal bound:
// <0 disables, 0 falls back to DefaultMaxInflightTraversals.
func (e *WorkflowEngine) inflightLimit() int {
	if e.MaxInflightTraversals < 0 {
		return -1
	}
	if e.MaxInflightTraversals == 0 {
		return DefaultMaxInflightTraversals
	}
	return e.MaxInflightTraversals
}

// inflightDone releases one traversal slot (called when a background
// traversal finishes).
func (e *WorkflowEngine) inflightDone() {
	e.runMu.Lock()
	e.inflight--
	e.runMu.Unlock()
}

// DeliverSync performs the same bookkeeping as inject but runs the
// downstream traversal on the calling goroutine, returning only when it
// completes. Used by the queue drainer, which must deliver queued
// items strictly in FIFO order — separate async traversals would race.
// ports restricts which outgoing edges of fromNodeID are seeded
// (nil = all edges); checkpoint route releases pass a single port.
func (e *WorkflowEngine) DeliverSync(workflowID, fromNodeID string, payload nodestore.MessageInterface, ports []string) error {
	if payload == nil {
		return fmt.Errorf("workflow %q: cannot deliver a nil message", workflowID)
	}
	pauseMu := e.pauseLock(workflowID)
	pauseMu.Lock()
	e.runMu.Lock()
	workflow, ok := e.flows[workflowID]
	if !ok {
		e.runMu.Unlock()
		pauseMu.Unlock()
		return fmt.Errorf("workflow %q is not running", workflowID)
	}
	paused := e.paused[workflowID]
	if !paused {
		if limit := e.inflightLimit(); limit >= 0 && e.inflight >= limit {
			e.runMu.Unlock()
			pauseMu.Unlock()
			return fmt.Errorf("workflow %q: inflight traversal limit %d reached", workflowID, limit)
		}
		e.inflight++
	}
	ctx, ok := e.runCtx[workflowID]
	if !ok || ctx == nil {
		ctx = context.Background()
	}
	e.runMu.Unlock()

	if payload.GetMetadata() == nil {
		payload.SetMetadata(map[string]interface{}{})
	}
	if paused {
		err := e.admitPausedDownstreamLocked(workflowID, fromNodeID, workflow, payload, ports)
		pauseMu.Unlock()
		return err
	}
	pauseMu.Unlock()

	defer e.inflightDone()

	report := &traversalReport{}
	e.executeDownstream(ctx, workflowID, fromNodeID, workflow, payload, ports, report)
	if report.err != nil {
		return report.err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("workflow %q: downstream delivery interrupted: %w", workflowID, err)
	}
	return nil
}

// HasOutgoingEdges reports whether nodeID has at least one outgoing
// edge in the running workflow — any edge when ports is nil, or an
// edge on one of the given ports. The queue drainer uses it to hold
// items rather than emit them into nowhere.
func (e *WorkflowEngine) HasOutgoingEdges(workflowID, nodeID string, ports []string) bool {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	workflow, ok := e.flows[workflowID]
	if !ok {
		return false
	}
	var allowed map[string]bool
	if ports != nil {
		allowed = make(map[string]bool, len(ports))
		for _, p := range ports {
			allowed[p] = true
		}
	}
	for _, edge := range workflow.GetEdges() {
		if edge.GetSource() != nodeID {
			continue
		}
		if allowed != nil && !allowed[edgePort(edge)] {
			continue
		}
		return true
	}
	return false
}

// emit schedules a fire-and-forget message from nodeID, restricted to
// the given output ports (nil = all outgoing edges). The pending
// response channel registered by inject is discarded — emitted messages
// never expect a reply. Used by nodes that produce asynchronous output
// (e.g. the trigger node's delayed second output).
func (e *WorkflowEngine) Emit(workflowID, nodeID string, payload nodestore.MessageInterface, ports []string) error {
	msgID, _, err := e.inject(workflowID, nodeID, payload, ports)
	if err != nil {
		return err
	}
	e.DiscardPending(msgID)
	return nil
}

// ResolveResponse delivers a payload to a pending injected message (e.g.
// an httpResponse node replying to an HTTP trigger request).
func (e *WorkflowEngine) ResolveResponse(msgID string, payload nodestore.MessageInterface) error {
	e.runMu.Lock()
	ch, ok := e.pending[msgID]
	if ok {
		delete(e.pending, msgID)
		delete(e.pendingWF, msgID)
	}
	e.runMu.Unlock()
	if !ok {
		return fmt.Errorf("no pending request for message %q", msgID)
	}
	ch <- payload
	return nil
}

// DiscardPending removes a pending response channel without answering it
// (e.g. when the requester timed out or disconnected).
func (e *WorkflowEngine) DiscardPending(msgID string) {
	e.runMu.Lock()
	delete(e.pending, msgID)
	delete(e.pendingWF, msgID)
	e.runMu.Unlock()
}

// LookupNode returns the definition of a node within a running workflow.
// Link nodes use it to resolve linkIn targets at message time. Note that
// link targets only resolve within the SAME workflow — cross-workflow
// (Node-RED cross-tab) links are not supported yet.
func (e *WorkflowEngine) LookupNode(workflowID, nodeID string) (nodestore.NodeInterface, bool) {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	workflow, ok := e.flows[workflowID]
	if !ok {
		return nodestore.NewNode("", ""), false
	}
	for _, node := range workflow.GetNodes() {
		if node.GetID() == nodeID {
			// The stored node carries injected credentials — return
			// a sentinel-masked copy so callers (link resolution,
			// config-node lookups) can never read live secrets.
			return scrubNodeCredentials(node), true
		}
	}
	return nodestore.NewNode("", ""), false
}

// LookupNodeForConfig resolves a config-node reference (mqttBroker,
// tlsConfig, …) from an executor at run time, returning the node with
// its injected credentials intact — unlike LookupNode, which masks
// them for admin surfaces. Internal seam only; never expose the result
// through status or inspection APIs.
func (e *WorkflowEngine) LookupNodeForConfig(workflowID, nodeID string) (nodestore.NodeInterface, bool) {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	workflow, ok := e.flows[workflowID]
	if !ok {
		return nodestore.NewNode("", ""), false
	}
	for _, node := range workflow.GetNodes() {
		if node.GetID() == nodeID {
			return node, true
		}
	}
	return nodestore.NewNode("", ""), false
}

// GetNodeState returns the buffered state stored for a node within a
// workflow (nil when none was stored yet). Stateful executors (join,
// sort, batch) are rebuilt per message so their buffers live here.
func (e *WorkflowEngine) GetNodeState(workflowID, nodeID string) interface{} {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	return e.states[workflowID][nodeID]
}

// SetNodeState stores buffered state for a node within a workflow.
func (e *WorkflowEngine) SetNodeState(workflowID, nodeID string, state interface{}) {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	if e.states[workflowID] == nil {
		e.states[workflowID] = make(map[string]interface{})
	}
	e.states[workflowID][nodeID] = state
}

// GetOrCreateNodeState returns the existing node state or stores and
// returns the value built by factory — atomically, so concurrent
// messages can't create duplicate buffers for the same node.
func (e *WorkflowEngine) GetOrCreateNodeState(workflowID, nodeID string, factory func() interface{}) interface{} {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	if e.states[workflowID] == nil {
		e.states[workflowID] = make(map[string]interface{})
	}
	if s, ok := e.states[workflowID][nodeID]; ok {
		return s
	}
	s := factory()
	e.states[workflowID][nodeID] = s
	return s
}

// DefaultMaxSteps is the fallback step quota when neither the engine
// (MaxSteps engine option) nor the workflow (view "maxSteps")
// configures one. A wired cycle re-executes nodes until this trips.
const DefaultMaxSteps = 10000

// stepCountMetaKey carries the running execution count on payload
// metadata so a cycle crossing traversal boundaries still accrues
// toward the quota: nodes that emit asynchronously (trigger, queue
// drainer, linkCall) start a fresh traversal per emission — without
// the inherited count each new traversal would restart at zero and a
// cross-emit cycle would never trip. Best-effort: a node that builds
// a fresh Metadata map drops the lineage count, but the per-traversal
// counter still bounds every burst.
const stepCountMetaKey = "stepCount"

// MaxStepsLimit resolves the effective step quota for a workflow:
// workflow.GetView()["maxSteps"] > engine MaxSteps > DefaultMaxSteps.
// A negative value at either level disables the quota entirely;
// zero or unparseable values fall through to the next level.
func (e *WorkflowEngine) MaxStepsLimit(workflow nodestore.WorkflowInterface) int {
	if workflow != nil && workflow.GetView() != nil {
		if v, ok := workflow.GetView()["maxSteps"]; ok {
			if n, err := nodebase.ToFloat(v); err == nil {
				if n < 0 {
					return 0
				}
				if n > 0 {
					return int(n)
				}
			}
		}
	}
	if e.MaxSteps < 0 {
		return 0
	}
	if e.MaxSteps > 0 {
		return e.MaxSteps
	}
	return DefaultMaxSteps
}

// activeTraversalContextKey marks contexts belonging to a delivery that
// was admitted before the workflow pause gate closed. Nested synchronous
// emissions (sequence nodes, function-node send, link helpers) inherit it,
// so an in-flight traversal can finish even after PauseWorkflow is called.
type activeTraversalContextKey struct{}

func contextWithActiveTraversal(ctx context.Context) context.Context {
	return context.WithValue(ctx, activeTraversalContextKey{}, true)
}

func isActiveTraversalContext(ctx context.Context) bool {
	active, _ := ctx.Value(activeTraversalContextKey{}).(bool)
	return active
}

// ExecuteDownstream performs an edge-aware breadth-first traversal of the
// nodes downstream of fromNodeID. Each queued item carries its own payload
// clone so fan-out branches do not share (and mutate) the same Data map.
// Nodes implementing nodebase.PortRouter select which of their outgoing edges fire;
// all other nodes fan out to every outgoing edge. A failing node aborts
// only its own branch — sibling branches continue.
func (e *WorkflowEngine) ExecuteDownstream(ctx context.Context, workflowID, fromNodeID string, workflow nodestore.WorkflowInterface, payload nodestore.MessageInterface, ports []string) {
	if !isActiveTraversalContext(ctx) {
		pauseMu := e.pauseLock(workflowID)
		pauseMu.Lock()
		e.runMu.Lock()
		paused := e.paused[workflowID]
		e.runMu.Unlock()
		if paused {
			err := e.admitPausedDownstreamLocked(workflowID, fromNodeID, workflow, payload, ports)
			pauseMu.Unlock()
			if err != nil {
				e.EmitEvent(workflowID, fromNodeID, "", "error", err.Error())
			}
			return
		}
		pauseMu.Unlock()
	}
	e.executeDownstream(ctx, workflowID, fromNodeID, workflow, payload, ports, nil)
}

// executeDownstream is ExecuteDownstream with an optional delivery report.
// The report records only infrastructure failures that prevent durable
// handoff persistence/traversal progress; ordinary node execution errors
// are flow errors and stay on the error-port/catch path.
func (e *WorkflowEngine) executeDownstream(ctx context.Context, workflowID, fromNodeID string, workflow nodestore.WorkflowInterface, payload nodestore.MessageInterface, ports []string, report *traversalReport) {
	// The injected message is fully handled once this traversal ends —
	// notify complete nodes scoped to the triggering node (Node-RED
	// "complete" semantics). Guarded against self-recursion.
	defer e.notifyComplete(workflowID, fromNodeID, payload)

	// ports restricts which outgoing edges of fromNodeID seed the
	// traversal (port-scoped emission); nil seeds all edges.
	var allowed map[string]bool
	if ports != nil {
		allowed = make(map[string]bool, len(ports))
		for _, p := range ports {
			allowed[p] = true
		}
	}

	e.traverse(ctx, workflowID, workflow, func(inboxes *inbox, outEdges map[string][]nodestore.EdgeInterface, offer func(nodeID string, p nodestore.MessageInterface) error) {
		for _, edge := range outEdges[fromNodeID] {
			if allowed != nil && !allowed[edgePort(edge)] {
				continue
			}
			_ = offer(edge.GetTarget(), nodebase.ClonePayload(payload))
		}
	}, report)
}

// loadDurableInboxRows snapshots rows that survived a previous run. The
// snapshot is taken before node activation so rows created by the new run
// cannot be mistaken for crash leftovers.
func (e *WorkflowEngine) loadDurableInboxRows(workflow nodestore.WorkflowInterface) []DurableInboxItem {
	if e.DurableInboxStore == nil {
		return nil
	}
	// Runner-aware load when the store supports it: pending rows +
	// expired claims + rows still claimed by THIS runner (same-runner
	// restarts requeue their own crash leftovers immediately instead of
	// waiting out the claim TTL). Stores without the extension fall
	// back to the runner-agnostic load.
	var rows []DurableInboxItem
	var err error
	if loader, ok := e.DurableInboxStore.(workflowClaimLoader); ok {
		rows, err = loader.LoadInboxFor(workflow.GetID(), e.runnerID())
	} else {
		rows, err = e.DurableInboxStore.LoadInbox(workflow.GetID())
	}
	if err != nil {
		e.EmitEvent(workflow.GetID(), "", "", "error", fmt.Sprintf("durable inbox reload failed: %v", err))
		return nil
	}
	return rows
}

// requeueDurable re-queues the pre-activation snapshot of inbox rows that
// survived a previous run — written by this runner before a crash/restart,
// or by a dead runner whose workflow lease this runner just took over on a
// shared store. Rows enter the traversal in seq order and each row is
// deleted as it dequeues (at-least-once per row; see inbox_durable.go).
func (e *WorkflowEngine) requeueDurable(ctx context.Context, workflow nodestore.WorkflowInterface, rows []DurableInboxItem) {
	if len(rows) == 0 {
		return
	}
	e.EmitEvent(workflow.GetID(), "", "", "info", fmt.Sprintf("re-queuing %d durable inbox message(s)", len(rows)))

	// Queue/checkpoint rows are NOT re-queued through the traversal: the
	// node's own activation recovery (arm) adopts them. Feeding them
	// here as well would double-deliver — the node would stage a fresh
	// row for each and still drain the original.
	byID := make(map[string]nodestore.NodeInterface, len(workflow.GetNodes()))
	for _, n := range workflow.GetNodes() {
		byID[n.GetID()] = n
	}

	e.traverse(ctx, workflow.GetID(), workflow, func(inboxes *inbox, _ map[string][]nodestore.EdgeInterface, _ func(string, nodestore.MessageInterface) error) {
		for i := range rows {
			if n, ok := byID[rows[i].NodeID]; ok && (n.GetType() == "queue" || n.GetType() == "checkpoint") {
				continue
			}
			// Auto problem-queue rows (onError="auto") are adopted by
			// the system queue's own arm() when it materializes.
			if rows[i].NodeID == ProblemQueueAutoNodeID {
				continue
			}
			item := InboxItem{NodeID: rows[i].NodeID, Payload: rows[i].Payload}
			if admitter, ok := inboxes.backend.(storedItemAdmitter); ok {
				// The row stays persisted until its entry dequeues —
				// a crash mid-drain re-queues the remainder.
				admitter.AdmitStored(item, rows[i].Seq)
				continue
			}
			// Backend without durable awareness (e.g. plain memory):
			// queue the item transiently and drop the row now.
			if err := inboxes.backend.Enqueue(item); err != nil {
				e.EmitEvent(workflow.GetID(), rows[i].NodeID, "", "error", fmt.Sprintf("durable requeue failed: %v", err))
				continue
			}
			_ = e.DurableInboxStore.DeleteInboxItem(workflow.GetID(), rows[i].Seq)
		}
	}, nil)
}

// traversalReport carries infrastructure delivery failures out of an
// otherwise event-driven traversal. Node execution errors are intentional
// flow outcomes and are deliberately not recorded here.
type traversalReport struct {
	err error
}

func (r *traversalReport) record(err error) {
	if r != nil && r.err == nil && err != nil {
		r.err = err
	}
}

// traverse runs one inbox-draining traversal over the workflow graph.
// seed is invoked once with the resolved inbox, the outgoing-edge map
// and the policy-aware offer closure; everything admitted is then
// executed in queue order until the inbox drains, the context is
// cancelled, or the step quota (MaxStepsLimit) trips.
func (e *WorkflowEngine) traverse(ctx context.Context, workflowID string, workflow nodestore.WorkflowInterface, seed func(inboxes *inbox, outEdges map[string][]nodestore.EdgeInterface, offer func(nodeID string, p nodestore.MessageInterface) error), report *traversalReport) {
	// Mark this delivery as admitted. Node code reusing its Execute ctx
	// for nested downstream emissions is part of the same in-flight
	// traversal and is therefore allowed to finish after a later pause.
	ctx = contextWithActiveTraversal(ctx)
	byID := make(map[string]nodestore.NodeInterface, len(workflow.GetNodes()))
	for _, node := range workflow.GetNodes() {
		byID[node.GetID()] = node
	}
	outEdges := make(map[string][]nodestore.EdgeInterface, len(workflow.GetEdges()))
	for _, edge := range workflow.GetEdges() {
		outEdges[edge.GetSource()] = append(outEdges[edge.GetSource()], edge)
	}

	// The traversal queue lives behind the per-node inbox abstraction:
	// offers honor each node's configured capacity + overflow policy
	// (see inbox.go). With no capacity configured the inbox is
	// unbounded and ordering is identical to a plain FIFO.
	inboxes := e.newInbox(workflowID, workflow)
	defer inboxes.close()

	// The step quota counts node EXECUTIONS, not messages — a
	// legitimate fan-out enqueues many items but each still executes
	// once, so only the runNode tally below can trip it.
	limit := e.MaxStepsLimit(workflow)
	executions := 0
	// carriedSteps lifts the counter to a payload's inherited step
	// count (see stepCountMetaKey): payloads queued by an earlier
	// traversal in the same lineage — emit, DeliverSync, durable
	// requeue — arrive already stamped, so a cycle spanning
	// traversal boundaries keeps accruing instead of resetting.
	carriedSteps := func(p nodestore.MessageInterface) {
		if p == nil || p.GetMetadata() == nil {
			return
		}
		if c, err := nodebase.ToFloat(p.GetMetadata()[stepCountMetaKey]); err == nil && int(c) > executions {
			executions = int(c)
		}
	}

	// offer enqueues one downstream message under the target node's
	// inbox policy and reports drops/evictions to the event log and
	// catch nodebase. Each offered clone is stamped with the current
	// step count so follow-up traversals continue the same lineage.
	offer := func(nodeID string, p nodestore.MessageInterface) error {
		if p != nil {
			carriedSteps(p)
			if p.GetMetadata() == nil {
				p.SetMetadata(map[string]interface{}{})
			}
			p.GetMetadata()[stepCountMetaKey] = executions
		}
		res, err := inboxes.offer(InboxItem{NodeID: nodeID, Payload: p})
		if err != nil {
			err = fmt.Errorf("inbox enqueue failed for node %q: %w", nodeID, err)
			report.record(err)
			e.EmitEvent(workflowID, nodeID, "", "error", err.Error())
			return err
		}
		e.reportOffer(workflowID, byID, res)
		return nil
	}

	// dispatchItem runs one queued item to completion: node lookup,
	// disabled-node bypass, executor resolution, runNode, output fan-out.
	// Returns true when the traversal must stop (step quota breach).
	dispatchItem := func(item InboxItem) (stop bool) {
		node, ok := byID[item.NodeID]
		if !ok {
			return false
		}
		// Items admitted outside offer (durable requeue via
		// AdmitStored) still carry their lineage's step count.
		carriedSteps(item.Payload)
		// Disabled nodes are bypassed: the message flows through
		// unchanged to every outgoing edge — the wire stays live and
		// the node does nothing (Node-RED "d" semantics).
		if nodeDisabled(workflow, node) {
			for _, edge := range outEdges[node.GetID()] {
				_ = offer(edge.GetTarget(), nodebase.ClonePayload(item.Payload))
			}
			return false
		}
		executor, err := e.executorForNode(workflowID, node)
		if err != nil {
			e.setNodeStatus(workflowID, node.GetID(), nodestore.NodeStatusError)
			e.EmitEvent(workflowID, node.GetID(), nodebase.NodeName(node), "error", err.Error())
			e.RouteFailure(workflowID, node, item.Payload, err)
			return false // abort this branch only
		}

		in := item.Payload
		// Items admitted outside offer (durable AdmitStored rows that
		// predate the envelope ID) still need an instance ID so the
		// parentage chain stays intact downstream.
		if in != nil && in.GetID() == "" {
			in.SetID(nodebase.GenerateID())
		}
		out, err := e.runNode(ctx, workflowID, node, executor, in)
		executions++
		if limit > 0 && executions >= limit {
			// Quota breach: stop dispatching further nodes (the
			// in-flight execution already finished above), record an
			// error event naming the quota and the last-executed
			// node, and surface it through the standard failure
			// ladder (error port → catch → problem queue).
			cause := fmt.Errorf("step quota reached: %d node executions (limit %d)", executions, limit)
			e.EmitEvent(workflowID, node.GetID(), nodebase.NodeName(node), "error",
				cause.Error()+" — traversal stopped (possible runaway cycle)")
			e.RouteFailure(workflowID, node, in, cause)
			return true
		}
		if err != nil {
			e.RouteFailure(workflowID, node, in, err)
			return false // node failed — abort this branch, keep siblings running
		}
		if out == nil {
			return false // node dropped the message (e.g. join/batch buffering)
		}

		// Routing nodes select a subset of their output ports; every
		// other node fires all of its outgoing edges.
		var selected map[string]bool
		if router, ok := executor.(nodebase.PortRouter); ok {
			selected = make(map[string]bool)
			for _, port := range router.SelectPorts(in, out) {
				selected[port] = true
			}
		}

		// Expanding nodes (e.g. split) replace their single output with
		// a sequence; each message travels every selected edge.
		outs := []nodestore.MessageInterface{out}
		if expander, ok := executor.(nodebase.MessageExpander); ok {
			outs = expander.Expand(out)
		}

		for _, edge := range outEdges[node.GetID()] {
			if selected != nil && !selected[edgePort(edge)] {
				continue
			}
			for _, o := range outs {
				_ = offer(edge.GetTarget(), nodebase.ClonePayload(o))
			}
		}
		return false
	}

	seed(inboxes, outEdges, offer)
	for {
		// Abort promptly when the workflow was stopped mid-traversal —
		// Node-RED's close() cancels in-flight work for the flow.
		if err := ctx.Err(); err != nil {
			report.record(fmt.Errorf("traversal stopped: %w", err))
			e.EmitEvent(workflowID, "", "", "warn", "traversal stopped: workflow is stopping")
			return
		}
		item, ok, err := inboxes.next()
		if err != nil {
			err = fmt.Errorf("inbox dequeue failed: %w", err)
			report.record(err)
			e.EmitEvent(workflowID, "", "", "error", err.Error())
			return
		}
		if !ok {
			break
		}
		stop := dispatchItem(item)
		// Post-dispatch ack (pending -> claimed -> deleted): the
		// persisted row outlives the dispatch, so a crash mid-process
		// leaves a reclaimable 'claimed' row instead of a silent loss.
		if item.Seq > 0 {
			if err := inboxes.ack(item.Seq); err != nil {
				e.EmitEvent(workflowID, item.NodeID, "", "error", fmt.Sprintf("inbox ack failed: %v", err))
			}
		}
		if stop {
			return
		}
	}
}

// nodeDisabled reports whether a node is inert for this deploy. A node
// is disabled when its own "d"/"disabled" data flag is set (Node-RED
// parity), when its child flow (data.childFlow — an id under the id-keyed
// model, a name in legacy documents) is listed in
// workflow.GetView()["disabledFlows"] or flagged enabled:false in
// workflow.GetView()["childFlows"], or when a view group containing it
// (workflow.GetView()["groups"] entry with disabled:true) is disabled.
// Resolution is runtime-side: the flags live on the node/tab/group
// model and the engine consults them at deploy and message time, so
// toggling a flag needs no graph rewriting. The predicate itself is
// nodebase.WorkflowNodeDisabled — single-sourced in the nodebase so
// runner-agnostic consumers (the dashboard hub's manifest collector)
// apply identical semantics.
func nodeDisabled(workflow nodestore.WorkflowInterface, node nodestore.NodeInterface) bool {
	return nodebase.WorkflowNodeDisabled(workflow, node)
}

// NodeDisabled reports whether node is disabled in workflow — the
// exported form of the deploy-time predicate, for optional packages
// (e.g. pkg/flowdashboard) that collect node subsets of a deployed
// workflow and must honor the same disabled semantics.
func NodeDisabled(workflow nodestore.WorkflowInterface, node nodestore.NodeInterface) bool {
	return nodeDisabled(workflow, node)
}

// edgePort normalizes an edge's source port key; an empty key is the
// node's first output.
func edgePort(edge nodestore.EdgeInterface) string {
	if edge.GetSourceKey() == "" {
		return "output_1"
	}
	return edge.GetSourceKey()
}

// stopperRegistered reports whether s is already represented in the
// stopper list — executors are rebuilt per message, so nodes sharing
// one long-lived resource dedupe via a stable stopKey.
func stopperRegistered(stoppers []nodebase.Stopper, s nodebase.Stopper) bool {
	keyed, ok := s.(nodebase.StopKeyer)
	if !ok {
		return false
	}
	for _, existing := range stoppers {
		if ek, ok := existing.(nodebase.StopKeyer); ok && ek.StopKey() == keyed.StopKey() {
			return true
		}
	}
	return false
}

// safeActivate runs a node's nodebase.Activator hook with the same panic safety
// as safeExecuteNode.
func safeActivate(act nodebase.Activator, ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("node panicked: %v", r)
		}
	}()
	return act.Activate(ctx)
}

// safeExecuteNode validates then executes a node, converting a panic in
// node code into an error instead of crashing the process.
func safeExecuteNode(executor nodebase.NodeExecutor, ctx context.Context, input nodestore.MessageInterface) (out nodestore.MessageInterface, err error) {
	defer func() {
		if r := recover(); r != nil {
			out = nil
			err = fmt.Errorf("node panicked: %v", r)
		}
	}()
	if verr := executor.Validate(); verr != nil {
		return nil, verr
	}
	return executor.Execute(ctx, input)
}

// topoOrder returns workflow nodes in dependency order (Kahn's algorithm).
// Leftover nodes (e.g. inside a cycle) are appended at the end so every
// node is visited exactly once.
func topoOrder(workflow nodestore.WorkflowInterface) []nodestore.NodeInterface {
	indegree := make(map[string]int, len(workflow.GetNodes()))
	byID := make(map[string]nodestore.NodeInterface, len(workflow.GetNodes()))
	adjacency := make(map[string][]string, len(workflow.GetEdges()))
	for _, n := range workflow.GetNodes() {
		indegree[n.GetID()] = 0
		byID[n.GetID()] = n
	}
	for _, edge := range workflow.GetEdges() {
		if _, ok := byID[edge.GetSource()]; !ok {
			continue
		}
		if _, ok := byID[edge.GetTarget()]; !ok {
			continue
		}
		adjacency[edge.GetSource()] = append(adjacency[edge.GetSource()], edge.GetTarget())
		indegree[edge.GetTarget()]++
	}

	var queue []string
	for _, n := range workflow.GetNodes() {
		if indegree[n.GetID()] == 0 {
			queue = append(queue, n.GetID())
		}
	}

	order := make([]nodestore.NodeInterface, 0, len(workflow.GetNodes()))
	visited := make(map[string]bool, len(workflow.GetNodes()))
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if visited[id] {
			continue
		}
		visited[id] = true
		order = append(order, byID[id])
		for _, next := range adjacency[id] {
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	// Append any nodes left out (cycles) so they still execute once
	for _, n := range workflow.GetNodes() {
		if !visited[n.GetID()] {
			order = append(order, n)
		}
	}
	return order
}

// executorForNode builds a nodebase.NodeExecutor from a workflow node definition.
// nodestore.NodeInterface configuration travels in node.GetData() and is decoded via JSON so node
// structs can keep their typed config fields. Nodes that interact with the
// engine (httpTrigger, httpResponse) get a reference to it.
func (e *WorkflowEngine) executorForNode(workflowID string, node nodestore.NodeInterface) (nodebase.NodeExecutor, error) {
	// The executor wraps the node entity: stamp the run's workflow id
	// so factories and long-lived nodes see a consistent identity even
	// when the caller built the node without one (tests, imports).
	// Guarded by the != check — startRun pre-stamps every node, so the
	// write only happens for nodes that bypass it, and concurrent
	// reads on already-stamped nodes stay race-free.
	if node.GetWorkflowID() != workflowID {
		node.SetWorkflowID(workflowID)
	}
	if factory, ok := e.nodeFactoryFor(node.GetType()); ok {
		return factory(node, e)
	}
	// Node-RED "type-registered" model: a workflow referencing an
	// unregistered type must not silently run. Deploy fails loudly —
	// the missing-types pre-pass in executeWorkflow collects the full
	// list; this branch is only reachable for single-node lookups.
	return nil, fmt.Errorf("unregistered node type %q (node %q)", node.GetType(), node.GetID())
}

// GetWorkflowSnapshot returns the workflow run status, per-node statuses
// and the events recorded since the given sequence number, plus the
// current last sequence for incremental polling.
func (e *WorkflowEngine) GetWorkflowSnapshot(workflowID string, since int64) (string, map[string]string, []NodeEvent, int64) {
	status, _ := e.GetWorkflowStatus(workflowID)

	e.runMu.Lock()
	defer e.runMu.Unlock()

	nodeStatuses := make(map[string]string, len(e.statuses[workflowID]))
	for id, s := range e.statuses[workflowID] {
		nodeStatuses[id] = s
	}

	var events []NodeEvent
	lastSeq := since
	for _, ev := range e.events[workflowID] {
		if ev.Seq > lastSeq {
			lastSeq = ev.Seq
		}
		if ev.Seq > since {
			events = append(events, ev)
		}
	}

	return status, nodeStatuses, events, lastSeq
}

// QueueHandle returns the live queue behind a queue node for the
// inspection API — the engine-agnostic nodebase.QueueInspector contract,
// so admin surfaces need no noderunner types. ok is false when the
// workflow is not running or the node has no queue yet.
func (e *WorkflowEngine) QueueHandle(workflowID, nodeID string) (nodebase.QueueInspector, bool) {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	q, ok := e.states[workflowID][nodeID].(*Queue)
	if !ok {
		return nil, false
	}
	return q, true
}

// GetWorkflowStatus returns the status of a workflow execution
func (e *WorkflowEngine) GetWorkflowStatus(workflowID string) (string, error) {
	e.runMu.Lock()
	paused := e.paused[workflowID]
	_, running := e.running[workflowID]
	e.runMu.Unlock()

	if paused {
		return "paused", nil
	}
	if running {
		return "running", nil
	}
	return "stopped", nil
}
