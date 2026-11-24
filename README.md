# noderun

The workflow engine for nodestore-based flows — deploys, runs, and
stops workflows, entirely over `nodestore` interfaces.

> Currently developed inside the goflow module as
> `pkg/nodestore/noderunner`; this repository is the extraction target
> once the package is fully working.

## Core types

- `WorkflowEngine` (`NewWorkflowEngine()`) — owns the node registry,
  per-workflow run state, node state, the durable inbox, and the run
  journal. Deploy with `DeployWorkflow` / `DeployWorkflowPartial`, stop
  with `StopWorkflow`, fire nodes with `InjectMessage` /
  `FireInjectNode`.
- `NodeExecutor` — the contract every node implements: `ID()`,
  `Type()`, `Execute(ctx, msg)`, `Validate()`. Executors embed
  `nodestore.NodeInterface` so identity/type/Data live on the node
  entity.
- `PortRouter` — optional executor interface: choose which output ports
  a message continues on (switch-node semantics).
- `MessageExpander` — optional executor interface: turn one output into
  a sequence (split-node semantics).
- `Stopper`, `Activator` — lifecycle hooks for nodes holding live
  resources or needing activation on deploy.
- `RegisterNodeType(typeID, NodeFactory)` — uniform registration for
  built-ins and node packs; duplicate typeID is an error. Registration
  carries the editor descriptor served to the admin palette.

## Execution model

- Bounded-inbox model: each node has an incremental queue; traversal
  runs per-message on its own goroutine — nothing preserves order
  between separate injects/emits. FIFO consumers use `deliverSync`.
- Durable by default: every hop persists to the shared store, so any
  runner attached to the same database can claim undelivered work;
  deployed workflows re-activate on restart and drain durable inboxes.
- Pending HTTP responses are tracked by message ID
  (`ResolveResponse` / `DiscardPending`); fire-and-forget injects must
  discard, and event-node messages must preserve the original msgId.

## Relationship to nodes

The built-in node executors live in the separate `nodebase` package —
they are engine-agnostic, written against the `NodeHost` contract
they define. `WorkflowEngine` implements `NodeHost`: it passes itself
to node factories and hosts any pack built on the same contract.
Dependency direction: `noderun → nodebase → nodestore`.

## Contents

- Engine core, leases, deploy diffing/observation
- Inbox, durable queues, run journal (execution log)
- Node-type registry (`RegisterNodeType`) and the `NodeHost`
  implementation backing executor runtime seams
- Subflow/module composition, context and credential persistence
- Filesystem abstractions (local/ftp/sftp) and inbound HTTP routing

## Rules

- Executors never resolve engine state inside `Stop()` (deadlock —
  `StopWorkflow` holds the run mutex); resolve eagerly in `Execute` and
  cache the state pointer on the executor shell.
- Imports `nodestore` only — never `nodeadmin`.
