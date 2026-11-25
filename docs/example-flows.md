# Node-RED example flows — smoke harness

`TestNodeREDExampleFlows` (in `examples_test.go`) replays the example
flows the upstream `node-red` project ships for its core nodes — the
same flows the **Import → Examples** menu offers — through this
module's engine.

## What it does

1. Imports each flow file via `nodestore.ImportNodeRED`.
2. Deploys it on a fresh `WorkflowEngine` (in-memory store).
3. Fires every `inject` node (`engine.InjectMessage`), then lets the
   traversal settle.
4. Asserts: import succeeds, deploy does not error, no panic/deadlock
   (deploy runs under a bounded 10 s context; a panic crashes the test
   — never tolerated).
5. Emits a per-file node-type usage report (see below).

## The corpus

- **Upstream**: `node-red/node-red` at the tag in the `examplesTag`
  constant of `examples_test.go`.
- **Upstream path**: `packages/node_modules/@node-red/nodes/examples/**/*.json`.
- **License**: the example flows are Apache-2.0 (© OpenJS Foundation /
  Node-RED contributors). They are **fetched, not committed** —
  `testdata/nodered-examples/` is gitignored. This NOTICE is the
  required attribution mention; do not vendor the flow files.

## Running

```bash
# from the noderun module root

# fetch from GitHub + run (network — the only online mode)
GOFLOW_EXAMPLES_FETCH=1 go test -run TestNodeREDExampleFlows -v .

# re-run against previously fetched files (offline)
go test -run TestNodeREDExampleFlows -v .

# run against any local directory of example .json files
GOFLOW_EXAMPLES_DIR=/path/to/examples go test -run TestNodeREDExampleFlows -v .
```

### Directory resolution order

1. `GOFLOW_EXAMPLES_DIR` — a local path containing the `.json` files.
2. `testdata/nodered-examples/` — files fetched by an earlier run.
3. `GOFLOW_EXAMPLES_FETCH=1` — fetch from GitHub into `testdata/` and run.
4. Otherwise the suite **skips** with a named reason — plain
   `go test ./...` is hermetic and never touches the network.

## The expectations map

`exampleExpectations` in `examples_test.go` is the named-tolerance
list, keyed by the example's path relative to the examples dir:

```go
"storage/read file/02 - Read data in specified encoding.json": {
    DeployFails: true,
    Note:       "why this is tolerated — investigated <date>",
},
```

Rules:

- Every entry **requires a written reason** — silent xfails are banned.
- `ImportFails` tolerates an `ImportNodeRED` error; `DeployFails`
  tolerates an `ExecuteWorkflow` error. Both default to "must succeed".
- A tolerated file that starts passing **fails the suite** until its
  entry is removed — the map tightens as parity improves.
- An unexplained import/deploy/inject failure fails the subtest:
  investigate, fix the engine, or add a named expectation.
- A panic is never a tolerated outcome — it crashes the test binary.

## The usage report

Each run writes `nodered-examples-usage.json` **inside the examples
dir** (so it lands wherever `GOFLOW_EXAMPLES_DIR` points; gitignored —
regenerated, not committed). Per file it records:

- `types` — the node type ids the flow uses,
- `unmapped` — types with no registered executor in the `nodebase`
  registry (upstream-only ids, e.g. the `websocket-client` /
  `websocket-listener` **config-node** types the websocket examples
  carry — their runtime nodes `websocketIn`/`websocketOut` are
  registered),
- `deploys` / `expected_to` / `note` — the outcome and any tolerated
  expectation.

Surprising `unmapped` entries are worth investigating — they usually
mean a real gap, not a harness bug.

## The oracle suite

`TestNodeREDExampleOracle` (`examples_oracle_test.go`) replays the same
corpus and diffs debug-node output against an `oracle.json` recorded
from a real Node-RED instance. It is opt-in: it runs only when
`GOFLOW_EXAMPLES_ORACLE` points at an oracle file. How that file is
produced is up to the consuming application — the suite only defines
the comparison contract (`oracleDifferences`, same
fail-on-newly-passing tightening rule).

## Bumping the upstream pin

1. Change `examplesTag` in `examples_test.go`.
2. Delete `testdata/nodered-examples/` and re-run with
   `GOFLOW_EXAMPLES_FETCH=1`.
3. Re-run the expectations review — entries may need updating.
