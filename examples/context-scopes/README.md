# Context Scopes

A `noderun` workflow demonstrating the three message-passing context
scopes available to function-node JavaScript:

```text
inject → function-write → function-read → debug
```

The writer stores one key in each scope; the reader reports what it can
see — proving that `context` (node scope) is isolated per node while
`flow` and `global` are shared.

```js
// writer
context.set("marker", "writer only");   // node scope — this node only
flow.set("shared", "flow value");       // flow scope — whole workflow
global.set("appName", "GoFlow");        // global scope — whole engine
```

Things to notice:

- **context** is namespaced per node (`workflowID/nodeID`), so the
  reader's `context.get("marker")` returns `null` — each node has its
  own private store.
- **flow** is namespaced to the workflow: every node in this flow sees
  `flow.get("shared")`.
- **global** is engine-wide: shared across all workflows on the engine.
- Each scope exposes `get` / `set` / `keys`; context values are backed
  by the host's `ContextStore` (in-memory by default, durable stores
  selectable per scope).
- The same `{{flow.X}}`, `{{global.X}}` and `{{env.X}}` values are also
  reachable from template nodes — see `examples/template-message`.

Run from the `noderun` module root:

```bash
go run ./examples/context-scopes
```

Expected output:

```text
{"flowSees":"flow value","globalSees":"GoFlow","nodeSees":null}
```

`nodeSees` being `null` is the point: the writer's node context is
invisible to a different node, while flow and global reads succeed.
