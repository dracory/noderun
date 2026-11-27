# Link In Link Out

A `noderun` workflow demonstrating virtual wires — messages that jump
across the graph with no edge drawn:

```text
inject → linkOut ──edge──→ debug "Hello direct!"
              ╲ (no edge)
linkIn → change → debug "arrived via link"
```

Things to notice:

- `linkOut` in the default `mode: "link"` does **two** things: pushes a
  clone of the message to every linkIn node in its `links` list, and
  passes the original message through its own downstream wiring. Both
  debug nodes fire here.
- The linkIn node is the landing point: it has no incoming edges — the
  engine emits the pushed clone *from* it, so the clone travels through
  linkIn's outgoing wiring with `msg.linkSource` metadata set to the
  sender's node id.
- A `links` entry pointing at a missing or non-linkIn node is silently
  skipped — dangling links are legal, matching Node-RED.
- `mode: "return"` instead answers the pending `linkCall` that injected
  the message — the pair for subflow-style call/return flows (a
  `linkCall` node type also exists for that pattern).

Run from the `noderun` module root:

```bash
go run ./examples/linkin-linkout
```

Expected output (ordering may vary):

```text
Hello direct!
arrived via link
```
