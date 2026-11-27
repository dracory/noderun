# Template Message

A `noderun` workflow demonstrating the template node:

```text
inject (JSON object) → template (mustache) → debug
```

The inject node delivers the object `{"name": "world", "source": "the
template node"}` as `msg.payload`. The template node renders
`Hello {{payload.name}}, from {{payload.source}}!` against the message and
writes the rendered string back to `msg.payload`.

Things to notice:

- `syntax: "mustache"` is the default — Node-RED parity. The node also
  supports `gotemplate` (a `noderun` extension rendering Go
  `text/template` against `msg.payload`) and `plain` (no substitution).
- The mustache context is the **whole message**, not just the payload:
  `{{payload.name}}` drills into the payload object, while metadata like
  `{{topic}}` is reachable at top level. `{{flow.X}}`, `{{global.X}}` and
  `{{env.X}}` tags are resolved against the host context before
  rendering.
- `field`/`fieldType` control where the rendered result is written —
  `msg.payload` by default, but any `msg.*` path or a flow/global context
  key works.

Run from the `noderun` module root:

```bash
go run ./examples/template-message
```

Expected output:

```text
Hello world, from the template node!
```
