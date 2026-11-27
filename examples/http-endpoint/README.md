# HTTP Endpoint

A `noderun` workflow that serves a real HTTP endpoint:

```text
httpTrigger (GET /hello) → template → httpResponse
```

The example picks a free local port, deploys the workflow, calls
`GET /hello?name=world` against the engine's own listener, prints the
response body, then stops the workflow.

Things to notice:

- `listener: "own"` binds a private TCP listener on the configured port.
  `listener: "main"` instead mounts the route on the host's shared admin
  server — the Node-RED model where routes live on the admin port behind
  admin auth.
- For GET requests the parsed **query object** becomes `msg.payload`, so
  `?name=world` renders through `{{payload.name}}`. Body-capable methods
  (POST/PUT/PATCH) carry the parsed request body instead. `msg.method`,
  `msg.path`, `msg.query`, `msg.headers` and a `msg.req` object are in
  metadata.
- The request/response correlation is implicit: `httpTrigger` injects
  the message with a `msgId`, and the engine holds the HTTP handler open
  until a downstream `httpResponse` node resolves that correlation
  (`responseTimeout` bounds the wait, default 15s → 504).
- Response status and headers can also be set per message via
  `msg.statusCode` / `msg.headers` — they take precedence over the
  httpResponse node's config.

Run from the `noderun` module root:

```bash
go run ./examples/http-endpoint
```

Expected output:

```text
Hello world!
```
