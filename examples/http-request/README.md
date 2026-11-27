# HTTP Request

A `noderun` workflow demonstrating the `httpRequest` node — with the
engine itself serving the target endpoint, so nothing external is needed:

```text
httpTrigger (GET /api) → template → httpResponse     ← server branch
inject → httpRequest (GET /api) → debug              ← client branch
```

Both branches live in one workflow on one engine: the trigger node binds
its own listener, and the request node calls it back.

Things to notice:

- The **httpRequest** response payload is an object:
  `{statusCode, status, headers, body, bodyTruncated, duration}` —
  this example's debug node prints just `payload.body`.
- If the response `Content-Type` is JSON (including `application/*+json`
  variants), `body` arrives **already parsed** as an object, not a string.
- An empty `url` config can be supplied per message (`msg.url`) — Node-RED
  parity — and `username`/`password` are credential-typed fields for
  HTTP Basic auth, stored outside the exported flow.
- The client branch is fired manually via `FireInjectNode` after a TCP
  probe confirms the trigger listener is bound — the inject must not
  race the listener, or the request would hit a port that isn't up yet.

Run from the `noderun` module root:

```bash
go run ./examples/http-request
```

Expected output:

```text
Hello from the endpoint!
```
