# Durable Inbox Resume

A `noderun` workflow demonstrating at-least-once delivery across a
stop/restart:

```text
inject → delay (1s) → debug
```

The example deploys the workflow, lets the injected message land
mid-flight inside the delay node's inbox, then **stops the workflow
before the delay drains**. On redeploy, the engine adopts the surviving
durable inbox row and the message completes its route — printing only
after the "restart".

Things to notice:

- With `engine.DurableInboxStore` set, delivery is **durable by
  default** — every hop between nodes persists a row in the store, and
  a row is only deleted when the receiving node dequeues it.
- On deploy, `loadDurableInboxRows` finds rows surviving from the
  previous run and `requeueDurable` admits them back — you'll see a
  `re-queuing 1 durable inbox message(s)` info event in the snapshot.
- This is at-least-once, not exactly-once: a message is never lost, but
  a crash between delivery and dequeue can replay it.
- `NewNeatInboxStore(db)` is the sqlite-backed store; here both runs
  share one database file, which is what makes the row "survive".

Run from the `noderun` module root:

```bash
go run ./examples/durable-inbox-resume
```

Expected output (after the redeploy's delay completes):

```text
Hello after restart!
```
