# Wait Notify

A `noderun` workflow demonstrating the distributed coordination pair —
one branch parks a message, another releases it:

```text
inject → wait (signal "job-1") → debug
inject → notify (signal "job-1")
```

The branches never meet in the graph: the **wait** node parks its
message against a global signal key, and the **notify** node increments
that key in a durable coordination store. When the count reaches the
wait's `targetCount`, the parked message is released downstream.

Things to notice:

- Coordination is **order-independent and durable**: if the notify ran
  first, the count would be stored and the waiter would consume it
  immediately — a surviving waiter even resumes across engine restarts.
- The engine needs stores wired explicitly — there is no in-memory
  fallback: `engine.CoordinationStore = noderun.NewNeatCoordinationStore(db)`
  and `engine.DurableInboxStore = noderun.NewNeatInboxStore(db)` over a
  sqlite database.
- `signal` accepts a mustache template like `{{payload.id}}`, so the
  waiter's identity can come from the message. Keys are global —
  shared across workflows *and* runners.
- `targetCount` is the number of signal increments a release consumes:
  `"2"` means two notifiers must arrive. `timeoutSeconds` routes an
  expired wait to `output_2` instead of waiting forever.
- `notify`'s `cacheKeys` copies chosen message metadata into the signal
  cache; `mergeMode` on the wait side decides whether cached
  notification metadata overrides the waiting message's own fields.

Run from the `noderun` module root:

```bash
go run ./examples/wait-notify
```

Expected output:

```text
the parked message is free!
```
