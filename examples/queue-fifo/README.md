# Queue FIFO

A `noderun` workflow demonstrating the engine-managed queue node — a
durable, inspectable staging buffer with a hold/release gate:

```text
inject [1,2,3] → split → queue (hold) → debug
```

The example deploys the flow with the queue **held**, waits until all
three items have staged, then releases the gate via the inspection API —
the drain delivers the items downstream in FIFO order.

Things to notice:

- Queue nodes are **engine-managed**, not ordinary executors: deliveries
  to the node are enqueued and a drainer forwards them to outgoing edges
  in order. `engine.QueueHandle(workflowID, nodeID)` returns the live
  `QueueInspector` (depth, item listing, `SetHold`, per-item
  delete/requeue/release).
- `hold: true` starts the gate closed — items accumulate until released.
  The same mechanism backs the workflow's problem queue, which is held
  by default.
- `backend: "dir"` stages each item as a `{seq}-{msgid}.json` file —
  inspectable with plain OS tools. `backend: "durable"` uses the
  engine's configured `DurableInboxStore` instead (shared, multi-runner).
- `capacity` and `policy` (`block` / `drop-newest` / `drop-oldest` /
  `route-to-catch`) bound the queue — unbounded and blocking by default.

Run from the `noderun` module root:

```bash
go run ./examples/queue-fifo
```

Expected output (the three payloads drained in order):

```text
1
2
3
1,2,3
```
