# Complete Status

A `noderun` workflow demonstrating the two lifecycle event nodes:

```text
inject → change → debug                 ← main branch
status (watching change) → debug        ← status observer
complete → change → debug               ← complete observer
```

Neither observer is wired to the main branch — the engine feeds them:

- The **status** node receives a message every time a node in its scope
  changes state (`running` → `ok` → `error`). Here it is scoped to the
  change node only, so it prints two `{"nodeId","status"}` objects: one
  `running`, one `ok`.
- The **complete** node fires once the injected message's downstream
  traversal has fully drained — the flow-level equivalent of "the work is
  done". Its branch prints `main branch finished`. It is scoped to the
  inject node on purpose: every observer traversal draining is itself a
  completion, so a scope-all complete node would re-fire for each status
  message too — scoping to the trigger keeps it exactly-once.

Both observer messages carry recursion guards (`fromStatus` / `fromComplete`
metadata), so observers cannot re-trigger themselves.

Run from the `noderun` module root:

```bash
go run ./examples/complete-status
```

Expected output (observer ordering may vary):

```text
doing work
{"nodeId":"change-main","status":"running"}
{"nodeId":"change-main","status":"ok"}
main branch finished
```
