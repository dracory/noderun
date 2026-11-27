# Partial Deploy

A `noderun` workflow demonstrating `DeployWorkflowPartial` — Node-RED's
"modified nodes" deploy mode:

```text
httpTrigger (GET /hello) → template → httpResponse
```

The example deploys a flow answering `Hello v1!`, calls it, then
partial-redeploys with only the template text changed — and calls the
same endpoint again to get `Hello v2!` on the *same* port.

Things to notice:

- The incoming flow is **diffed against the running one**: only changed
  nodes are stopped and re-activated. Here the template node restarts;
  the httpTrigger's config is byte-identical so its executor — and its
  bound TCP listener — survives untouched.
- That's the payoff for long-lived nodes (`Stoppers`): tcpIn/mqttIn
  sockets, inject timers, queue drainers don't drop connections or lose
  state when an unrelated node is edited.
- On a cold deploy (nothing running), `DeployWorkflowPartial` is
  identical to a full `DeployWorkflow`.
- Per-node state and statuses survive for untouched nodes — only the
  dirty node IDs are activated.

Run from the `noderun` module root:

```bash
go run ./examples/partial-deploy
```

Expected output:

```text
Hello v1!
Hello v2!
```
