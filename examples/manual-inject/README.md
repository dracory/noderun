# Manual Inject

A minimal `noderun` workflow demonstrating manual injection:

```text
inject → debug
```

Unlike the `hello-world` example, the inject node is configured with
`once: false`, so it does not fire when the workflow is deployed. The example
deploys the workflow, calls `engine.FireInjectNode()`, and the debug node
prints the injected payload to stdout.

Run from the `noderun` module root:

```bash
go run ./examples/manual-inject
```

Expected output:

```text
Hello from a manual inject!
```
