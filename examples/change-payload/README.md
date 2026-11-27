# Change Payload

A minimal `noderun` workflow demonstrating the change node:

```text
inject → change → debug
```

The inject node fires once with the payload `Hello world!`. The change node
applies a replace rule that rewrites `world` to `change node`. The debug node
prints the changed payload to stdout.

Run from the `noderun` module root:

```bash
go run ./examples/change-payload
```

Expected output:

```text
Hello change node!
```
