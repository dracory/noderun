# Function Transform

A minimal `noderun` workflow demonstrating the function node:

```text
inject → function → debug
```

The inject node fires once with the payload `hello`. The function node runs
JavaScript that uppercases the payload and appends ` WORLD!`. The debug node
prints the transformed payload to stdout.

Run from the `noderun` module root:

```bash
go run ./examples/function-transform
```

Expected output:

```text
HELLO WORLD!
```
