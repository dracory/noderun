# Hello World

A minimal `noderun` workflow:

```text
inject → debug
```

The inject node fires once when the workflow is deployed with the payload
`Hello world!`. The debug node receives that payload and prints it to stdout.

Run from the `noderun` module root:

```bash
go run ./examples/hello-world
```

Expected output:

```text
Hello world!
```
