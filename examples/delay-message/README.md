# Delay Message

A `noderun` workflow demonstrating the delay node:

```text
inject → delay (500ms) → debug
```

The inject node fires once on deploy. The delay node holds the message for
half a second before releasing it downstream, so the print appears noticeably
after the program starts instead of instantly.

The example also verifies the hold actually happened: `run()` fails if the
message arrives faster than the configured delay allows — proving the node
did not act as a pass-through.

Run from the `noderun` module root:

```bash
go run ./examples/delay-message
```

Expected output (about half a second after start):

```text
Hello after a delay!
```
