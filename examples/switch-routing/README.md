# Switch Routing

A `noderun` workflow demonstrating conditional routing with the switch node:

```text
inject (42) ──╮
              ├─→ switch ──output_1 (gt 30)──→ change "hot"  ──╮
inject (10) ──╯        └─output_2 (otherwise)─→ change "cold" ──┴─→ debug
```

Two inject nodes fire once on deploy with numeric payloads `42` and `10`.
The switch node routes messages above 30 to its first output and everything
else to the otherwise port. Each branch formats the reading with a JSONata
change rule, and both branches converge on one debug node printing to stdout.

Run from the `noderun` module root:

```bash
go run ./examples/switch-routing
```

Expected output (order may vary):

```text
42 is hot (above 30)
10 is cold (30 or below)
```
