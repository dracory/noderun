# Split Join

A `noderun` workflow demonstrating message sequences with the split and join nodes:

```text
                    ┌─→ debug (each item)
inject [1,2,3] → split
                    └─→ join → debug (joined array)
```

The inject node fires once with the JSON array `[1, 2, 3]`. The split node
expands it into a sequence of three messages, each carrying `parts` metadata
(group id, index, count). One branch prints every item as it passes; the
other feeds the join node, which buffers the sequence until all three parts
arrive and reassembles the original array.

Run from the `noderun` module root:

```bash
go run ./examples/split-join
```

Expected output:

```text
1
2
3
[1,2,3]
```
