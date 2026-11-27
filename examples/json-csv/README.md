# JSON CSV

A `noderun` workflow demonstrating the two structured-data parser nodes on
independent branches:

```text
inject "{\"name\": \"ada\", \"count\": 2}" → json → debug
inject "name,age\nada,30\ngrace,36"       → csv  → debug
```

Things to notice:

- **json**: the default `action` (empty) is auto — strings get parsed into
  objects, objects get stringified. `action: "str"` / `"obj"` force one
  direction. `property` can point at any dotted path inside the payload,
  and `outputs: 2` adds a second port that receives messages whose JSON
  failed to parse (with the error in `msg.error`).
- **csv**: `headers: true` treats the first row as a header so rows become
  objects keyed by column name. `multi: "mult"` returns the whole table as
  an array of row objects in a single message (with the column list in
  `msg.columns`); the default mode instead implements `MessageExpander`
  and emits one message per row. Numeric-looking cells are coerced to
  numbers — `"30"` parses as `30`, which is why the ages print unquoted.
- The debug node JSON-marshals non-string payloads for display, so both
  parsed structures print as canonical JSON.

Run from the `noderun` module root:

```bash
go run ./examples/json-csv
```

Expected output (branch ordering may vary):

```text
{"count":2,"name":"ada"}
[{"age":30,"name":"ada"},{"age":36,"name":"grace"}]
```
