# File Write Read

A `noderun` workflow that writes a file and reads it back in one flow:

```text
inject "Hello file!" → file (write) → fileIn (read) → debug
```

The write goes to a file in the OS temp directory, which the example
cleans up afterwards.

Things to notice:

- The **file** node writes `msg.payload` to its configured `filename`
  and forwards the message with `msg.filename` set — so a downstream
  **fileIn** node with `filenameType: "msg"` needs no path of its own;
  it just reads whatever the writer wrote. `filenameType` also accepts
  `"flow"`/`"global"` to read the path from a context scope.
- `createDir: true` makes the writer create missing parent directories
  (`mkdir -p` semantics). `appendNewline` adds a trailing newline on
  each write when set.
- The reader's `format` option selects how file contents land in
  `msg.payload` (text by default).

Run from the `noderun` module root:

```bash
go run ./examples/file-write-read
```

Expected output:

```text
Hello file!
```
