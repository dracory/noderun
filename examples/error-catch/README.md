# Error Catch

A `noderun` workflow demonstrating error handling with the catch node:

```text
inject → function (fails)     ← the happy path dies here
catch (scope: all) ──────────→ debug
```

The function node raises an error with `node.error('something went wrong')`.
Its branch aborts, but the engine routes the failure to every catch node in
scope as a message shaped `{error: {message, source}, originalData}`.

The debug node drills straight into that envelope — its `complete` option is
the dotted path `payload.error.message`, so only the error text is printed.

Run from the `noderun` module root:

```bash
go run ./examples/error-catch
```

Expected output:

```text
function node.error: something went wrong
```
