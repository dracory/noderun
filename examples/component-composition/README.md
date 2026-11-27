# Component Composition

A `noderun` workflow demonstrating reusable components — one definition,
two expanded instances:

```text
inject "alpha" → component:def-wrap → debug
inject "beta"  → component:def-wrap → debug

def-wrap (definition): in → function "[" + payload + "]" → out
```

Things to notice:

- A node of type `"component"` carries the **definition**: `in`/`out`
  port arrays wiring into an internal `nodes`/`edges` graph, an optional
  `env` parameter list, and a name. Definition nodes are templates —
  they never execute.
- An instance node's type is `component:<defId>`. At deploy,
  `ExpandComponents` replaces each instance with a **namespaced copy**
  of the internal graph (`inst-a` gets `inst-a/wrap`) and grafts the
  outside edges onto the port endpoints.
- Each instance is fully independent — its own nodes, state, node
  context. Definitions can declare `env` vars that instances override
  via their `env` data, including `env`-typed reads of the process
  environment when `engine.AllowOSEnv` is on.
- Expansion is bounded: nested components are allowed, but total
  expanded nodes are capped (`maxExpandedNodes`) and recursion is
  rejected.

Run from the `noderun` module root:

```bash
go run ./examples/component-composition
```

Expected output (branch ordering may vary):

```text
[alpha]
[beta]
```
