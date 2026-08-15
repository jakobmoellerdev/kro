# F3b — Instance Status Projection (author conditions)

## What status projection does

After `executor.Apply` finishes, `rt.Scope()` holds each reconciled node's
observed value keyed by node ID.  `ProjectInstanceStatus` (in
`pkg/graphengine/rgdadapter/status.go`) projects the RGD's
`spec.schema.status` block onto a `map[string]any` by:

1. Unmarshalling `rgd.Spec.Schema.Status.Raw` to a `map[string]interface{}`.
2. Calling `graph.ParseStatusExpressions` (the new thin exported wrapper in
   `pkg/graph/status_expressions.go`) which:
   - calls the unexported `extractConditionExpressions` (removes `conditions:`)
   - calls `parser.ParseSchemalessResource` to get `(path, inner-CEL-expr)` pairs
3. Building a transient `*cel.Env` via `krocel.DefaultEnvironment` with every
   scope key declared as an untyped (`dyn`) variable and `WithRuntimeLibrary(false)`.
4. For each field: `env.Parse → env.Check → env.Program → expr.Eval(scope)` → sets
   value at the dotted path in the output map.

The entrypoint F6's controller calls is:

```go
status, err := rgdadapter.ProjectInstanceStatus(rt, rgd)
// then: unstructured.SetNestedField(instance.Object, status, "status") + patch
```

## Author conditions

`ProjectInstanceConditions` evaluates `status.conditions` entries (each a
`${runtime.newCondition(…)}` expression) and returns `[]metav1.Condition`.

**Gap / scoreboard rows 15, 17**: The Graph engine compiler calls
`WithRuntimeLibrary(false)` — `library.Runtime()` / `runtime.newCondition` is
deliberately excluded from the Graph CEL environment.  Condition expressions
live in the RGD, not in Graph nodes, so there is no compiled `cel.Program`
cached in the runtime for them.  `ProjectInstanceConditions` works around this
by building a **fresh** `*cel.Env` with `WithRuntimeLibrary(true)` on every
reconcile and re-compiling the condition expressions at projection time.

A second subtlety: `krocel.Expression.Eval` routes through
`conversion.GoNativeType`, which does not handle `*library.Condition` ref.Val
and returns `ErrUnsupportedType`.  `evalConditionExpr` therefore calls
`cel.Program.Eval` directly and pattern-matches the raw `ref.Val` to
`*library.Condition`.

Once F6 adds a per-RGD CEL cache, this per-reconcile compilation can be
amortised.  Correctness is unaffected.

## pkg/graph export added

`pkg/graph/status_expressions.go` — `ParseStatusExpressions` and
`StatusFieldExpr`.  Single-purpose, documented, no logic copied from builder;
thin wrapper over `extractConditionExpressions` (unexported in builder.go)
and `parser.ParseSchemalessResource` (already exported).

## Scoreboard rows moved

| Row | Description                          | State after F3b |
|-----|--------------------------------------|-----------------|
| 19  | Status block parsed from RGD         | DONE            |
| 28  | Status fields projected from scope   | DONE            |
| 29  | Status set on instance               | DONE (adapter)  |
| 15  | Author conditions compiled           | PARTIAL (re-compiled per reconcile, not cached) |
| 17  | Conditions set on instance           | DONE (adapter)  |

Row 29 and 17 require the F6 controller to call these functions and patch the
instance; the adapter itself is complete.

## Test result

```
--- PASS: TestProjectInstanceStatus_ResourceField
--- PASS: TestProjectInstanceStatus_SchemaField
--- PASS: TestProjectInstanceStatus_MultiField
--- PASS: TestProjectInstanceStatus_NoStatus
--- PASS: TestProjectInstanceConditions_Basic
```

Full `go test ./pkg/... -count=1`: all green.
