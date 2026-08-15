# F3a — RGD Apply Reconcile via Graph Engine

## What F3a proves

An RGD's composition (template resources with CEL references) translated into a
Graph, applied to a **real cluster** through `executor.Simple.Apply` in envtest,
produces the correct Kubernetes objects.

Specifically, the envtest in `test/integration/graphengine/suites/core/rgd_reconcile_test.go`:

1. Synthesises and serves the `WebApp` CRD via `rgdadapter.EnsureInstanceCRD`.
2. Creates a `WebApp` instance (`spec.name = "demo-app"`) in the cluster.
3. Calls **`rgdadapter.BuildRuntimeForInstance(rgd, instance, compiler)`** —
   the F6 controller entrypoint — which translates the RGD → Graph, prepends
   the `schema` def node for the instance, compiles, and builds the runtime.
4. Runs `executor.Simple.Apply(ctx, rt, watchrouter.NoopWatcher{})`.
5. Asserts against the live cluster:
   - `cm1` exists with `data.app == "demo-app"` (instance spec via schema def node)
   - `cm2` exists with `data.ref == "cm1"` (cross-node CEL reference)
   - `ApplyResult.Applied` carries both managed resources (NodeID + UID populated)

## Adapter entrypoint for F6

```go
// pkg/graphengine/rgdadapter/runtime.go
func BuildRuntimeForInstance(
    rgd     *v1alpha1.ResourceGraphDefinition,
    instance *unstructured.Unstructured,
    c       Compiler,
) (*runtime.Runtime, *v1alpha1.Graph, error)
```

This is the single call the F6 controller will make per reconcile cycle:

1. `ResourceGraphDefinitionToGraph(rgd)` — RGD resources → Graph nodes
2. `InstanceSchemaNode(instance)` prepended — `${schema.spec.*}` references work
3. `g.ObjectMeta` set from the instance (name + namespace for executor namespace-defaulting)
4. `c.Compile(g)` — Graph engine compiler
5. `runtime.New(prog, g)` — runtime ready for `executor.Simple.Apply`

The `Compiler` interface is a one-method subset of `*compiler.Compiler` so
production code and tests can both use it without circular imports.

## What F3b (status projection + conditions) still needs

### Status projection

After `executor.Simple.Apply` returns, the RGD's `schema.status` CEL expressions
(e.g. `${database.status.endpoint}`) must be evaluated against the observed
cluster state and written back onto the instance object via a status patch.

Work needed:
- Parse `rgd.Spec.Schema.Status` (SimpleSchema → field → CEL expression map).
- Walk that map; for each expression, evaluate it against `rt.Scope()` (which
  is populated by the executor's `publishScope` calls after each `SetObserved`).
- Collect results into a `map[string]any` and patch the instance's `.status`
  via `env.Client.Status().Patch(...)` with SSA.

This is purely additive to the adapter — `BuildRuntimeForInstance` stays
unchanged; the controller calls it then does the status patch itself.

### Conditions

The RGD controller currently authors four conditions on instances:
`Accepted`, `Progressing`, `Deployed`, `Ready` (via `runtime.newCondition`
helpers in the RGD runtime package).

For the Graph-engine path (F3b+), an equivalent set of conditions must be
authored on the **instance** object (not a Graph CR). The implementation
should read the `ApplyResult` and any `ErrNotReady` from `executor.Apply`
and translate them into the appropriate condition values. The helper
can live in `pkg/graphengine/rgdadapter` to keep it co-located with
`BuildRuntimeForInstance`.

### Prune / delete

`ApplyResult.Applied` now carries the managed resource list. F3b or later
needs to persist this list (e.g. on the instance's status or in an
annotation) and call `executor.Simple.Delete(ctx, previousResources)` on
reconcile cycles where resources were removed (forEach shrunk, includeWhen
flipped false) or on instance deletion.

## Beyond F3b

- **Ref/Watch nodes**: `executor.Simple.Apply` returns `ErrUnsupported` for
  `NodeKindRef`/`NodeKindWatch`. A future increment must wire cluster-fetch
  (GET by identity) for ref nodes before `ResourceGraphDefinitionToGraph`
  can safely produce ref nodes from externalRef resources.
- **Controller wiring (F6)**: the F6 controller watches instances of each
  RGD's served CRD and drives `BuildRuntimeForInstance` → `executor.Apply`
  → status-patch in a reconcile loop, replacing the current RGD instance
  controller.
