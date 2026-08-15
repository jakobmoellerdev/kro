# F6b — Graph engine at parity; classic instance engine removed

F6b is complete. Instance reconciliation now runs **only** through the Graph
engine. The classic runtime (`pkg/runtime`) and the classic instance reconcile
path are deleted, and the `RGDOnGraph` feature gate is gone — there is no longer
a flag or a second path.

This closes the F6b work items listed at the end of
[`f6a-routing.md`](./f6a-routing.md).

## Terminal state

- `pkg/controller/instance/controller.go` routes every non-deleted,
  non-suspended instance through `reconcileViaGraphEngine`. Deletion is handled
  before the engine branch by the shared, runtime-independent `reconcileDeletion`
  (persisted ApplySet inventory); suspension is handled by a shared
  `reconcileSuspended` branch.
- Deleted: `pkg/runtime/{runtime,node,node_*,collection,conditions,eval,state,
  errors}.go` and `pkg/controller/instance/{resources,resources_collection,
  resources_external}.go`, `ReconcileContext`, and the `RGDOnGraph` gate.
- Kept: `pkg/runtime/resolver` (imported by `pkg/graphengine/runtime`),
  `DeletionContext`, the shared deletion path, `ConditionsMarker`, telemetry,
  and all of `pkg/graph` (used by `rgdadapter` and the RGD controller for CRD
  synthesis / topological order).

## f6a "remaining work" → status

| # | Item | Status |
|---|------|--------|
| 1 | Finalizer + management labels | Done (`stampInstanceMetadata`; child labeler stamps `managed-by`, `node-id`, instance-* and `part-of`) |
| 2 | ApplySet inventory + prune | Done (`reconcileApplySetInventory`: superset `Project`, `ListOrphans`/`DeleteOrphan` in reverse apply-order, shrink only after a conflict-free prune) |
| 3 | kro-owned conditions | Done (`InstanceManaged`/`GraphResolved`/`ResourcesReady`/`Ready` via the shared `ConditionsMarker`) |
| 4 | Suspend annotation | Done (`reconcileSuspended`, engine-agnostic, mirrors classic `ReconciliationSuspended`) |
| 5 | Status idempotency | Done (`statusesMatch` skip-write) |
| 6 | Author-condition stamp/merge | Done (`ProjectInstanceConditions` with data-pending skip, dedup, degraded surfacing, generation stamping) |
| 7 | externalRef selector collections | Done (Route A — see below) |
| 8 | Deletion via inventory | Done (shared `deletion.go`, engine-agnostic) |
| 9 | Drop the old path | Done (this change) |

## Parity work landed in F6b

- **Schema-var typing** — the `schema` def node is typed from the RGD
  SimpleSchema (`compiler.CompileWithOptions` + `WithNodeSchemaOverride`,
  `graph.InstanceSchemaForCEL`), so a fresh instance missing optional fields
  still compiles and CEL overloads match the classic builder.
- **Full instance metadata** — `InstanceSchemaNode` exposes the instance's
  whole `metadata` (uid, labels, annotations, generation, …) as
  `${schema.metadata.*}`.
- **Zero-resource RGDs** — NoOp / arbitrary-object RGDs translate (the `schema`
  def node keeps the compiled Graph non-empty).
- **Condition + status projection** — data-pending skip, dedup, degraded
  surfacing; status env built from all node IDs with per-field data-pending
  skip and literal copy-through (partial-field visibility).
- **External refs** — single-object `ref` reads (read-only GET + watch +
  publish) and, via **Route A**, selector **collections**: a `ref`/`externalRef`
  carrying a selector is modeled as `compiler.Node.Collection` (list-by-selector,
  publish a `[]any`, one selector watch), which also supports `matchExpressions`
  (which the old `WatchSpec` could not).
- **forEach collections** — `each` typed in the collection `readyWhen` compile
  env; per-item `each` runtime readiness with an observed-count gate;
  collection-index/size labels; one selector-based collection watch per node
  (instead of colliding per-item scalar watches); per-item apply tolerance.
- **Readiness gating** — dependents are withheld until each dependency's
  `readyWhen` passes (opt-in `Simple.GateReadiness`, enabled only on the RGD
  path; the generic Graph engine keeps applying across a not-ready upstream so
  drift watches register).
- **Terminating resources** — a managed object with a `deletionTimestamp` gates
  downstream and reports `ResourceDeleting`.
- **Apply-order** — one-based reverse-topological `ApplyOrder` (excluding def
  nodes), stamped as `internal.kro.run/apply-order` for the deletion path.
- **Suspend requeue** — `reconcileSuspensionChangedInUpdate` force-enqueues on
  both suspend transitions; the Graph engine does not periodically requeue, so
  the suspended status is written promptly on the `enabled→suspended` edge.

## Accepted divergences from the classic path

- **Readiness gating** is a deliberate RGD-vs-generic-engine divergence
  (RGD gates dependents on upstream readiness; the generic engine does not).
- **Suspend** does not re-evaluate author-condition CEL while suspended
  (reconciliation is suspended); previously-persisted author conditions are
  preserved with kro's `ResourcesReady` overlaid.
- **`NodeKindWatch` removed** — the experimental Graph `watch:` node (an
  always-`ErrUnsupported` executor stub) is deleted; ref-collections supersede
  it. See the removal commit.

## Verification

| Check | Result |
|---|---|
| `go build ./...`, `go vet ./...` | PASS |
| Unit (`go test ./...`) | PASS (2912) |
| Integration — Core | 157/157 |
| Integration — DeploymentService (incl. suspend), NetworkingStack, EKSCluster | PASS |
| Integration — GraphRevision | 7/7 |
| Generic graphengine suite | PASS |
| e2e (chainsaw) | 10/10 GE-relevant (the 11th, `check-rgd-deletion`, is a deploy-flag artifact: `deploy-kind-helm` sets `allowCRDDeletion=true`, contradicting the test's CRD-retention premise) |
| `pkg/runtime` non-test importers | 0 (only `pkg/runtime/resolver`, used by `pkg/graphengine/runtime`) |

## Notes / follow-ups

- **`DeletionPolicy`** is a dead/reserved `ReconcileConfig` field — never
  consumed by either engine (deletion always prunes orphans). If `Retain`/
  `Orphan` semantics are ever needed they must be implemented in the shared
  `deletion.go`; not a flip concern.
- **Test coverage gap that was closed** — before the flip only the `core`
  suite wired `RGD_ON_GRAPH`→gate, so `deploymentservice`/`networkingstack`/
  `ackekscluster` never actually ran the Graph engine. Making the engine
  unconditional exercised them on GE and surfaced the suspend gap (now fixed).
- **`helm/crds/experimental.kro.run_graphs.yaml`** is a stale, untracked
  artifact from before the Graph API group rename (`experimental.kro.run` →
  `kro.run`); regen no longer produces it. Safe to delete locally.
- **`pkg/runtime/resolver`** could later move under `pkg/graphengine/` since it
  is now only used there.
