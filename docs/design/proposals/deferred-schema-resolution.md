# Deferred (Soft) Schema Resolution for Unresolved CRDs

## Problem statement

An RGD today must have **every** referenced resource type (CRD) present on the
cluster at compile time. Schema resolution is eager and all-or-nothing: if any
one resource's GVK cannot be resolved, the entire graph build fails, the RGD is
marked `ResourceGraphInvalid`, and — critically — the RGD's **own** instance CRD
is never created.

This blocks true composable, self-bootstrapping platforms. The canonical case
([#1293](https://github.com/kubernetes-sigs/kro/issues/1293), building on
[#1243](https://github.com/kubernetes-sigs/kro/issues/1243)):

```yaml
resources:
  - id: operator          # deploys an operator that registers CRDs (e.g. addons.example.io)
    template: { apiVersion: platform.example.io/v1alpha1, kind: InstallOperator, ... }

  - id: addon             # uses a CRD that only exists AFTER `operator` is Ready
    includeWhen:
      - ${operator.status.conditions.exists(c, c.type == 'Ready' && c.status == 'True')}
    template: { apiVersion: platform.example.io/v1alpha1, kind: InstallAddon, ... }
```

`InstallAddon`'s backing CRD does not exist until `operator` is running. Because
schema resolution happens for *all* resources up front, the whole RGD fails with
`schema not found`, its `MyPlatform` CRD is never served, so `operator` can never
be instantiated — a deadlock that only external orchestration (scripts,
`kubectl wait`, sync-waves) can currently break.

We already have **soft dependencies**: `includeWhen` skips a resource at runtime
when its condition is false or references not-yet-available data (`ErrDataPending`).
What's missing is **soft schema resolution**: letting a resource whose *type* is
not yet known defer instead of failing the whole graph.

### Why the existing machinery almost solves this

kro already contains a complete "type not known until later" path — it's just
currently reachable only for **dynamic-GVK templates** (templates whose
`apiVersion`/`kind` is a CEL expression):

| Concern                        | Existing mechanism                                            | Location                                               |
|--------------------------------|---------------------------------------------------------------|--------------------------------------------------------|
| Compile a node with no schema  | `DynamicGVK` node, schema `nil`, typed as `dyn`               | `compiler/context.go: buildDynamicTemplateNode`        |
| Resolve GVR at apply time      | `mappingFor` → `RESTMapper`                                   | `executor/simple.go:~755`                              |
| "CRD not here yet" soft signal | `errSchemaNotReady` (from `meta.IsNoMatchError`)              | `executor/simple.go:~406`                              |
| Don't prune, requeue           | `ApplyResult.Unresolved` + soft requeue                       | `executor/simple.go`, `controller/graph/controller.go` |
| Re-trigger when CRD lands      | `SchemaWatcher` re-enqueues affected graphs                   | `schemawatcher/watcher.go`                             |
| Skip a resource entirely       | `includeWhen` → `Node.IsIgnored()` (contagious to dependents) | `runtime/node.go:82`                                   |
| Graceful status degradation    | per-field `isDataPendingCEL` skip                             | `rgdadapter/status.go`                                 |

**The core idea of this proposal: extend that same deferral path from
dynamic-GVK templates to *static* templates whose CRD is merely absent.**

## Proposal

Introduce **deferred resources**: a resource whose schema cannot be resolved at
compile time does not fail the build. It compiles into a `Deferred` node
(schema-less, typed `dyn`, GVR resolved lazily at apply time), the RGD's own CRD
is served, and the resource is applied automatically once its CRD appears —
reusing the dynamic-GVK runtime rails end to end.

Deferral is **opt-in** and safe by construction: it never silently masks a typo
in an always-required resource.

#### Overview

Three coordinated changes:

1. **Compile-time (both compilers): soft-fail unresolved schemas → `Deferred` node.**
   In `pkg/graphengine/compiler/context.go:buildNode` and
   `pkg/graph/builder.go:buildResourceNode`, when `ResolveSchema`/`RESTMapping`
   fail with a *"type not found"* error (see error taxonomy below) **and** the
   resource is eligible for deferral, build a schema-less node (`Deferred=true`,
   schema `nil`, GVR zero) instead of returning an error. Everything downstream
   already tolerates a schema-less node (it lands in the `dyn` identifier set).

2. **RGD lifecycle: serve the CRD despite deferred nodes.**
   Because a build with deferred nodes now *succeeds*, the GraphRevision compiles
   to `Active`, `ensureServingState` runs, and the instance CRD is created and the
   micro-controller starts — exactly as issue #1293 asks. No reordering of the
   reconcile is required; the fix is that the build no longer errors.

3. **Runtime: defer, then heal.**
   The executor's existing `mappingFor` lazy path is generalized so **any**
   `Deferred` node (not only CEL-GVK ones) resolves its GVR via the `RESTMapper`
   at apply time. Still missing → `errSchemaNotReady` → node goes to
   `Unresolved`, soft requeue, no prune. When the CRD lands, the `SchemaWatcher`
   invalidates caches and re-enqueues; the node now maps and applies.

#### Eligibility: when is a resource allowed to defer?

Deferral must be intentional. A resource is eligible when **any** of:

1. **It carries `includeWhen`.** This is issue #1293's *preferred* semantics: an
   author who conditionally includes a resource is already signalling that the
   resource may not always apply, so requiring its type to exist unconditionally
   is wrong. This makes the motivating example work with **no new field**.
   *Refinement:* only defer when `includeWhen` references another **resource**
   (a runtime signal, e.g. `operator.status...`), not purely `schema.*` — a
   condition that depends only on instance input is knowable and shouldn't hide a
   missing type. (`includeWhen` resource references already exist and participate
   in the DAG, per the `include-when-resource-references` KREP / #1104.)

2. **It sets an explicit marker** (issue #1293 alternative #1). New optional
   field on a resource:

   ```yaml
   - id: addon
     optional: true          # or: deferSchemaResolution: true
     template: { apiVersion: platform.example.io/v1alpha1, kind: InstallAddon, ... }
   ```

   This covers resources that legitimately have no `includeWhen` but whose CRD is
   installed out-of-band.

All deferral is additionally behind an alpha **feature gate**
`DeferredSchemaResolution` (default off) so cluster operators opt in globally
before any RGD can defer. When the gate is off, unresolved schemas fail exactly
as today.

A resource that is *not* eligible and whose schema is missing still hard-fails —
preserving today's typo-catching behaviour for always-required resources.

#### Design details

##### 1. Error taxonomy — distinguishing "absent" from "broken"

We must only defer on genuine *absence*, never on a malformed template or a
transient discovery outage (deferring those would hide real errors).

- **`RESTMapping`** already returns a typed sentinel: `meta.IsNoMatchError(err)`
  (`*meta.NoKindMatchError`). This is the reliable "GVK not registered" signal
  and is exactly what the executor's dynamic path already keys on.
- **`ResolveSchema`** (upstream `resolver.ClientDiscoveryResolver`) returns a
  plain, untyped error for unknown GVKs. We wrap kro's resolver
  (`pkg/graph/schema/resolver`) to translate an unknown-GVK miss into a new
  exported sentinel `resolver.ErrSchemaNotFound`, so callers detect absence
  without string matching. This is a small, well-contained change in the resolver
  package (it already has the cache/singleflight seam).
- **Gotcha — `DynamicRESTMapper` and group-discovery failures.** controller-runtime's
  dynamic mapper has historically returned `ErrGroupDiscoveryFailed` (not
  `NoKindMatchError`) when an entire API *group* is absent, and can transiently
  fail under apiserver unavailability
  ([controller-runtime #2424](https://github.com/kubernetes-sigs/controller-runtime/issues/2424),
  [#2571](https://github.com/kubernetes-sigs/controller-runtime/pull/2571)). We treat
  **only** `IsNoMatchError` + `ErrSchemaNotFound` as "absent → defer"; a
  discovery/transport error is a *transient* failure → normal error + requeue
  (never a silent defer). This keeps a flaky apiserver from being mistaken for a
  missing CRD.

##### 2. Node model

Add one field to the compiled `Node` (`compiler/program.go`) and its classic
`graph` twin:

```go
// Deferred is true when the node's literal GVK could not be resolved at
// compile time (CRD absent) and the resource was eligible to defer. Unlike
// DynamicGVK, the literal GVK IS known — it is kept on Object so the schema
// watcher can subscribe to the exact GroupKind and the executor can retry it.
Deferred bool
```

`Deferred` differs from `DynamicGVK` in one useful way: the literal GVK **is**
known. So a deferred node contributes its concrete `GroupKind` to
`Program.RequiredGroupKinds` (in `emitSchemaDependencies`), letting the
`SchemaWatcher` subscribe to that **specific** CRD rather than falling back to
watch-all (`HasDynamicGVK`). Precise re-enqueue, no cluster-wide CRD fan-out.

Compile behaviour for a deferred node (mirrors `buildDynamicTemplateNode`):

- schema `nil` → node is absent from `NodeSchemas` → lands in the `dyn`
  identifier set → all its own field expressions type-check permissively, and
  cross-node references to it are `dyn` (same as dynamic-GVK today).
- `GVR` zero, `Namespaced` unknown. Namespaced-ness is resolved at apply time
  with the mapping; until then, executor namespace-defaulting for the node is
  deferred with it. The cluster-scope-vs-`metadata.namespace` validation
  (`context.go`) is likewise deferred to first successful mapping.
- `IncludeWhen`/`ReadyWhen`/`ForEach`/`Variables` are parsed exactly as normal.

##### 3. Compile paths — both of them

The RGD flows through **two** eager-resolution sites; both must learn to defer:

- `pkg/graph/builder.go: buildResourceNode` (classic builder) — used by the RGD
  controller's validation build **and** the GraphRevision controller's compile.
  This is the one that gates CRD serving: it must succeed for
  `resolveGraphRevisions` to reach `RevisionStateActive` and for
  `ensureServingState` to create the instance CRD.
- `pkg/graphengine/compiler/context.go: buildNode` (graph engine) — used per
  instance via `rgdadapter.BuildRuntimeForInstance`. This is the one the executor
  actually runs.

Both get the same `resolveOrDefer(gvk, eligible)` helper:

```go
sch, err := schemaResolver.ResolveSchema(gvk)
mapping, mErr := restMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
switch {
case err == nil && mErr == nil:
    // normal typed node
case eligible && isAbsent(err) && isAbsent(mErr):   // ErrSchemaNotFound / NoMatch
    // build Deferred node (schema nil, GVR zero), keep literal GVK on Object
default:
    // transient or malformed, or not eligible → hard error (today's behaviour)
```

Status-schema inference (`CompileSource` → `crd.SetCRDStatus`) simply skips
deferred nodes; status expressions that reference a deferred node degrade via the
existing per-field `isDataPendingCEL` skip.

##### 4. Runtime — generalize `mappingFor`

Today `mappingFor` takes the lazy `RESTMapper` branch only when
`n.DynamicGVK()`. Change the predicate to `n.DynamicGVK() || n.Deferred()`:

```go
if !n.DynamicGVK() && !n.Deferred() {
    return n.GVR(), n.Namespaced(), nil   // static, already resolved
}
gvk := obj.GroupVersionKind()             // from the rendered object (literal for Deferred)
m, err := s.Client.RESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version)
if err != nil {
    if meta.IsNoMatchError(err) {
        return ..., errSchemaNotReady      // → Unresolved, soft requeue, no prune
    }
    return ..., err
}
return m.Resource, m.Scope.Name() == meta.RESTScopeNameNamespace, nil
```

Everything after this point — `Unresolved` tracking, prune deferral
(`controller/graph/tracking.go`), soft requeue, `ResourcesConverged=False`
marker — is already in place and needs no change.

Interaction with `includeWhen`: `Node.IsIgnored()` is evaluated **before**
apply. A deferred node whose `includeWhen` is currently false is *ignored*
(contagiously skipping its dependents) and never even attempts a mapping — so the
motivating example spends zero effort on `addon` until `operator` is Ready. Once
`includeWhen` flips true, the node attempts to map; if the CRD is now present it
applies, otherwise it goes `Unresolved` and waits.

##### 5. Recovery / re-enqueue

- **RGD / Graph object:** `SchemaWatcher` already re-enqueues on CRD add/change
  and invalidates the schema + program caches. Because deferred nodes now publish
  their literal `GroupKind` to `RequiredGroupKinds`, the affected RGD is enqueued
  the moment the exact CRD is established — the graph recompiles, the previously
  deferred node becomes a normal typed node.
- **Instance gap (must close).** Investigation found the `SchemaWatcher` re-enqueues
  the *Graph/RGD* but **not** running *instances*; instances re-reconcile only on
  their own spec change, RGD-revision change, or watched-child drift. For
  self-bootstrapping this is usually covered anyway — a deferred node returns
  `ErrNotReady`, and the instance controller already soft-requeues on
  `ErrNotReady` (`controller_graph_engine.go` → `requeue.NeededAfter`), so it
  polls until the CRD appears. We nonetheless add a **belt-and-braces**: route
  `SchemaWatcher` CRD-add events to the dynamic controller so instances of an
  affected RGD are enqueued promptly instead of waiting for the poll interval.
  (Low-risk, and removes a "why is my addon slow to appear" surprise.)
- **RESTMapper cache.** A dynamic `RESTMapper` can cache a negative lookup; a
  freshly-added CRD may not be visible until the mapper reloads
  ([controller-runtime #2589](https://github.com/kubernetes-sigs/controller-runtime/issues/2589)).
  controller-runtime's `DynamicRESTMapper` reloads on `NoMatch`, which our retry
  path triggers naturally, but we add a `RESTMapper.Reset()` (or rebuild) hook on
  the `SchemaWatcher`'s CRD-add notification to remove the last-mile latency.

##### 6. Status surface

New, non-blocking, observable state so operators can see *why* an instance isn't
fully converged:

- Instance/RGD condition reason `SchemaPending` on `ResourcesConverged=False`
  (sibling of the existing `DataPending`), set when the only thing keeping the
  graph from converging is one or more deferred-and-still-absent nodes.
- `status.deferredResources: [<id>...]` (and the awaited `GroupKind`) for
  visibility, populated from `ApplyResult.Unresolved` filtered to deferred nodes.
- An Event (`WaitingForCRD`, gvk=...) on first defer, mirroring Crossplane's
  "waiting for CRD to be established" UX.

Importantly the RGD itself reports `Active` (CRD served, controller running) even
while some resources are deferred — partial success is a first-class state, not a
failure.

## Other solutions considered

1. **Deferred/incremental schema resolution for *all* resources, no opt-in**
   (#1293 alternative #2). Simplest to describe, but it silently turns a typo'd
   `apiVresion`/`kind` into "waiting forever," destroying today's fast, clear
   validation errors. Rejected as the default; the feature gate + eligibility
   rules give the same power without the footgun.

2. **`optional: true` only, ignore `includeWhen`.** Explicit and clear, but misses
   #1293's insight that a resource-referencing `includeWhen` *already* expresses
   conditionality. Supporting both (this proposal) makes the motivating example
   work with zero new fields while still offering the explicit escape hatch.

3. **Ordering primitive (sync-waves / phases).** Helm HIP-0025, Argo sync-waves,
   Flux `dependsOn` all order *applies*. kro already has a DAG + `includeWhen`, so
   ordering is solved; the missing piece is tolerating an *unknown type*, which
   ordering alone does not address (Argo/Flux users still hit "no matches for
   kind" until they add `SkipDryRunOnMissingResource` / `validation: none`). This
   proposal is the kro equivalent of `SkipDryRunOnMissingResource`, but
   type-aware and self-healing via the schema watcher.

4. **Two-phase compile (resolve required first, defer the rest in a second pass).**
   More machinery than needed: the single-pass `resolveOrDefer` with an
   eligibility predicate achieves the same result because the DAG/type-check
   layers already tolerate schema-less nodes.

5. **Pre-create the missing CRDs from the RGD.** Out of scope and often
   impossible — the CRD is registered by an operator the RGD itself installs.

## Scoping

#### In scope

- Feature gate `DeferredSchemaResolution` (alpha, default off).
- `resolveOrDefer` in both compile paths; `Deferred` node field; `resolver.ErrSchemaNotFound` sentinel.
- Eligibility: resource-referencing `includeWhen`, and an explicit `optional`/`deferSchemaResolution` field.
- Generalized `mappingFor` (`Deferred` shares the dynamic-GVK lazy path).
- Deferred nodes contribute literal `GroupKind` to `RequiredGroupKinds`.
- `SchemaPending` condition reason, `status.deferredResources`, `WaitingForCRD` event.
- `SchemaWatcher` → instance enqueue bridge + `RESTMapper` reset on CRD add.

#### Not in scope

- Deferring the RGD *instance* CRD's own schema (it depends only on
  `spec.schema` and never needs deferral).
- Deferring on validation/typo errors or transient discovery failures (explicitly
  excluded — those stay hard errors).
- Cross-RGD ordering guarantees beyond what the DAG + `includeWhen` already give.
- Versioned-CRD / schema-drift handling once a CRD exists (the `SchemaWatcher`
  recompile path already covers content changes).

## Testing strategy

#### Requirements

- envtest apiserver (already used by `test/integration/graphengine/...`).
- A CRD that is installed *after* the RGD (the existing
  `schemawatch_test.go: installCRD` + `TestSchemaWatchEnqueuesOnCRDAdd` harness is
  the template — it already exercises "graph fails, CRD appears, graph recovers").

#### Test plan

Unit:

- `resolver`: unknown GVK → `ErrSchemaNotFound`; transport error → *not* that sentinel.
- `compiler`/`builder`: eligible + absent → `Deferred` node (schema nil, GVR zero,
  literal GK in `RequiredGroupKinds`); ineligible + absent → error; malformed →
  error; gate off → error.
- `executor`: `Deferred` node + absent CRD → `Unresolved` + `errSchemaNotReady`,
  not pruned; CRD present → applies. `includeWhen=false` deferred node → ignored,
  no mapping attempt, dependents contagiously skipped.
- `emitSchemaDependencies`: deferred node subscribes the *specific* GK, not watch-all.

Integration (envtest):

- **The #1293 scenario end-to-end:** apply the `MyPlatform` RGD where `addon`'s
  CRD is absent; assert (a) `MyPlatform` CRD is served and RGD is `Active`,
  (b) an instance creates `operator`, (c) `operator` Ready → its CRD registers →
  `includeWhen` flips → `addon` applies with no manual intervention.
- Negative: RGD with a typo'd kind and gate off (or resource ineligible) still
  fails fast with `ResourceGraphInvalid`.
- Recovery latency: deferred node applies promptly after CRD add (asserts the
  instance-enqueue bridge + RESTMapper reset).
- CRD *removed* after being used: deferred node returns to `Unresolved` without a
  hard crash (guards against the Crossplane #7284 "stale ref blocks forever" trap).

## Discussion and notes

- Relies on and complements: `include-when-resource-references` (#1104, resource
  refs in `includeWhen`), the `SchemaWatcher`, and the push-invalidated
  `CachedSchemaResolver` (#1176). No new watch infrastructure is introduced.
- Prior art surveyed: Argo CD `SkipDryRunOnMissingResource` + sync-waves; Flux
  `dependsOn` + `validation: none`; Helm `crds/` dir + HIP-0025 ordered batches;
  Crossplane XRD `Established` gating and the #7284 stale-ref cautionary tale;
  kops PR #18473 ("defer apply when CRD is not yet registered", treats
  `NoKindMatchError` as retriable). The common lesson: make "type absent"
  *retriable*, not fatal — and be careful to distinguish absence from transient
  discovery failure.

## Appendix: implementation plan

### Two facts that shape the work

1. **Recovery is largely free on the RGD path.** The production
   `ResourceGraphDefinition` path does **not** use the `SchemaWatcher` (that is
   wired only into the experimental `kro.run/v1alpha1` **Graph** controller,
   `cmd/controller/graphengine.go`). It does not need it: a `Deferred` node
   resolves its GVR *lazily at apply time* (identical to `DynamicGVK`), and the
   instance controller already soft-requeues on `ErrNotReady`
   (`controller_graph_engine.go` → `requeue.NeededAfter`) and re-reconciles on
   child-resource events. In the motivating example the re-trigger is `operator`
   becoming Ready — a child-watch event that re-runs the instance, at which point
   the now-present `InstallAddon` CRD maps successfully. So **no new recovery
   wiring is required for correctness**; the CRD-add→enqueue bridge and
   `RESTMapper` reset are latency optimizations only.

2. **The classic builder is the harder half, and cannot be retired here.**
   Both the RGD controller and the GraphRevision controller compile via the
   classic `graph.Builder` → `*graph.Graph`, which owns instance **status-schema
   inference** for the served CRD (`inferStatusSchema` → `crd.SetCRDStatus(.., true)`).
   `rgdadapter.SynthesizeInstanceCRD` only stamps an empty status placeholder, so
   the graph engine cannot yet produce the served CRD on its own. Collapsing to a
   single compiler (the real "no split" endgame) is the in-progress RGD-on-graph
   flip and is **out of scope**; here the classic builder must learn to defer too
   — but minimally.

### Design principle for this change: rework, don't split

The first draft invented a parallel `Deferred` node flag, an `optional` API
field, a `SchemaPending` status vocabulary, and new recovery wiring. Closer
reading shows almost none of that is needed — the existing **dynamic-GVK**
machinery already *is* "a node whose type is resolved lazily at apply time."
Deferral is not a new kind of node; it is the **same** schema-less/lazy node,
reached by a second entry condition (literal GVK whose CRD is absent) instead of
only the first (CEL-expression GVK). The whole change is widening that funnel.

**No new node flag.** A deferred node is exactly `{Kind: Template, DynamicGVK:
false, GVR: empty, schema: nil, Object: literal}`. Verified that this shape
already flows correctly through every existing graph-engine stage with **zero**
changes:

- `dynIDs` (compiler.go): built as "every node absent from `celSchemas`" — a
  nil-schema node is auto-included, so its own field expressions and any
  cross-node reference to it type-check as `dyn`.
- `emitSchemaDependencies` (compiler.go): a `DynamicGVK: false` node with a
  literal `Object` falls into `case n.Object != nil` and contributes its
  **specific** `GroupKind` to `RequiredGroupKinds` (precise re-enqueue), while a
  true CEL-GVK node keeps flipping `HasDynamicGVK` (watch-all). Exactly the
  behaviour we want, for free.
- type-check (`expectedTypeForField`): returns `dyn` on a nil schema.

The **only** runtime consumer of the `DynamicGVK` flag is `mappingFor`
(`executor/simple.go:754`). Re-key it from "is this GVK dynamic" to "is this
GVR already resolved" (`!n.GVR().Empty()`) and both dynamic and deferred nodes
take the lazy `RESTMapper` path identically. `DynamicGVK` stays a compile-time
descriptor ("GVK is a CEL expression"); it simply stops being the runtime gate.

**No new API field (v1).** Eligibility = *the resource has `includeWhen`*.
That single rule covers both motivating issues: #1293 (resource-referencing
condition, e.g. `operator.status…Ready`) and #1243 (schema-referencing
condition, e.g. `schema.spec.platform == "gcp"` gating a Kind absent on non-GCP
clusters). `includeWhen`'s own API doc already says resources "may be created
later or pruned later as conditions change" — deferring the *type* of a
conditional resource is a natural extension of that contract, so no new field is
needed to express intent. (An explicit `optional: true` for the no-`includeWhen`
case remains a clean future add, not a v1 requirement.)

**No new recovery wiring (v1).** Correctness comes from the instance
controller's existing `ErrNotReady` soft-requeue plus child-resource watches
(see fact #1). `findRGDsForCRD` / `RESTMapper` reset are latency-only follow-ups.

### Detecting "absent" robustly

Key on the **typed** `meta.IsNoMatchError` from `RESTMapping`, not the untyped
`ResolveSchema` error. Concretely, **reorder** both build sites to call
`RESTMapping` first: on `IsNoMatchError` + eligible + gate-on, build the
`Deferred` node and skip `ResolveSchema` entirely. A `RESTMapping` success
followed by a `ResolveSchema` failure stays a hard error (real problem). A
discovery/transport error (`ErrGroupDiscoveryFailed`) is **not** `IsNoMatchError`
→ stays a hard error + requeue. This sidesteps the untyped-schema-error problem
and the `resolver.ErrSchemaNotFound` sentinel proposed earlier becomes optional.

### Work items (rework-first)

Shared:

| # | Change | Files | Size |
| --- | --- | --- | --- |
| 1 | Feature gate `DeferredSchemaResolution` (alpha, default off) | `pkg/features/features.go` | XS |
| 2 | Eligibility helper `deferAllowed(res)` = gate on **and** `len(includeWhen) > 0`; a small `isAbsent(err)` = `meta.IsNoMatchError` | shared util | XS |

Graph engine (the executed path) — a genuine generalization, not an addition:

| # | Change | Files | Size |
| --- | --- | --- | --- |
| 3 | Extract `buildDynamicTemplateNode` body into `buildSchemalessTemplateNode(dynamic bool)` (sets `DynamicGVK=dynamic`). Dynamic case calls it with `true` | `pkg/graphengine/compiler/context.go` | XS |
| 4 | In `buildNode`'s literal-GVK branch: call `RESTMapping` first; on `isAbsent` + `deferAllowed` build via `buildSchemalessTemplateNode(false)` (strict structural validation already ran; keep literal `Object`); else today's typed path | `pkg/graphengine/compiler/context.go` | S |
| 5 | Re-key `mappingFor` from `if !n.DynamicGVK()` to `if !n.GVR().Empty()`; update comment | `pkg/graphengine/executor/simple.go` | XS |
| — | `emitSchemaDependencies`, `dynIDs`, type-check, `RequiredGroupKinds`/`HasDynamicGVK`: **no change** (verified) | — | — |

Classic builder (CRD-serving gate) — minimal retrofit reusing existing seams:

| # | Change | Files | Size |
| --- | --- | --- | --- |
| 6 | In `buildResourceNode`: `RESTMapping` first; on `isAbsent` + `deferAllowed` parse fields schemaless (reuse the existing externalRef `ParseSchemalessResource` branch), return `nil` schema + empty GVR, skip `validateTemplateConstraints` | `pkg/graph/builder.go` | S |
| 7 | In `CompileSource`: don't insert a nil into `schemas`; collect `deferredIDs`; swap `TypedEnvironmentWithProvider(celSchemas)` → `TypedEnvironmentWithIDsAndProvider(celSchemas, deferredIDs)` (function already exists); skip `validateAndCompileNode` for deferred nodes (redundant — the engine validates them permissively at instance compile) | `pkg/graph/builder.go` | S |
| — | `collectNodeSchemas` already skips ids absent from `nodeSchemas`; `inferStatusSchema` resolves status exprs against the typed env, where deferred ids are now `dyn` | — | — |

Carrier plumbing:

| # | Change | Files | Size |
| --- | --- | --- | --- |
| 8 | Thread `includeWhen` presence to the eligibility check in both builders (`ResourceSpec` already carries `IncludeWhen`; the engine has the API node) | `pkg/graph/builder.go`, `pkg/graphengine/compiler/context.go` | XS |

Explicitly **dropped from v1** vs. the first draft (add later only if needed):
new `Deferred` node flag; `optional` API field; `resolver.ErrSchemaNotFound`
sentinel; `SchemaPending` condition + `status.deferredResources` + event;
`findRGDsForCRD`/`RESTMapper`-reset recovery wiring.

### Sequencing (suggested PRs)

1. **PR-1 (shared + engine):** items 1–5. Independently unit-testable at the
   `compiler`/`executor` level (deferred node compiles; `mappingFor` lazy-resolves;
   applies once the CRD exists; `includeWhen=false` node is ignored with no
   mapping attempt). Behaviour is dark until the classic builder also defers.
2. **PR-2 (classic builder):** items 6–8 — the change that actually lets the RGD
   CRD be served with an absent child CRD. The #1293 end-to-end envtest lands
   here (RGD `Active` + CRD served; `operator` Ready → `addon` applies).
3. **PR-3 (polish, optional):** re-add any of the dropped observability/recovery
   items that review asks for.

### Rough effort

- **Core (PRs 1–2), gated, correct, self-healing via instance requeue:**
  ~150–300 LOC + tests — roughly half the first estimate, because the engine side
  is a re-key + a helper extraction (no new flag/type) and the classic side
  reuses the externalRef-schemaless path and the existing `…WithIDsAndProvider`.

### Risks to verify during implementation

- Classic `inferStatusSchema` / `validateAndCompileNode` with a deferred (dyn)
  id present — confirm status expressions referencing a deferred resource compile
  as `dyn` rather than panic; this is why item 7 adds the id to the env rather
  than dropping it.
- `mappingFor` re-key: confirm no non-Template/Ref node with an empty GVR reaches
  it (Def and subgraph nodes are handled by earlier switch arms — verified).
- `RESTMapper` negative-cache latency after a CRD lands (controller-runtime
  #2589) — correctness holds via the retry loop; a reset is a latency-only add.
- CRD **removed** after use: a deferred node must return to `Unresolved` without a
  hard crash (Crossplane #7284 trap) — regression test.
- Scope guard: v1 defers **Template** nodes only; externalRef deferral is a
  follow-up (keeps the change surface tight).
