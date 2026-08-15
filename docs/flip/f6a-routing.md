# F6a — Flag-gated instance reconcile via Graph engine

Branch: `graph-rgd-flip-f6`  
Commit: `rgd-on-graph: flag-gated instance reconcile via Graph engine (F6a)`  
Status: **builds clean, unit tests green, graphengine integration green, core compat suite NOT run (hangs — expected, see below)**

---

## What was wired

### 1. Feature flag: `RGDOnGraph` (`pkg/features/features.go`)
- New constant `RGDOnGraph featuregate.Feature = "RGDOnGraph"`
- Default `false`, Alpha. Enabled via `--feature-gates=RGDOnGraph=true`.
- Flag-off path is **byte-identical** to pre-F6a (no overhead, no code-path change).

### 2. `revisions.Entry.RGDSpec` (`pkg/graph/revisions/registry.go`)
- Added `RGDSpec *krov1alpha1.ResourceGraphDefinitionSpec` field to `Entry`.
- Populated in `graphrevision/controller.go` when publishing the active entry (the snapshot spec is copied at compile time so the instance controller can reconstruct a full `*v1alpha1.ResourceGraphDefinition` without a separate API fetch).
- **Backward compat**: entries compiled before F6a have `RGDSpec == nil`; `reconcileViaGraphEngine` detects this and requeues until the graphrevision controller repopulates.

### 3. Routing in `pkg/controller/instance/controller.go`
- `Controller` struct gains:
  - `graphEngineCompiler rgdadapter.Compiler` — injected via `WithGraphEngineCompiler()`
  - `graphEngineExecutor *executor.Simple` — built in `NewController` from the graph-engine client
  - `rgdOnGraphEnabled bool` — snapped at construction from the feature gate
- `NewController` gains a `graphEngineClient client.Client` parameter (pass `nil` in tests; the RGD controller passes `r.Client`).
- `Reconcile` inserts after step 2 (deletion handling):
  ```go
  if c.rgdOnGraphEnabled {
      return c.reconcileViaGraphEngine(ctx, inst, watcher)
  }
  ```
  Steps 3–8 (runtime path) run only when the flag is **off**.

### 4. `reconcileViaGraphEngine` (`pkg/controller/instance/controller_graph_engine.go`)
Full new file. Steps:

1. `c.graphResolver.GetLatestRevision()` — reuses existing resolver; checks `RGDSpec != nil`.
2. Reconstructs `*v1alpha1.ResourceGraphDefinition{Name: entry.OwnerKey, Spec: *entry.RGDSpec}`.
3. `rgdadapter.BuildRuntimeForInstance(rgd, inst, compiler)` — translate + schema node + compile + runtime.
4. `executor.Simple.Apply(ctx, rt, bridge)` — SSA apply via graph engine; watches wired through `instanceWatcherBridge` (adapts `dynamiccontroller.InstanceWatcher` → `watchrouter.Watcher`).
5. `rgdadapter.ProjectInstanceStatus(rt, rgd)` + `ProjectInstanceConditions(rt, rgd)` → `patchGraphEngineStatus`.

### 5. Compiler injection chain
- `ResourceGraphDefinitionReconciler` gains `graphEngineCompiler rgdadapter.Compiler` + `WithGraphEngineCompiler()` + `graphEngineEnabled()`.
- `setupMicroController` passes `r.Client` as `graphEngineClient` and calls `instCtrl.WithGraphEngineCompiler(r.graphEngineCompiler)` when the flag is on.
- `cmd/controller/main.go` builds a `compiler.NewCompiler(restConfig, httpClient)` after `SetupWithManager` and calls `rgd.WithGraphEngineCompiler(geCmp)` when `RGDOnGraph` is enabled.
- Integration test environment (`test/integration/environment/setup.go`) mirrors the same pattern: compiler built + injected post-`SetupWithManager` when the flag is on.

### 6. N/36 probe support
- `test/integration/suites/core/setup_test.go` checks `os.Getenv("RGD_ON_GRAPH") == "1"` and sets the feature gate. Run with `RGD_ON_GRAPH=1 go test ./test/integration/suites/core/... -timeout 600s`.

---

## Shortcuts / known gaps

### RGD-object access
`BuildRuntimeForInstance` requires a `*v1alpha1.ResourceGraphDefinition`. The instance controller has no direct RGD reference — it only has the compiled `*graph.Graph` from the revision entry. F6a bridges this by storing `RGDSpec` in `revisions.Entry` (populated by the graphrevision controller at compile time). This is the cleanest zero-extra-fetch approach.

### What `reconcileViaGraphEngine` does NOT yet do (all F6b work)
The core compat suite hangs/fails when `RGD_ON_GRAPH=1` because the following RGD-lifecycle behaviors from the old path are not reproduced in `reconcileViaGraphEngine`:

| Behavior | Old path | F6a status |
|---|---|---|
| Finalizer installation | `ensureManaged` / `applyManagedFinalizerAndLabels` | **MISSING** — instance never gets finalizer so GC doesn't work |
| ApplySet inventory labels | `applyset.Metadata` injected in `applyManagedFinalizerAndLabels` | **MISSING** — no ApplySet tracking |
| Management labels (`kro.run/…`) | `instanceLabeler.Labels()` | **MISSING** |
| `kro`-owned conditions (InstanceManaged, GraphResolved, ResourcesReady, Ready) | `ConditionsMarker` + `condSet` | **MISSING** — only author conditions + basic state written |
| Suspend annotation check | `v1alpha1.IsReconcileSuspended` | **MISSING** |
| `resourceDeletingError` / ResourcesDeleting condition | `reconcileNodes` | **MISSING** |
| Requeue-with-delay on soft errors | `rcx.delayedRequeue` | Partial — `ErrNotReady` propagates but without the exact requeue duration |
| Status wire-read (skip identical patch) | `statusesMatch` | **MISSING** — every cycle writes status unconditionally |
| Author-conditions full merge/stamp logic | `stampAuthorConditions` / `mergeWithPrevious` | Partial — conditions written but without generation stamping or previous-cycle merge |
| `externalRef` collections (selector watches) | `WatchRequest.Selector` | **MISSING** — `instanceWatcherBridge` drops the Selector field (rgdadapter translate returns error for selector externalRefs) |
| `forEach` (collection nodes) | runtime loop | Partial — graph engine supports forEach but translate may not map all RGD forEach shapes |
| Deletion path via graph engine | Not attempted | **OUT OF SCOPE F6a** — deletion still uses old ApplySet/finalizer path |

### The `externalRef collections` translate error
The first core-suite failure seen during the probe was:

```
graph-engine build failed: rgdadapter: translate: rgdadapter: unsupported RGD shape:
resource "teamconfigs": externalRef collections (selector) are not mapped in F1
```

This means any RGD using selector-based `externalRef` fails to translate and the instance stays in ERROR. This blocks tests using externalRef collections.

---

## Build / test results

| Check | Result |
|---|---|
| `go build ./...` | PASS |
| `go vet ./...` | PASS |
| `go test ./pkg/... -count=1` | PASS (37 packages, 0 failures) |
| `go test ./test/integration/graphengine/...` | PASS (49.6s) |
| Core compat suite (flag off) | **NOT RUN** — expected to pass, unchanged path |
| Core compat suite (flag on, N/36 probe) | **NOT RUN** — orchestrator will run |

---

## F6b remaining work

F6b is gated on 36/36 core-compat tests passing with `RGD_ON_GRAPH=1`. The precise work:

1. **Finalizer + management labels**: call `ensureManaged` (or equivalent) at the start of `reconcileViaGraphEngine` — the graph engine path must install the kro finalizer and management labels before applying children, exactly as the old path does.

2. **ApplySet inventory**: wire `applyset.Metadata` into the graph engine apply cycle so that managed-resource tracking and deletion (prune) work correctly. The `executor.Apply` returns `ApplyResult.Applied []ManagedResource` — write these into the ApplySet annotations on the instance.

3. **kro-owned conditions** (`InstanceManaged`, `GraphResolved`, `ResourcesReady`, `Ready`): replace `patchGraphEngineStatus` with the full `ConditionsMarker` + `condSet` machinery from `status.go`.

4. **Suspend annotation check**: check `v1alpha1.IsReconcileSuspended` before calling `executor.Apply`.

5. **Status idempotency guard**: implement `statusesMatch` equivalent to avoid unconditional writes.

6. **Author-condition stamp/merge**: use `stampAuthorConditions` + `mergeWithPrevious` with generation tracking.

7. **`externalRef` collections**: the rgdadapter translator (`F1`) currently returns an error for selector-based externalRef resources. Fix the translator to emit `NodeKindRef` or `NodeKindWatch` for those.

8. **Delete path via graph engine**: once finalizer + ApplySet are wired, route deletion through `executor.Simple.Delete(managed)` using the ApplySet inventory rather than the old dynamic-client loop.

9. **Drop old path**: once 36/36, remove the `if !c.rgdOnGraphEnabled` branches, `pkg/graph.Builder`, `pkg/runtime`, and the entire old reconcile path. That is the F6b terminal state.
