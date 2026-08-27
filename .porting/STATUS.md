# RGD → RGD-as-Graph (v1alpha2) test porting

Goal: every behavioral scenario covered by the built-in ResourceGraphDefinition
integration tests is also exercised against the RGD-as-Graph v1alpha2 backend,
via the `rgdBackend` parity harness (`rgd_backend_test.go`,
`rgd_graph_backend_test.go`, `rgd_parity_test.go`).

## Approach

Ported scenarios live as `DescribeTable` entries in
`test/integration/suites/core/rgd_parity_*_test.go`, each running the SAME
fixture through both backends:

- `builtinRGDBackend` — the built-in RGD controller (reference).
- `graphRGDBackend` — the pure-Graph controller served at v1alpha2.

Each ported scenario asserts the SHARED contract (child resources converge with
correct resolved values; instance reaches ready). Backend-specific behavior
(RGD `status.state`/`TopologicalOrder`, `ApplyOrderAnnotation`, readiness gating
which is opt-in for standalone Graphs) is branched on `be.Name()` and documented
as a parity boundary rather than forced.

Not every scenario maps 1:1 — some assert RGD-controller internals with no Graph
analog (CRD lifecycle owned by the built-in controller, GraphRevision registry,
admission webhooks). Those are marked `N/A` with a reason; the goal is coverage
of resource-composition behavior, not controller plumbing.

## Status legend

- `[ ]` not started
- `[~]` in progress (agent assigned)
- `[x]` ported + individually test-passing on BOTH backends
- `[N/A]` no Graph-backend analog (reason noted)

## Per-scenario map

Tracked in `scenarios.md` (source inventory). This file tracks disposition +
where each ported scenario lives.

| # | source file | scenario | status | ported to | notes |
|---|---|---|---|---|---|

## Batches (agent assignments)

Harness is package-level (rgd_parity_harness_test.go). Each agent writes ONE new
file rgd_parity_<theme>_test.go so parallel work never collides. Already ported
(in rgd_parity_test.go): configmap-resolve, cross-ref, readyWhen-data,
includeWhen, forEach, readiness-observed-status.

| batch | file to create | source tests | status |
| --- | --- | --- | --- |
| status | rgd_parity_status_test.go | status_test, status_schema_ref_test, status_array_projection_test, kubernetes_time_test | DONE — 8 tables, 16 specs green both backends |
| cel | rgd_parity_cel_test.go | format_test, omit_test, two_var_comprehensions_test, metadata_fields_test, schema_aware_cel_test, unknown_fields_test | DONE — 8 tables, 16 specs green both backends |
| includeref | rgd_parity_includeref_test.go | include_when_test, externalref_test, externalref_watch_test, externalref_deletion_test | DONE — 9 tables (15 pass/3 graph-Skip); ext-collection status + deletion-ordering builtin-only/N/A |
| collection | rgd_parity_collection_test.go | collection_test, collection_behavior_test, collection_watch_test | DONE — 16 tables (32 specs); size-limit N/A; drift-restore + readyWhen-order builtin-only |
| lifecycle | rgd_parity_lifecycle_test.go | lifecycle_test, recover_test, resource_pruning_test, terminating_managed_resource_test, cascading_deletion_test | DONE — 5 tables (10 specs); scenarios 4,5 graph-Skip; prune-on-includeWhen-flip + ResourcesReady cond builtin-only |
| conditions | rgd_parity_conditions_test.go | instance_conditions_test, instance_custom_conditions_test | DONE — 4 shared tables (8 specs) + 8 builtin-only It; green both backends |
| cel | rgd_parity_cel_test.go | format_test, omit_test, two_var_comprehensions_test, metadata_fields_test, schema_aware_cel_test, unknown_fields_test | pending |
| shapes | rgd_parity_shapes_test.go | resource_shape_compatibility_test, data_pending_test, dependency_readiness_test, nested_test, instance_cluster_scoped_test, instance_expression_isolation_test, instance_resource_watch_test | DONE — 12 tables (29 specs); nested-RGD + cluster-scoped-children graph-Skip; strict ordering builtin-only |
| ownership | rgd_parity_ownership_test.go | resource_ownership_test, instance_conflict_test, annotation_label_test, applyset_test (resource-level parts) | DONE — 11 tables (22 specs); applyset label/fieldmgr/GVK-label shapes builtin-only |

## Findings (real graph-backend gaps discovered during porting)

1. GateReadiness off for standalone Graphs: dependents apply across not-ready
   upstream (both converge; strict ordering builtin-only). Widely branched.
2. includeWhen-flip pruning of an ALREADY-created child: graph does not prune
   on a live instance dropping a resource (prunes on instance delete only).
   (lifecycle agent)
3. Collection-item drift restore on child-triggered reactive reconcile: graph
   backend fails to re-resolve L2 `schema` self-ref ("no such attribute(s):
   schema") — does NOT restore drift on collection items. (collection agent)
   >> Candidate real bug in the RGD-as-Graph reactive path; worth a follow-up.

## Reconciliation watchlist (check during aggregate compile)

- SHARED HELPER `parityCtx`: referenced by lifecycle AND collection agents
  (maybe others). If >1 file DEFINES it -> redeclare compile error. Must confirm
  exactly one definition after all agents finish.

- !! VERIFY rgd_parity_cel_test.go INTEGRITY: shapes agent reported it found the
  cel file corrupted/truncated (missing package clause) mid-run and restored it
  from /tmp/rgd_parity_cel_test.go.bak + re-appended podFirstContainerCommand.
  MUST confirm the cel file is intact + its 8 tables still pass after all agents
  finish (re-run the cel focus).

- CROSS-FILE HELPER COLLISIONS (go test -c will catch): agents defined
  package-level helpers. Known helper names by file:
  - lifecycle: plc-prefixed + `parityCtx`
  - ownership: getUnstructured, updateInstance, awaitDeleted,
    assertInstanceGVKLabels, assertChildApplysetLabels, saGVK
  - (collection/includeref/shapes TBD when they report)
  If two files define the same name -> compile error; reconcile by renaming the
  later one or hoisting a single copy into the harness file.
- Concurrent-agent stashing churn deleted/recreated several files mid-run;
  confirm all 8 batch files present + intact once agents finish.
- Graph-backend parity boundaries confirmed by agents (assert builtin-only):
  RGD status shapes (state/conditions/TopologicalOrder), applyset
  labels/annotations + ApplyOrderAnnotation, includeWhen-flip pruning of an
  already-created child, RGD-object mutation after Deploy (graph bakes
  templates at Deploy), ResourcesReady/ResourceDeleting conditions.

## Known N/A (built-in-controller-only, no Graph analog)

### conditions batch (graph writes only status.ready bool via .ready(); no kro

### condition machinery). Ported as built-in-only It specs, documented

- preserves lastTransitionTime when unchanged (internal LTT dedup)
- emits kro built-in conditions when no conditions block (lifecycle types)
- honors author Ready overriding kro lifecycle Ready
- runtime.condition reads kro internal not wire override
- drops runtime-duplicate types, state=Error
- preserves LTT for author overrides of built-in types
- preserves a previously written condition while data pending
- removes leftover author conditions after block removed (requires mutating RGD
  object at runtime; graph has synthesized CRD + L0 Graph, no RGD to mutate)

- status_test "only show status fields when all referenced resources available":
  built-in user-status-projection presence semantics; graph backend writes only
  status.ready (bool), never the RGD schema status block. Converging slice ported
  as "partial status resolution follows includeWhen dependencies"; full 5-state
  walk N/A. (status agent)
- Cross-cutting: graph backend does NOT reproduce the RGD user `status.*` block
  (writes status.ready bool via .ready()); every status-projection scenario
  asserts user status fields under isBuiltin(be) only. This is the primary
  documented parity boundary.

- crd_test: CRD lifecycle/breaking-change/short-names are RGD-controller CRD
  management, not resource composition. (candidate N/A — agent to confirm)
- graphrevision_test: GraphRevision registry internals. N/A.
- validation_test / webhook_denial_test: admission/validation on the RGD object. N/A.
- reconcile_error_metrics_test: RGD controller metrics. N/A.

## FINAL GATE (aggregate run, both backends, 23-way parallel)

`go tool ginkgo -p --focus "RGD parity"`:
**Ran 168 of 353 Specs — SUCCESS! 168 Passed | 0 Failed | 0 Pending | 185 Skipped.**

All 9 parity Describe blocks passed together (no cross-scenario name/namespace
collisions, no flakes). Package `go test -c` exit 0. `make lint` → 0 issues.

Porting COMPLETE: 41 source RGD test files mapped; 77 DescribeTables (154 backend
specs) + built-in-only It specs across 8 ported files + base. Parity boundaries
and 3 real graph-backend gaps documented above.

## Findings triage (addressing pass)

- Finding #1 (GateReadiness off): CONFIRMED INTENDED, not a bug. Production
  (cmd/controller/graphengine.go) and the integration env both create the Graph
  executor WITHOUT GateReadiness (ConflictDetection=true only). Standalone Graphs
  never gate readiness by design; RGD-as-Graph inherits this. Parity boundary is
  correct — no code change. (Enabling it controller-wide would change all Graph
  behavior; out of scope.)

- Finding #2 (includeWhen-flip prune of already-created child): NEEDS CARE. The
  top-level standalone Graph controller DOES prune includeWhen-flipped children
  (controller.go diffManagedResources: "includeWhen flipped" is an explicit
  prune candidate, pruned on clean apply). The RGD-as-Graph gap is likely in
  REACTIVE PROPAGATION through the nested L0->L1->L2 stamping (the flipped child
  lives in an L2 child Graph; L1 must re-stamp L2 on instance change for L2 to
  prune). May share a root cause with #3.

- Finding #3 (collection drift `no such attribute(s): schema`): root-cause
  investigation dispatched (agent c967c6dc). Concrete runtime error in the
  reactive reconcile path where a forEach axis referencing a ref node (`schema`)
  loses that node's scope value on a child-triggered reconcile.

## Findings — RESOLUTION

- #1 GateReadiness: WONTFIX (intended). Standalone Graphs never gate readiness
  in production (cmd/controller/graphengine.go) or the integration env. Parity
  boundary is correct.

- #3 forEach-axis data-pending (`no such attribute(s): schema`): FIXED in
  pkg/graphengine/runtime/node.go expand() — forEach-axis CEL errors now
  classified as soft ErrDataPending (matching includeWhen/readyWhen/template-body
  eval sites), so a reactive reconcile requeues + re-expands instead of hard-
  aborting. Root cause: expand()/evalList were the ONLY eval sites bypassing
  IsCELDataPending. Drift-restore parity assertion un-branched (asserts both
  backends now). Verification agent b5b39f67 running.

- #2 includeWhen-flip prune of already-created child: likely SAME root cause as
  #3 (reactive re-resolution). Re-evaluate after #3 fix lands — the L2 child
  Graph re-reconcile that was hard-aborting on the missing `schema` ref may now
  requeue and prune correctly. Pending #3 verification.

## Findings — RESOLUTION (corrected after verification)

- #3 CORRECTED: The `no such attribute(s): schema` hard-abort was NOT the live
  cause. Verified: on collection-item drift the graph backend produces NO error
  and simply goes IDLE — the child modification never drives a re-apply within
  60s. Real cause is a watch/re-enqueue-or-reapply gap for COLLECTION children
  in the standalone Graph engine (under investigation, agent 3a70c85c).
  - The expand() forEach-axis data-pending classification change is KEPT: it is
    a correct, regression-free consistency fix (forEach axis was the only eval
    site not classifying CEL data-pending as soft), but it does NOT fix the
    drift bug. Commit framing must not claim it does.
  - Drift-restore parity assertion RE-BRANCHED to builtin-only pending a real
    fix for the collection-child watch/reapply gap.

## Findings — RESOLUTION (v2, real root cause)

- #3 REAL ROOT CAUSE + FIX: standalone Graph collection drift-watch registers a
  selector scoped by {node-id, instance-id}, but standalone collection children
  were stamped with node-id ONLY (instance-id came solely from the RGD path's
  LabelInjector). Selector never matched -> child drift never re-enqueued the
  Graph -> no restore. Non-collection children were unaffected (name-keyed
  scalar watch, no selector). Re-apply itself always worked (unconditional SSA).
  FIX: stampKROMeta now stamps instance-id = Graph UID on standalone collection
  children (only when absent), symmetric with watchCollection's existing UID
  fallback. Executor unit tests (incl. collection-watch selector tests) pass.
  Drift-restore parity assertion un-branched (both backends). Verifying
  (agent fecf490b).
  - The earlier expand() forEach-axis data-pending change is KEPT as an
    independent correctness/consistency improvement (regression-free) but was
    NOT the cause of this bug.

## Findings — RESOLUTION (v3, #2 assessed)

- #2 includeWhen-flip prune: NOT an engine bug. Investigation confirmed the
  reactive chain works in the engine — a top-level standalone Graph prunes
  includeWhen-flipped children (controller.go diffManagedResources + ex.Delete
  on clean apply; unit-pinned tracking_test.go/wiring_coverage_test.go), and the
  L2 child Graph re-enqueues on instance change via its SCALAR `schema` ref watch
  (name-keyed via coordinator scalarIndex — no label/selector mismatch like #3).
  The parity test had conservatively branched the delete to builtin-only. Now
  UN-BRANCHED (both backends assert AwaitDeleted, 90s window for the extra
  instance-event->L2-enqueue hop). Verifying empirically (agent 6db5b884): if the
  graph backend prunes reliably it's resolved parity; if flaky on latency, revert
  to builtin-only.

## SUMMARY of findings disposition

- #1 GateReadiness: WONTFIX (intended production behavior).
- #3 collection drift: FIXED (stampKROMeta instance-id) + unit test + un-branched;
  committed 8454e7e.
- #2 includeWhen-flip prune: engine already supports it; test un-branched,
  verification in flight.

## Findings — RESOLUTION (v4, FINAL — #2 verified)

- #2 includeWhen-flip prune: VERIFIED REAL GAP, kept as builtin-only boundary.
  Empirical run (3/3 deterministic): the graph backend does NOT prune a child
  whose includeWhen flips false on a live instance spec change — the L2 instance
  Graph never re-evaluates the includeWhen (managedResources stays 2, zero
  prune/ignore activity), retired child survives past 90s. The root-cause
  agent's theory that the scalar-ref reactive chain drives an L2 re-eval was
  WRONG in practice. This is a live-instance-spec-change -> nested-L2-re-eval gap
  (distinct from #3's collection label/selector bug). Reverted the un-branch;
  assertion is builtin-only. NOTE: this is a genuine RGD-as-Graph reactive gap
  worth a future engine fix (nested L2 re-reconcile on instance spec change), but
  out of scope for this porting effort.

## FINAL DISPOSITION

- #1 GateReadiness: WONTFIX (intended production behavior). Builtin-only boundary.
- #3 collection drift restore: FIXED + verified + unit-tested; committed 8454e7e.
  (Real bug: missing instance-id label on standalone collection children.)
- #2 includeWhen-flip prune: VERIFIED real gap; kept builtin-only boundary +
  documented as future engine work. Not fixed this pass (nested reactive re-eval).
