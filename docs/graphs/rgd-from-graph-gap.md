# Phase 3 — gap report (what Graph must gain before RGD can flip)

Source: the 36-file scoreboard (`.pi/notes/phase3-scoreboard.md`) plus
the running spike `TestRGDAsGraphSpike`.

**Spike result:** the Graph engine *can* express two CEL-wired
templates today. Cross-node `${cm.metadata.name}`, apply, Ready, and
`status.managedResources` tracking all work. The flip is **not**
blocked on the composition kernel.

What *does* block the flip is everything around that kernel: the RGD
product surface (CRD + instance + revisions + instance status) and a
handful of semantic mismatches in the engine itself.

Sizes are engineering-week estimates for one person who already knows
the Graph package. "Semantic" = behavior users rely on that Graph does
not reproduce even if you hand-author the Graph; "plumbing" = Graph
already has the shape, RGD just isn't pointed at it / the executor
isn't finished.

## Punch-list

### 1. CRD synthesis — **L** (3–5w) — semantic, **blocks the flip**

RGD turns `spec.schema` into a served CRD (create / update /
non-breaking vs breaking / ownership conflict / recreate-on-delete).
Graph has no CRD job. Without this there is no instance type for users
to apply, so RGD cannot flip.

Scoreboard: `crd_test.go`, most of `validation_test.go` (kind names),
`unknown_fields_test.go`, `instance_cluster_scoped_test.go` (scope).

### 2. SimpleSchema → instance — **L** (3–4w) — semantic, **blocks the flip**

`schema.spec` / `schema.status` / `schema.metadata` is the RGD input
surface. Graph has `def` (free-form data) and templates; it has no
instance object, no SimpleSchema compiler, no defaulting. A translator
could stuff instance spec into a `def` named `schema`, but that is not
an instance CR — no admission, no `kubectl get <kind>`, no per-instance
lifecycle.

Scoreboard: every file that creates an instance. Closest Graph analog
is a `def` node, which the spike deliberately did *not* use.

### 3. GraphRevisions / last-good-config — **M–L** (2–4w) — semantic, **blocks the flip**

RGD pins each instance to a hashed GraphRevision; a bad RGD update
keeps serving the last good revision (`recover_test.go`). Graph writes
`Accepted=False` on the live object — in-place, no pin. Flipping RGD
onto Graph without this is a user-visible regression (a typo takes
every instance down).

Scoreboard: `recover_test.go`. (The two excluded
`graphrevision_*` files are the full contract.)

### 4. Instance status inference + author conditions — **M** (2–3w) — semantic, **blocks the flip**

RGD projects `spec.schema.status` onto the instance (resource fields,
`schema.spec` refs, `runtime.newCondition`, hierarchical
`Ready`/`ResourcesReady`). Graph status is `Accepted`/`Ready` +
`managedResources` on the Graph object. Users read instance status
today; losing it is a flip-blocker.

Scoreboard: `status_test.go`, `status_schema_ref_test.go`,
`instance_conditions_test.go`, `instance_custom_conditions_test.go`,
`rgd_conditions_test.go`, half of `kubernetes_time_test.go`.

### 5. Ordered / cascading deletion semantics — **M** (1–2w) — semantic, **blocks nested-RGD users**

Graph Delete walks `managedResources` in reverse apply order with a
UID precondition. That covers a flat Graph. It does **not**:

- stamp kro / ApplySet deletion-order annotations
  (`applyset_test.go`, `annotation_label_test.go`);
- wait for a terminating managed object before creating a newly-desired
  replacement (`terminating_managed_resource_test.go`);
- cascade "parent instance → child RGD → grandchild instances"
  (`cascading_deletion_test.go`, `nested_test.go`). Graph nesting
  deletes a subgraph, which is a different object model.

Needed before flip for anyone using nested RGDs or relying on ApplySet
prune. Flat single-Graph delete already works (spike +
`TestReconcileApplyAndDelete`).

### 6. `ref` / `watch` executor — **M** (1–2w) — plumbing, **blocks ExternalRef RGDs**

`Node.Ref` and `Node.Watch` compile (`Accepted=True`) but Simple
returns `ErrUnsupported` (`refwatch_test.go`). ExternalRef is a widely
used RGD feature. Wiring is "read object / list-by-selector, publish to
scope, register watchrouter" — no new node kind.

Scoreboard: `externalref_test.go`, `externalref_watch_test.go`,
`externalref_deletion_test.go`.

### 7. ApplySet / kro instance-identity metadata — **S–M** (1–2w) — semantic for GC, plumbing for labels

RGD children carry ApplySet parent labels and kro instance annotations.
Graph SSA-applies the template as written under field manager
`kro-graphengine`. Tracking is `status.managedResources`, which is
enough for Graph-owned delete/prune, **not** enough for:

- `kubectl apply --prune` / ApplySet tooling;
- instance-isolation tests that assert kro labels
  (`annotation_label_test.go`, `instance_conflict_test.go` in its
  label-asserting form).

Decide: either implement an ApplySet executor (the package comment
already names "none, labels, ApplySet") or formally drop ApplySet from
the RGD-on-Graph contract.

### 8. ReadyWhen as an apply-order gate — **S** (3–5d) — semantic

RGD does not create a dependent until the upstream `readyWhen` is
true. Graph Simple records `ErrNotReady` and *continues the walk* so
watches still register (`simple.go`). Dependents that read the
not-ready upstream typically hit `ErrDataPending`, which is close but
not "do not apply". `dependency_readiness_test.go` will fail as
written until this is a hard gate (or the tests are rewritten against
the Graph semantics).

### 9. Nested-RGD vs nested-Graph — **L** (if required) — semantic

`Node.Graph` is a lexical frame (capture + shadow, see
`nesting_test.go`). An RGD resource of kind `ResourceGraphDefinition`
is "apply another RGD, which synths a CRD, which serves instances".
Those are not the same. Either:

- keep nested-RGD as an RGD-controller job that *emits* child Graph
  CRs (and child CRDs) — plumbing on top of #1/#2; or
- declare nested-RGD out of scope for the first flip.

Scoreboard: `nested_test.go`, `cascading_deletion_test.go`.

### 10. Translator / admission surface — **M** (2–3w) — plumbing

Even after #1–#4, something has to compile an RGD + instance into a
`*v1alpha1.Graph` (or a nested `Node.Graph`) and keep it in sync.
Validation that is RGD-shaped (kind names, status-must-be-expression,
forEach iterator vs resource-ID clash) stays on that translator, not
in `pkg/graphengine`. Not an engine gap; it is the flip's glue.

## What the spike already proved is *not* a gap

- Two (or N) `template` nodes, CEL `${upstream.field}` wiring, topo
  apply, namespace defaulting, Ready, `managedResources` tracking.
- `def`, `forEach`, `includeWhen`, `readyWhen` (Graph-level), nested
  `graph`, omit(), format(), two-var comprehensions, schema-aware CEL
  conversion, drift-recreate via watchrouter.

Those are the 19 executor-wired expressible rows. Do not rebuild them.

## Suggested order toward a flip

1. **#6 ref/watch** — unblocks 3 scoreboard files, no product-surface
   change, proves the remaining node kinds.
2. **#8 readyWhen gate** — small, semantic, cheap to get wrong later.
3. **#1 + #2 CRD+instance** — the actual product; without them RGD is
   still a different API.
4. **#4 instance status** — users cannot flip without it.
5. **#3 revisions** — last-good-config; do not flip production RGDs
   without this.
6. **#5 / #7 / #9** — deletion + ApplySet + nested-RGD; can slip past
   a *preview* flip if scoped out in release notes, not a GA flip.

Until #1–#4 land, the honest scoreboard stays **22/36 expressible,
0/36 running as RGD-from-Graph**. The spike is the kernel existence
proof, not the flip.
