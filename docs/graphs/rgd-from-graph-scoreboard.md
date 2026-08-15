# Phase 3 — RGD compat scoreboard

Question this board answers: **can a trivial RGD be expressed as a Graph
and reconciled by the Graph engine to the same effect?** The spike in
`test/integration/graphengine/suites/core/rgd_as_graph_spike_test.go`
answers that for the two-template CEL-wired case. This file enumerates
the rest of the RGD core suite so later phases can tally `N/36`.

Denominator is **36 files** under `test/integration/suites/core/`,
excluding `setup_test.go` and the two GraphRevision suites
(`graphrevision_test.go`, `graphrevision_conditions_test.go`). Those
two are themselves an RGD-only job (see gap report).

## How to run as an N/36 count

The RGD core suite is Ginkgo, wired through the Makefile:

```bash
# Full RGD integration (includes core + other suites).
make test WHAT=integration

# Core suite only (the 36-file board + setup + the two excluded GR files).
KUBEBUILDER_ASSETS="$(./bin/setup-envtest use 1.31.0 --bin-dir ./bin -p path)" \
  go tool ginkgo -v ./test/integration/suites/core/

# Graph-engine analog of the same harness (this worktree).
KA=$(realpath "$(./bin/setup-envtest use 1.31.0 --bin-dir ./bin -p path)")
KUBEBUILDER_ASSETS="$KA" go test ./test/integration/graphengine/suites/core/ \
  -run RGDAsGraphSpike -count=1 -timeout 300s
```

**Tally method (file-level, the `N/36` this board tracks):**

1. Walk the 36 files in the matrix below.
2. A file counts as **expressible** if its core RGD feature maps onto
   Graph node kinds that exist today (`template` / `ref` / `watch` /
   `def` / nested `graph`) plus `forEach` / `includeWhen` / `readyWhen`.
   Executor wiring is *not* required for the expressible count — `ref`
   and `watch` are on the type map even though Simple returns
   `ErrUnsupported` (see `refwatch_test.go`).
3. A file counts as **RGD-only** if the behavior is a job Graph does
   not have and is not supposed to grow as a node kind: CRD synthesis,
   SimpleSchema instance, GraphRevisions, ApplySet / kro instance
   labels, instance `status` / author conditions.
4. `N` = number of **expressible** rows. Today: **22 / 36**.
5. Live pass/fail of the RGD Ginkgo suite is a *different* number
   (many `It`s per file, still on the RGD controller). Do not confuse
   Ginkgo's `X Passed` with this board. Re-score the matrix when the
   Graph engine gains a row's capability; do not expect
   `make test WHAT=integration` to move `N` until RGD is pointed at
   Graph.

**Tally method (later, once RGD-from-Graph exists):** re-run the 36
files against the Graph-backed controller and count files whose
`Describe`s go green. That live `N/36` is the flip gate; this board is
the *could-it-even-be-said-in-Graph* gate.

## Gap-matrix key

| Mark | Meaning |
|---|---|
| **expressible** | Feature is a Graph node / CEL / includeWhen / readyWhen / forEach / nested graph. Engine may still lack executor wiring. |
| **RGD-only** | Needs a job Graph does not have: CRD synth, SimpleSchema instance, GraphRevisions, ApplySet / kro-instance metadata, instance status / author conditions. |

Partials are called out in the notes column. They count as
**expressible** only when the *core* behavior (the thing the file is
named for) has a Graph shape; otherwise **RGD-only**.

## Matrix (36)

| # | File | RGD feature | Graph today |
|---|---|---|---|
| 1 | `annotation_label_test.go` | kro / ApplySet labels + annotations stamped on instance children | **RGD-only** — Graph SSA-applies templates as written; no kro instance-identity labels |
| 2 | `applyset_test.go` | KEP ApplySet parent/child labels, prune, multi-GVK | **RGD-only** — Graph tracks via `status.managedResources`, not ApplySet |
| 3 | `cascading_deletion_test.go` | Parent nested-RGD instance delete tears down child RGD + its instances | **RGD-only** — Graph nesting deletes a subgraph, not an RGD-that-synths-a-CRD |
| 4 | `collection_test.go` | `forEach` expansion of a resource | **expressible** — `Node.ForEach` + template (wired; see graphengine `foreach_test.go`) |
| 5 | `collection_watch_test.go` | Drift / watch on a `forEach` item triggers reconcile | **expressible** — tracking + watchrouter already cover collection children |
| 6 | `crd_test.go` | Synthesize / update / delete / conflict / recreate the instance CRD | **RGD-only** — Graph does not emit CRDs |
| 7 | `data_pending_test.go` | Dependents wait; `DataPending` while upstream status is empty | **expressible** — runtime `ErrDataPending` + `ApplyResult.Unresolved` |
| 8 | `dependency_readiness_test.go` | Do not apply dependents until upstream `readyWhen` holds | **expressible** — `ReadyWhen` exists; **semantic caveat**: Simple does not *block* the walk (see gap) |
| 9 | `externalref_deletion_test.go` | ExternalRef is not deleted with the instance | **expressible** — `Node.Ref` (executor: `ErrUnsupported`) |
| 10 | `externalref_test.go` | Read-only ExternalRef imported into CEL scope | **expressible** — `Node.Ref` (executor: `ErrUnsupported`) |
| 11 | `externalref_watch_test.go` | Reconcile when the referenced object changes | **expressible** — `Node.Ref` + watches (executor: `ErrUnsupported`) |
| 12 | `format_test.go` | CEL `format()` in templates | **expressible** — Graph CEL ships `ext.Strings()` |
| 13 | `include_when_test.go` | `includeWhen` skips a node (and contagion) | **expressible** — `Node.IncludeWhen` (wired; see `TestReconcileIncludeWhenFalse`) |
| 14 | `instance_cluster_scoped_test.go` | Cluster-scoped *instance CRD* owning namespaced children | **RGD-only** — Graph CR is namespaced; no instance CRD. (Namespaced Graph *can* apply cluster-scoped children.) |
| 15 | `instance_conditions_test.go` | Hierarchical instance conditions (`Ready` / `ResourcesReady` / …) | **RGD-only** — Graph has `Accepted` + `Ready` on the Graph object, not an instance condition tree |
| 16 | `instance_conflict_test.go` | Two instances isolated; no cross-clobber | **expressible** — two Graphs are two apply identities; isolation is namespace + template names, not kro labels |
| 17 | `instance_custom_conditions_test.go` | Author `status.conditions` via `runtime.newCondition` | **RGD-only** — no instance status surface, no `runtime.newCondition` |
| 18 | `instance_resource_watch_test.go` | Child mutation (Secret) re-triggers instance reconcile | **expressible** — watchrouter + drift (see graphengine `drift_test.go`) |
| 19 | `kubernetes_time_test.go` | `metadata.creationTimestamp` in CEL / projected to instance status | **expressible** — schema-aware time conversion; status *write-back* is RGD-only (notes) |
| 20 | `lifecycle_test.go` | Instance spec update converges children | **expressible** — Graph spec update + SSA (see `TestReconcileApplyAndDelete`) |
| 21 | `metadata_fields_test.go` | All metadata fields (incl. namespace) readable from CEL | **expressible** — applied unstructured is published to scope |
| 22 | `nested_test.go` | Parent RGD creates a child RGD (which synths its own CRD + instances) | **RGD-only** — Graph `Node.Graph` is a lexical subgraph, not "apply an RGD" |
| 23 | `omit_test.go` | `omit()` drops a field from the rendered object | **expressible** — Graph CEL `library.Omit` + resolver |
| 24 | `readiness_test.go` | `readyWhen` holds Graph/instance Ready | **expressible** — `Node.ReadyWhen` (wired; see `TestReconcileReadyWhen`) |
| 25 | `recover_test.go` | Invalid RGD update keeps last-good GraphRevision serving instances | **RGD-only** — Graph has no revisions; a bad spec is `Accepted=False` in place |
| 26 | `rgd_conditions_test.go` | RGD-level condition contract (`CRDSynced`, `GraphVerified`, …) | **RGD-only** — Graph condition set is `Accepted` + `Ready` |
| 27 | `schema_aware_cel_test.go` | `string(secret.data.x)` via OpenAPI `byte` format | **expressible** — Graph CEL unstructured conversion |
| 28 | `status_schema_ref_test.go` | Instance `status` from `schema.spec` / mixed resource fields | **RGD-only** — no instance, no status projection |
| 29 | `status_test.go` | RGD + instance status population / failure conditions | **RGD-only** — same |
| 30 | `terminating_managed_resource_test.go` | Do not create a newly-desired child while an old managed object is terminating | **expressible** — tracking + prune/delete; **semantic caveat**: no explicit "wait-for-terminating" gate |
| 31 | `topology_test.go` | CEL refs produce a DAG / apply order | **expressible** — compiler DAG (see `TestReconcileDependencyOrder`) |
| 32 | `two_var_comprehensions_test.go` | `transformMap` / `transformList` in templates | **expressible** — Graph CEL `ext.TwoVarComprehensions()` |
| 33 | `type_compatibility_test.go` | SimpleSchema custom types + structural assignability across resources | **expressible** — compiler typechecks against live OpenAPI of target GVKs; SimpleSchema typedefs themselves are RGD-only |
| 34 | `unknown_fields_test.go` | Instance schema `x-kubernetes-preserve-unknown-fields` + CEL into them | **RGD-only** — instance CRD schema |
| 35 | `validation_test.go` | RGD validation (IDs, kind names, forEach, status-must-be-CEL, …) | **RGD-only** — Graph validates Graph specs (unique IDs, acyclic, one-of node kind); RGD schema/kind/status rules are the RGD admission surface |
| 36 | `webhook_denial_test.go` | Admission denial on a child surfaces on instance status; recovers when policy is removed | **expressible** — apply error keeps Graph un-Ready; status *shape* differs (Graph conditions, not instance) |

## Count

| Bucket | N | Files |
|---|---|---|
| Expressible in Graph today (type map) | **22** | 4, 5, 7, 8, 9, 10, 11, 12, 13, 16, 18, 19, 20, 21, 23, 24, 27, 30, 31, 32, 33, 36 |
| Of those, executor not wired (`ref`/`watch`) | 3 | 9, 10, 11 |
| RGD-only jobs | **14** | 1, 2, 3, 6, 14, 15, 17, 22, 25, 26, 28, 29, 34, 35 |
| **Total** | **36** | |

Headline: **22/36 expressible**, **14/36 blocked on RGD-only jobs**.
Of the 22, **19** have a working Simple-executor path; **3** (`ref` /
`watch`) are on the Graph API but return `ErrUnsupported` at apply.

The spike (`TestRGDAsGraphSpike`) covers the intersection of rows 20 +
21 + 31 — two templates, CEL cross-node wiring, apply, tracking — and
is the minimum "RGD-from-Graph is not dead on arrival" proof.
