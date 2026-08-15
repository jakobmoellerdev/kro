# F2 parity notes — forEach / includeWhen / readyWhen / externalRef

Branch: `graph-rgd-flip-f2`

## Summary table

| Feature       | Translate | Compile | Resolve/Runtime | Executor/Apply | Scoreboard rows |
|---------------|-----------|---------|-----------------|----------------|-----------------|
| forEach       | PASS      | PASS    | PASS            | deferred F3+   | 4, 5            |
| includeWhen   | PASS      | PASS    | PASS            | deferred F3+   | 7, 8, 9         |
| readyWhen     | PASS      | PASS    | PASS (gate)     | deferred F3+   | 10, 11, 13      |
| externalRef   | PASS      | PASS    | n/a (ref=read)  | GAP — F3+      | 18, 24, 30, 31  |

---

## forEach (rows 4, 5)

**Proven:**
- `ResourceGraphDefinitionToGraph` copies all `ForEachDimension` entries via
  `copyForEach` — the translated `Node.ForEach` slice is structurally identical to the
  RGD resource's slice (same `map[string]string` type, same keys and CEL strings).
- The Graph compiler accepts a template node with a forEach dimension referring to
  `${schema.spec.items}` (a `def`-backed list field).
- `runtime.Node.Resolve()` expands the cartesian product: with `spec.items = ["a","b"]`
  the node produces exactly 2 ConfigMaps with names/data equal to the element values.

**Deferred (F3+):**
- Executor wiring: the `Simple` executor's topological apply loop must publish the
  collection back into scope so downstream nodes can index into it.  Not exercised here;
  no envtest cluster needed for the parity proof.
- `forEach` on an `externalRef` is currently rejected by the translator
  (`ErrUnsupported`); that gap is intentional (external-ref collections require a
  different fetch path and are tracked separately).

---

## includeWhen (rows 7, 8, 9)

**Proven:**
- `copyStrings` preserves the `IncludeWhen` slice verbatim on both template and ref
  nodes.
- The compiler type-checks `${schema.spec.enabled}` (a bool field from the def node)
  as a valid bool expression.
- `runtime.Node.IsIgnored()`:
  - Returns `true` when `spec.enabled = false`.
  - Returns `false` when `spec.enabled = true`.
  - Contagious propagation (downstream node ignored when its dep is ignored) is already
    proven by the graphengine/runtime tests; the parity test exercises the RGD→Graph
    translation path into that same runtime.

**Deferred (F3+):**
- Executor: ignored nodes must be skipped during apply and their owned resources pruned.
  The Simple executor has a `IsIgnored` check in its loop; wiring RGD instances through
  it is F3+ work.

---

## readyWhen (rows 10, 11, 13)

**Proven:**
- `copyStrings` preserves the `ReadyWhen` slice verbatim.
- The compiler type-checks `${cm.data.ready == 'yes'}` — the expression references the
  node's own field after it is published into scope and returns bool.
- Before `SetObserved`: `Node.CheckReadiness()` returns `ErrWaitingForReadiness` (no
  observed state yet).
- After `Resolve()` + `SetObserved()` with the rendered ConfigMap (data.ready = "yes"):
  `CheckReadiness()` returns nil (ready).

**Deferred (F3+):**
- Full readiness gate needs the executor to: (1) apply the resource, (2) read back the
  cluster-observed state, (3) call `SetObserved`, (4) re-evaluate `CheckReadiness`, (5)
  requeue if `ErrWaitingForReadiness`.  The Runtime API is already wired for this; the
  executor integration is F3+ work.
- readyWhen on a collection node (forEach + readyWhen combined) is implicitly handled by
  the runtime's per-element loop but needs executor-level testing.

---

## externalRef / named (rows 18, 24, 30, 31)

**Proven:**
- A named RGD `externalRef` (apiVersion + kind + metadata.name + namespace) translates
  to `Node.Ref = *ExternalRef` (the same shared type).  All four fields are copied
  faithfully — the test asserts apiVersion, kind, name, and namespace individually.
- `Node.Template` is nil (the two discriminator fields are mutually exclusive).
- The Graph compiler accepts the Ref node: it calls
  `runtime.DefaultUnstructuredConverter.ToUnstructured(n.Ref)` to extract the GVK,
  resolves the schema via the fake resolver (ConfigMap), and builds a typed scope entry
  so downstream templates can reference `${cfg.metadata.name}`.
- The downstream template node (`consumer`) that references `${cfg.metadata.name}`
  compiles correctly with the Ref node's schema in scope.

**Executor gap (F3+):**
- `executor.Simple.applyNode` returns `ErrUnsupported` for `NodeKindRef` (ref nodes are
  cluster-reads, not applies).  The Graph executor does not yet implement a
  `client.Get`-based fetch path for Ref nodes.
- Consequence: at apply time the ref node's observed state is never populated, so
  downstream templates that read `${cfg.data.something}` would see data-pending errors.
- **Do NOT call executor.Apply in tests for ref nodes until F3+.**
- `externalRef` with a label-selector (collection ref) also returns `ErrUnsupported`
  from the translator and is tracked separately.

---

## Translator fix (none required)

The existing `graph_translate.go` `copyForEach` / `copyStrings` / `copyRaw` /
`res.ExternalRef.DeepCopy()` already maps all four features correctly.  No translator
changes were needed in F2.

The only surprise was that node ID `"item"` is in the compiler's `reservedNodeIDs` set
— the test was updated to use `"entry"` instead.  This is a test naming issue, not a
translator bug.

---

## Scoreboard mapping

| Row | Feature                          | F2 status                          |
|-----|----------------------------------|------------------------------------|
| 4   | forEach (single axis)            | translate+compile+resolve PASS      |
| 5   | forEach (multi-axis cartesian)   | covered by graphengine/runtime test; translation path proven |
| 7   | includeWhen (simple bool)        | PASS end-to-end at compile+resolve  |
| 8   | includeWhen (contagious)         | covered by graphengine/runtime test |
| 9   | includeWhen (dep-of-ignored)     | covered by graphengine/runtime test |
| 10  | readyWhen (type-check)           | PASS (compiler accepts bool expr)   |
| 11  | readyWhen (gate before observed) | PASS (ErrWaitingForReadiness)       |
| 13  | readyWhen (gate met)             | PASS (nil after SetObserved)        |
| 18  | externalRef named                | translate+compile PASS; apply gap F3+ |
| 24  | externalRef field access         | compile PASS (schema in scope); runtime gap F3+ |
| 30  | externalRef selector (collect.)  | ErrUnsupported in translator        |
| 31  | externalRef forEach              | ErrUnsupported in translator        |
