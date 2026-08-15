# F5 – Ordered-Deletion Parity

## What is proved

`executor.Simple.Apply` appends `ManagedResource` entries in strict
topological (dependencies-first) order because the compiler's
`TopologicalSort` produces that order and the runtime walks it exactly as
given.  `executor.Simple.Delete` iterates the slice from the tail to the
head (index `len-1 … 0`), which is therefore dependents-first.

For the canonical test graph `cm1 ← cm2`:

| Step | Applied slice | Delete iteration |
|------|--------------|-----------------|
| Apply | `[cm1, cm2]` | — |
| Delete | — | `cm2, cm1` |

The compiler DAG's `ReverseTopologicalLayers` returns `[[cm2], [cm1]]` for
the same graph.  Layer 0 (`cm2`) is visited before layer 1 (`cm1`), matching
the reverse-slice order exactly.

**Parity result: PASS for linear chains.**

For parallel siblings (two nodes in the same topological layer, neither
depending on the other) the executor emits them consecutively in heap-order
(by original index, then lexicographic ID) and Delete reverses them.
`ReverseTopologicalLayers` places them in the same layer; order within the
layer is arbitrary.  Both approaches leave intra-layer ordering undefined, so
parity holds at the layer level.

## What the envtest asserts (`TestRGDDeletion`)

1. `Apply` order: `cm1` precedes `cm2` in `ApplyResult.Applied` (dependencies-first).
2. Reverse-slice delete order: `[cm2, cm1]` (dependents-first, proved on the
   slice — no cluster I/O needed for the ordering proof).
3. `DAG.ReverseTopologicalLayers` (after filtering the `schema` def node)
   yields `[[cm2], [cm1]]`, matching the reverse-slice order.
4. After `executor.Delete(ctx, applied)` both ConfigMaps are gone from the
   cluster (verified with `Eventually`).
5. Post-delete: `cm1` gone AND `cm2` gone (no ordering inversion possible
   since cm1 is only deleted after cm2).

All assertions green; `TestRGDDeletion` **PASS**.

## Scoreboard mapping

| Row | Status | Notes |
|-----|--------|-------|
| 30 (ordered_deletion) | PROVED | reverse-slice == ReverseTopologicalLayers for translated RGDs |
| cascading_deletion_test | PARTIAL | executor removes every ManagedResource; cascading finalizers / owner-refs from RGD's deletion controller are out of scope for F5 |

## Remaining gaps for F6+

### 1. Persisted deletion-order annotations
The current design re-derives the delete order from the live `Applied` list
returned by the last `Apply` call.  If the instance is deleted before the
next reconcile runs (i.e. between Apply and Delete), the controller must
persist the `Applied` slice so Delete can replay it even when
`BuildRuntimeForInstance` can no longer resolve all CEL expressions.  F4's
revision registry stores the last-good config but not the ordered
ManagedResources list as a deletion manifest.  F6 should write
`status.managedResources` (or a separate annotation) after every successful
Apply.

### 2. CRD-last on RGD teardown
RGD's own deletion controller deletes the *instance CRD* after all instances
are gone.  The Graph executor has no concept of "CRD teardown after all
managed resources".  This is a controller-layer concern (not executor), but
the ordering must be explicit: delete all managed-resource instances, then
delete the CRD itself.  Not a regression for F5 (executor never owned the
CRD lifecycle), but must be wired in the F6 controller.

### 3. Cascading nested deletion (subgraph nodes)
`applySubgraph` prefixes child NodeIDs with `parentID/`.  The `Applied` slice
therefore already encodes the full flat list in topological order (parent
nodes before their children, children before their dependencies — all
flattened by the recursive Apply walk).  Reverse-slice should be correct for
nested cases too, but F5 only proves the single-level case.  An F6 test with
a nested subgraph would close this gap.

### 4. Parallel-sibling delete ordering (cosmetic)
Within a single topological layer, reverse-slice reverses the heap-order of
siblings.  `ReverseTopologicalLayers` leaves intra-layer order undefined.
Both are valid; neither is "wrong".  A future annotation could record the
full flat order (not layers) if deterministic replay is required.

### 5. UID-precondition on out-of-band deletion
Already handled by `executor.Simple.Delete` (Conflict → skip).  Covered by
the existing unit tests in `simple_test.go`.  No gap.
