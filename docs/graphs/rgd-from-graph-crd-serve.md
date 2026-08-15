# CRD serving increment (RGD-on-Graph)

## What now works

The Graph-world adapter can **synthesize and serve** an RGD instance CRD
using kro's existing machinery — no new CRD diffing, compatibility, or
apply logic.

`pkg/graphengine/rgdadapter`:

- `SynthesizeInstanceCRD` — SimpleSchema → OpenAPI → `crd.SynthesizeCRD`
  (empty-status placeholder). Proven in the previous increment.
- `EnsureInstanceCRD` — synthesize, stamp `NewKROMetaLabeler` +
  `NewResourceGraphDefinitionLabeler` (same merge as
  `ensureServingState`), `crd.SetCRDStatus(..., Type=object, override=false)`
  so the apiserver accepts the empty-status placeholder, then
  `crdClient.Ensure`. Returns the Established CRD via `Get`.
- `DeleteInstanceCRD` — `Get` + ownership-label check + `Delete`. Missing
  CRDs are a no-op.

Envtest `TestRGDCRDServe*` (direct adapter + real `CRDClient` against
envtest; Graph controller is not involved) covers the `crd_test.go`
behaviors that do not need an RGD controller:

| `crd_test.go` | This increment |
|---|---|
| create (GVK / plural / scope / spec fields) | yes |
| non-breaking schema + shortNames / categories update | yes |
| delete + recreate | yes |
| ownership conflict (different RGD's label is not clobbered) | yes |
| breaking-change block + allow-breaking annotation | no — annotation + Inactive state live on the RGD controller |
| CRD watch / external delete → controller recreate | no — needs a CRD metadata watch on a Graph-backed RGD controller |
| conflicting-RGD delete must not stop the owner's instance controller | no — instance microcontroller |

## Ownership check: reuse vs replicate

- **Ensure / conflict:** not replicated. `CRDClient.Ensure` already calls
  `metadata.CompareRGDOwnership` and returns
  `CRD is owned by another ResourceGraphDefinition %s`. The adapter only
  stamps the same labels RGD stamps so that check fires.
- **Delete / skip-unowned:** replicated the 8-line
  `cleanupResourceGraphDefinitionCRD` label check
  (`ResourceGraphDefinitionNameLabel` must equal this RGD). That helper
  lives in `pkg/controller/resourcegraphdefinition`, which is too heavy
  to import from the adapter. RGD behavior is unchanged.

## Scoreboard

`crd_test.go` can move from **RGD-only** toward **expressible**: create /
non-breaking update / delete+recreate / ownership are now Graph-world
library calls. It cannot go green under a Graph-backed controller yet —
that suite creates RGDs and waits for the controller to serve / watch /
condition them.

## Still needed for `crd_test.go` under Graph

Wire a Graph-backed RGD reconciler to call `EnsureInstanceCRD` on
reconcile and `DeleteInstanceCRD` on cleanup, plus:

- CRD metadata watch → re-ensure (external delete / drift)
- `allow-breaking-changes` annotation → `allowBreakingChanges`
- KindReady / Inactive conditions so breaking-change and ownership
  conflicts surface as RGD status
- instance microcontroller (preserve-controller-on-conflict case)
- `SetCRDStatus` after Graph-engine status inference
- optional RGD → CRD owner references

That is the instance-controller / RGD-on-Graph reconcile increment, not
this one.
