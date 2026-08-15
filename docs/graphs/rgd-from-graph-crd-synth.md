# CRD synthesis increment (RGD-on-Graph)

## What this proves

An RGD's instance CRD can be synthesized in the Graph-engine world by
**wiring**, not by writing new synthesis code.

`pkg/graphengine/rgdadapter.SynthesizeInstanceCRD` is a thin adapter:

1. `graph.BuildInstanceSpecSchema` — the exported form of RGD's
   `buildInstanceSpecSchema` (SimpleSchema → OpenAPI via `pkg/simpleschema`)
2. `crd.SynthesizeCRD` — same call `NewResourceGraphDefinition` makes
   before `SetCRDStatus`, with an empty-status placeholder and
   `statusFieldsOverride=false`

The unit test asserts the served-version shape RGD produces (group /
version / kind / plural, `Scope=Namespaced`, spec fields in OpenAPI) and
parity against that shared `SynthesizeCRD` call. It also builds the same
RGD through `Builder.NewResourceGraphDefinition` (fake schema resolver +
REST mapper, no resources) and diffs the CRD **minus status** — the
builder then fills status via `SetCRDStatus` / CEL inference, which this
increment deliberately does not do.

The flip-blocker is wiring this adapter into a Graph-engine RGD
controller, not inventing another CRD synthesizer.

## Deferred: controller-half CRD lifecycle

The scoreboard's `crd_test.go` (integration: serve / update /
breaking-vs-nonbreaking / ownership-conflict / recreate-on-delete) is
**not** in this increment. That is instance-CRD *ownership* — apply,
watch, conflict, recreate — and belongs on the RGD-on-Graph controller,
not in synthesis.

Remaining CRD-synth / CRD-lifecycle work:

- Serve the synthesized CRD (create + establish + wait for Established)
- Update an existing instance CRD when the RGD schema changes
- Distinguish breaking vs non-breaking schema changes
- Ownership conflict if another controller/RGD already owns the CRD
- Recreate-on-delete if the CRD is deleted while the RGD still exists
- `SetCRDStatus` after Graph-engine status inference (default
  `state` / `conditions` fields)
- Owner references from RGD → instance CRD

Do not implement those here.
