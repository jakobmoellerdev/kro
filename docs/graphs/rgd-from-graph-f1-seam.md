# RGD-from-Graph — F1 seam (RGD → Graph reconcile)

**Result: PASS.** `TestReconcileParity_SchemaAndCrossNode` proves an RGD's
composition runs on the Graph engine.

## What F1 establishes
- `rgdadapter.ResourceGraphDefinitionToGraph(rgd)` maps RGD resources → Graph
  nodes (template → Node.Template, named externalRef → Node.Ref; forEach /
  readyWhen / includeWhen carried through). Unmapped shapes return ErrUnsupported.
- **Instance scope seam:** the Graph compiler rejects unknown top-level
  identifiers, so `${schema.spec.*}` cannot resolve via post-compile seeded
  scope. Instead `rgdadapter.InstanceSchemaNode(instance)` materialises the
  instance as a `def` node named `schema`. The translated Graph + this def node
  compiles, and `${schema.spec.value}` resolves from the instance at runtime.
- Cross-node CEL (`${cm1.metadata.name}`) resolves through the Graph runtime
  after topological publish — same as the executor's walk.

## Key finding
The instance surface (F3 "SimpleSchema→instance") reduces to a `def schema`
node — the Graph engine already resolves it once the node is declared. WithSeedScope
(subgraph capture) is NOT the mechanism; a declared def node is.

## Next (F2+)
- F2: forEach / includeWhen / readyWhen / externalRef parity (extend translator + tests).
- F3: full instance surface — schema.status projection, author conditions, defaulting;
  wire InstanceSchemaNode into a reconcile that drives executor.Apply in envtest.
- F5: ordered-deletion parity. F6: flag-gated RGD-on-Graph controller + 36/36 gate.
