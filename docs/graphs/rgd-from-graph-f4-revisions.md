# F4 – GraphRevisions / last-good-config (in-memory)

## What F4 provides

F4 gives the RGD-on-Graph path the same **last-good-config** guarantee that
the classic RGD controller's `recover_test.go` proves (scoreboard row 25):
when an RGD spec update fails to compile, existing instances keep running on
the last successfully-compiled program while the RGD itself is marked
not-ready (degraded).

## How it works

### Hash + Registry

`pkg/graph/hash.Spec(rgd.Spec)` computes a normalized FNV-64a fingerprint of
an RGD spec (sorted resources, canonical JSON). This is the cache key used
alongside `types.NamespacedName` to identify revisions in memory.

`pkg/graphengine/registry.Registry` was the existing compile cache.  It
stores `(key → {hash, *Program})` entries. Its `Lookup(key, hash)` returns a
program only when the hash matches (i.e. the spec hasn't changed).

### Registry method added: `LastGood`

```go
func (r *Registry) LastGood(key types.NamespacedName) (*compiler.Program, string, bool)
```

Returns whatever program is **currently stored** for `key`, regardless of its
hash. The registry stores the **most recent successful compile** per key, so
`LastGood` is exactly "give me the last program that compiled, even if the
spec has since changed."

- Thread-safe (read lock only).
- Returns `(nil, "", false)` if nothing has ever been stored for the key.
- Non-destructive: does not change the registry state.

### `ResolveProgram` (pkg/graphengine/rgdadapter/revision.go)

```go
func ResolveProgram(
    reg *registry.Registry,
    key types.NamespacedName,
    rgd *v1alpha1.ResourceGraphDefinition,
    c Compiler,
) (prog *compiler.Program, servedHash string, degraded bool, err error)
```

Decision tree:

| Condition | prog | servedHash | degraded | err |
|-----------|------|------------|----------|-----|
| Cache hit (spec unchanged) | cached | current hash | false | nil |
| Fresh compile OK | new | current hash | false | nil |
| Compile fails + prior good exists | **last good** | **prior hash** | **true** | compile err |
| Compile fails + no prior | nil | "" | false | compile err |

The `degraded=true` + `err!=nil` + `prog!=nil` triple is the signal for the
caller (F6 RGD controller reconciler) to:
1. Mark the RGD condition `Ready=False` with the compile error message.
2. Still pass `prog` to `BuildRuntimeForInstance` so in-flight instances keep
   executing against the last known-good program.

## Scoreboard row 25 mapping

Row 25 (`recover_test.go`): "bad RGD update → instances served by last good
revision; RGD goes not-ready."

`ResolveProgram` is the in-memory half of this contract.  The controller wiring
(F6) must:
- Call `ResolveProgram` on every RGD reconcile.
- When `degraded=true`, set RGD condition `Programmed=False` / `Ready=False`.
- Pass the returned `prog` (not nil) to `BuildRuntimeForInstance` so instances
  still reconcile.

## Relation to graphrevision_* suites

The two `graphrevision_*` test suites (rows ~26-28) test:
1. **Immutable GraphRevision CR creation** — each successful compile writes a
   `GraphRevision` CR into the cluster (name = RGD-name + hash suffix,
   immutable spec). F4 does NOT write CRs; it only maintains in-memory
   last-good state.
2. **GraphRevision controller** — a separate controller that watches
   `GraphRevision` CRs and reloads the compiled program from etcd after a
   pod restart (persistence across restarts). F4 does NOT do this.

## What remains for full GraphRevision-CR parity (F6)

1. **GraphRevision CR write**: after `reg.Store(...)` succeeds, create/update
   a `GraphRevision` CR with `spec.programHash = currentHash` and an
   immutable annotation. The revision spec could embed the compiled program
   or a reference to it.
2. **GraphRevision controller**: on pod restart, the in-memory registry is
   empty. The controller must re-populate `reg.Store(key, hash, program)` by
   recompiling from the GraphRevision CR's stored graph spec, so `LastGood`
   returns immediately without needing a fresh RGD reconcile.
3. **Controller wiring**: thread `reg *registry.Registry` into the RGD
   reconciler and replace the current `BuildRuntimeForInstance` call with
   `ResolveProgram` + `BuildRuntimeForInstance(rgd, instance, c)` using the
   returned program.
4. **Status conditions**: emit `Degraded=True` / `Programmed=False` conditions
   on the RGD when `degraded=true` (the `scoreboard row 25` not-ready signal).
