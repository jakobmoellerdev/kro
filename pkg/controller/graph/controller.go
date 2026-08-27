// Copyright 2025 The Kube Resource Orchestrator Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package graph

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlrtcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/apis"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/compiler"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/executor"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/registry"
	krotruntime "github.com/kubernetes-sigs/kro/pkg/graphengine/runtime"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/schemawatcher"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/watchrouter"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
)

// notReadyRequeueAfter is the requeue delay for the first not-ready attempt
// when an executor surfaces ErrNotReady. Subsequent consecutive not-ready
// attempts back off exponentially (see backoff.go) up to backoffMax, so a
// never-resolving reference decays to a slow poll instead of a 1/sec hammer.
// A clean reconcile resets the streak. Kept as backoffBase.
const notReadyRequeueAfter = backoffBase

// Compiler is the narrow surface the reconciler needs from a Compiler. Kept
// as an interface so tests can substitute a fake without spinning up a real
// cluster.
type Compiler interface {
	Compile(*expv1alpha1.Graph) (*compiler.Program, error)
}

// Reconciler reconciles Graph objects. Compilation is mediated through a
// Registry so Graphs only recompile when their normalized spec hash changes.
// The Executor is consulted on every reconcile to converge the cluster
// toward the compiled Program's desired state. The Router hands out
// per-Graph Watchers so the executor can register interest in each
// resolved resource — drift events flow back through Router.Source()
// into the controller-runtime work queue.
//
// The SchemaWatcher (if wired) tracks which CRD GroupKinds each Graph
// depends on. On a CRD content change the watcher invalidates the
// compile cache for affected Graphs and enqueues them — the next
// reconcile recompiles against the fresh schema.
type Reconciler struct {
	Client                  client.Client
	Compiler                Compiler
	Registry                *registry.Registry
	Executor                executor.Interface
	Router                  *watchrouter.Router
	SchemaWatcher           *schemawatcher.SchemaWatcher
	MaxConcurrentReconciles int
	MaxCollectionSize       int

	// Impersonation, when set, resolves a per-Graph executor that applies the
	// Graph's resources while impersonating a ServiceAccount in the Graph's
	// namespace. When nil, the Graph's resources are applied with the kro
	// controller identity (the base Executor).
	Impersonation *impersonationCache

	// RequireImpersonation makes the Graph path fail closed: when true, a Graph
	// reconcile MUST have a working impersonation path (Impersonation and its
	// executor factory wired). If it is not, the Graph is refused rather than
	// silently applied under the kro controller's own (broad) identity — a
	// wiring regression fails safe instead of escalating. Production sets this
	// true; unit tests that construct a Reconciler with only an Executor leave
	// it false to keep the base-executor fallback (see executorFor).
	RequireImpersonation bool

	// ControllerServiceAccount is the impersonation username of kro's OWN
	// ServiceAccount ("system:serviceaccount:<ns>:<name>"), used to refuse a
	// Graph that would impersonate the controller's own (privileged) identity
	// — otherwise `create graphs` in the controller's namespace escalates to
	// whatever the controller SA can do. Empty when not wired (unit tests /
	// impersonation disabled), in which case the guard is a no-op. Any OTHER
	// privileged SA reachable in a namespace is the operator's RBAC concern.
	ControllerServiceAccount string

	// backoff tracks per-Graph consecutive not-ready attempts so the soft
	// ErrNotReady requeue delay grows (capped) instead of polling a
	// never-resolving reference once per second forever. Lazily initialized
	// via backoffOnce so a directly-constructed Reconciler (tests) works too.
	backoff     *requeueBackoff
	backoffOnce sync.Once
}

// ensureBackoff lazily initializes the per-Graph requeue backoff tracker.
// Safe to call from multiple reconcile workers.
func (r *Reconciler) ensureBackoff() {
	r.backoffOnce.Do(func() {
		if r.backoff == nil {
			r.backoff = newRequeueBackoff()
		}
	})
}

// Reconcile is the main reconcile loop for Graph objects.
//
// Order: deletion handling first, then ensure the finalizer is set, run the
// reconcile body, and finally publish status (with a single
// retry-on-conflict patch). Each path writes its condition via the typed
// ConditionsMarker — never touches Status.Conditions directly.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.ensureBackoff()
	logger := log.FromContext(ctx).WithValues("graph", req.NamespacedName)

	var g expv1alpha1.Graph
	if err := r.Client.Get(ctx, req.NamespacedName, &g); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get graph: %w", err)
	}

	if !g.DeletionTimestamp.IsZero() {
		logger.V(1).Info("graph is deleting")
		// Resolve the executor from the identity the Graph ACTUALLY applied
		// under (persisted in status), not the current spec.serviceAccountName:
		// editing that field between apply and delete would otherwise run
		// teardown as an identity that can no longer see the resources, orphaning
		// children and wedging the finalizer. Fall back to the current spec when
		// no applied identity was recorded (never applied, or a pre-field kro).
		ex, err := r.teardownExecutorFor(&g)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("resolve impersonated executor: %w", err)
		}
		// Delete operates entirely from the persisted tracking record.
		// No compile, no resolve — a Graph whose spec was edited
		// (rename, forEach shrunk, node dropped) still gets every
		// resource it ever applied removed.
		if len(g.Status.ManagedResources) > 0 {
			if err := ex.Delete(ctx, g.Status.ManagedResources); err != nil {
				return ctrl.Result{}, fmt.Errorf("executor delete: %w", err)
			}
		}
		// Release every recorded patch contribution so the field managers
		// relinquish their fields. Targets survive — patches never own them.
		contribs, err := ReadContributions(&g)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("read patch contributions on delete: %w", err)
		}
		if len(contribs) > 0 {
			if err := ex.Release(ctx, contribs); err != nil {
				return ctrl.Result{}, fmt.Errorf("executor release: %w", err)
			}
		}
		r.Registry.Delete(req.NamespacedName)
		r.backoff.reset(req.NamespacedName)
		if r.Router != nil {
			r.Router.RemoveGraph(req.NamespacedName)
		}
		if r.SchemaWatcher != nil {
			r.SchemaWatcher.RemoveGraph(req.NamespacedName)
		}
		if err := r.setUnmanaged(ctx, &g); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if err := r.setManaged(ctx, &g); err != nil {
		return ctrl.Result{}, err
	}

	reconcileErr := r.reconcileGraph(ctx, &g)

	// Keep the status-write error separate from the apply error: a soft
	// ErrNotReady is a benign requeue, but a failed status write must never be
	// swallowed (the ErrNotReady classification below is done on the apply
	// error only, so a stale reported state can't hide behind not-ready).
	statusErr := r.updateStatus(ctx, &g)

	// A not-ready signal is a soft requeue: the spec compiled, apply
	// succeeded, the cluster just hasn't converged on readyWhen yet (or a
	// referenced field isn't visible). Return nil error so controller-runtime
	// doesn't apply its own error backoff, but ask for a timed requeue with a
	// capped exponential delay so a never-resolving reference (e.g. a typo)
	// decays to a slow poll instead of a 1/sec hammer.
	if errors.Is(reconcileErr, executor.ErrNotReady) {
		// Apply is a soft requeue — but never discard a status-write failure.
		if statusErr != nil {
			r.backoff.reset(req.NamespacedName)
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: r.backoff.next(req.NamespacedName)}, nil
	}
	// Any other outcome (clean converge or a hard error that will be retried
	// with controller-runtime backoff) ends the not-ready streak, so a fixed
	// typo returns to fast requeues on its next stall.
	r.backoff.reset(req.NamespacedName)
	return ctrl.Result{}, errors.Join(reconcileErr, statusErr)
}

// reconcileGraph runs the actual reconciliation body. Compilation goes
// through the Registry so identical specs across reconciles share one
// compiled Program. Conditions are written via the ConditionsMarker; status
// is flushed by the caller via updateStatus.
//
// If a Router is wired in, we open a per-Graph Watcher around
// Apply so each resolved resource registers a watch. On success commit
// the new watch set (clearing any nodes that were removed in this
// revision); on error abort, keeping the previously committed set
// authoritative so drift detection survives transient failures.
func (r *Reconciler) reconcileGraph(ctx context.Context, g *expv1alpha1.Graph) error {
	marker := NewConditionsMarkerFor(g)
	key := client.ObjectKeyFromObject(g)

	// Refuse a Graph that would impersonate the controller's OWN ServiceAccount.
	// Because the SA is always resolved in the Graph's own namespace, this can
	// only match a Graph in the controller's namespace naming the controller SA
	// (or its default) — which would let `create graphs` there escalate to the
	// controller's privileges. Reject before compile/apply and never requeue as
	// an error: this is a permanent config problem the author must fix.
	if r.impersonatesControllerSelf(g) {
		marker.GraphInvalid(fmt.Sprintf(
			"refusing to apply: Graph would impersonate the kro controller's own ServiceAccount (%q); "+
				"choose a different serviceAccountName or move the Graph out of the controller's namespace",
			r.ControllerServiceAccount))
		return nil
	}

	// Fail closed: when impersonation is required (production) but not wired,
	// refuse to apply rather than falling back to the kro controller's own
	// (broad) identity. This turns a wiring regression into a visible, non-
	// escalating failure. Never requeue as an error: it is a permanent
	// controller misconfiguration an operator must fix.
	if r.RequireImpersonation && (r.Impersonation == nil || r.Impersonation.newExec == nil) {
		marker.ResourcesApplyFailed(
			"refusing to apply: impersonation is required but not configured; " +
				"the Graph controller must be wired with an impersonation path")
		return nil
	}

	// Declare this Graph's schema dependencies from the parsed spec before
	// compilation. This ensures that even if Compile fails (e.g. because a
	// referenced Kind's CRD does not exist yet), the SchemaWatcher tracks
	// the dependency and re-enqueues the Graph when that CRD appears.
	schemaSub := r.schemaSubFor(key)
	trackSchemaDependencies(schemaSub, g.Spec.Nodes)
	schemaSub.Done(true)

	prog, cached, err := r.Registry.Compile(key, g, r.Compiler.Compile)
	if err != nil {
		marker.GraphInvalid(err.Error())
		return err
	}
	if cached {
		log.FromContext(ctx).V(1).Info("compile cache hit", "nodes", len(prog.Nodes))
	}
	marker.GraphCompiled(len(prog.Nodes))

	var rtOpts []krotruntime.Option
	if r.MaxCollectionSize > 0 {
		rtOpts = append(rtOpts, krotruntime.WithMaxCollectionSize(r.MaxCollectionSize))
	}
	rt := krotruntime.New(prog, g, rtOpts...)
	watcher := r.watcherFor(key)
	previous := g.Status.ManagedResources
	priorContribs, err := ReadContributions(g)
	if err != nil {
		return fmt.Errorf("read patch contributions: %w", err)
	}

	// Resolve the executor bound to this Graph's impersonated identity. All
	// resource writes (apply, prune, release) for this Graph go through it so
	// they share one identity confined to the Graph's namespace.
	ex, err := r.executorFor(g)
	if err != nil {
		marker.ResourcesApplyFailed(err.Error())
		return fmt.Errorf("resolve impersonated executor: %w", err)
	}

	// Write-ahead the pre-apply intent BEFORE any cluster write. Teardown runs
	// entirely from g.Status.ManagedResources, so a status write lost between
	// Apply (which creates children) and the post-apply persist would orphan
	// those children permanently. Persist the union of previous + intended
	// identities so a crash after Apply still leaves teardown a superset to
	// delete from (mirrors the instance ApplySet union-never-shrinks path).
	// Intent is best-effort and UID-free; keyOf dedups post-apply entries.
	intent := unionManagedResources(previous, intendedManagedResources(rt))
	if len(intent) > len(previous) {
		g.Status.ManagedResources = intent
		if err := r.persistManagedResources(ctx, g); err != nil {
			return fmt.Errorf("write-ahead managed-resource intent: %w", err)
		}
	}

	result, applyErr := ex.Apply(ctx, rt, watcher)

	// Commit on full success or soft ErrNotReady — the executor walks
	// every reachable node even when some are not ready, so the watch
	// set is authoritative either way. Abort only on hard errors that
	// interrupted the walk before downstream watches could register.
	switch {
	case applyErr == nil:
		watcher.Done(true)
		marker.ResourcesConverged()
	case errors.Is(applyErr, executor.ErrNotReady):
		watcher.Done(true)
		// Distinguish the two not-ready flavors so principals can tell
		// "apply succeeded, cluster still settling" from "upstream data
		// isn't visible yet, can't even resolve dependents."
		if errors.Is(applyErr, krotruntime.ErrDataPending) {
			marker.ResourcesDataPending(applyErr.Error())
		} else {
			marker.ResourcesNotReady(applyErr.Error())
		}
	default:
		watcher.Done(false)
		marker.ResourcesApplyFailed(applyErr.Error())
	}

	// Record the identity that applied under, but ONLY when the apply reached
	// the cluster (clean or soft not-ready) — never on a hard failure, which
	// must preserve the last-good identity so teardown can still see resources a
	// prior identity applied. Empty when impersonation is inactive (the base
	// controller identity), in which case teardown falls back to the spec.
	if !errors.Is(applyErr, executor.ErrNotReady) && applyErr != nil {
		// hard failure: leave AppliedServiceAccount untouched
	} else if user := r.appliedIdentity(g); user != "" {
		g.Status.AppliedServiceAccount = user
	}

	// Diff previous vs the new applied set. Entries whose NodeID is
	// in result.Unresolved are kept (we don't know their identities
	// this cycle, so don't prune them). Everything else missing from
	// Applied is a prune candidate: node dropped from spec, forEach
	// shrunk, includeWhen flipped, or rename.
	newSet, pruneCandidates := diffManagedResources(previous, result)

	// A soft ErrNotReady still walked every reachable node (that is why the
	// watch set was committed above via Done(true)), so the Applied/Unresolved
	// partition — and therefore pruneCandidates — is authoritative: a candidate
	// here is a resource the executor is confident is no longer wanted (node
	// dropped, forEach shrunk, includeWhen flipped, rename), never one merely
	// data-pending (those are recorded Unresolved and preserved by
	// diffManagedResources). Pruning must therefore run on a clean apply OR a
	// soft not-ready — otherwise a Graph that never converges (e.g. a sibling
	// resource with an unsatisfiable readyWhen) could never prune a
	// legitimately-retired resource, diverging from the built-in controller.
	// A HARD error may have aborted the walk before downstream nodes were
	// reached (watcher.Done(false)); those nodes are neither Applied nor
	// Unresolved and would be misclassified as prune candidates, so pruning is
	// still withheld and the union is kept for a future reconcile.
	walkComplete := applyErr == nil || errors.Is(applyErr, executor.ErrNotReady)

	if walkComplete {
		if len(pruneCandidates) > 0 {
			if err := ex.Delete(ctx, pruneCandidates); err != nil {
				// Prune failure isn't catastrophic — next reconcile
				// retries with the same diff. But we shouldn't shrink
				// status to newSet if some prune candidates are still
				// in the cluster, so keep the union.
				log.FromContext(ctx).Error(err, "prune failed; keeping union in status")
				g.Status.ManagedResources = unionManagedResources(
					unionManagedResources(previous, result.Applied), intent)
				return fmt.Errorf("prune: %w", err)
			}
		}
		if applyErr == nil {
			g.Status.ManagedResources = newSet
		} else {
			// Soft not-ready: the retired candidates were pruned above, so
			// they must not reappear in status. newSet already excludes them
			// and retains the Unresolved (data-pending) entries; fold intent
			// back in so a lost status write can't shrink the server inventory
			// below the pre-apply superset (UID-free intent dedups via keyOf).
			g.Status.ManagedResources = unionManagedResources(newSet, intent)
		}
	} else {
		// Hard failure — keep the union so a future reconcile can still
		// prune or restore. Fold intent back in so the terminal updateStatus
		// cannot shrink the server inventory below the pre-apply superset and
		// re-open the orphan window; UID-free intent dedups against Applied via
		// keyOf.
		g.Status.ManagedResources = unionManagedResources(
			unionManagedResources(previous, result.Applied), intent)
	}

	// Release contributions whose patch node was removed or whose target
	// changed is done on the CLEAN-apply path only (below), symmetric with
	// prune: on a soft/hard error we cannot tell a genuinely-removed patch node
	// apart from one that is merely data-pending this cycle (its contribution
	// is simply absent from result.Contributions either way, and Contribution
	// carries no NodeID to correlate with result.Unresolved), so releasing here
	// would drop fields a still-wanted patch node set — a transient flap. The
	// field-manager-identity-change deadlock this used to guard is now fixed at
	// the source in the executor (contributeApply force-reclaims a same-Graph
	// stale patch identity), so a re-keyed patch node resolves and re-appears in
	// result.Contributions rather than needing a release to break the wedge.

	if applyErr != nil {
		// Soft or hard failure — keep the union so a future reconcile can
		// release contributions we couldn't observe cleanly this cycle.
		if err := r.persistContributions(ctx, g, UnionContributions(priorContribs, result.Contributions)); err != nil {
			return errors.Join(fmt.Errorf("apply: %w", applyErr), err)
		}
		if !errors.Is(applyErr, executor.ErrNotReady) {
			log.FromContext(ctx).Error(applyErr, "executor apply failed")
		}
		return fmt.Errorf("apply: %w", applyErr)
	}

	// Clean apply: release contributions whose patch node was removed or
	// whose target changed, then persist the current inventory. Release runs
	// before persist so a release failure keeps the prior inventory for the
	// next reconcile.
	if released := DiffContributions(priorContribs, result.Contributions); len(released) > 0 {
		if err := ex.Release(ctx, released); err != nil {
			if perr := r.persistContributions(ctx, g, UnionContributions(priorContribs, result.Contributions)); perr != nil {
				return errors.Join(fmt.Errorf("release contributions: %w", err), perr)
			}
			return fmt.Errorf("release contributions: %w", err)
		}
	}
	if err := r.persistContributions(ctx, g, result.Contributions); err != nil {
		return err
	}
	return nil
}

// persistContributions writes the patch-contribution inventory onto the
// Graph as an annotation, patching only when the value changed. An empty
// inventory drops the annotation.
func (r *Reconciler) persistContributions(
	ctx context.Context,
	g *expv1alpha1.Graph,
	contribs []executor.Contribution,
) error {
	value, err := MarshalContributions(contribs)
	if err != nil {
		return fmt.Errorf("marshal contributions: %w", err)
	}
	if g.GetAnnotations()[metadata.PatchContributionsAnnotation] == value {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &expv1alpha1.Graph{}
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(g), current); err != nil {
			return err
		}
		if current.GetAnnotations()[metadata.PatchContributionsAnnotation] == value {
			return nil
		}
		dc := current.DeepCopy()
		anns := dc.GetAnnotations()
		if anns == nil {
			anns = make(map[string]string, 1)
		}
		if value == "" {
			delete(anns, metadata.PatchContributionsAnnotation)
		} else {
			anns[metadata.PatchContributionsAnnotation] = value
		}
		dc.SetAnnotations(anns)
		if err := r.Client.Patch(ctx, dc, client.MergeFrom(current)); err != nil {
			return err
		}
		if g.Annotations == nil {
			g.Annotations = make(map[string]string, 1)
		}
		if value == "" {
			delete(g.Annotations, metadata.PatchContributionsAnnotation)
		} else {
			g.Annotations[metadata.PatchContributionsAnnotation] = value
		}
		return nil
	})
}

// watcherFor returns a per-Graph Watcher when a Router is
// wired, or a NoopWatcher otherwise. The Noop fallback keeps the
// reconciler usable in unit tests and dry-run contexts.
func (r *Reconciler) watcherFor(key client.ObjectKey) watchrouter.Watcher {
	if r.Router == nil {
		return watchrouter.NoopWatcher{}
	}
	return r.Router.ForGraph(key)
}

// schemaSubFor returns a per-Graph schema Subscription when a watcher
// is wired, or a no-op subscription otherwise.
func (r *Reconciler) schemaSubFor(key client.ObjectKey) schemawatcher.Subscription {
	if r.SchemaWatcher == nil {
		return noopSchemaSubscription{}
	}
	return r.SchemaWatcher.ForGraph(key)
}

// noopSchemaSubscription is the inert fallback used when the
// reconciler has no SchemaWatcher (unit tests, CLI / dry-run).
type noopSchemaSubscription struct{}

func (noopSchemaSubscription) Track(schema.GroupKind) {}
func (noopSchemaSubscription) TrackDynamic()          {}
func (noopSchemaSubscription) Done(bool)              {}

type typeMeta struct {
	APIVersion string `json:"apiVersion" yaml:"apiVersion"`
	Kind       string `json:"kind" yaml:"kind"`
}

// trackSchemaDependencies extracts GroupKind dependencies and dynamic-GVK flags
// directly from the declared GraphSpec nodes and registers them with the
// schema subscription.
func trackSchemaDependencies(sub schemawatcher.Subscription, nodes []expv1alpha1.Node) {
	for i := range nodes {
		n := &nodes[i]
		switch {
		case n.Def != nil:
			// Def nodes define local values without cluster schemas.
		case n.Graph != nil:
			var subSpec expv1alpha1.GraphSpec
			if len(n.Graph.Raw) > 0 {
				if err := yaml.Unmarshal(n.Graph.Raw, &subSpec); err == nil {
					trackSchemaDependencies(sub, subSpec.Nodes)
				} else {
					log.Log.V(1).Info("failed to parse subgraph in trackSchemaDependencies",
						"nodeID", n.ID, "error", err)
				}
			}
		case n.Patch != nil:
			if len(n.Patch.Raw) > 0 {
				var tm typeMeta
				if err := yaml.Unmarshal(n.Patch.Raw, &tm); err == nil {
					extractAndTrackGVK(sub, n.ID, tm.APIVersion, tm.Kind)
				} else {
					log.Log.V(1).Info("failed to parse patch in trackSchemaDependencies",
						"nodeID", n.ID, "error", err)
				}
			}
		case n.Ref != nil:
			extractAndTrackGVK(sub, n.ID, n.Ref.APIVersion, n.Ref.Kind)
		case n.Template != nil:
			if len(n.Template.Raw) > 0 {
				var tm typeMeta
				if err := yaml.Unmarshal(n.Template.Raw, &tm); err == nil {
					extractAndTrackGVK(sub, n.ID, tm.APIVersion, tm.Kind)
				} else {
					log.Log.V(1).Info("failed to parse template in trackSchemaDependencies",
						"nodeID", n.ID, "error", err)
				}
			}
		}
	}
}

func extractAndTrackGVK(sub schemawatcher.Subscription, nodeID, apiVersion, kind string) {
	isDynamic := strings.Contains(apiVersion, "${") || strings.Contains(kind, "${")
	if isDynamic {
		sub.TrackDynamic()
	}

	if kind == "" || strings.Contains(kind, "${") {
		return
	}

	// If apiVersion has no expressions, parse standard GroupVersion.
	if !strings.Contains(apiVersion, "${") {
		if apiVersion != "" {
			gv, err := schema.ParseGroupVersion(apiVersion)
			if err != nil {
				log.Log.V(1).Info("failed to parse apiVersion in trackSchemaDependencies",
					"nodeID", nodeID, "apiVersion", apiVersion, "kind", kind, "error", err)
				return
			}
			sub.Track(schema.GroupKind{Group: gv.Group, Kind: kind})
		}
		return
	}

	// apiVersion is dynamic, but group might be static: e.g. "apps/${version}" or "example.com/${version}".
	if before, _, ok := strings.Cut(apiVersion, "/"); ok {
		groupPart := before
		if !strings.Contains(groupPart, "${") && groupPart != "" {
			sub.Track(schema.GroupKind{Group: groupPart, Kind: kind})
		}
	}
}

// setManaged ensures the Graph carries the finalizer. Uses a strategic patch
// against the freshly-fetched object with retry-on-conflict.
func (r *Reconciler) setManaged(ctx context.Context, g *expv1alpha1.Graph) error {
	if metadata.HasGraphFinalizer(g) {
		return nil
	}
	log.FromContext(ctx).V(1).Info("setting graph as managed")
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &expv1alpha1.Graph{}
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(g), current); err != nil {
			return err
		}
		if metadata.HasGraphFinalizer(current) {
			return nil
		}
		dc := current.DeepCopy()
		metadata.SetGraphFinalizer(dc)
		if err := r.Client.Patch(ctx, dc, client.MergeFrom(current)); err != nil {
			return err
		}
		metadata.SetGraphFinalizer(g)
		return nil
	})
	if err != nil {
		return fmt.Errorf("set managed: %w", err)
	}
	return nil
}

// setUnmanaged drops the Graph finalizer if present so the API server can
// complete deletion. The deletion path removes managed resources before
// calling this.
func (r *Reconciler) setUnmanaged(ctx context.Context, g *expv1alpha1.Graph) error {
	if !metadata.HasGraphFinalizer(g) {
		return nil
	}
	log.FromContext(ctx).V(1).Info("setting graph as unmanaged")
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &expv1alpha1.Graph{}
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(g), current); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if !metadata.HasGraphFinalizer(current) {
			return nil
		}
		dc := current.DeepCopy()
		metadata.RemoveGraphFinalizer(dc)
		return r.Client.Patch(ctx, dc, client.MergeFrom(current))
	})
	if err != nil {
		return fmt.Errorf("set unmanaged: %w", err)
	}
	return nil
}

// updateStatus flushes Status fields onto the API server with a retry on
// conflict. Conditions are only published if g.Generation still matches
// the live object's generation — a stale generation means the spec
// changed mid-reconcile and our condition values (computed against the
// old spec) would be misleading. In that case we skip the write and let
// the next reconcile re-evaluate against the fresh spec.
//
// The DeepEqual short-circuit avoids no-op writes, which keeps the
// generation churn down and prevents needless re-reconciles.
func (r *Reconciler) updateStatus(ctx context.Context, g *expv1alpha1.Graph) error {
	logger := log.FromContext(ctx)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &expv1alpha1.Graph{}
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(g), current); err != nil {
			return fmt.Errorf("refetch graph: %w", err)
		}
		dc := current.DeepCopy()
		if current.Generation != g.Generation {
			logger.V(1).Info("skipping stale status conditions write but preserving managed resources union",
				"observed-generation", g.Generation,
				"current-generation", current.Generation)
			dc.Status.ManagedResources = unionManagedResources(current.Status.ManagedResources, g.Status.ManagedResources)
		} else {
			dc.Status.Conditions = g.Status.Conditions
			dc.Status.ManagedResources = g.Status.ManagedResources
		}
		// Persist the applied identity whenever we have one (never regress a
		// recorded identity to empty), independent of the generation gate: it
		// tracks the identity that applied the resources, which teardown depends
		// on and which is orthogonal to spec generation.
		if g.Status.AppliedServiceAccount != "" {
			dc.Status.AppliedServiceAccount = g.Status.AppliedServiceAccount
		}

		if equality.Semantic.DeepEqual(current.Status, dc.Status) {
			return nil
		}
		logger.V(1).Info("updating graph status",
			"conditions", len(dc.Status.Conditions),
			"managedResources", len(dc.Status.ManagedResources))
		return r.Client.Status().Patch(ctx, dc, client.MergeFrom(current))
	})
}

// impersonatesControllerSelf reports whether g would apply its resources under
// the kro controller's OWN ServiceAccount identity. Guards the escalation where
// a Graph in the controller's namespace names the controller SA (or the
// namespace default resolves to it). A no-op when ControllerServiceAccount is
// unset (impersonation disabled / unit tests).
func (r *Reconciler) impersonatesControllerSelf(g *expv1alpha1.Graph) bool {
	return r.ControllerServiceAccount != "" &&
		serviceAccountUsername(g) == r.ControllerServiceAccount
}

// persistManagedResources flushes ONLY g.Status.ManagedResources (retry on
// conflict), leaving Status.Conditions untouched. The write-ahead vehicle for
// the pre-apply intent: unlike updateStatus it never gates on Generation and
// unions with the live inventory, so the write can only grow the tracked set.
func (r *Reconciler) persistManagedResources(ctx context.Context, g *expv1alpha1.Graph) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &expv1alpha1.Graph{}
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(g), current); err != nil {
			return fmt.Errorf("refetch graph: %w", err)
		}
		dc := current.DeepCopy()
		union := unionManagedResources(current.Status.ManagedResources, g.Status.ManagedResources)
		dc.Status.ManagedResources = union
		// Keep the in-memory Graph consistent with what we persisted so the
		// post-apply diff/prune below reasons about the same superset.
		g.Status.ManagedResources = union
		if equality.Semantic.DeepEqual(current.Status, dc.Status) {
			return nil
		}
		return r.Client.Status().Patch(ctx, dc, client.MergeFrom(current))
	})
}

// SetupWithManager registers the reconciler with the manager. When a
// Router is wired, its event channel is added as a raw source so drift
// events on watched resources flow into the same work queue as Graph
// spec updates. Same applies to the SchemaWatcher — CRD content
// changes feed Graph re-reconciles through a second raw source.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).For(&expv1alpha1.Graph{})
	if r.MaxConcurrentReconciles > 0 {
		b = b.WithOptions(ctrlrtcontroller.Options{
			MaxConcurrentReconciles: r.MaxConcurrentReconciles,
		})
	}
	if r.Router != nil {
		b = b.WatchesRawSource(r.Router.Source())
	}
	if r.SchemaWatcher != nil {
		b = b.WatchesRawSource(r.SchemaWatcher.Source())
	}
	return b.Complete(r)
}

// --- Condition vocabulary ---------------------------------------------------

// Condition type names exposed by the Graph reconciler. Ready is the
// root condition; ConditionSet rolls it up from the listed dependents.
const (
	Ready              = string(expv1alpha1.GraphConditionTypeReady)
	GraphAccepted      = string(expv1alpha1.GraphConditionTypeAccepted)
	ResourcesConverged = "ResourcesConverged"
)

// graphConditionTypes registers Accepted and ResourcesConverged as
// dependents of Ready. Accepted reports compilation; ResourcesConverged
// reports the executor's terminal apply state — Ready stays Unknown
// until both flip True, False if either is False.
var graphConditionTypes = apis.NewReadyConditions(GraphAccepted, ResourcesConverged)

// ConditionsMarker is the typed surface for writing Graph conditions. Each
// method touches exactly one condition; the root Ready condition is
// recomputed by the underlying ConditionSet.
type ConditionsMarker struct {
	cs apis.ConditionSet
}

// NewConditionsMarkerFor binds a ConditionsMarker to a specific Graph. The
// returned marker mutates g.Status.Conditions in place.
func NewConditionsMarkerFor(g *expv1alpha1.Graph) *ConditionsMarker {
	return &ConditionsMarker{cs: graphConditionTypes.For(g)}
}

// GraphCompiled marks Accepted=True with reason "Compiled" and a message
// summarising the compiled node count.
func (m *ConditionsMarker) GraphCompiled(nodes int) {
	m.cs.SetTrueWithReason(GraphAccepted, "Compiled", fmt.Sprintf("compiled %d nodes", nodes))
}

// GraphInvalid marks Accepted=False with reason "InvalidGraph" and the
// supplied compile error as the message.
func (m *ConditionsMarker) GraphInvalid(msg string) {
	m.cs.SetFalse(GraphAccepted, "InvalidGraph", msg)
}

// ResourcesConverged marks ResourcesConverged=True with reason "Applied"
// after every node has applied and reported ready.
func (m *ConditionsMarker) ResourcesConverged() {
	m.cs.SetTrueWithReason(ResourcesConverged, "Applied", "all nodes applied and ready")
}

// ResourcesNotReady marks ResourcesConverged=False with reason
// "WaitingForReadiness" — the apply succeeded but readyWhen
// expressions evaluated false.
func (m *ConditionsMarker) ResourcesNotReady(msg string) {
	m.cs.SetFalse(ResourcesConverged, "WaitingForReadiness", msg)
}

// ResourcesDataPending marks ResourcesConverged=False with reason
// "DataPending" — a node's CEL expression referenced data the cluster
// hasn't surfaced yet (typically a status field). Distinct from
// WaitingForReadiness so operators can tell a stuck readyWhen from a
// resolution gap.
func (m *ConditionsMarker) ResourcesDataPending(msg string) {
	m.cs.SetFalse(ResourcesConverged, "DataPending", msg)
}

// ResourcesApplyFailed marks ResourcesConverged=False with reason
// "ApplyFailed" when the executor returned a hard error.
func (m *ConditionsMarker) ResourcesApplyFailed(msg string) {
	m.cs.SetFalse(ResourcesConverged, "ApplyFailed", msg)
}
