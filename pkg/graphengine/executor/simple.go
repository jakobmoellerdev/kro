// Copyright 2026 The Kubernetes Authors.
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

package executor

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	apimachineryruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/compiler"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/runtime"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/watchrouter"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
)

// Simple is the v1 executor: walk nodes in topological order, SSA-apply
// each Template, record observed state on the runtime so dependents see
// the live cluster values, and on Delete tear them down in reverse.
//
// Ignored nodes (includeWhen=false, or contagiously via an ignored
// upstream) are skipped entirely — no resolve, no apply, no scope
// publication. ReadyWhen checks gate the loop: an unsatisfied readyWhen
// returns ErrNotReady so the reconciler requeues.
type Simple struct {
	Client client.Client
	// LabelInjector, when non-nil, is called on every child object
	// immediately before SSA apply so kro's per-instance labels are
	// stamped by the graph-engine path. Safe to leave nil — no-op.
	LabelInjector func(*unstructured.Unstructured)
	// GateReadiness makes the executor withhold a node until every one of
	// its dependencies has reached a terminal ready state this cycle
	// (classic RGD ordering). When false (the default, used by the generic
	// Graph engine), every reachable node is applied regardless of upstream
	// readiness so drift watches register across a not-ready node.
	GateReadiness bool
}

// NewSimple constructs a Simple executor bound to the given client.
func NewSimple(c client.Client) *Simple {
	return &Simple{Client: c}
}

// WithLabelInjector sets a labeling function that is called on each child
// object before SSA apply. Calling this a second time replaces the previous
// injector. Returns the receiver for chaining.
func (s *Simple) WithLabelInjector(fn func(*unstructured.Unstructured)) *Simple {
	s.LabelInjector = fn
	return s
}

// ApplyWithLabeler is like Apply but stamps every child object with
// the supplied labeler just before SSA, in addition to the struct-level
// LabelInjector (if any). Uses a per-call override via the context so
// concurrent reconciles for different instances of the same GVR are safe.
func (s *Simple) ApplyWithLabeler(
	ctx context.Context,
	rt *runtime.Runtime,
	w watchrouter.Watcher,
	extraLabeler func(*unstructured.Unstructured),
) (ApplyResult, error) {
	if extraLabeler == nil {
		return s.Apply(ctx, rt, w)
	}
	// Compose: first apply struct-level labels, then per-call extra labels.
	prev := s.LabelInjector
	composed := func(obj *unstructured.Unstructured) {
		if prev != nil {
			prev(obj)
		}
		extraLabeler(obj)
	}
	// Build a shadow executor with the composed injector so we don't mutate s.
	shadow := &Simple{Client: s.Client, LabelInjector: composed, GateReadiness: s.GateReadiness}
	return shadow.Apply(ctx, rt, w)
}

var _ Interface = (*Simple)(nil)

// Apply walks rt in topological order. For each node it checks ignore
// status (contagious), resolves the desired state, registers a watch on
// the resulting GVR/Name/Namespace via w, applies to cluster (or just
// renders for Def), records observed state on the node, and finally
// checks readyWhen — surfacing an unsatisfied result as ErrNotReady so
// the reconciler requeues without backoff.
//
// Per-template watches are registered BEFORE SSA apply. Doing it before
// closes the window where an external actor could mutate the object
// between apply and watch registration — the informer cache picks up
// the next change either way, but the watch must exist first or the
// event gets dropped.
//
// Soft errors (ErrDataPending from Resolve, ErrWaitingForReadiness from
// CheckReadiness) do NOT abort the walk. The reconciler relies on every
// reachable node getting its watch declared so drift detection stays
// authoritative — bailing early on a not-ready upstream node would
// leave downstream nodes' watches missing, and the next reconcile
// would lose drift events on them. Soft errors are remembered and the
// first one is returned at the end wrapped in ErrNotReady. Hard errors
// (apply failure, type errors, etc.) still abort immediately.
//
// Dependency-readiness gating: a node is applied only once every node it
// depends on is ready this cycle (readyWhen satisfied). A dependency that
// was applied-but-not-ready, blocked, or unresolved leaves the dependent
// blocked too — it is recorded Unresolved and skipped (never applied) so a
// dependent resource is not created before its dependencies converge, and
// its own dependents cascade-block via the readiness map. This mirrors
// classic kro (RGD) ordering; it is a deliberate divergence from applying
// every reachable node regardless of upstream readiness.
func (s *Simple) Apply(ctx context.Context, rt *runtime.Runtime, w watchrouter.Watcher) (ApplyResult, error) {
	var result ApplyResult
	var firstSoft error
	recordSoft := func(err error) {
		if firstSoft == nil {
			firstSoft = err
		}
	}

	// ready tracks which nodes reached a terminal ready state this cycle, so
	// dependents can be gated until their dependencies converge.
	ready := make(map[string]bool, len(rt.Nodes()))

	for _, n := range rt.Nodes() {
		ignored, err := n.IsIgnored()
		if err != nil {
			if isSoftRuntimeErr(err) {
				// includeWhen referenced data the cluster hasn't
				// produced yet. We can't decide whether to apply,
				// so identities are unknown — caller preserves the
				// previous entries for this NodeID.
				result.Unresolved = append(result.Unresolved, n.ID())
				recordSoft(fmt.Errorf("apply %q: includeWhen: %w (%w)", n.ID(), err, ErrNotReady))
				continue
			}
			return result, fmt.Errorf("apply %q: %w", n.ID(), err)
		}
		if ignored {
			// Intentionally skipped — not Unresolved. The caller
			// will treat any previous entries for this NodeID as
			// prune candidates. Ignored nodes are non-blocking for
			// dependents (their dependents are contagiously ignored too).
			ready[n.ID()] = true
			continue
		}

		// Gate on dependency readiness (classic RGD ordering): do not apply
		// until every dependency is ready this cycle. Opt-in — the generic
		// Graph engine leaves this off so dependents apply across a not-ready
		// upstream (drift watches still register).
		if s.GateReadiness {
			if dep, blocked := firstUnreadyDep(n, ready); blocked {
				result.Unresolved = append(result.Unresolved, n.ID())
				recordSoft(fmt.Errorf("apply %q: waiting for dependency %q: %w", n.ID(), dep, ErrNotReady))
				continue
			}
		}

		// A subgraph node has no payload of its own — it runs a child
		// Program in a scope seeded from this one. Handle it before
		// Resolve (which would deref the nil Object).
		if n.Kind() == compiler.NodeKindGraph {
			applied, unresolved, err := s.applySubgraph(ctx, rt, w, n)
			result.Applied = append(result.Applied, applied...)
			result.Unresolved = append(result.Unresolved, unresolved...)
			if err != nil {
				if errors.Is(err, ErrNotReady) || isSoftRuntimeErr(err) {
					recordSoft(fmt.Errorf("apply %q (subgraph): %w", n.ID(), err))
					continue
				}
				return result, fmt.Errorf("apply %q (subgraph): %w", n.ID(), err)
			}
			ready[n.ID()] = true
			continue
		}

		desired, err := n.Resolve()
		if err != nil {
			if isSoftRuntimeErr(err) {
				result.Unresolved = append(result.Unresolved, n.ID())
				recordSoft(fmt.Errorf("apply %q: resolve: %w (%w)", n.ID(), err, ErrNotReady))
				continue
			}
			return result, fmt.Errorf("apply %q: resolve: %w", n.ID(), err)
		}
		switch n.Kind() {
		case compiler.NodeKindDef:
			// Def nodes have no cluster I/O — no managed-resource entries.
			n.SetObserved(desired, desired)
			publishScope(rt, n, n.Observed())
		case compiler.NodeKindTemplate:
			applied, err := s.applyTemplate(ctx, rt, w, n, desired)
			// Record whatever landed before any error so tracking never
			// loses a resource that actually reached the cluster.
			result.Applied = append(result.Applied, applied...)
			if err != nil {
				// A dynamic-GVK template whose target CRD isn't installed
				// or discoverable yet: identities for this node are
				// uncertain this cycle, so preserve previous entries via
				// Unresolved and requeue. The SchemaWatcher's dynamic set
				// re-enqueues the Graph when the CRD lands.
				if errors.Is(err, errSchemaNotReady) {
					result.Unresolved = append(result.Unresolved, n.ID())
					recordSoft(fmt.Errorf("apply %q: %w (%w)", n.ID(), err, ErrNotReady))
					continue
				}
				// A terminating live object (ResourceDeletingError) or a
				// tolerated per-item collection failure surfaces as soft
				// not-ready: record Unresolved so prune is withheld, remember
				// the (possibly distinguishable) error, and continue so the
				// node never reaches ready — gating its dependents — and its
				// downstream watches still register. ResourceDeletingError
				// satisfies errors.Is(err, ErrNotReady).
				if errors.Is(err, ErrNotReady) {
					result.Unresolved = append(result.Unresolved, n.ID())
					recordSoft(fmt.Errorf("apply %q: %w", n.ID(), err))
					continue
				}
				return result, fmt.Errorf("apply %q: %w", n.ID(), err)
			}
			n.SetObserved(desired, desired)
			publishScope(rt, n, n.Observed())
		case compiler.NodeKindRef:
			var observed []*unstructured.Unstructured
			var err error
			isColl := n.IsCollection()
			if isColl {
				// A selector externalRef reads a read-only COLLECTION of
				// external objects by label selector.
				observed, err = s.applyRefCollection(ctx, w, rt, n, desired)
			} else {
				observed, err = s.applyRef(ctx, w, rt, n, desired)
			}
			if err != nil {
				// A referenced object that isn't in the cluster yet is a
				// soft condition: it may be applied separately or created
				// later. Its identity is not ours to prune (read-only), so
				// record it Unresolved and requeue instead of failing hard.
				if errors.Is(err, ErrNotReady) || isSoftRuntimeErr(err) {
					result.Unresolved = append(result.Unresolved, n.ID())
					recordSoft(fmt.Errorf("apply %q (ref): %w", n.ID(), err))
					continue
				}
				return result, fmt.Errorf("apply %q (ref): %w", n.ID(), err)
			}
			// Read-only: publish the live object(s) so dependents resolve, but
			// never append to result.Applied — kro must not own, prune, or
			// delete a resource it only reads. For a selector collection the
			// desired set is a single projected ExternalRef (the selector
			// object), which must NOT be intersected against the observed list
			// of matched objects, so SetObserved is called with a nil desired
			// to preserve the list verbatim. publishScope emits a []any because
			// the node IsCollection.
			if isColl {
				n.SetObserved(observed, nil)
			} else {
				n.SetObserved(observed, desired)
			}
			publishScope(rt, n, n.Observed())
		default:
			return result, fmt.Errorf("apply %q: unknown kind %v", n.ID(), n.Kind())
		}

		// readyWhen is checked after observed state is recorded.
		// Soft → ErrNotReady (already tracked in Applied); continue
		// so downstream watches still register. Hard → abort.
		if err := n.CheckReadiness(); err != nil {
			if isSoftRuntimeErr(err) {
				recordSoft(fmt.Errorf("apply %q: %w (%w)", n.ID(), err, ErrNotReady))
				continue
			}
			return result, fmt.Errorf("apply %q: %w", n.ID(), err)
		}
		// Node reached a terminal ready state — unblock its dependents.
		ready[n.ID()] = true
	}
	return result, firstSoft
}

// firstUnreadyDep returns the first direct dependency of n that has not
// reached a ready state this cycle, and whether such a dependency exists.
func firstUnreadyDep(n *runtime.Node, ready map[string]bool) (string, bool) {
	for _, id := range n.DepIDs() {
		if !ready[id] {
			return id, true
		}
	}
	return "", false
}

// managedResourceFrom builds a ManagedResource pointer from a node and
// its post-apply unstructured object. The UID is captured from the SSA
// response so the Reconciler can use it as a delete precondition later.
func managedResourceFrom(n *runtime.Node, obj *unstructured.Unstructured) expv1alpha1.ManagedResource {
	gvk := obj.GroupVersionKind()
	return expv1alpha1.ManagedResource{
		NodeID:     n.ID(),
		APIVersion: gvk.GroupVersion().String(),
		Kind:       gvk.Kind,
		Namespace:  obj.GetNamespace(),
		Name:       obj.GetName(),
		UID:        string(obj.GetUID()),
	}
}

// isSoftRuntimeErr classifies a runtime-package error as a retryable
// "cluster hasn't converged yet" signal versus a hard error. Both
// ErrDataPending and ErrWaitingForReadiness mean "try again later" —
// the executor records them and continues so the rest of the graph
// still gets its watches declared.
func isSoftRuntimeErr(err error) bool {
	return errors.Is(err, runtime.ErrDataPending) || errors.Is(err, runtime.ErrWaitingForReadiness)
}

// publishScope writes the supplied objects back to the runtime scope.
// Collection nodes get a []any so downstream CEL list functions work;
// singletons get the lone object map.
func publishScope(rt *runtime.Runtime, n *runtime.Node, objs []*unstructured.Unstructured) {
	if n.IsCollection() {
		list := make([]any, 0, len(objs))
		for _, obj := range objs {
			list = append(list, obj.Object)
		}
		rt.Set(n.ID(), list)
		return
	}
	if len(objs) == 0 {
		return
	}
	rt.Set(n.ID(), objs[0].Object)
}

// Delete removes resources in reverse of the supplied slice order so
// dependents go before dependencies. Identity comes from the persisted
// ManagedResources list — no re-resolve of the current spec, so a
// Graph whose templates were renamed or whose forEach shrunk between
// apply and delete still gets every prior resource removed.
//
// UID precondition guards against deleting an impostor that some other
// actor created after the resource we tracked was removed out of band.
// NotFound and "already deleted by something else" are tolerated.
func (s *Simple) Delete(ctx context.Context, resources []expv1alpha1.ManagedResource) error {
	for i := len(resources) - 1; i >= 0; i-- {
		r := resources[i]
		obj := &unstructured.Unstructured{}
		obj.SetAPIVersion(r.APIVersion)
		obj.SetKind(r.Kind)
		obj.SetNamespace(r.Namespace)
		obj.SetName(r.Name)

		opts := []client.DeleteOption{}
		if r.UID != "" {
			uid := types.UID(r.UID)
			opts = append(opts, &client.DeleteOptions{
				Preconditions: &metav1.Preconditions{UID: &uid},
			})
		}

		if err := s.Client.Delete(ctx, obj, opts...); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			// UID-precondition mismatch surfaces as Conflict — the
			// resource we tracked is gone and a different object now
			// occupies its identity. Not our problem; skip.
			if apierrors.IsConflict(err) {
				continue
			}
			return fmt.Errorf("delete %s/%s %s: %w", r.APIVersion, r.Kind, refName(r), err)
		}
	}
	return nil
}

func refName(r expv1alpha1.ManagedResource) string {
	if r.Namespace == "" {
		return r.Name
	}
	return r.Namespace + "/" + r.Name
}

// errSchemaNotReady signals that a dynamic-GVK template resolved to a GVK
// the cluster can't yet map to a resource (the CRD isn't installed or the
// REST mapper hasn't discovered it). It is an internal soft signal: Apply
// maps it to Unresolved + ErrNotReady so the reconcile requeues.
var errSchemaNotReady = errors.New("executor: target GVK not yet known to the cluster")

// applyTemplate renders, watches, and SSA-applies every object a Template
// node produced. GVR and REST scope are resolved per object — from the
// compiled spec for static nodes, from the live REST mapper for dynamic-GVK
// nodes. Mappings for all objects are resolved up front so a dynamic node
// whose CRD isn't installed yet fails (errSchemaNotReady) before any
// partial apply. Returns the resources that actually landed, even on a
// later hard error, so the caller never loses tracking for them.
//
// Before SSA-applying each object the live object is fetched (mirroring
// classic processRegularNode): if it exists with a deletionTimestamp the
// node is held soft not-ready via ResourceDeletingError so its dependents
// gate and the reconcile requeues without recreating a resource that is
// still terminating. External refs/collections are exempt (they are
// read-only and handled by applyRef/applyRefCollection, not here).
//
// Collection nodes apply each item INDEPENDENTLY and tolerate per-item SSA
// failures (mirroring classic's applyset per-item apply): a failure to
// UPDATE an already-present item (e.g. an immutable field) does not abort
// the node — the live object is recorded as applied so tracking/prune stay
// correct and the node can still converge. A failure to CREATE an item that
// is not yet present marks the node soft not-ready so downstream gates and
// the reconcile requeues. A scalar node still returns an SSA error as a hard
// error, unchanged.
func (s *Simple) applyTemplate(ctx context.Context, rt *runtime.Runtime, w watchrouter.Watcher, n *runtime.Node, desired []*unstructured.Unstructured) ([]expv1alpha1.ManagedResource, error) {
	type mapping struct {
		gvr        schema.GroupVersionResource
		namespaced bool
	}
	mappings := make([]mapping, len(desired))
	for i, obj := range desired {
		gvr, namespaced, err := s.mappingFor(n, obj)
		if err != nil {
			return nil, err
		}
		mappings[i] = mapping{gvr: gvr, namespaced: namespaced}
	}

	isCollection := n.IsCollection()
	applied := make([]expv1alpha1.ManagedResource, 0, len(desired))
	size := len(desired)
	collectionWatched := false
	var deletingErr *ResourceDeletingError
	var itemFailures []error
	for i, obj := range desired {
		s.defaultNamespace(rt, mappings[i].namespaced, obj)
		stampKROMeta(rt, n, obj, i, size)
		// Collection nodes register ONE selector-based watch for the whole
		// node (keyed by NodeID, matching every item by label) instead of N
		// scalar watches — the coordinator keys state by NodeID, so per-item
		// scalar watches would collapse to only the last item. Scalar nodes
		// keep their per-object watch. Watches register before SSA apply.
		if isCollection {
			if !collectionWatched {
				if err := s.watchCollection(w, n, mappings[i].gvr, obj); err != nil {
					return applied, fmt.Errorf("register collection watch: %w", err)
				}
				collectionWatched = true
			}
		} else {
			if err := s.watchObject(w, n.ID(), mappings[i].gvr, obj); err != nil {
				return applied, fmt.Errorf("register watch: %w", err)
			}
		}

		// Fetch the live object (classic parity): a hard GET error aborts,
		// NotFound means the object is absent (current == nil).
		current, err := s.getLive(ctx, obj)
		if err != nil {
			return applied, err
		}
		// Terminating: do not re-apply while the live object is being deleted.
		if current != nil && current.GetDeletionTimestamp() != nil {
			de := &ResourceDeletingError{NodeID: n.ID(), Namespace: obj.GetNamespace(), Name: obj.GetName()}
			if !isCollection {
				return applied, de
			}
			// Collection: a terminating item gates the whole node, but keep
			// applying siblings so their watches/identities are recorded.
			if deletingErr == nil {
				deletingErr = de
			}
			continue
		}

		if err := s.ssaApply(ctx, obj); err != nil {
			if !isCollection {
				// Scalar Template: an SSA failure is a hard error, unchanged.
				return applied, err
			}
			// Collection per-item tolerance (classic applyset parity).
			if current != nil {
				// The object already exists; only the UPDATE was rejected
				// (e.g. an immutable field). It is present in the cluster, so
				// record the live identity and let the node converge rather
				// than block it forever on an unfixable update.
				applied = append(applied, managedResourceFrom(n, current))
				continue
			}
			// The object does not exist and CREATE failed: record the error
			// and continue so siblings still apply; the node is held soft
			// not-ready below.
			itemFailures = append(itemFailures, fmt.Errorf("item %s/%s: %w", obj.GetNamespace(), obj.GetName(), err))
			continue
		}
		// SSA returned UID + server-managed fields on obj. Record the
		// identity AFTER apply succeeded so we never advertise tracking
		// for resources that didn't actually land.
		applied = append(applied, managedResourceFrom(n, obj))
	}

	// A terminating item takes priority: surface the ResourceDeleting signal
	// so the reconciler marks the ResourcesReady condition accordingly.
	if deletingErr != nil {
		return applied, deletingErr
	}
	// Any create failure holds the collection soft not-ready so downstream
	// gates and the reconcile requeues (never a hard abort).
	if len(itemFailures) > 0 {
		return applied, fmt.Errorf("collection %q: %d item(s) failed to apply: %w (%w)", n.ID(), len(itemFailures), errors.Join(itemFailures...), ErrNotReady)
	}
	return applied, nil
}

// getLive fetches the current cluster state of obj by GVK/namespace/name.
// A NotFound is reported as (nil, nil) — the object is simply absent. Any
// other error is returned so the caller can abort. Used to detect a
// terminating object (deletionTimestamp) and to distinguish create-vs-update
// failures for collection per-item apply tolerance.
func (s *Simple) getLive(ctx context.Context, obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	live := &unstructured.Unstructured{}
	live.SetGroupVersionKind(obj.GroupVersionKind())
	key := client.ObjectKey{Namespace: obj.GetNamespace(), Name: obj.GetName()}
	if err := s.Client.Get(ctx, key, live); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get current state for %s %q: %w", obj.GetKind(), key, err)
	}
	return live, nil
}

// stampKROMeta stamps the classic-runtime identity metadata the graph-engine
// path would otherwise drop: the kro.run/node-id label (used by selectors and
// managed-resource discovery) and the internal.kro.run/apply-order annotation
// (the reverse-topological deletion wave read by the instance deletion path).
// For collection items it also stamps the collection-index / collection-size
// labels (index within the expansion, total item count) matching classic
// NewCollectionItemLabeler. Per-instance labels are added separately by the
// executor's LabelInjector.
func stampKROMeta(rt *runtime.Runtime, n *runtime.Node, obj *unstructured.Unstructured, index, size int) {
	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[metadata.NodeIDLabel] = n.ID()
	if n.IsCollection() {
		labels[metadata.CollectionIndexLabel] = strconv.Itoa(index)
		labels[metadata.CollectionSizeLabel] = strconv.Itoa(size)
	}
	obj.SetLabels(labels)

	if order, ok := rt.ApplyOrder(n.ID()); ok {
		annotations := obj.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[metadata.ApplyOrderAnnotation] = strconv.Itoa(order)
		obj.SetAnnotations(annotations)
	}
}

// applyRef reads the external resource a ref node points at and returns its
// live cluster state for publication into scope. A ref is READ-ONLY: kro
// registers a watch so the Graph re-reconciles when the referenced object
// changes, but never applies, owns, or prunes it. The caller therefore must
// NOT record the returned object in ApplyResult.Applied.
//
// A referenced object that doesn't exist yet is a soft condition, not a
// failure: it may be applied separately or created later, so we wrap
// ErrNotReady and let the reconciler requeue. The watch is registered before
// the read (and fires on create), so the Graph re-enqueues once it appears.
func (s *Simple) applyRef(ctx context.Context, w watchrouter.Watcher, rt *runtime.Runtime, n *runtime.Node, desired []*unstructured.Unstructured) ([]*unstructured.Unstructured, error) {
	// forEach is rejected on ref nodes at compile time, so Resolve produced
	// exactly one projected {apiVersion, kind, metadata} object.
	if len(desired) != 1 {
		return nil, fmt.Errorf("ref node resolved to %d objects, want 1", len(desired))
	}
	ref := desired[0]
	// Fill metadata.namespace from the Graph for namespaced kinds when the
	// ExternalRef left it empty — matches the "defaults to the instance's
	// namespace" contract on ExternalRefMetadata.
	s.defaultNamespace(rt, n.Namespaced(), ref)

	if err := s.watchObject(w, n.ID(), n.GVR(), ref); err != nil {
		return nil, fmt.Errorf("register watch: %w", err)
	}

	live := &unstructured.Unstructured{}
	live.SetGroupVersionKind(ref.GroupVersionKind())
	key := client.ObjectKey{Namespace: ref.GetNamespace(), Name: ref.GetName()}
	if err := s.Client.Get(ctx, key, live); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("external ref %s %q not found: %w", ref.GetKind(), key, ErrNotReady)
		}
		return nil, fmt.Errorf("get external ref %s %q: %w", ref.GetKind(), key, err)
	}
	return []*unstructured.Unstructured{live}, nil
}

// applyRefCollection reads the read-only COLLECTION of external objects a
// selector externalRef points at and returns their live cluster state for
// publication into scope. It mirrors the classic observeExternalCollection:
// resolve the label selector off the rendered ExternalRef, register ONE
// selector-based watch keyed by NodeID, list the GVR by that selector, and
// publish the matched objects. A selector ref is READ-ONLY: kro never applies,
// owns, or prunes the matched objects, so the caller must NOT record them in
// ApplyResult.Applied.
//
// Namespace handling deliberately skips defaultNamespace: an empty
// metadata.namespace for a namespaced GVR means "list across ALL namespaces"
// (matching classic external-collection semantics), not "the instance's
// namespace". An empty selector lists everything. A List error is hard; an
// empty result is valid (an empty collection, treated as ready).
func (s *Simple) applyRefCollection(ctx context.Context, w watchrouter.Watcher, rt *runtime.Runtime, n *runtime.Node, desired []*unstructured.Unstructured) ([]*unstructured.Unstructured, error) {
	// forEach is rejected on ref nodes at translate time, so Resolve produced
	// exactly one projected {apiVersion, kind, metadata{selector,namespace?}}
	// object with CEL (incl. matchExpressions[].values[]) already evaluated.
	if len(desired) != 1 {
		return nil, fmt.Errorf("ref collection node resolved to %d objects, want 1", len(desired))
	}
	ref := desired[0]

	selector, err := refCollectionSelector(n.ID(), ref)
	if err != nil {
		return nil, err
	}

	// Namespace comes straight from the rendered ExternalRef. For a namespaced
	// GVR an empty namespace lists across all namespaces; cluster-scoped GVRs
	// are always list-all. No defaultNamespace — classic explicitly skips
	// namespace normalization for external collections.
	ns := ref.GetNamespace()
	if !n.Namespaced() {
		ns = ""
	}

	// Register the selector watch BEFORE the list so a matching object that
	// appears between the list and watch registration still re-enqueues the
	// Graph. The watch is keyed by NodeID with the user's label selector
	// (matching classic requestCollectionWatch).
	if w != nil {
		if err := w.Watch(watchrouter.WatchRequest{
			NodeID:    n.ID(),
			GVR:       n.GVR(),
			Namespace: ns,
			Selector:  selector,
		}); err != nil {
			return nil, fmt.Errorf("register collection watch: %w", err)
		}
	}

	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(ref.GroupVersionKind())
	opts := []client.ListOption{client.MatchingLabelsSelector{Selector: selector}}
	if ns != "" {
		opts = append(opts, client.InNamespace(ns))
	}
	if err := s.Client.List(ctx, list, opts...); err != nil {
		return nil, fmt.Errorf("list external collection %q (%s): %w", n.ID(), n.GVR().String(), err)
	}

	items := make([]*unstructured.Unstructured, len(list.Items))
	for i := range list.Items {
		items[i] = &list.Items[i]
	}
	return items, nil
}

// refCollectionSelector extracts the label selector from a rendered
// external-collection ExternalRef. A missing or empty metadata.selector means
// "select everything" (labels.Everything), matching the classic
// resolveExternalCollectionSelector semantics.
func refCollectionSelector(id string, ref *unstructured.Unstructured) (labels.Selector, error) {
	selectorRaw, found, err := unstructured.NestedMap(ref.Object, "metadata", "selector")
	if err != nil || !found {
		return labels.Everything(), nil
	}
	ls := &metav1.LabelSelector{}
	if err := apimachineryruntime.DefaultUnstructuredConverter.FromUnstructured(selectorRaw, ls); err != nil {
		return nil, fmt.Errorf("convert selector for %q: %w", id, err)
	}
	selector, err := metav1.LabelSelectorAsSelector(ls)
	if err != nil {
		return nil, fmt.Errorf("invalid label selector for %q: %w", id, err)
	}
	return selector, nil
}

// applySubgraph runs a nested Graph node's child Program. The child Runtime
// is seeded with a snapshot of this scope so child expressions can capture
// (and shadow) parent values. The child's node outputs are published back to
// the parent scope as a map under the subgraph node's ID, making them
// addressable as ${nodeID.childNode.field}. Managed resources and unresolved
// NodeIDs from the child are returned with their IDs qualified by the
// subgraph node ID so the reconciler's tracking stays unambiguous across
// frames. The child's watches register against the same per-Graph Watcher,
// so drift on a nested resource re-enqueues the owning Graph.
func (s *Simple) applySubgraph(ctx context.Context, rt *runtime.Runtime, w watchrouter.Watcher, n *runtime.Node) ([]expv1alpha1.ManagedResource, []string, error) {
	sub := n.Spec().SubProgram
	if sub == nil {
		return nil, nil, fmt.Errorf("subgraph node has no compiled program")
	}
	childRT := runtime.New(sub, rt.Graph(),
		runtime.WithSeedScope(rt.Scope()),
		runtime.WithMaxCollectionSize(rt.MaxCollectionSize()),
	)

	childResult, applyErr := s.Apply(ctx, childRT, w)

	prefix := n.ID() + "/"
	applied := make([]expv1alpha1.ManagedResource, 0, len(childResult.Applied))
	for _, mr := range childResult.Applied {
		mr.NodeID = prefix + mr.NodeID
		applied = append(applied, mr)
	}
	unresolved := make([]string, 0, len(childResult.Unresolved))
	for _, u := range childResult.Unresolved {
		unresolved = append(unresolved, prefix+u)
	}

	// Publish the child's node outputs as a map under the subgraph node ID,
	// best-effort even on a soft error so the children that did resolve are
	// visible to downstream parent expressions.
	out := make(map[string]any, len(childRT.Nodes()))
	for _, childNode := range childRT.Nodes() {
		if v, ok := childRT.Scope()[childNode.ID()]; ok {
			out[childNode.ID()] = v
		}
	}
	rt.Set(n.ID(), out)

	return applied, unresolved, applyErr
}

// mappingFor returns the GVR and REST scope to apply obj under. Static
// nodes use the GVR resolved at compile time. Dynamic-GVK nodes resolve
// the rendered object's concrete GVK through the live REST mapper; a
// missing mapping (CRD not installed yet) becomes errSchemaNotReady.
func (s *Simple) mappingFor(n *runtime.Node, obj *unstructured.Unstructured) (schema.GroupVersionResource, bool, error) {
	if !n.DynamicGVK() {
		return n.GVR(), n.Namespaced(), nil
	}
	gvk := obj.GroupVersionKind()
	if gvk.Kind == "" || gvk.Version == "" {
		return schema.GroupVersionResource{}, false, fmt.Errorf("dynamic template resolved to incomplete GVK %q", gvk.String())
	}
	m, err := s.Client.RESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		if meta.IsNoMatchError(err) {
			return schema.GroupVersionResource{}, false, fmt.Errorf("%s: %w", gvk.String(), errSchemaNotReady)
		}
		return schema.GroupVersionResource{}, false, fmt.Errorf("rest mapping for %s: %w", gvk.String(), err)
	}
	return m.Resource, m.Scope.Name() == meta.RESTScopeNameNamespace, nil
}

// watchObject registers a scalar watch on a resolved Template object so the
// dynamic controller re-enqueues the Graph when the resource changes out
// from under us. Cluster-scoped resources are watched with Namespace="".
// A nil watcher is treated as a Noop.
func (s *Simple) watchObject(w watchrouter.Watcher, nodeID string, gvr schema.GroupVersionResource, obj *unstructured.Unstructured) error {
	if w == nil {
		return nil
	}
	return w.Watch(watchrouter.WatchRequest{
		NodeID:    nodeID,
		GVR:       gvr,
		Name:      obj.GetName(),
		Namespace: obj.GetNamespace(),
	})
}

// watchCollection registers a single selector-based watch for a collection
// node. The coordinator keys watch state by NodeID, so N per-item scalar
// watches would collapse to only the last item and drift on the others would
// go unobserved. One selector watch matching {instance-id, node-id} tracks
// every item under this node. The instance-id label is stamped by the
// LabelInjector (invoked here so it is present before the selector is built,
// idempotent with the apply-time call); if it is absent the selector falls
// back to node-id only. Namespace comes from the (already namespace-defaulted)
// sample item and is empty for cluster-scoped resources.
func (s *Simple) watchCollection(w watchrouter.Watcher, n *runtime.Node, gvr schema.GroupVersionResource, sample *unstructured.Unstructured) error {
	if w == nil {
		return nil
	}
	if s.LabelInjector != nil {
		s.LabelInjector(sample)
	}
	set := labels.Set{metadata.NodeIDLabel: n.ID()}
	if uid := sample.GetLabels()[metadata.InstanceIDLabel]; uid != "" {
		set[metadata.InstanceIDLabel] = uid
	}
	return w.Watch(watchrouter.WatchRequest{
		NodeID:    n.ID(),
		GVR:       gvr,
		Namespace: sample.GetNamespace(),
		Selector:  labels.SelectorFromSet(set),
	})
}

// defaultNamespace fills in metadata.namespace from the Graph for
// namespaced resources when the template left it empty. Cluster-scoped
// resources are untouched.
func (s *Simple) defaultNamespace(rt *runtime.Runtime, namespaced bool, obj *unstructured.Unstructured) {
	if !namespaced || obj.GetNamespace() != "" {
		return
	}
	if ns := rt.Graph().GetNamespace(); ns != "" {
		obj.SetNamespace(ns)
	}
}

// ssaApply server-side applies obj with the graph-engine field manager. We
// force ownership so re-applies after a hand-edit converge back.
func (s *Simple) ssaApply(ctx context.Context, obj *unstructured.Unstructured) error {
	if s.LabelInjector != nil {
		s.LabelInjector(obj)
	}
	return s.Client.Patch(ctx, obj, client.Apply, client.FieldOwner(FieldManager), client.ForceOwnership)
}
