// Copyright 2025 The Kubernetes Authors.
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

// controller_graph_engine.go — F6a: flag-gated instance reconcile via the
// Graph engine.  Enabled by features.RGDOnGraph (default off).  The existing
// pkg/runtime reconcile path is untouched and runs when the flag is off.

package instance

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/dynamic"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/controller/instance/applyset"
	"github.com/kubernetes-sigs/kro/pkg/dynamiccontroller"
	"github.com/kubernetes-sigs/kro/pkg/graph/revisions"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/executor"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/rgdadapter"
	geruntime "github.com/kubernetes-sigs/kro/pkg/graphengine/runtime"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/watchrouter"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
	"github.com/kubernetes-sigs/kro/pkg/requeue"
)

// reconcileViaGraphEngine is the Graph-engine reconcile path.  It is called
// from Reconcile when features.RGDOnGraph is enabled and the instance is NOT
// being deleted (deletion still uses the existing ApplySet/finalizer path).
//
// Steps:
//  1. Resolve the RGD spec from the revision registry (the graphrevision
//     controller stores it as Entry.RGDSpec since F6a).
//  2. Build a per-reconcile Runtime via rgdadapter.BuildRuntimeForInstance.
//  3. Apply all Graph nodes via the executor.Simple, wired to the instance's
//     dynamiccontroller.InstanceWatcher (via instanceWatcherBridge).
//  4. Project .status fields via rgdadapter.ProjectInstanceStatus and patch
//     them onto the instance.
//  5. Project author conditions via rgdadapter.ProjectInstanceConditions and
//     merge them into the status patch.
//
// Error policy mirrors the old path: hard errors propagate to controller-runtime
// for requeue-with-backoff; ErrNotReady from the executor is a soft signal that
// the call succeeds but the instance stays in InProgress.
func (c *Controller) reconcileViaGraphEngine(
	ctx context.Context,
	inst *unstructured.Unstructured,
	dcWatcher dynamiccontroller.InstanceWatcher,
) error {
	log := c.log.WithValues(
		"namespace", inst.GetNamespace(),
		"name", inst.GetName(),
		"path", "graph-engine",
	)

	//------------------------------------------------------------------
	// 1. Resolve RGD spec from the revision registry
	//------------------------------------------------------------------
	latest, ok := c.graphResolver.GetLatestRevision()
	if !ok {
		return requeue.NeededAfter(
			fmt.Errorf("graph-engine: latest issued revision not available"),
			c.reconcileConfig.DefaultRequeueDuration,
		)
	}
	if latest.State != revisions.RevisionStateActive {
		return requeue.NeededAfter(
			fmt.Errorf("graph-engine: latest issued revision %d is not active (state=%s)", latest.Revision, latest.State),
			c.reconcileConfig.DefaultRequeueDuration,
		)
	}
	if latest.RGDSpec == nil {
		// F6a shortcut: revisions compiled before F6a don't carry RGDSpec.
		// Fall back to the old path so we don't break already-running
		// controllers after a rolling upgrade.
		log.V(1).Info("graph-engine: RGDSpec not available in revision (pre-F6a entry); falling back to runtime path")
		return c.reconcileViaRuntimeFallback(ctx, inst)
	}

	rgd := &v1alpha1.ResourceGraphDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: latest.OwnerKey,
		},
		Spec: *latest.RGDSpec,
	}

	//------------------------------------------------------------------
	// 2. Guard: compiler must be wired (RGDOnGraph flag + compiler injection)
	//------------------------------------------------------------------
	if c.graphEngineCompiler == nil {
		return fmt.Errorf("graph-engine: compiler not wired (WithGraphEngineCompiler not called); this is a programming error")
	}
	if c.graphEngineExecutor == nil {
		return fmt.Errorf("graph-engine: executor not wired (graphEngineClient not supplied to NewController); this is a programming error")
	}

	//------------------------------------------------------------------
	// 2.5. Stamp kro finalizer and management labels on the instance
	//------------------------------------------------------------------
	if patched, err := c.stampInstanceMetadata(ctx, inst); err != nil {
		return err
	} else if patched != nil {
		inst = patched
	}

	// Snapshot wire status BEFORE the marker writes built-in defaults, and
	// build the condition marker on the (possibly rebound) instance.
	wireStatus := captureWireStatus(inst)
	mark := NewConditionsMarkerFor(inst)
	mark.InstanceManaged()

	//------------------------------------------------------------------
	// 3. Build per-reconcile Runtime
	//------------------------------------------------------------------
	rt, _, err := rgdadapter.BuildRuntimeForInstance(rgd, inst, c.graphEngineCompiler)
	if err != nil {
		log.Error(err, "graph-engine: BuildRuntimeForInstance failed")
		// Mark the instance so the operator can see the build failure.
		mark.GraphResolutionFailed("graph-engine build failed: %v", err)
		_ = c.updateConditionsStatus(ctx, inst)
		return err
	}
	mark.GraphResolved()

	//------------------------------------------------------------------
	// 4. Apply through the executor (SSA + watches)
	//------------------------------------------------------------------
	// Build a per-reconcile child labeler: instance labels + applyset part-of
	// + struct-level KRO-meta labels are composed inside ApplyWithLabeler.
	instanceLabeler := metadata.NewInstanceLabeler(inst, c.namespaced)
	nodeLabeler := metadata.NewNodeLabeler()
	applysetPartOf := applyset.ID(inst)
	extraLabel := func(obj *unstructured.Unstructured) {
		instanceLabeler.ApplyLabels(obj)
		// app.kubernetes.io/managed-by=kro, matching the classic child labeler.
		nodeLabeler.ApplyLabels(obj)
		l := obj.GetLabels()
		if l == nil {
			l = map[string]string{}
		}
		l[applyset.ApplysetPartOfLabel] = applysetPartOf
		obj.SetLabels(l)
	}
	bridge := &instanceWatcherBridge{w: dcWatcher}
	applyResult, applyErr := c.graphEngineExecutor.ApplyWithLabeler(ctx, rt, bridge, extraLabel)

	hardErr := false
	switch {
	case applyErr == nil:
		mark.ResourcesReady()
	case isResourceDeleting(applyErr):
		// A managed resource is terminating (has a deletionTimestamp). Mirror
		// classic kro: hold ResourcesReady=False with reason "ResourceDeleting"
		// and a message naming the resource, keep the instance InProgress, and
		// let the node gate its dependents (via GateReadiness) so the downstream
		// resource is not created until deletion completes. Checked before the
		// generic ErrNotReady branch because ResourceDeletingError satisfies
		// both sentinels.
		var delErr *executor.ResourceDeletingError
		if errors.As(applyErr, &delErr) {
			mark.ResourcesDeleting("%v", delErr)
		} else {
			mark.ResourcesDeleting("%v", applyErr)
		}
	case errors.Is(applyErr, executor.ErrNotReady):
		// Soft: a node is waiting on data/readiness. State stays InProgress;
		// child watch events (and the requeue below) drive the next cycle.
		mark.ResourcesNotReady("waiting for unresolved resource: %v", applyErr)
	default:
		hardErr = true
		mark.ResourcesNotReady("resource reconciliation failed: %v", applyErr)
	}

	// Reconcile the ApplySet inventory and prune orphans.  The inventory
	// grows to the union of the newly-applied set and the prior parent memory
	// (it never shrinks on a not-ready/degraded cycle, so the deletion path
	// keeps finding managed members).  Only when the desired set is fully
	// resolved and apply had no hard error do we prune resources that left the
	// desired set, then shrink the inventory to the current set after a
	// conflict-free prune.
	fullyResolved := applyErr == nil && len(applyResult.Unresolved) == 0
	if invErr := c.reconcileApplySetInventory(ctx, log, inst, applyResult.Applied, fullyResolved); invErr != nil {
		log.V(1).Info("graph-engine: ApplySet inventory/prune failed (non-fatal)", "error", invErr)
	}

	//------------------------------------------------------------------
	// 5. Project status fields and persist the full status (built-in
	//    conditions + projected fields + author conditions), skip-write
	//    guarded by statusesMatch.
	//------------------------------------------------------------------
	statusFields, projErr := rgdadapter.ProjectInstanceStatus(rt, rgd)
	if projErr != nil {
		log.Error(projErr, "graph-engine: status projection failed")
	}
	degraded := hardErr || projErr != nil

	if err := c.persistGraphEngineStatus(ctx, inst, wireStatus, statusFields, rt, rgd, degraded); err != nil {
		log.Error(err, "graph-engine: status persist failed")
		return err
	}

	// Classify soft-not-ready as a requeue (like the old path with
	// runtime.ErrWaitingForReadiness) instead of a hard reconcile failure.
	if applyErr != nil && errors.Is(applyErr, executor.ErrNotReady) {
		return requeue.NeededAfter(applyErr, c.reconcileConfig.DefaultRequeueDuration)
	}
	return applyErr
}

// reconcileViaRuntimeFallback is called when RGDOnGraph is on but the
// revision entry pre-dates F6a (RGDSpec == nil).  It requeues so that once
// the graphrevision controller has processed the revision and populated RGDSpec
// the instance picks up the Graph-engine path on the next cycle.
func (c *Controller) reconcileViaRuntimeFallback(_ context.Context, _ *unstructured.Unstructured) error {
	return requeue.NeededAfter(
		fmt.Errorf("graph-engine: revision entry has no RGDSpec (pre-F6a entry); waiting for graphrevision controller to repopulate"),
		c.reconcileConfig.DefaultRequeueDuration,
	)
}

// reconcileApplySetInventory writes the ApplySet inventory metadata on the
// instance and prunes resources that left the desired set.
//
// The inventory is written as the UNION of the newly-applied group-kinds/
// namespaces and the prior parent "memory" (the values already recorded in the
// instance's ApplySet annotations).  Mirroring applyset.Project, this union
// guarantees the inventory never shrinks on a not-ready/degraded cycle — which
// is what keeps the deletion path from finding zero managed resources and
// orphaning children when a dependent is transiently withheld.
//
// Pruning is gated on fullyResolved (applyErr == nil && no Unresolved nodes):
// we must never prune while anything is unresolved, or we would delete
// still-wanted members that were merely omitted from Applied this cycle.  Only
// after a conflict-free prune that actually removed orphans do we shrink the
// inventory to the exact current set, mirroring classic pruneOrphans.
func (c *Controller) reconcileApplySetInventory(
	ctx context.Context,
	log logr.Logger,
	inst *unstructured.Unstructured,
	applied []v1alpha1.ManagedResource,
	fullyResolved bool,
) error {
	// Construct the ApplySet from the instance BEFORE overwriting its inventory
	// annotations so the prior parent memory is captured for the prune scope
	// union (applyset.New clones parent.GetAnnotations()).
	applier := applyset.New(applyset.Config{
		Client:          c.client.Dynamic(),
		RESTMapper:      c.client.RESTMapper(),
		Log:             log,
		ParentNamespace: inst.GetNamespace(),
	}, inst)

	batchMeta := applySetMetadataFromApplied(inst, applied)

	// Superset = union(batch GKs/namespaces, prior parent annotations).
	// applyset.Project performs exactly this union (batch + parent memory).
	supersetMeta, projErr := applier.Project(managedResourcesToResources(applied))
	if projErr != nil {
		// Cannot compute the union safely; keep the prior annotations rather
		// than risk shrinking the inventory, and skip prune this cycle.
		log.V(1).Info("graph-engine: applyset project failed; keeping prior inventory", "error", projErr)
		return nil
	}

	// 1. Grow (never shrink) the inventory to the superset.
	if err := c.patchInstanceApplySetMetadata(ctx, inst, supersetMeta); err != nil {
		return fmt.Errorf("patch superset inventory: %w", err)
	}

	// 2. Prune only when the desired set is fully resolved.
	if !fullyResolved {
		return nil
	}
	pruned, conflictFree, err := c.pruneGraphEngineOrphans(ctx, log, applier, applied, supersetMeta)
	if err != nil {
		return err
	}

	// 3. Shrink the inventory to the exact current set only after a
	//    conflict-free prune that actually removed orphans.
	if pruned && conflictFree {
		if err := c.patchInstanceApplySetMetadata(ctx, inst, batchMeta); err != nil {
			log.V(1).Info("graph-engine: failed to shrink inventory after prune", "error", err)
		}
	}
	return nil
}

// applySetMetadataFromApplied builds ApplySet inventory metadata from only the
// resources applied this cycle (the "batch" set), excluding the parent
// namespace from AdditionalNamespaces per KEP-3659.
func applySetMetadataFromApplied(inst *unstructured.Unstructured, applied []v1alpha1.ManagedResource) applyset.Metadata {
	meta := applyset.Metadata{
		ID:                   applyset.ID(inst),
		Tooling:              applyset.ToolingID(),
		GroupKinds:           sets.New[schema.GroupKind](),
		AdditionalNamespaces: sets.New[string](),
	}
	parentNS := inst.GetNamespace()
	for _, r := range applied {
		gv, err := schema.ParseGroupVersion(r.APIVersion)
		if err != nil {
			continue
		}
		meta.GroupKinds.Insert(schema.GroupKind{Group: gv.Group, Kind: r.Kind})
		// AdditionalNamespaces excludes the parent namespace per KEP-3659.
		if r.Namespace != "" && r.Namespace != parentNS {
			meta.AdditionalNamespaces.Insert(r.Namespace)
		}
	}
	return meta
}

// managedResourcesToResources reconstructs minimal applyset.Resource inputs
// (GVK + namespace + name) from the executor's ManagedResource records so
// applyset.Project can compute the union scope.  UID/status are not needed for
// projection.
func managedResourcesToResources(applied []v1alpha1.ManagedResource) []applyset.Resource {
	out := make([]applyset.Resource, 0, len(applied))
	for _, r := range applied {
		obj := &unstructured.Unstructured{}
		obj.SetAPIVersion(r.APIVersion)
		obj.SetKind(r.Kind)
		obj.SetNamespace(r.Namespace)
		obj.SetName(r.Name)
		out = append(out, applyset.Resource{ID: r.NodeID, Object: obj})
	}
	return out
}

// pruneGraphEngineOrphans discovers applyset members not in the applied set and
// deletes them in reverse apply-order (dependents before dependencies).  It
// returns whether any orphan was actually removed and whether the prune was
// free of UID conflicts.  NotFound and UID-conflict deletes are tolerated by
// DeleteOrphan; a conflict leaves the object in place and is reported so the
// caller keeps the superset inventory for a later retry.
func (c *Controller) pruneGraphEngineOrphans(
	ctx context.Context,
	log logr.Logger,
	applier *applyset.ApplySet,
	applied []v1alpha1.ManagedResource,
	supersetMeta applyset.Metadata,
) (pruned bool, conflictFree bool, err error) {
	keepUIDs := sets.New[types.UID]()
	for _, r := range applied {
		if r.UID != "" {
			keepUIDs.Insert(types.UID(r.UID))
		}
	}

	candidates, err := applier.ListOrphans(ctx, applyset.PruneOptions{
		KeepUIDs: keepUIDs,
		Scope:    supersetMeta.PruneScope(),
	})
	if err != nil {
		return false, false, fmt.Errorf("list orphans: %w", err)
	}
	if len(candidates) == 0 {
		return false, true, nil
	}

	// Delete dependents before dependencies: sort by the persisted apply-order
	// annotation descending.  Unmapped/invalid orders sort first (treated as
	// the highest wave), matching classic prune's handling of nodes removed
	// from the graph entirely.
	sort.SliceStable(candidates, func(i, j int) bool {
		return orphanApplyOrder(candidates[i]) > orphanApplyOrder(candidates[j])
	})

	conflicts := 0
	for _, candidate := range candidates {
		res, derr := applier.DeleteOrphan(ctx, candidate)
		if derr != nil {
			return pruned, false, fmt.Errorf("delete orphan: %w", derr)
		}
		if res.Pruned != nil {
			pruned = true
		}
		if res.Conflict {
			conflicts++
		}
	}
	if conflicts > 0 {
		log.V(1).Info("graph-engine: prune skipped resources due to UID conflicts; keeping superset inventory", "conflicts", conflicts)
		return pruned, false, nil
	}
	return pruned, true, nil
}

// orphanApplyOrder reads the persisted reverse-topological apply-order wave for
// an orphan candidate.  Missing/invalid orders return max int so unmapped
// resources (whose node was removed from the graph) are deleted first.
func orphanApplyOrder(candidate applyset.OrphanCandidate) int {
	raw := candidate.Object.GetAnnotations()[metadata.ApplyOrderAnnotation]
	order, err := strconv.Atoi(raw)
	if err != nil {
		return int(^uint(0) >> 1)
	}
	return order
}

// patchInstanceApplySetMetadata writes the supplied ApplySet inventory metadata
// on the instance.  All four KEP-3659 annotations (tooling, contains-group-
// kinds, additional-namespaces, and the inventory hash) plus the parent-id
// label are written together so they stay mutually consistent — writing
// group-kinds without recomputing the hash would fail ValidateParentInventory
// and wedge deletion.
func (c *Controller) patchInstanceApplySetMetadata(ctx context.Context, inst *unstructured.Unstructured, meta applyset.Metadata) error {
	wantLabels := meta.Labels()
	wantAnnotations := meta.Annotations()

	// Fast-path: skip the write when the inventory is already correct.
	if inventoryUpToDate(inst, wantLabels, wantAnnotations) {
		return nil
	}

	patchObj := instanceSSAPatch(inst)
	patchObj.SetLabels(wantLabels)
	patchObj.SetAnnotations(wantAnnotations)

	ri := c.client.Dynamic().Resource(c.gvr)
	var instClient dynamic.ResourceInterface = ri
	if c.namespaced {
		instClient = ri.Namespace(inst.GetNamespace())
	}
	_, err := instClient.Apply(ctx, inst.GetName(), patchObj, metav1.ApplyOptions{
		FieldManager: applyset.FieldManager + "-parent",
		Force:        true,
	})
	return err
}

// inventoryUpToDate reports whether inst already carries every supplied
// ApplySet label/annotation with the same value.
func inventoryUpToDate(inst *unstructured.Unstructured, wantLabels, wantAnnotations map[string]string) bool {
	haveLabels := inst.GetLabels()
	for k, v := range wantLabels {
		if haveLabels[k] != v {
			return false
		}
	}
	haveAnnotations := inst.GetAnnotations()
	for k, v := range wantAnnotations {
		if haveAnnotations[k] != v {
			return false
		}
	}
	return true
}

// persistGraphEngineStatus composes the wire status for the Graph-engine path
// and persists it through persistStatus, reusing statusesMatch skip-write and
// state-transition metrics. It mirrors updateStatus: built-in conditions and
// state come from the marker-mutated instance, projected fields merge in, and
// author conditions (when the RGD declares them) replace the built-ins after
// stamping and merging with the previous cycle.
//
// statusFields == nil preserves the non-condition/state fields already on the
// wire. degraded forces state=Error regardless of condition readiness.
func (c *Controller) persistGraphEngineStatus(
	ctx context.Context,
	inst *unstructured.Unstructured,
	wireStatus map[string]interface{},
	statusFields map[string]any,
	rt *geruntime.Runtime,
	rgd *v1alpha1.ResourceGraphDefinition,
	degraded bool,
) error {
	previousState, _ := wireStatus["state"].(string)

	status := map[string]interface{}{}
	if statusFields == nil {
		for k, v := range wireStatus {
			if k != "conditions" && k != "state" {
				status[k] = v
			}
		}
	} else {
		for k, v := range statusFields {
			status[k] = v
		}
	}

	builtins := builtinConditions(inst)
	status["conditions"] = conditionsToInterfaceSlice(builtins)

	cs := condSet.For(&unstructuredWrapper{inst})
	switch {
	case degraded:
		status["state"] = string(v1alpha1.InstanceStateError)
	case cs.IsRootReady():
		status["state"] = string(v1alpha1.InstanceStateActive)
	default:
		status["state"] = string(v1alpha1.InstanceStateInProgress)
	}

	if c.reconcileConfig.HasAuthorConditions {
		authored, incomplete, condErr := rgdadapter.ProjectInstanceConditions(rt, rgd, builtins)
		prev, _ := wireStatus["conditions"].([]interface{})
		previous := decodeConditions(prev)
		stamped := stampAuthorConditions(authored, previous, inst.GetGeneration())
		if incomplete {
			stamped = mergeWithPrevious(stamped, previous)
		}
		status["conditions"] = conditionsToInterfaceSlice(stamped)
		if condErr != nil {
			c.log.Error(condErr, "graph-engine: author conditions degraded; setting state=Error")
			status["state"] = string(v1alpha1.InstanceStateError)
		}
	}

	ri := c.client.Dynamic().Resource(c.gvr)
	var instanceClient dynamic.ResourceInterface = ri
	if c.namespaced {
		instanceClient = ri.Namespace(inst.GetNamespace())
	}
	return c.persistStatus(ctx, instanceClient, inst, wireStatus, status, previousState)
}

// instanceWatcherBridge adapts a dynamiccontroller.InstanceWatcher to the
// watchrouter.Watcher interface expected by executor.Simple.  The two
// WatchRequest types are structurally equivalent (NodeID, GVR, Name,
// Namespace) — only the package path differs.
type instanceWatcherBridge struct {
	w dynamiccontroller.InstanceWatcher
}

func (b *instanceWatcherBridge) Watch(req watchrouter.WatchRequest) error {
	return b.w.Watch(dynamiccontroller.WatchRequest{
		NodeID:    req.NodeID,
		GVR:       req.GVR,
		Name:      req.Name,
		Namespace: req.Namespace,
		Selector:  req.Selector,
	})
}

func (b *instanceWatcherBridge) Done(commit bool) {
	b.w.Done(commit)
}

// isResourceDeleting reports whether err (an executor apply error) signals a
// managed resource that is currently terminating. It matches both the
// distinguishable sentinel and the typed error the executor wraps.
func isResourceDeleting(err error) bool {
	if errors.Is(err, executor.ErrResourceDeleting) {
		return true
	}
	var delErr *executor.ResourceDeletingError
	return errors.As(err, &delErr)
}
