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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	case errors.Is(applyErr, executor.ErrNotReady):
		// Soft: a node is waiting on data/readiness. State stays InProgress;
		// child watch events (and the requeue below) drive the next cycle.
		mark.ResourcesNotReady("awaiting resource readiness: %v", applyErr)
	default:
		hardErr = true
		mark.ResourcesNotReady("resource reconciliation failed: %v", applyErr)
	}

	// Stamp the full ApplySet inventory (parent-id label + tooling + GKs +
	// additional-namespaces + recomputed hash) on the instance so the
	// deletion path's ValidateParentInventory stays consistent and prune
	// can discover managed members.
	if invErr := c.patchApplySetInventory(ctx, inst, applyResult.Applied); invErr != nil {
		log.V(1).Info("graph-engine: ApplySet inventory patch failed (non-fatal)", "error", invErr)
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

// patchApplySetInventory writes the complete ApplySet inventory metadata on
// the instance to reflect the resources the graph engine just applied. It
// mirrors the old path's patchInstanceWithApplySetMetadata: all four KEP-3659
// annotations (tooling, contains-group-kinds, additional-namespaces, and the
// inventory hash) are written together so they stay mutually consistent —
// writing group-kinds without recomputing the hash would fail
// ValidateParentInventory and wedge deletion.
func (c *Controller) patchApplySetInventory(ctx context.Context, inst *unstructured.Unstructured, applied []v1alpha1.ManagedResource) error {
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
