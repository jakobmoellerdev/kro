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
	"encoding/json"
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/retry"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/controller/instance/applyset"
	"github.com/kubernetes-sigs/kro/pkg/dynamiccontroller"
	"github.com/kubernetes-sigs/kro/pkg/graph/revisions"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/executor"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/rgdadapter"
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

// reconcileViaGraphEngine is the Graph-engine reconcile path.
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

	// Initialise the kro-builtin conditions marker.  Conditions are merged into
	// the status patch at the end via patchGraphEngineStatus.
	mark := NewConditionsMarkerFor(inst)

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

	//------------------------------------------------------------------
	// 3. Build per-reconcile Runtime
	//------------------------------------------------------------------
	rt, _, err := rgdadapter.BuildRuntimeForInstance(rgd, inst, c.graphEngineCompiler)
	if err != nil {
		log.Error(err, "graph-engine: BuildRuntimeForInstance failed")
		mark.GraphResolutionFailed("graph-engine build failed: %v", err)
		_ = c.updateConditionsStatus(ctx, inst)
		return err
	}
	mark.InstanceManaged()
	mark.GraphResolved()

	//------------------------------------------------------------------
	// 4. Apply through the executor (SSA + watches)
	//------------------------------------------------------------------
	// Build a per-reconcile child labeler: instance labels + applyset part-of
	// + struct-level KRO-meta labels are composed inside ApplyWithLabeler.
	instanceLabeler := metadata.NewInstanceLabeler(inst, c.namespaced)
	applysetPartOf := applyset.ID(inst)
	extraLabel := func(obj *unstructured.Unstructured) {
		instanceLabeler.ApplyLabels(obj)
		l := obj.GetLabels()
		if l == nil {
			l = map[string]string{}
		}
		l[applyset.ApplysetPartOfLabel] = applysetPartOf
		obj.SetLabels(l)
	}
	bridge := &instanceWatcherBridge{w: dcWatcher}
	applyResult, applyErr := c.graphEngineExecutor.ApplyWithLabeler(ctx, rt, bridge, extraLabel)

	// Set kro-builtin conditions based on apply outcome.
	if applyErr == nil {
		mark.ResourcesReady()
	} else if errors.Is(applyErr, executor.ErrResourceTerminating) {
		mark.ResourcesDeleting("resource is terminating: %v", applyErr)
		log.V(1).Info("graph-engine: Apply returned terminating resource", "error", applyErr)
	} else if errors.Is(applyErr, executor.ErrNotReady) {
		mark.ResourcesNotReady("unresolved or awaiting readiness: %v", applyErr)
		log.V(1).Info("graph-engine: Apply returned soft not-ready", "error", applyErr)
	} else {
		mark.ResourcesNotReady("resource reconciliation failed: %v", applyErr)
		log.V(1).Info("graph-engine: Apply returned hard error", "error", applyErr)
	}

	// Stamp the ApplySet contains-group-kinds annotation on the instance so
	// ApplySet inventory stays correct (needed by deletion and compat tests).
	if gkErr := c.patchApplySetGKs(ctx, inst, applyResult.Applied); gkErr != nil {
		log.V(1).Info("graph-engine: ApplySet GKs patch failed (non-fatal)", "error", gkErr)
	}

	//------------------------------------------------------------------
	// 5. Project and patch instance status
	//------------------------------------------------------------------
	statusFields, projErr := rgdadapter.ProjectInstanceStatus(rt, rgd)
	if projErr != nil {
		log.Error(projErr, "graph-engine: status projection failed")
		// Non-fatal: update conditions only
	}

	conditions, condErr := rgdadapter.ProjectInstanceConditions(rt, rgd)
	if condErr != nil {
		log.V(1).Info("graph-engine: condition projection failed (non-fatal)", "error", condErr)
	}

	patchErr := c.patchGraphEngineStatus(ctx, inst, statusFields, conditions, mark, applyErr)
	if patchErr != nil {
		log.Error(patchErr, "graph-engine: status patch failed")
		return patchErr
	}

	// Return the apply error last so controller-runtime requeues if needed.
	// ErrNotReady and ErrResourceTerminating are both soft requeue signals.
	if applyErr != nil && (errors.Is(applyErr, executor.ErrNotReady) || errors.Is(applyErr, executor.ErrResourceTerminating)) {
		return requeue.NeededAfter(applyErr, c.reconcileConfig.DefaultRequeueDuration)
	}
	return applyErr
}

// managedResourcesToInterfaceSlice is reserved for future use when
// managedResources needs to be persisted via a schema-registered field.
// Currently unused — deletion uses the ApplySet inventory path.

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

// patchApplySetInventory updates the full ApplySet inventory metadata on the
// instance to reflect the set of GKs and namespaces from the graph engine's
// last apply.  It stamps all four required annotations (tooling, GKs,
// additional-namespaces, inventory-hash) via applyset.Metadata so that the
// deletion path's ValidateParentInventory check succeeds.
func (c *Controller) patchApplySetGKs(ctx context.Context, inst *unstructured.Unstructured, applied []v1alpha1.ManagedResource) error {
	// Collect unique GKs and child namespaces from the applied resources.
	gkSet := sets.New[schema.GroupKind]()
	nsSet := sets.New[string]()
	instNS := inst.GetNamespace()
	for _, r := range applied {
		gv, err := schema.ParseGroupVersion(r.APIVersion)
		if err != nil {
			continue
		}
		gkSet.Insert(schema.GroupKind{Group: gv.Group, Kind: r.Kind})
		if r.Namespace != "" && r.Namespace != instNS {
			nsSet.Insert(r.Namespace)
		}
	}

	meta := applyset.Metadata{
		ID:                   applyset.ID(inst),
		Tooling:              applyset.ToolingID(),
		GroupKinds:           gkSet,
		AdditionalNamespaces: nsSet,
	}

	// Fast-path: if annotations are already correct, skip the write.
	wantAnns := meta.Annotations()
	currentAnns := inst.GetAnnotations()
	changed := false
	for k, v := range wantAnns {
		if currentAnns[k] != v {
			changed = true
			break
		}
	}
	if !changed {
		return nil
	}

	patchObj := instanceSSAPatch(inst)
	patchObj.SetLabels(meta.Labels())
	patchObj.SetAnnotations(meta.Annotations())

	ri := c.client.Dynamic().Resource(c.gvr)
	var instClient interface {
		Apply(context.Context, string, *unstructured.Unstructured, metav1.ApplyOptions, ...string) (*unstructured.Unstructured, error)
	}
	if c.namespaced {
		instClient = ri.Namespace(inst.GetNamespace())
	} else {
		instClient = ri
	}
	_, err := instClient.Apply(ctx, inst.GetName(), patchObj, metav1.ApplyOptions{
		FieldManager: applyset.FieldManager + "-parent",
		Force:        true,
	})
	return err
}

// patchGraphEngineStatus writes the projected status map and conditions onto
// the instance using a plain UpdateStatus call (same pattern as persistStatus).
// mark carries kro-builtin conditions which are merged with author conditions.
func (c *Controller) patchGraphEngineStatus(
	ctx context.Context,
	inst *unstructured.Unstructured,
	statusFields map[string]any,
	authorConditions []metav1.Condition,
	mark *ConditionsMarker,
	applyErr error,
) error {
	ri := c.client.Dynamic().Resource(c.gvr)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cur *unstructured.Unstructured
		var err error
		if c.namespaced {
			cur, err = ri.Namespace(inst.GetNamespace()).Get(ctx, inst.GetName(), metav1.GetOptions{})
		} else {
			cur, err = ri.Get(ctx, inst.GetName(), metav1.GetOptions{})
		}
		if err != nil {
			return err
		}

		status, _, _ := unstructured.NestedMap(cur.Object, "status")
		if status == nil {
			status = map[string]any{}
		}

		// Write projected fields.
		for k, v := range statusFields {
			status[k] = v
		}

		// State: Active when no apply error, InProgress otherwise.
		if applyErr != nil {
			status["state"] = string(v1alpha1.InstanceStateInProgress)
		} else {
			status["state"] = string(v1alpha1.InstanceStateActive)
		}

		// Merge kro-builtin conditions (from mark) with author-defined conditions.
		// When the RGD declares no author conditions we only write builtins so
		// the surface matches the old path.
		builtins := builtinConditions(inst)
		var allConds []v1alpha1.Condition
		if len(authorConditions) > 0 {
			// Start with builtins, then append author conditions (which may have
			// different types; types are not de-duped since author conditions use
			// custom type strings while builtins use kro's canonical types).
			allConds = builtins
			for _, ac := range authorConditions {
				var v1cond v1alpha1.Condition
				b, e := json.Marshal(ac)
				if e == nil {
					_ = json.Unmarshal(b, &v1cond)
					allConds = append(allConds, v1cond)
				}
			}
		} else {
			allConds = builtins
		}
		if len(allConds) > 0 {
			status["conditions"] = conditionsToInterfaceSlice(allConds)
		}

		cur.Object["status"] = status
		inst.Object["status"] = status

		if c.namespaced {
			_, err = ri.Namespace(inst.GetNamespace()).UpdateStatus(ctx, cur, metav1.UpdateOptions{})
		} else {
			_, err = ri.UpdateStatus(ctx, cur, metav1.UpdateOptions{})
		}
		return err
	})
}

// conditionToMap converts a metav1.Condition to a plain map for unstructured
// storage.  Uses JSON round-trip for correctness.
func conditionToMap(cond metav1.Condition) (map[string]interface{}, error) {
	b, err := json.Marshal(cond)
	if err != nil {
		return nil, err
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
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
	})
}

func (b *instanceWatcherBridge) Done(commit bool) {
	b.w.Done(commit)
}
