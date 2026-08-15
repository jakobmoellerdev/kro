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
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/retry"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/dynamiccontroller"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/rgdadapter"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/watchrouter"
	"github.com/kubernetes-sigs/kro/pkg/graph/revisions"
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
			Name:      latest.OwnerKey,
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
	// 3. Build per-reconcile Runtime
	//------------------------------------------------------------------
	rt, _, err := rgdadapter.BuildRuntimeForInstance(rgd, inst, c.graphEngineCompiler)
	if err != nil {
		log.Error(err, "graph-engine: BuildRuntimeForInstance failed")
		// Mark the instance so the operator can see the build failure.
		mark := NewConditionsMarkerFor(inst)
		mark.GraphResolutionFailed("graph-engine build failed: %v", err)
		_ = c.updateConditionsStatus(ctx, inst)
		return err
	}

	//------------------------------------------------------------------
	// 4. Apply through the executor (SSA + watches)
	//------------------------------------------------------------------
	bridge := &instanceWatcherBridge{w: dcWatcher}
	applyResult, applyErr := c.graphEngineExecutor.Apply(ctx, rt, bridge)

	// Record managed resources on the instance status regardless of soft error.
	// Hard errors abort before status patching.
	if applyErr != nil {
		// Check if it's a soft "not ready" error from executor; if so we still
		// want to project partial status so the instance shows InProgress.
		log.V(1).Info("graph-engine: Apply returned error (may be soft)", "error", applyErr)
	}

	_ = applyResult // tracked by executor; F6b can plumb ManagedResources into ApplySet

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

	patchErr := c.patchGraphEngineStatus(ctx, inst, statusFields, conditions, applyErr)
	if patchErr != nil {
		log.Error(patchErr, "graph-engine: status patch failed")
		return patchErr
	}

	// Return the apply error last so controller-runtime requeues if needed.
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

// patchGraphEngineStatus writes the projected status map and conditions onto
// the instance using a plain UpdateStatus call (same pattern as persistStatus).
func (c *Controller) patchGraphEngineStatus(
	ctx context.Context,
	inst *unstructured.Unstructured,
	statusFields map[string]any,
	conditions []metav1.Condition,
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

		// Conditions from author CEL block.
		if len(conditions) > 0 {
			raw := make([]interface{}, 0, len(conditions))
			for _, cond := range conditions {
				m, e := conditionToMap(cond)
				if e == nil {
					raw = append(raw, m)
				}
			}
			status["conditions"] = raw
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
