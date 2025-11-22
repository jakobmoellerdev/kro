package instance

import (
	"fmt"

	"github.com/kubernetes-sigs/kro/pkg/applyset"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
)

func (c *Controller) reconcileDeletion(rcx *ReconcileContext) error {
	rcx.StateManager.State = InstanceStateDeleting
	rcx.Mark.ResourcesUnderDeletion("deleting resources")

	// ---------------------------------------------------------
	// 1. Resolve current DAG levels
	// ---------------------------------------------------------
	levels, err := rcx.Runtime.DAG().TopologicalSortLevels()
	if err != nil {
		return fmt.Errorf("cannot compute levelled order for deletion: %w", err)
	}

	// ---------------------------------------------------------
	// 2. Prune from highest → lowest
	// ---------------------------------------------------------
	for lvl := len(levels) - 1; lvl >= 0; lvl-- {
		rcx.Log.V(2).Info("Pruning level", "level", lvl, "ids", levels[lvl])

		// ---------------------------------------------------------
		// 2a. Load stable *level parent*
		// ---------------------------------------------------------
		parent, err := c.newLevelParent(rcx, lvl)
		if err != nil {
			return rcx.delayedRequeue(fmt.Errorf("cannot load level parent %d: %w", lvl, err))
		}

		// ---------------------------------------------------------
		// 2b. Create ApplySet for this level (no Add!)
		// ---------------------------------------------------------
		lbl, err := metadata.NewInstanceLabeler(rcx.Runtime.GetInstance()).
			Merge(rcx.Labeler)
		if err != nil {
			return fmt.Errorf("labeler merge failed: %w", err)
		}

		cfg := applyset.Config{
			ToolLabels:   lbl.Labels(),
			FieldManager: FieldManagerForApplyset,
			ToolingID:    KROTooling,
			Log:          rcx.Log,
		}

		levelSet, err := applyset.New(parent, rcx.RestMapper, rcx.Client, cfg)
		if err != nil {
			return fmt.Errorf("applyset setup failed for level %d: %w", lvl, err)
		}

		// ---------------------------------------------------------
		// IMPORTANT:
		// This performs:
		// - prune-only delete of all children belonging to this parent
		// - NO ADD (so everything under this parent is considered missing)
		//
		// Deletion proceeds concurrently inside Apply():
		// ApplySet will issue concurrent DELETE (or PATCH apply=prune) calls.
		// ---------------------------------------------------------
		result, err := levelSet.Apply(rcx.Ctx, true /* prune */)
		if err != nil {
			return rcx.delayedRequeue(fmt.Errorf("prune failed for level %d: %w", lvl, err))
		}

		// ---------------------------------------------------------
		// 2d. Update state for resources that belonged to this level-parent
		// ---------------------------------------------------------
		for _, r := range result.AppliedObjects {
			st := rcx.StateManager.ResourceStates[r.ID]
			if st == nil {
				st = &ResourceState{}
				rcx.StateManager.ResourceStates[r.ID] = st
			}

			if r.Error != nil {
				st.State = ResourceStateError
				st.Err = r.Error
			} else {
				st.State = ResourceStateDeleted
			}
		}

		// stop early if we hit errors
		if err := rcx.StateManager.ResourceErrors(); err != nil {
			return rcx.delayedRequeue(err)
		}

		if result.HasClusterMutation() {
			return rcx.delayedRequeue(fmt.Errorf("cluster mutated during prune"))
		}

		// ExternalRef handling:
		// They never belonged to level parents → thus never appear in prune results → safe.
	}

	// ---------------------------------------------------------
	// 3. Finalizer removal
	// ---------------------------------------------------------
	return c.removeFinalizer(rcx)
}

func (c *Controller) removeFinalizer(rcx *ReconcileContext) error {
	inst := rcx.Runtime.GetInstance()
	patched, err := c.setUnmanaged(rcx, inst)
	if err != nil {
		rcx.Mark.InstanceNotManaged("failed removing finalizer: %v", err)
		return err
	}
	rcx.Runtime.SetInstance(patched)
	return nil
}

func (c *Controller) getClientFor(rcx *ReconcileContext, rid string) dynamic.ResourceInterface {
	desc := rcx.Runtime.ResourceDescriptor(rid)
	gvr := desc.GetGroupVersionResource()
	ns := c.getNamespaceFor(rcx, rid)

	if desc.IsNamespaced() {
		return rcx.Client.Resource(gvr).Namespace(ns)
	}
	return rcx.Client.Resource(gvr)
}

func (c *Controller) getNamespaceFor(rcx *ReconcileContext, rid string) string {
	inst := rcx.Runtime.GetInstance()
	resource, _ := rcx.Runtime.GetResource(rid)

	if ns := resource.GetNamespace(); ns != "" {
		return ns
	}
	if ns := inst.GetNamespace(); ns != "" {
		return ns
	}
	return metav1.NamespaceDefault
}

func (c *Controller) setUnmanaged(rcx *ReconcileContext, obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	if exist := metadata.HasInstanceFinalizer(obj); !exist {
		return obj, nil
	}
	rcx.Log.Info("Removing managed state", "name", obj.GetName(), "namespace", obj.GetNamespace())
	instancePatch := &unstructured.Unstructured{}
	instancePatch.SetUnstructuredContent(map[string]interface{}{"apiVersion": obj.GetAPIVersion(), "kind": obj.GetKind(), "metadata": map[string]interface{}{"name": obj.GetName(), "namespace": obj.GetNamespace()}})
	instancePatch.SetFinalizers(obj.GetFinalizers())
	metadata.RemoveInstanceFinalizer(instancePatch)
	updated, err := rcx.InstanceClient().Apply(rcx.Ctx, instancePatch.GetName(), instancePatch, metav1.ApplyOptions{FieldManager: FieldManagerForLabeler, Force: true})
	if err != nil {
		return nil, fmt.Errorf("failed to update unmanaged state: %w", err)
	}
	return updated, nil
}
