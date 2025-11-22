package instance

import (
	"fmt"

	"github.com/kubernetes-sigs/kro/pkg/applyset"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
	"github.com/kubernetes-sigs/kro/pkg/runtime"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

func (c *Controller) reconcileResources(rcx *ReconcileContext) error {
	rcx.Log.V(2).Info("Reconciling resources")

	// 1. Compute per-level topological order
	levels, err := rcx.Runtime.DAG().TopologicalSortLevels()
	if err != nil {
		return rcx.delayedRequeue(fmt.Errorf("topological order failed: %w", err))
	}

	// unresolved resource blocks next level
	var unresolved string

	// 2. Process level-by-level in order
	for lvlIdx, levelIDs := range levels {
		rcx.Log.V(2).Info("Processing level", "level", lvlIdx, "ids", levelIDs)

		// 2a. Ensure parent exists for this level
		parent, err := c.newLevelParent(rcx, lvlIdx)
		if err != nil {
			return rcx.delayedRequeue(err)
		}

		// 2b. Create ApplySet for this level
		lbl, err := metadata.NewInstanceLabeler(rcx.Runtime.GetInstance()).
			Merge(rcx.Labeler)
		if err != nil {
			return rcx.delayedRequeue(fmt.Errorf("labeler merge: %w", err))
		}

		cfg := applyset.Config{
			ToolLabels:   lbl.Labels(),
			FieldManager: FieldManagerForApplyset,
			ToolingID:    KROTooling,
			Log:          rcx.Log,
		}

		levelSet, err := applyset.New(parent, rcx.RestMapper, rcx.Client, cfg)
		if err != nil {
			return rcx.delayedRequeue(fmt.Errorf("applyset (level=%d) creation failed: %w", lvlIdx, err))
		}

		prune := true

		// 2c. Sequential Add (prepare + add)
		for _, id := range levelIDs {

			st := &ResourceState{State: ResourceStateInProgress}
			rcx.StateManager.ResourceStates[id] = st

			// Should we process?
			want, err := rcx.Runtime.ReadyToProcessResource(id)
			if err != nil || !want {
				st.State = ResourceStateSkipped
				rcx.Runtime.IgnoreResource(id)
				continue
			}

			// Must be resolved
			desired, rstate := rcx.Runtime.GetResource(id)
			if rstate != runtime.ResourceStateResolved {
				unresolved = id
				prune = false
				continue
			}

			// Dependencies must be ready
			if !rcx.Runtime.AreDependenciesReady(id) {
				unresolved = id
				prune = false
				continue
			}

			desc := rcx.Runtime.ResourceDescriptor(id)

			if desc.IsExternalRef() {
				// ExternalRef: read-only, never applied or pruned
				if err := c.handleExternalRef(rcx, id, st); err != nil {
					return rcx.delayedRequeue(err)
				}
				continue
			}

			// Normal managed resource → Add to the applySet
			applyable := applyset.ApplyableObject{
				Unstructured: desired,
				ID:           id,
			}

			actual, err := levelSet.Add(rcx.Ctx, applyable)
			if err != nil {
				st.State = ResourceStateError
				st.Err = err
				return rcx.delayedRequeue(err)
			}

			// Immediate update from Add (SSA readback)
			if actual != nil {
				rcx.Runtime.SetResource(id, actual)
				updateReadiness(rcx, id)
				if _, err := rcx.Runtime.Synchronize(); err != nil {
					return rcx.delayedRequeue(err)
				}
			}
		}

		// 2d. Perform concurrent Apply
		result, err := levelSet.Apply(rcx.Ctx, prune)
		if err != nil {
			return rcx.delayedRequeue(fmt.Errorf("apply/prune (level=%d) failed: %w", lvlIdx, err))
		}

		// 2e. Process Apply results
		if err := c.processApplyResults(rcx, result); err != nil {
			return rcx.delayedRequeue(err)
		}

		if result.HasClusterMutation() {
			return rcx.delayedRequeue(fmt.Errorf("cluster mutated"))
		}

		if unresolved != "" {
			return rcx.delayedRequeue(
				fmt.Errorf("waiting for unresolved resource %q", unresolved),
			)
		}
	}

	// All levels finished
	return nil
}

func (c *Controller) reconcileResource(
	rcx *ReconcileContext,
	aset applyset.Set,
	id string,
	unresolved *string,
	prune *bool,
) error {
	rcx.Log.V(3).Info("Reconciling resource", "id", id)

	st := &ResourceState{State: ResourceStateInProgress}
	rcx.StateManager.ResourceStates[id] = st

	// 1. Should we process?
	want, err := rcx.Runtime.ReadyToProcessResource(id)
	if err != nil || !want {
		st.State = ResourceStateSkipped
		rcx.Log.V(2).Info("Skipping resource", "id", id, "reason", err)
		rcx.Runtime.IgnoreResource(id)
		return nil
	}

	// 2. Must be resolved
	_, rstate := rcx.Runtime.GetResource(id)
	if rstate != runtime.ResourceStateResolved {
		*unresolved = id
		*prune = false
		return nil
	}

	// 3. Dependencies must be ready
	if !rcx.Runtime.AreDependenciesReady(id) {
		*unresolved = id
		*prune = false
		return nil
	}

	// 4. External reference
	if rcx.Runtime.ResourceDescriptor(id).IsExternalRef() {
		return c.handleExternalRef(rcx, id, st)
	}

	// 5. Regular resource via ApplySet
	return c.handleApplySetResource(rcx, aset, id, st)
}

func (c *Controller) createApplySet(rcx *ReconcileContext) (applyset.Set, error) {
	lbl, err := metadata.NewInstanceLabeler(rcx.Runtime.GetInstance()).
		Merge(rcx.Labeler)
	if err != nil {
		return nil, fmt.Errorf("labeler merge: %w", err)
	}

	cfg := applyset.Config{
		ToolLabels:   lbl.Labels(),
		FieldManager: FieldManagerForApplyset,
		ToolingID:    KROTooling,
		Log:          rcx.Log,
	}

	return applyset.New(rcx.Runtime.GetInstance(), rcx.RestMapper, rcx.Client, cfg)
}

func (c *Controller) handleApplySetResource(
	rcx *ReconcileContext,
	aset applyset.Set,
	id string,
	st *ResourceState,
) error {
	desired, _ := rcx.Runtime.GetResource(id)

	applyable := applyset.ApplyableObject{
		Unstructured: desired,
		ID:           id,
	}

	actual, err := aset.Add(rcx.Ctx, applyable)
	if err != nil {
		st.State = ResourceStateError
		st.Err = err
		return err
	}

	if actual != nil {
		rcx.Runtime.SetResource(id, actual)
		updateReadiness(rcx, id)
		_, err := rcx.Runtime.Synchronize()
		return err
	}
	return nil
}

func updateReadiness(rcx *ReconcileContext, id string) {
	st := rcx.StateManager.ResourceStates[id]

	ready, reason, err := rcx.Runtime.IsResourceReady(id)
	if err != nil || !ready {
		st.State = ResourceStateWaitingForReadiness
		st.Err = fmt.Errorf("not ready: %s: %w", reason, err)
	} else {
		st.State = ResourceStateSynced
	}
}

func (c *Controller) handleExternalRef(
	rcx *ReconcileContext,
	id string,
	st *ResourceState,
) error {
	desired, _ := rcx.Runtime.GetResource(id)

	actual, err := c.readExternalRef(rcx, id, desired)
	if err != nil {
		st.State = ResourceStateError
		st.Err = err
		return nil
	}

	rcx.Runtime.SetResource(id, actual)
	updateReadiness(rcx, id)
	_, err = rcx.Runtime.Synchronize()
	return err
}

func (c *Controller) readExternalRef(
	rcx *ReconcileContext,
	resourceID string,
	desired *unstructured.Unstructured,
) (*unstructured.Unstructured, error) {

	gvk := desired.GroupVersionKind()

	// 1. Map GVK → GVR
	mapping, err := c.client.RESTMapper().RESTMapping(
		gvk.GroupKind(),
		gvk.Version,
	)
	if err != nil {
		return nil, fmt.Errorf("externalRef: RESTMapping for %s: %w", gvk, err)
	}

	// 2. Determine which client to use
	var ri dynamic.ResourceInterface

	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		ns := desired.GetNamespace()
		if ns == "" {
			ns = rcx.getResourceNamespace(resourceID)
		}
		ri = c.client.Dynamic().Resource(mapping.Resource).Namespace(ns)
	} else {
		ri = c.client.Dynamic().Resource(mapping.Resource)
	}

	// 3. Fetch existing object
	name := desired.GetName()

	obj, err := ri.Get(rcx.Ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("externalRef: GET %s %s/%s: %w",
			gvk.String(), desired.GetNamespace(), name, err,
		)
	}

	rcx.Log.Info("External reference resolved",
		"resourceID", resourceID,
		"gvk", gvk.String(),
		"namespace", obj.GetNamespace(),
		"name", obj.GetName(),
	)

	return obj, nil
}

func (c *Controller) processApplyResults(
	rcx *ReconcileContext,
	result *applyset.ApplyResult,
) error {

	rcx.Log.V(2).Info("Processing apply results")

	for _, r := range result.AppliedObjects {
		st := rcx.StateManager.ResourceStates[r.ID]

		// ---------------------------------------------------------
		// 1. Handle apply error for this specific resource
		// ---------------------------------------------------------
		if r.Error != nil {
			st.State = ResourceStateError
			st.Err = r.Error
			rcx.Log.V(1).Info("Apply error", "id", r.ID, "error", r.Error)
			continue
		}

		// ---------------------------------------------------------
		// 2. Update runtime with the new applied object
		// ---------------------------------------------------------
		if r.LastApplied != nil {
			rcx.Runtime.SetResource(r.ID, r.LastApplied)
			updateReadiness(rcx, r.ID)

			if _, err := rcx.Runtime.Synchronize(); err != nil {
				st.State = ResourceStateError
				st.Err = fmt.Errorf("failed to synchronize after apply: %w", err)
				continue
			}
		}
	}

	// ---------------------------------------------------------
	// 3. Aggregate all resource errors
	// ---------------------------------------------------------
	if err := rcx.StateManager.ResourceErrors(); err != nil {
		return fmt.Errorf("apply results contain errors: %w", err)
	}

	return nil
}

// newLevelParent returns the stable parent object for a given DAG level.
// Parents:
//   - Are ConfigMaps (harmless, cheap, namespaced, always present)
//   - Have stable names: <instance>-lvl-<level>
//   - Are CREATE-or-GET: never mutated after creation
//   - Carry labels to bind children to this parent for lineage pruning
//
// This keeps ApplySet semantics consistent even if DAG levels change.
func (c *Controller) newLevelParent(
	rcx *ReconcileContext,
	level int,
) (*unstructured.Unstructured, error) {

	inst := rcx.Runtime.GetInstance()
	name := fmt.Sprintf("%s-lvl-%d", inst.GetName(), level)

	ns := inst.GetNamespace()
	if ns == "" {
		ns = metav1.NamespaceDefault
	}

	// -----------------------------
	// Build desired object structure
	// -----------------------------
	parent := &unstructured.Unstructured{}
	parent.SetAPIVersion("v1")
	parent.SetKind("ConfigMap")
	parent.SetName(name)
	parent.SetNamespace(ns)

	// Labels used by ApplySet to group children
	labels := map[string]string{
		"kro.run/applyset-scope": "level-parent",
		"kro.run/instance":       inst.GetName(),
		"kro.run/level":          fmt.Sprintf("%d", level),
	}
	parent.SetLabels(labels)

	gvr := schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "configmaps",
	}

	// -----------------------------
	// Try to CREATE
	// -----------------------------
	created, err := c.client.Dynamic().
		Resource(gvr).
		Namespace(ns).
		Create(rcx.Ctx, parent, metav1.CreateOptions{})

	if err == nil {
		rcx.Log.V(3).Info("Created new level parent",
			"name", name, "namespace", ns, "level", level)
		return created, nil
	}

	// -----------------------------
	// Already exists → GET
	// -----------------------------
	if apierrors.IsAlreadyExists(err) {
		existing, err := c.client.Dynamic().
			Resource(gvr).
			Namespace(ns).
			Get(rcx.Ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to get existing level parent %s/%s: %w", ns, name, err)
		}

		// DO NOT mutate existing → keeps lineage clean
		rcx.Log.V(3).Info("Reusing existing level parent",
			"name", name, "namespace", ns, "level", level)
		return existing, nil
	}

	return nil, fmt.Errorf("failed to create level parent %s/%s: %w", ns, name, err)
}
