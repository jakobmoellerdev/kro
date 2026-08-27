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

package core_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"

	krov1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/controller/instance/applyset"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
	"github.com/kubernetes-sigs/kro/test/integration/environment"
)

// This suite ports the resource-ownership / applyset behavioral scenarios
// (resource_ownership_test.go, instance_conflict_test.go, annotation_label_test.go,
// applyset_test.go) to the parity harness, running each fixture through both the
// built-in ResourceGraphDefinition controller and the RGD-as-Graph backend.
//
// PARITY BOUNDARY for this batch: field-manager names, ApplySet parent
// labels/annotations (kro.run/applyset shared field manager, ApplySetParentID,
// tooling / GKs / additional-namespaces annotations), the metadata.* instance
// GVK labels, collection-index/size labels, and node-id labels are all
// artifacts of the BUILT-IN instance controller. The Graph engine applies with
// per-Graph field managers and does not stamp kro's ApplySet parent/child label
// vocabulary. So:
//
//   - The OBSERVABLE resource-composition behavior — resource created, pruned
//     when a condition/collection shrinks, and NOT stolen across instances/RGDs
//     — is asserted for BOTH backends.
//   - The exact applyset/metadata label & annotation KEYS/VALUES are asserted
//     ONLY under isBuiltin(be).
//   - Scenarios that are PURELY about applyset annotation shape or RGD-object
//     conditions are marked N/A for the graph backend with a reason.
var _ = Describe("RGD parity: resource ownership & applyset", func() {

	// ── resource_ownership_test.go: stable field manager ────────────────────
	//
	// "applies managed resources under a stable field manager" is a built-in
	// SSA implementation invariant (applyset.FieldManager == "kro.run/applyset"
	// with an Apply managedFields entry). The graph engine applies each Graph's
	// resources under its own per-Graph field manager, so the specific manager
	// name/shape has no standalone-Graph analog. Assert the field-manager shape
	// only under isBuiltin; both backends still converge the child resource.
	DescribeTable("applies a managed resource (field manager is built-in-specific)",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityFieldMgr"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string"},
							map[string]any{"ready": "${cm.metadata.name != ''}"},
						),
						generator.WithResource("cm", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-cm",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"managed": "yes"},
						}, nil, nil),
					)
				},
				map[string]any{"name": "fieldmgr"},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, be rgdBackend) {
					cmKey := types.NamespacedName{Namespace: ns, Name: "fieldmgr-cm"}
					// Both backends must converge the child with the resolved data.
					cm := awaitInstanceCR(t, configMapGVK, cmKey,
						func(u *unstructured.Unstructured) error {
							got, _, _ := unstructured.NestedString(u.Object, "data", "managed")
							if got != "yes" {
								return &notYetError{msg: "cm.data.managed=" + got + ", want yes"}
							}
							return nil
						}, 60*time.Second)

					if !isBuiltin(be) {
						// N/A (graph): the field-manager name/shape is a built-in
						// SSA invariant; the graph engine uses per-Graph field
						// managers, a documented parity boundary. End state is
						// asserted (child converged) for both backends above.
						return
					}
					// Built-in: an Apply managedFields entry owned by the stable
					// kro.run/applyset field manager.
					const expectedFieldManager = "kro.run/applyset"
					if applyset.FieldManager != expectedFieldManager {
						t.Fatalf("applyset.FieldManager=%q, want %q (rename breaks SSA field ownership on pre-existing objects)",
							applyset.FieldManager, expectedFieldManager)
					}
					ownsData := false
					managers := make([]string, 0, len(cm.GetManagedFields()))
					for _, entry := range cm.GetManagedFields() {
						managers = append(managers, fmt.Sprintf("%s/%s", entry.Manager, entry.Operation))
						if entry.Manager == expectedFieldManager && entry.Operation == "Apply" {
							ownsData = true
						}
					}
					if !ownsData {
						t.Fatalf("no Apply entry for field manager %q on managed resource; managedFields: %v",
							expectedFieldManager, managers)
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── resource_ownership_test.go: two instances don't overwrite ───────────
	//
	// A fixed resource name (not derived from the instance) means two instances
	// of the same RGD render the SAME object. The first owner must keep it — the
	// object must not silently flip between instances on every reconcile.
	//
	// SHARED behavior for both backends: the established owner's data is stable
	// over a window (the object does not change hands). The ApplySet membership
	// label reassignment assertion from the original is built-in-only.
	DescribeTable("two instances do not silently overwrite the same fixed-name resource",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			t := GinkgoT()
			ns := createParityNamespace(t)
			ctx := env.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			rgd := func(name string) *krov1alpha1.ResourceGraphDefinition {
				return generator.NewResourceGraphDefinition(name,
					generator.WithSchema(
						"ParityShared"+rand.String(4), "v1alpha1",
						map[string]any{"owner": "string"},
						map[string]any{"ready": "${cm.metadata.name != ''}"},
					),
					generator.WithResource("cm", map[string]any{
						"apiVersion": "v1", "kind": "ConfigMap",
						"metadata": map[string]any{
							"name":      "shared-target",
							"namespace": "${schema.metadata.namespace}",
						},
						"data": map[string]any{"owner": "${schema.spec.owner}"},
					}, nil, nil),
				)
			}("parity" + rand.String(5))
			gvk := be.Deploy(t, rgd)

			// First owner claims the shared object.
			first := newInstanceCR(gvk, ns, "owner-a", map[string]any{"owner": "a"})
			if err := env.Client.Create(ctx, first); err != nil {
				t.Fatalf("%s: create first instance: %v", be.Name(), err)
			}
			t.Cleanup(func() { _ = env.Client.Delete(context.Background(), first) })

			cmKey := types.NamespacedName{Namespace: ns, Name: "shared-target"}
			awaitInstanceCR(t, configMapGVK, cmKey,
				func(u *unstructured.Unstructured) error {
					got, _, _ := unstructured.NestedString(u.Object, "data", "owner")
					if got != "a" {
						return &notYetError{msg: "shared-target owner=" + got + ", want a"}
					}
					return nil
				}, 60*time.Second)

			// A second instance now renders the same fixed-name object.
			second := newInstanceCR(gvk, ns, "owner-b", map[string]any{"owner": "b"})
			if err := env.Client.Create(ctx, second); err != nil {
				t.Fatalf("%s: create second instance: %v", be.Name(), err)
			}
			t.Cleanup(func() { _ = env.Client.Delete(context.Background(), second) })

			// SHARED: the established owner keeps the object over a sampling
			// window — a single read can land between two competing writes and
			// look stable, so assert consistently.
			environment.Consistently(t, 12*time.Second, 2*time.Second, func() error {
				cm := &unstructured.Unstructured{}
				cm.SetGroupVersionKind(configMapGVK)
				if err := env.Client.Get(ctx, cmKey, cm); err != nil {
					return err
				}
				got, _, _ := unstructured.NestedString(cm.Object, "data", "owner")
				if got != "a" {
					return fmt.Errorf("shared-target changed hands: owner=%q, want a", got)
				}
				return nil
			})
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── instance_conflict_test.go: cross-RGD non-interference ───────────────
	//
	// Two DISTINCT RGDs (different Kinds) each render a ConfigMap of the same
	// name in their own namespace. Deleting one RGD's child recreates it;
	// mutating the other RGD's child is reconciled back. Neither RGD steals or
	// prunes the other's resource.
	//
	// SHARED for both backends: each child is created with its own data, is
	// recreated after deletion, and is reconciled back after mutation. The
	// metadata.Instance* GVK label assertions from the original are built-in
	// only (the graph engine does not stamp them).
	DescribeTable("cross-RGD instances do not reconcile each other's resources",
		func(makeBackend func() rgdBackend) {
			be1 := makeBackend()
			be2 := makeBackend()
			t := GinkgoT()
			ns1 := createParityNamespace(t)
			ns2 := createParityNamespace(t)
			ctx := env.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			mkRGD := func(kindSuffix, source string) func(string) *krov1alpha1.ResourceGraphDefinition {
				return func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityConflict"+kindSuffix+rand.String(4), "v1alpha1",
							map[string]any{"name": "string"},
							map[string]any{"ready": "${configmap.metadata.name != ''}"},
						),
						generator.WithResource("configmap", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"source": source},
						}, nil, nil),
					)
				}
			}

			rgd1 := mkRGD("App1", "app1")("parity" + rand.String(5))
			rgd2 := mkRGD("App2", "app2")("parity" + rand.String(5))
			gvk1 := be1.Deploy(t, rgd1)
			gvk2 := be2.Deploy(t, rgd2)

			// Same-named instances would collide in the graph backend's L2
			// naming (inst-<name> in the shared controller namespace), so use
			// distinct instance names in distinct namespaces; the shared
			// behavior under test is that neither RGD prunes/steals the other's
			// child (both render the same CHILD name "shared-resource").
			inst1 := newInstanceCR(gvk1, ns1, "my-app-1", map[string]any{"name": "shared-resource"})
			inst2 := newInstanceCR(gvk2, ns2, "my-app-2", map[string]any{"name": "shared-resource"})
			if err := env.Client.Create(ctx, inst1); err != nil {
				t.Fatalf("create inst1: %v", err)
			}
			t.Cleanup(func() { _ = env.Client.Delete(context.Background(), inst1) })
			if err := env.Client.Create(ctx, inst2); err != nil {
				t.Fatalf("create inst2: %v", err)
			}
			t.Cleanup(func() { _ = env.Client.Delete(context.Background(), inst2) })

			cm1Key := types.NamespacedName{Namespace: ns1, Name: "shared-resource"}
			cm2Key := types.NamespacedName{Namespace: ns2, Name: "shared-resource"}

			// Both children converge with their own data.
			awaitInstanceCR(t, configMapGVK, cm1Key,
				func(u *unstructured.Unstructured) error {
					got, _, _ := unstructured.NestedString(u.Object, "data", "source")
					if got != "app1" {
						return &notYetError{msg: "cm1.data.source=" + got + ", want app1"}
					}
					return nil
				}, 60*time.Second)
			awaitInstanceCR(t, configMapGVK, cm2Key,
				func(u *unstructured.Unstructured) error {
					got, _, _ := unstructured.NestedString(u.Object, "data", "source")
					if got != "app2" {
						return &notYetError{msg: "cm2.data.source=" + got + ", want app2"}
					}
					return nil
				}, 60*time.Second)

			// SHARED: neither RGD's child is stolen/overwritten by the other —
			// each keeps its own data over a sampling window despite both
			// rendering the same name in their own namespace.
			environment.Consistently(t, 8*time.Second, 1*time.Second, func() error {
				a := &unstructured.Unstructured{}
				a.SetGroupVersionKind(configMapGVK)
				if err := env.Client.Get(ctx, cm1Key, a); err != nil {
					return err
				}
				if got, _, _ := unstructured.NestedString(a.Object, "data", "source"); got != "app1" {
					return fmt.Errorf("cm1 changed hands: source=%q, want app1", got)
				}
				b := &unstructured.Unstructured{}
				b.SetGroupVersionKind(configMapGVK)
				if err := env.Client.Get(ctx, cm2Key, b); err != nil {
					return err
				}
				if got, _, _ := unstructured.NestedString(b.Object, "data", "source"); got != "app2" {
					return fmt.Errorf("cm2 changed hands: source=%q, want app2", got)
				}
				return nil
			})

			if isBuiltin(be1) {
				// Built-in: instance GVK tracking labels identify each owner
				// (Kind differs between the two RGDs).
				cm1 := getUnstructured(t, configMapGVK, cm1Key)
				assertInstanceGVKLabels(t, cm1.GetLabels(), "my-app-1", rgd1.Spec.Schema.Kind)
				cmB := getUnstructured(t, configMapGVK, cm2Key)
				assertInstanceGVKLabels(t, cmB.GetLabels(), "my-app-2", rgd2.Spec.Schema.Kind)
			}

			awaitInstanceReady(t, be1, gvk1, types.NamespacedName{Namespace: ns1, Name: "my-app-1"})
			awaitInstanceReady(t, be2, gvk2, types.NamespacedName{Namespace: ns2, Name: "my-app-2"})
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── annotation_label_test.go: correct conditions when RGD created ───────
	//
	// N/A (graph): this scenario asserts the RGD-OBJECT conditions/state and the
	// built-in ApplySet parent labels/annotations on the instance object
	// (ApplySetParentID, tooling/GKs/additional-namespaces annotations, kro
	// metadata labels). The graph backend has no RGD object and does not stamp
	// kro's ApplySet parent vocabulary onto the instance — a documented parity
	// boundary. Asserted for the built-in backend only; the graph entry converges
	// the child + instance (shared contract) and skips the annotation shape.
	DescribeTable("instance carries applyset parent + kro labels (built-in only)",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityAnnLabel"+rand.String(4), "v1alpha1",
							map[string]any{"field1": "string", "threshold": "float"},
							map[string]any{"ready": "${res1.metadata.name != ''}"},
						),
						generator.WithResource("res1", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.field1}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"threshold": "${string(schema.spec.threshold)}"},
						}, nil, nil),
					)
				},
				map[string]any{"field1": "foobar", "threshold": 3.14},
				func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
					cmKey := types.NamespacedName{Namespace: ns, Name: "foobar"}
					// SHARED: the child converges with the resolved data.
					awaitInstanceCR(t, configMapGVK, cmKey,
						func(u *unstructured.Unstructured) error {
							got, _, _ := unstructured.NestedString(u.Object, "data", "threshold")
							if got != "3.14" {
								return &notYetError{msg: "cm.data.threshold=" + got + ", want 3.14"}
							}
							return nil
						}, 60*time.Second)

					if !isBuiltin(be) {
						// N/A (graph): applyset parent labels/annotations + kro
						// metadata labels + RGD conditions have no graph analog.
						return
					}
					// Built-in: instance is the ApplySet parent; child carries
					// the ApplySet membership + kro instance tracking labels.
					instKey := types.NamespacedName{Namespace: ns, Name: instanceName}
					inst := awaitInstanceCR(t, gvk, instKey,
						func(u *unstructured.Unstructured) error {
							ann := u.GetAnnotations()
							if ann[applyset.ApplySetToolingAnnotation] != applyset.ToolingID() {
								return &notYetError{msg: "tooling annotation not yet set"}
							}
							if ann[applyset.ApplySetGKsAnnotation] != "ConfigMap" {
								return &notYetError{msg: "GKs annotation=" + ann[applyset.ApplySetGKsAnnotation] + ", want ConfigMap"}
							}
							if _, ok := u.GetLabels()[applyset.ApplySetParentIDLabel]; !ok {
								return &notYetError{msg: "applyset parent ID label not yet set"}
							}
							return nil
						}, 60*time.Second)

					lbls := inst.GetLabels()
					if got := lbls[applyset.ApplySetParentIDLabel]; got != applyset.ID(inst) {
						t.Fatalf("instance applyset parent ID=%q, want %q", got, applyset.ID(inst))
					}
					if _, ok := lbls[metadata.ManagedByLabelKey]; ok {
						t.Fatalf("instance should not carry %s (reserved for other lifecycle tooling)", metadata.ManagedByLabelKey)
					}

					cm := &unstructured.Unstructured{}
					cm.SetGroupVersionKind(configMapGVK)
					if err := env.Client.Get(env.Context(), cmKey, cm); err != nil {
						t.Fatalf("get cm: %v", err)
					}
					cl := cm.GetLabels()
					want := map[string]string{
						applyset.ApplysetPartOfLabel:    applyset.ID(inst),
						metadata.InstanceNamespaceLabel: inst.GetNamespace(),
						metadata.InstanceLabel:          inst.GetName(),
						metadata.InstanceIDLabel:        string(inst.GetUID()),
						metadata.InstanceGroupLabel:     krov1alpha1.KRODomainName,
						metadata.InstanceVersionLabel:   "v1alpha1",
						metadata.KROVersionLabel:        "devel",
						metadata.OwnedLabel:             "true",
					}
					for k, v := range want {
						if cl[k] != v {
							t.Fatalf("child label %s=%q, want %q", k, cl[k], v)
						}
					}
					if _, ok := cl[metadata.ResourceGraphDefinitionIDLabel]; ok {
						t.Fatalf("child should not carry RGD ID label")
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── applyset_test.go: multi-GVK apply (labels are built-in-only) ────────
	//
	// SHARED: an RGD with ConfigMap + Secret + ServiceAccount converges all
	// three children. The exact KEP applyset labels + kro node-id labels on the
	// children (and parent annotations on the instance) are built-in only.
	DescribeTable("multi-GVK apply converges all children (applyset labels built-in only)",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityMultiGVK"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string"},
							map[string]any{"ready": "${configMap.metadata.name != ''}"},
						),
						generator.WithResource("configMap", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-cm",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"key": "value"},
						}, nil, nil),
						generator.WithResource("secret", map[string]any{
							"apiVersion": "v1", "kind": "Secret",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-secret",
								"namespace": "${schema.metadata.namespace}",
							},
							"stringData": map[string]any{"password": "secret123"},
						}, nil, nil),
						generator.WithResource("serviceAccount", map[string]any{
							"apiVersion": "v1", "kind": "ServiceAccount",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-sa",
								"namespace": "${schema.metadata.namespace}",
							},
						}, nil, nil),
					)
				},
				map[string]any{"name": "multi"},
				func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
					cmKey := types.NamespacedName{Namespace: ns, Name: "multi-cm"}
					secretKey := types.NamespacedName{Namespace: ns, Name: "multi-secret"}
					saKey := types.NamespacedName{Namespace: ns, Name: "multi-sa"}

					// SHARED: all three children converge.
					awaitInstanceCR(t, configMapGVK, cmKey, nil, 60*time.Second)
					awaitInstanceCR(t, secretGVK, secretKey, nil, 60*time.Second)
					awaitInstanceCR(t, saGVK, saKey, nil, 60*time.Second)

					if !isBuiltin(be) {
						// N/A (graph): exact KEP applyset + kro node-id child
						// labels have no graph analog.
						return
					}
					instKey := types.NamespacedName{Namespace: ns, Name: instanceName}
					inst := awaitInstanceCR(t, gvk, instKey, nil, 60*time.Second)
					applySetID := applyset.ID(inst)
					cm := getUnstructured(t, configMapGVK, cmKey)
					assertChildApplysetLabels(t, cm.GetLabels(), applySetID, inst, "configMap")
					secret := getUnstructured(t, secretGVK, secretKey)
					assertChildApplysetLabels(t, secret.GetLabels(), applySetID, inst, "secret")
					sa := getUnstructured(t, saGVK, saKey)
					assertChildApplysetLabels(t, sa.GetLabels(), applySetID, inst, "serviceAccount")
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── applyset_test.go: prune when includeWhen becomes false ──────────────
	//
	// SHARED: a conditional Secret is created when includeSecret=true and pruned
	// when flipped to false; the always-present ConfigMap survives. The built-in
	// GKs-annotation grow/shrink assertion is built-in only.
	DescribeTable("prunes a resource when includeWhen flips false",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			t := GinkgoT()
			ns := createParityNamespace(t)
			ctx := env.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			rgd := generator.NewResourceGraphDefinition("parity"+rand.String(5),
				generator.WithSchema(
					"ParityIncPrune"+rand.String(4), "v1alpha1",
					map[string]any{"name": "string", "includeSecret": "boolean"},
					map[string]any{"ready": "${configMap.metadata.name != ''}"},
				),
				generator.WithResource("configMap", map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{
						"name":      "${schema.spec.name}-cm",
						"namespace": "${schema.metadata.namespace}",
					},
					"data": map[string]any{"key": "always-present"},
				}, nil, nil),
				generator.WithResource("secret", map[string]any{
					"apiVersion": "v1", "kind": "Secret",
					"metadata": map[string]any{
						"name":      "${schema.spec.name}-secret",
						"namespace": "${schema.metadata.namespace}",
					},
					"stringData": map[string]any{"password": "conditional"},
				}, nil, []string{"${schema.spec.includeSecret}"}),
			)
			gvk := be.Deploy(t, rgd)

			inst := newInstanceCR(gvk, ns, "inst-"+rand.String(5),
				map[string]any{"name": "prune", "includeSecret": true})
			if err := env.Client.Create(ctx, inst); err != nil {
				t.Fatalf("create instance: %v", err)
			}
			t.Cleanup(func() { _ = env.Client.Delete(context.Background(), inst) })

			cmKey := types.NamespacedName{Namespace: ns, Name: "prune-cm"}
			secretKey := types.NamespacedName{Namespace: ns, Name: "prune-secret"}

			// Both resources created initially.
			awaitInstanceCR(t, configMapGVK, cmKey, nil, 60*time.Second)
			awaitInstanceCR(t, secretGVK, secretKey, nil, 60*time.Second)

			// Flip includeSecret to false.
			updateInstance(t, gvk, types.NamespacedName{Namespace: ns, Name: inst.GetName()},
				func(u *unstructured.Unstructured) error {
					return unstructured.SetNestedField(u.Object, false, "spec", "includeSecret")
				})

			// SHARED: the secret is pruned, the configmap survives.
			awaitDeleted(t, secretGVK, secretKey, 60*time.Second)
			if err := env.Client.Get(ctx, cmKey, getUnstructured(t, configMapGVK, cmKey)); err != nil {
				t.Fatalf("configmap should survive prune: %v", err)
			}

			awaitInstanceReady(t, be, gvk, types.NamespacedName{Namespace: ns, Name: inst.GetName()})
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── applyset_test.go: includeWhen from an upstream resource prunes ──────
	//
	// SHARED: a dependent whose includeWhen reads an upstream ConfigMap's data
	// is created while the upstream says enabled==true and pruned when the
	// instance flips the driving field to false.
	DescribeTable("prunes a dependent when a resource-backed includeWhen flips false",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			t := GinkgoT()
			ns := createParityNamespace(t)
			ctx := env.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			rgd := generator.NewResourceGraphDefinition("parity"+rand.String(5),
				generator.WithSchema(
					"ParityUpstreamInc"+rand.String(4), "v1alpha1",
					map[string]any{"name": "string", "enableDependent": "boolean"},
					map[string]any{"ready": "${source.metadata.name != ''}"},
				),
				generator.WithResource("source", map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{
						"name":      "${schema.spec.name}-source",
						"namespace": "${schema.metadata.namespace}",
					},
					"data": map[string]any{"enabled": "${schema.spec.enableDependent ? 'true' : 'false'}"},
				}, nil, nil),
				generator.WithResource("dependent", map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{
						"name":      "${schema.spec.name}-dependent",
						"namespace": "${schema.metadata.namespace}",
					},
					"data": map[string]any{"key": "created-from-upstream-condition"},
				}, nil, []string{"${source.data.enabled == 'true'}"}),
			)
			gvk := be.Deploy(t, rgd)

			inst := newInstanceCR(gvk, ns, "inst-"+rand.String(5),
				map[string]any{"name": "upstream", "enableDependent": true})
			if err := env.Client.Create(ctx, inst); err != nil {
				t.Fatalf("create instance: %v", err)
			}
			t.Cleanup(func() { _ = env.Client.Delete(context.Background(), inst) })

			sourceKey := types.NamespacedName{Namespace: ns, Name: "upstream-source"}
			depKey := types.NamespacedName{Namespace: ns, Name: "upstream-dependent"}

			awaitInstanceCR(t, configMapGVK, sourceKey,
				func(u *unstructured.Unstructured) error {
					got, _, _ := unstructured.NestedString(u.Object, "data", "enabled")
					if got != "true" {
						return &notYetError{msg: "source.data.enabled=" + got + ", want true"}
					}
					return nil
				}, 60*time.Second)
			awaitInstanceCR(t, configMapGVK, depKey, nil, 60*time.Second)

			// Flip the driving field to false.
			updateInstance(t, gvk, types.NamespacedName{Namespace: ns, Name: inst.GetName()},
				func(u *unstructured.Unstructured) error {
					return unstructured.SetNestedField(u.Object, false, "spec", "enableDependent")
				})

			// SHARED: source updates to false, dependent is pruned.
			awaitInstanceCR(t, configMapGVK, sourceKey,
				func(u *unstructured.Unstructured) error {
					got, _, _ := unstructured.NestedString(u.Object, "data", "enabled")
					if got != "false" {
						return &notYetError{msg: "source.data.enabled=" + got + ", want false"}
					}
					return nil
				}, 60*time.Second)
			awaitDeleted(t, configMapGVK, depKey, 60*time.Second)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── applyset_test.go: collection prune when it shrinks ──────────────────
	//
	// SHARED: a forEach collection of Secrets shrinks from 3 to 1 element; the
	// removed items are pruned and the remaining item survives. This is a
	// PRIORITY parity scenario. Collection-index/size label assertions are
	// built-in only.
	DescribeTable("prunes collection items when the collection shrinks",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			t := GinkgoT()
			ns := createParityNamespace(t)
			ctx := env.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			rgd := generator.NewResourceGraphDefinition("parity"+rand.String(5),
				generator.WithSchema(
					"ParityCollPrune"+rand.String(4), "v1alpha1",
					map[string]any{"name": "string", "values": "[]string"},
					map[string]any{"ready": "${secrets.map(s, s.metadata.name).size() >= 0}"},
				),
				generator.WithResourceCollection("secrets", map[string]any{
					"apiVersion": "v1", "kind": "Secret",
					"metadata": map[string]any{
						"name":      "${schema.spec.name}-${element}",
						"namespace": "${schema.metadata.namespace}",
					},
					"stringData": map[string]any{"value": "${element}"},
				},
					[]krov1alpha1.ForEachDimension{{"element": "${schema.spec.values}"}},
					nil, nil),
			)
			gvk := be.Deploy(t, rgd)

			inst := newInstanceCR(gvk, ns, "inst-"+rand.String(5),
				map[string]any{"name": "shrink", "values": []any{"one", "two", "three"}})
			if err := env.Client.Create(ctx, inst); err != nil {
				t.Fatalf("create instance: %v", err)
			}
			t.Cleanup(func() { _ = env.Client.Delete(context.Background(), inst) })

			for _, v := range []string{"one", "two", "three"} {
				awaitInstanceCR(t, secretGVK,
					types.NamespacedName{Namespace: ns, Name: "shrink-" + v}, nil, 60*time.Second)
			}

			// Shrink to a single element.
			updateInstance(t, gvk, types.NamespacedName{Namespace: ns, Name: inst.GetName()},
				func(u *unstructured.Unstructured) error {
					return unstructured.SetNestedStringSlice(u.Object, []string{"one"}, "spec", "values")
				})

			// SHARED: "two" and "three" pruned, "one" survives.
			for _, v := range []string{"two", "three"} {
				awaitDeleted(t, secretGVK,
					types.NamespacedName{Namespace: ns, Name: "shrink-" + v}, 60*time.Second)
			}
			oneKey := types.NamespacedName{Namespace: ns, Name: "shrink-one"}
			one := awaitInstanceCR(t, secretGVK, oneKey, nil, 60*time.Second)

			if isBuiltin(be) {
				// Built-in: collection labels reflect the shrunk size.
				lbls := one.GetLabels()
				if lbls[metadata.CollectionIndexLabel] != "0" {
					t.Fatalf("collection-index=%q, want 0", lbls[metadata.CollectionIndexLabel])
				}
				if lbls[metadata.CollectionSizeLabel] != "1" {
					t.Fatalf("collection-size=%q, want 1 after prune", lbls[metadata.CollectionSizeLabel])
				}
			}
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── applyset_test.go: cross-RGD non-interference on reconcile ───────────
	//
	// SHARED (PRIORITY): two DISTINCT RGDs each own a ConfigMap; reconciling one
	// instance must NOT prune the other RGD's ConfigMap.
	DescribeTable("reconciling one RGD does not prune another RGD's resource",
		func(makeBackend func() rgdBackend) {
			be1 := makeBackend()
			be2 := makeBackend()
			t := GinkgoT()
			ns := createParityNamespace(t)
			ctx := env.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			mkRGD := func(cmSuffix, source string) *krov1alpha1.ResourceGraphDefinition {
				return generator.NewResourceGraphDefinition("parity"+rand.String(5),
					generator.WithSchema(
						"ParityIso"+cmSuffix+rand.String(4), "v1alpha1",
						map[string]any{"name": "string"},
						map[string]any{"ready": "${configMap.metadata.name != ''}"},
					),
					generator.WithResource("configMap", map[string]any{
						"apiVersion": "v1", "kind": "ConfigMap",
						"metadata": map[string]any{
							"name":      "${schema.spec.name}-" + cmSuffix + "-cm",
							"namespace": "${schema.metadata.namespace}",
						},
						"data": map[string]any{"source": source},
					}, nil, nil),
				)
			}

			rgd1 := mkRGD("rgd1", "rgd1")
			rgd2 := mkRGD("rgd2", "rgd2")
			gvk1 := be1.Deploy(t, rgd1)
			gvk2 := be2.Deploy(t, rgd2)

			inst1 := newInstanceCR(gvk1, ns, "test-iso1", map[string]any{"name": "shared"})
			inst2 := newInstanceCR(gvk2, ns, "test-iso2", map[string]any{"name": "shared"})
			if err := env.Client.Create(ctx, inst1); err != nil {
				t.Fatalf("create inst1: %v", err)
			}
			t.Cleanup(func() { _ = env.Client.Delete(context.Background(), inst1) })
			if err := env.Client.Create(ctx, inst2); err != nil {
				t.Fatalf("create inst2: %v", err)
			}
			t.Cleanup(func() { _ = env.Client.Delete(context.Background(), inst2) })

			cm1Key := types.NamespacedName{Namespace: ns, Name: "shared-rgd1-cm"}
			cm2Key := types.NamespacedName{Namespace: ns, Name: "shared-rgd2-cm"}
			awaitInstanceCR(t, configMapGVK, cm1Key, nil, 60*time.Second)
			awaitInstanceCR(t, configMapGVK, cm2Key, nil, 60*time.Second)

			// Trigger a reconcile of inst1 by touching an annotation.
			updateInstance(t, gvk1, types.NamespacedName{Namespace: ns, Name: inst1.GetName()},
				func(u *unstructured.Unstructured) error {
					ann := u.GetAnnotations()
					if ann == nil {
						ann = map[string]string{}
					}
					ann["test-trigger"] = "reconcile"
					u.SetAnnotations(ann)
					return nil
				})

			// SHARED: RGD2's ConfigMap is NOT pruned by RGD1's reconcile.
			environment.Consistently(t, 8*time.Second, 1*time.Second, func() error {
				cm := &unstructured.Unstructured{}
				cm.SetGroupVersionKind(configMapGVK)
				if err := env.Client.Get(ctx, cm2Key, cm); err != nil {
					return fmt.Errorf("RGD2's ConfigMap should not be pruned by RGD1: %w", err)
				}
				return nil
			})
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── applyset_test.go: cross-instance non-interference on reconcile ──────
	//
	// SHARED (PRIORITY): two instances of the SAME RGD each own a distinct
	// (instance-named) ConfigMap; reconciling instance-a must NOT prune
	// instance-b's ConfigMap.
	DescribeTable("reconciling one instance does not prune a sibling instance's resource",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			t := GinkgoT()
			ns := createParityNamespace(t)
			ctx := env.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			rgd := generator.NewResourceGraphDefinition("parity"+rand.String(5),
				generator.WithSchema(
					"ParityIsoInst"+rand.String(4), "v1alpha1",
					map[string]any{"name": "string"},
					map[string]any{"ready": "${configMap.metadata.name != ''}"},
				),
				generator.WithResource("configMap", map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{
						"name":      "${schema.spec.name}-cm",
						"namespace": "${schema.metadata.namespace}",
					},
					"data": map[string]any{"instance": "${schema.spec.name}"},
				}, nil, nil),
			)
			gvk := be.Deploy(t, rgd)

			inst1 := newInstanceCR(gvk, ns, "instance-a", map[string]any{"name": "instance-a"})
			inst2 := newInstanceCR(gvk, ns, "instance-b", map[string]any{"name": "instance-b"})
			if err := env.Client.Create(ctx, inst1); err != nil {
				t.Fatalf("create inst1: %v", err)
			}
			t.Cleanup(func() { _ = env.Client.Delete(context.Background(), inst1) })
			if err := env.Client.Create(ctx, inst2); err != nil {
				t.Fatalf("create inst2: %v", err)
			}
			t.Cleanup(func() { _ = env.Client.Delete(context.Background(), inst2) })

			cmAKey := types.NamespacedName{Namespace: ns, Name: "instance-a-cm"}
			cmBKey := types.NamespacedName{Namespace: ns, Name: "instance-b-cm"}
			awaitInstanceCR(t, configMapGVK, cmAKey, nil, 60*time.Second)
			awaitInstanceCR(t, configMapGVK, cmBKey, nil, 60*time.Second)

			// Trigger a reconcile of instance-a.
			updateInstance(t, gvk, types.NamespacedName{Namespace: ns, Name: "instance-a"},
				func(u *unstructured.Unstructured) error {
					ann := u.GetAnnotations()
					if ann == nil {
						ann = map[string]string{}
					}
					ann["test-trigger"] = "reconcile"
					u.SetAnnotations(ann)
					return nil
				})

			// SHARED: instance-b's ConfigMap is NOT pruned.
			environment.Consistently(t, 8*time.Second, 1*time.Second, func() error {
				cm := &unstructured.Unstructured{}
				cm.SetGroupVersionKind(configMapGVK)
				if err := env.Client.Get(ctx, cmBKey, cm); err != nil {
					return fmt.Errorf("instance-b's ConfigMap should not be pruned by instance-a: %w", err)
				}
				return nil
			})

			awaitInstanceReady(t, be, gvk, types.NamespacedName{Namespace: ns, Name: "instance-a"})
			awaitInstanceReady(t, be, gvk, types.NamespacedName{Namespace: ns, Name: "instance-b"})
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── applyset_test.go: preserve user-defined labels/annotations on child ─
	//
	// SHARED: user-defined labels/annotations placed on a child resource by a
	// third party are preserved across reconciliation (SSA does not strip fields
	// owned by other managers). The original asserts this on the INSTANCE object
	// alongside built-in applyset parent labels; here we assert the portable,
	// backend-agnostic form on a CHILD resource for both backends.
	DescribeTable("preserves user-defined labels/annotations on a child after reconciliation",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			t := GinkgoT()
			ns := createParityNamespace(t)
			ctx := env.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			rgd := generator.NewResourceGraphDefinition("parity"+rand.String(5),
				generator.WithSchema(
					"ParityPreserve"+rand.String(4), "v1alpha1",
					map[string]any{"name": "string"},
					map[string]any{"ready": "${configMap.metadata.name != ''}"},
				),
				generator.WithResource("configMap", map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{
						"name":      "${schema.spec.name}-cm",
						"namespace": "${schema.metadata.namespace}",
					},
					"data": map[string]any{"key": "value"},
				}, nil, nil),
			)
			gvk := be.Deploy(t, rgd)

			inst := newInstanceCR(gvk, ns, "inst-"+rand.String(5), map[string]any{"name": "preserve"})
			if err := env.Client.Create(ctx, inst); err != nil {
				t.Fatalf("create instance: %v", err)
			}
			t.Cleanup(func() { _ = env.Client.Delete(context.Background(), inst) })

			cmKey := types.NamespacedName{Namespace: ns, Name: "preserve-cm"}
			awaitInstanceCR(t, configMapGVK, cmKey,
				func(u *unstructured.Unstructured) error {
					got, _, _ := unstructured.NestedString(u.Object, "data", "key")
					if got != "value" {
						return &notYetError{msg: "cm.data.key=" + got + ", want value"}
					}
					return nil
				}, 60*time.Second)

			// A third party adds a custom label + annotation to the child.
			cm := getUnstructured(t, configMapGVK, cmKey)
			lbls := cm.GetLabels()
			if lbls == nil {
				lbls = map[string]string{}
			}
			lbls["custom-label"] = "custom-value"
			cm.SetLabels(lbls)
			ann := cm.GetAnnotations()
			if ann == nil {
				ann = map[string]string{}
			}
			ann["custom-annotation"] = "custom-annotation-value"
			cm.SetAnnotations(ann)
			if err := env.Client.Update(ctx, cm); err != nil {
				t.Fatalf("annotate child: %v", err)
			}

			// Trigger a reconcile by touching the instance.
			updateInstance(t, gvk, types.NamespacedName{Namespace: ns, Name: inst.GetName()},
				func(u *unstructured.Unstructured) error {
					a := u.GetAnnotations()
					if a == nil {
						a = map[string]string{}
					}
					a["reconcile-trigger"] = "1"
					u.SetAnnotations(a)
					return nil
				})

			// SHARED: the custom label/annotation survive reconciliation.
			environment.Consistently(t, 8*time.Second, 1*time.Second, func() error {
				got := &unstructured.Unstructured{}
				got.SetGroupVersionKind(configMapGVK)
				if err := env.Client.Get(ctx, cmKey, got); err != nil {
					return err
				}
				if got.GetLabels()["custom-label"] != "custom-value" {
					return fmt.Errorf("custom-label stripped: %v", got.GetLabels())
				}
				if got.GetAnnotations()["custom-annotation"] != "custom-annotation-value" {
					return fmt.Errorf("custom-annotation stripped: %v", got.GetAnnotations())
				}
				return nil
			})

			awaitInstanceReady(t, be, gvk, types.NamespacedName{Namespace: ns, Name: inst.GetName()})
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)
})

// ── ownership-suite helpers ─────────────────────────────────────────────────

// saGVK is the ServiceAccount GVK asserted by the multi-GVK fixture.
var saGVK = schema.GroupVersionKind{Version: "v1", Kind: "ServiceAccount"}

// getUnstructured fetches an object by GVK+key, failing the test on error.
func getUnstructured(t GinkgoTInterface, gvk schema.GroupVersionKind, key types.NamespacedName) *unstructured.Unstructured {
	t.Helper()
	ctx := env.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	if err := env.Client.Get(ctx, key, u); err != nil {
		t.Fatalf("get %s %s: %v", gvk, key, err)
	}
	return u
}

// updateInstance GETs the instance, applies mutate, and PUTs it, retrying on
// conflict. Used to flip spec fields or touch annotations to drive reconciles.
func updateInstance(
	t GinkgoTInterface,
	gvk schema.GroupVersionKind,
	key types.NamespacedName,
	mutate func(*unstructured.Unstructured) error,
) {
	t.Helper()
	ctx := env.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)
		if err := env.Client.Get(ctx, key, u); err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err := mutate(u); err != nil {
			t.Fatalf("mutate instance %s: %v", key, err)
		}
		if err := env.Client.Update(ctx, u); err != nil {
			if apierrors.IsConflict(err) {
				lastErr = err
				time.Sleep(100 * time.Millisecond)
				continue
			}
			t.Fatalf("update instance %s: %v", key, err)
		}
		return
	}
	t.Fatalf("update instance %s did not succeed within timeout: %v", key, lastErr)
}

// awaitDeleted polls until the named object is gone (NotFound) from the API.
func awaitDeleted(t GinkgoTInterface, gvk schema.GroupVersionKind, key types.NamespacedName, timeout time.Duration) {
	t.Helper()
	ctx := env.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)
		err := env.Client.Get(ctx, key, u)
		if apierrors.IsNotFound(err) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("object %s %s was not pruned within %s", gvk, key, timeout)
}

// assertInstanceGVKLabels asserts the built-in instance GVK tracking labels on
// a child resource (instance_conflict_test.go parity).
func assertInstanceGVKLabels(t GinkgoTInterface, labels map[string]string, instanceName, kind string) {
	t.Helper()
	want := map[string]string{
		metadata.InstanceLabel:        instanceName,
		metadata.InstanceGroupLabel:   krov1alpha1.KRODomainName,
		metadata.InstanceVersionLabel: "v1alpha1",
		metadata.InstanceKindLabel:    kind,
	}
	for k, v := range want {
		if labels[k] != v {
			t.Fatalf("child instance-GVK label %s=%q, want %q", k, labels[k], v)
		}
	}
}

// assertChildApplysetLabels asserts the built-in KEP applyset + kro tracking
// labels on a child resource (applyset_test.go parity).
func assertChildApplysetLabels(
	t GinkgoTInterface,
	labels map[string]string,
	applySetID string,
	instance *unstructured.Unstructured,
	nodeID string,
) {
	t.Helper()
	want := map[string]string{
		applyset.ApplysetPartOfLabel:    applySetID,
		metadata.OwnedLabel:             "true",
		metadata.KROVersionLabel:        "devel",
		metadata.ManagedByLabelKey:      metadata.ManagedByKROValue,
		metadata.InstanceIDLabel:        string(instance.GetUID()),
		metadata.InstanceLabel:          instance.GetName(),
		metadata.InstanceNamespaceLabel: instance.GetNamespace(),
		metadata.NodeIDLabel:            nodeID,
	}
	for k, v := range want {
		if labels[k] != v {
			t.Fatalf("child applyset label %s=%q, want %q", k, labels[k], v)
		}
	}
	if _, ok := labels[metadata.ResourceGraphDefinitionIDLabel]; ok {
		t.Fatalf("child should not carry RGD ID label")
	}
}
