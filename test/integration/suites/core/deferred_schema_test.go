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

package core_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"

	krov1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
)

// Deferred schema resolution (kubernetes-sigs/kro#1293).
//
// Before this feature, an RGD referencing a resource whose target CRD is absent
// failed to compile, so the RGD's OWN instance CRD was never served — a
// deadlock (you could never install the missing CRD via that RGD's instance,
// and the RGD stayed stuck InvalidResourceGraph).
//
// With the DeferredSchemaResolution feature gate on, a `template` resource whose
// target CRD is absent AND which carries a non-empty includeWhen is DEFERRED:
// it compiles as a schema-less node resolved lazily at apply time. The RGD then
// becomes Active and serves its instance CRD; the deferred resource is applied
// later, once its CRD exists and its includeWhen is true.
var _ = Describe("Deferred schema resolution", func() {
	var (
		namespace string
	)

	BeforeEach(func(ctx SpecContext) {
		namespace = fmt.Sprintf("test-%s", rand.String(5))
		Expect(env.Client.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: namespace,
			},
		})).To(Succeed())
	})

	AfterEach(func(ctx SpecContext) {
		Expect(env.Client.Delete(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: namespace,
			},
		})).To(Succeed())
	})

	It("should serve the instance CRD when a conditional resource's CRD is absent, "+
		"then apply the deferred resource once its CRD exists and includeWhen is true", func(ctx SpecContext) {
		// The Widget group is made unique per Ginkgo parallel process so that the
		// cluster-scoped Widget CRD (which the group-isolating client leaves at
		// its real group, unlike kro.run) never collides across processes sharing
		// the single envtest control plane.
		widgetGroup := fmt.Sprintf("widgets.p%d.example.com", GinkgoParallelProcess())
		widgetAPIVersion := widgetGroup + "/v1"
		widgetCRDName := "widgets." + widgetGroup

		// RGD with:
		//   - config: a resolvable core ConfigMap, always included.
		//   - widget: a template for a Kind whose CRD does NOT exist at compile
		//     time, gated by includeWhen ${schema.spec.enableWidget}. This is the
		//     resource that gets deferred.
		rgd := generator.NewResourceGraphDefinition("test-deferred-schema",
			generator.WithSchema(
				"DeferredSchemaTest", "v1alpha1",
				map[string]interface{}{
					"name":         "string",
					"enableWidget": "boolean",
				},
				nil,
			),
			generator.WithResource("config", map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name":      "${schema.spec.name}-config",
					"namespace": namespace,
				},
				"data": map[string]interface{}{
					"key": "value",
				},
			}, nil, nil),
			generator.WithResource("widget", map[string]interface{}{
				"apiVersion": widgetAPIVersion,
				"kind":       "Widget",
				"metadata": map[string]interface{}{
					"name":      "${schema.spec.name}-widget",
					"namespace": namespace,
				},
				"spec": map[string]interface{}{
					"size": "${schema.spec.name}",
				},
			}, nil, []string{"${schema.spec.enableWidget}"}),
		)

		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})

		// Core of #1293: despite the widget's CRD being absent, the RGD compiles
		// (widget node deferred) and becomes Active, and its instance CRD is
		// served/Established.
		createdRGD := &krov1alpha1.ResourceGraphDefinition{}
		Eventually(func(g Gomega, ctx SpecContext) {
			err := env.Client.Get(ctx, types.NamespacedName{Name: rgd.Name}, createdRGD)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(createdRGD.Status.State).To(Equal(krov1alpha1.ResourceGraphDefinitionStateActive))
			g.Expect(createdRGD.Status.TopologicalOrder).To(ContainElements("config", "widget"))
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		instanceCRD := &apiextensionsv1.CustomResourceDefinition{}
		Eventually(func(g Gomega, ctx SpecContext) {
			err := env.Client.Get(ctx, types.NamespacedName{
				Name: "deferredschematests.kro.run",
			}, instanceCRD)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(instanceCRD.Spec.Names.Kind).To(Equal("DeferredSchemaTest"))
			var established bool
			for _, cond := range instanceCRD.Status.Conditions {
				if cond.Type == apiextensionsv1.Established &&
					cond.Status == apiextensionsv1.ConditionTrue {
					established = true
				}
			}
			g.Expect(established).To(BeTrue(), "instance CRD should be Established")
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		// Create an instance with enableWidget=false: the widget is skipped by
		// includeWhen (so its absent CRD must NOT block the instance) while the
		// always-included ConfigMap is created.
		name := "deferred-instance"
		instance := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
				"kind":       "DeferredSchemaTest",
				"metadata": map[string]interface{}{
					"name":      name,
					"namespace": namespace,
				},
				"spec": map[string]interface{}{
					"name":         name,
					"enableWidget": false,
				},
			},
		}
		Expect(env.Client.Create(ctx, instance)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			_ = env.Client.Delete(ctx, instance)
		})

		// The ConfigMap is created.
		config := &corev1.ConfigMap{}
		Eventually(func(g Gomega, ctx SpecContext) {
			err := env.Client.Get(ctx, types.NamespacedName{
				Name:      name + "-config",
				Namespace: namespace,
			}, config)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(config.Data).To(HaveKeyWithValue("key", "value"))
		}, 20*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		// The instance progresses to ACTIVE (the skipped deferred widget does not
		// block it).
		Eventually(func(g Gomega, ctx SpecContext) {
			err := env.Client.Get(ctx, types.NamespacedName{
				Name:      name,
				Namespace: namespace,
			}, instance)
			g.Expect(err).ToNot(HaveOccurred())
			state, found, err := unstructured.NestedString(instance.Object, "status", "state")
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(found).To(BeTrue())
			g.Expect(state).To(Equal("ACTIVE"))
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		// And the widget is NOT created while enableWidget is false.
		Consistently(func(g Gomega, ctx SpecContext) {
			widget := &unstructured.Unstructured{}
			widget.SetGroupVersionKind(schema.GroupVersionKind{Group: widgetGroup, Version: "v1", Kind: "Widget"})
			err := env.Client.Get(ctx, types.NamespacedName{
				Name:      name + "-widget",
				Namespace: namespace,
			}, widget)
			// The Widget CRD does not exist yet, so a NoKindMatch/NotFound is
			// expected; either way the widget resource must be absent.
			g.Expect(err).To(HaveOccurred())
		}, 5*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		// Recovery leg: install the Widget CRD and wait for it to be Established.
		widgetCRD := &apiextensionsv1.CustomResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{
				Name: widgetCRDName,
			},
			Spec: apiextensionsv1.CustomResourceDefinitionSpec{
				Group: widgetGroup,
				Names: apiextensionsv1.CustomResourceDefinitionNames{
					Kind:   "Widget",
					Plural: "widgets",
				},
				Scope: apiextensionsv1.NamespaceScoped,
				Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
					{
						Name:    "v1",
						Served:  true,
						Storage: true,
						Schema: &apiextensionsv1.CustomResourceValidation{
							OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"spec": {
										Type:                   "object",
										XPreserveUnknownFields: ptrBool(true),
									},
								},
							},
						},
					},
				},
			},
		}
		Expect(env.Client.Create(ctx, widgetCRD)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, widgetCRD)).To(Succeed())
		})

		Eventually(func(g Gomega, ctx SpecContext) {
			err := env.Client.Get(ctx, types.NamespacedName{Name: widgetCRDName}, widgetCRD)
			g.Expect(err).ToNot(HaveOccurred())
			var established bool
			for _, cond := range widgetCRD.Status.Conditions {
				if cond.Type == apiextensionsv1.Established &&
					cond.Status == apiextensionsv1.ConditionTrue {
					established = true
				}
			}
			g.Expect(established).To(BeTrue(), "Widget CRD should be Established")
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		// Flip enableWidget=true: the deferred node now resolves (its CRD exists)
		// and the includeWhen is true, so the controller applies a Widget CR.
		Eventually(func(g Gomega, ctx SpecContext) {
			err := env.Client.Get(ctx, types.NamespacedName{
				Name:      name,
				Namespace: namespace,
			}, instance)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(unstructured.SetNestedField(instance.Object, true, "spec", "enableWidget")).To(Succeed())
			g.Expect(env.Client.Update(ctx, instance)).To(Succeed())
		}, 10*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		Eventually(func(g Gomega, ctx SpecContext) {
			widget := &unstructured.Unstructured{}
			widget.SetGroupVersionKind(schema.GroupVersionKind{Group: widgetGroup, Version: "v1", Kind: "Widget"})
			err := env.Client.Get(ctx, types.NamespacedName{
				Name:      name + "-widget",
				Namespace: namespace,
			}, widget)
			g.Expect(err).ToNot(HaveOccurred())
			size, found, err := unstructured.NestedString(widget.Object, "spec", "size")
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(found).To(BeTrue())
			g.Expect(size).To(Equal(name))
		}, 60*time.Second, 500*time.Millisecond).WithContext(ctx).Should(Succeed())
	})

	It("should NOT defer (RGD stays inactive) when a resource references an absent "+
		"CRD but has no includeWhen", func(ctx SpecContext) {
		// Negative guard: deferral requires a non-empty includeWhen. An always-
		// required resource whose CRD is absent must still fail the whole graph so
		// typos are surfaced rather than masked.
		gadgetGroup := fmt.Sprintf("gadgets.p%d.example.com", GinkgoParallelProcess())
		rgd := generator.NewResourceGraphDefinition("test-deferred-schema-negative",
			generator.WithSchema(
				"DeferredSchemaNegative", "v1alpha1",
				map[string]interface{}{
					"name": "string",
				},
				nil,
			),
			generator.WithResource("gadget", map[string]interface{}{
				"apiVersion": gadgetGroup + "/v1",
				"kind":       "Gadget",
				"metadata": map[string]interface{}{
					"name":      "${schema.spec.name}-gadget",
					"namespace": namespace,
				},
			}, nil, nil),
		)

		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})

		Eventually(func(g Gomega, ctx SpecContext) {
			err := env.Client.Get(ctx, types.NamespacedName{Name: rgd.Name}, rgd)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(rgd.Status.State).ToNot(Equal(krov1alpha1.ResourceGraphDefinitionStateActive))

			// The failure should surface as a false Ready-style condition
			// referencing the unresolved REST mapping.
			var sawFailure bool
			for _, cond := range rgd.Status.Conditions {
				if cond.Status == metav1.ConditionFalse && cond.Message != nil &&
					*cond.Message != "" {
					sawFailure = true
				}
			}
			g.Expect(sawFailure).To(BeTrue(), "expected a failing condition on the RGD")
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
	})
})

func ptrBool(b bool) *bool { return &b }
