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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/client"

	internalv1alpha1 "github.com/kubernetes-sigs/kro/api/internal.kro.run/v1alpha1"
	krov1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/apis"
	"github.com/kubernetes-sigs/kro/pkg/controller/resourcegraphdefinition"
	graphhash "github.com/kubernetes-sigs/kro/pkg/graph/hash"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
)

// mustComputeSpecHash computes the spec hash from a snapshot spec.
// Panics on error since test fixtures should always produce valid hashes.
func mustComputeSpecHash(spec krov1alpha1.ResourceGraphDefinitionSpec) string {
	h, err := graphhash.Spec(spec)
	if err != nil {
		panic(fmt.Sprintf("failed to compute spec hash: %v", err))
	}
	return h
}

var _ = Describe("GraphRevision Lifecycle", func() {

})

var _ = Describe("GraphRevision Hash Deduplication", func() {

})

var _ = Describe("GraphRevision Bumping", func() {

})

var _ = Describe("GraphRevision Invalid Updates", func() {

	It("should keep serving the last good revision when a later RGD update is invalid", func(ctx SpecContext) {
		namespace := fmt.Sprintf("test-%s", rand.String(5))
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
		Expect(env.Client.Create(ctx, ns)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, ns)).To(Succeed())
		})

		rgdName := fmt.Sprintf("gv-invalid-%s", rand.String(5))
		kind := fmt.Sprintf("GvInvalid%s", rand.String(5))
		rgd := configmapRGD(rgdName, kind)

		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})

		waitForRGDActive(ctx, rgdName)

		instanceName := fmt.Sprintf("inst-%s", rand.String(5))
		instance := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
				"kind":       kind,
				"metadata": map[string]interface{}{
					"name":      instanceName,
					"namespace": namespace,
				},
				"spec": map[string]interface{}{
					"data": "hello",
				},
			},
		}
		Expect(env.Client.Create(ctx, instance)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, instance)).To(Succeed())
		})

		configMapName := fmt.Sprintf("cm-%s", instanceName)
		configMap := &corev1.ConfigMap{}
		Eventually(func(g Gomega) {
			err := env.Client.Get(ctx, types.NamespacedName{Name: configMapName, Namespace: namespace}, configMap)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(configMap.Data).To(HaveKeyWithValue("key", "hello"))
		}, 20*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		invalidRGD := invalidConfigmapRGD(rgdName, kind)
		Eventually(func(g Gomega) {
			fresh := &krov1alpha1.ResourceGraphDefinition{}
			err := env.Client.Get(ctx, types.NamespacedName{Name: rgdName}, fresh)
			g.Expect(err).ToNot(HaveOccurred())
			fresh.Spec = *invalidRGD.Spec.DeepCopy()
			err = env.Client.Update(ctx, fresh)
			g.Expect(err).ToNot(HaveOccurred())
		}, 10*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		Eventually(func(g Gomega) {
			fresh := &krov1alpha1.ResourceGraphDefinition{}
			err := env.Client.Get(ctx, types.NamespacedName{Name: rgdName}, fresh)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(fresh.Status.LastIssuedRevision).To(Equal(int64(1)))
			g.Expect(fresh.Status.State).To(Equal(krov1alpha1.ResourceGraphDefinitionStateInactive))

			accepted := findRGDCondition(
				fresh.Status.Conditions,
				krov1alpha1.ConditionType(resourcegraphdefinition.GraphAccepted),
			)
			g.Expect(accepted).ToNot(BeNil())
			g.Expect(accepted.Status).To(Equal(metav1.ConditionFalse))
			g.Expect(accepted.ObservedGeneration).To(Equal(fresh.GetGeneration()))

			graphRevisionsResolved := findRGDCondition(
				fresh.Status.Conditions,
				krov1alpha1.ConditionType(resourcegraphdefinition.GraphRevisionsResolved),
			)
			g.Expect(graphRevisionsResolved).ToNot(BeNil())
			g.Expect(graphRevisionsResolved.Status).To(Equal(metav1.ConditionTrue))
			g.Expect(graphRevisionsResolved.ObservedGeneration).To(Equal(fresh.GetGeneration()))

			kindReady := findRGDCondition(
				fresh.Status.Conditions,
				krov1alpha1.ConditionType(resourcegraphdefinition.KindReady),
			)
			g.Expect(kindReady).ToNot(BeNil())
			g.Expect(kindReady.Status).To(Equal(metav1.ConditionTrue))

			controllerReady := findRGDCondition(
				fresh.Status.Conditions,
				krov1alpha1.ConditionType(resourcegraphdefinition.ControllerReady),
			)
			g.Expect(controllerReady).ToNot(BeNil())
			g.Expect(controllerReady.Status).To(Equal(metav1.ConditionTrue))

			ready := findRGDCondition(fresh.Status.Conditions, krov1alpha1.ConditionType(apis.ConditionReady))
			g.Expect(ready).ToNot(BeNil())
			g.Expect(ready.Status).To(Equal(metav1.ConditionFalse))
			g.Expect(ready.ObservedGeneration).To(Equal(fresh.GetGeneration()))
		}, 20*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		Consistently(func(g Gomega) {
			g.Expect(listGraphRevisions(ctx, rgdName)).To(HaveLen(1))
		}, 5*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		Eventually(func(g Gomega) {
			current := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
					"kind":       kind,
				},
			}
			err := env.Client.Get(ctx, types.NamespacedName{Name: instanceName, Namespace: namespace}, current)
			g.Expect(err).ToNot(HaveOccurred())
			current.Object["spec"] = map[string]interface{}{"data": "world"}
			err = env.Client.Update(ctx, current)
			g.Expect(err).ToNot(HaveOccurred())
		}, 10*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		Eventually(func(g Gomega) {
			err := env.Client.Get(ctx, types.NamespacedName{Name: configMapName, Namespace: namespace}, configMap)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(configMap.Data).To(HaveKeyWithValue("key", "world"))
		}, 20*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
	})
})

var _ = Describe("GraphRevision Instance Resolution", func() {

	It("should resolve the compiled graph from the registry and reconcile instances", func(ctx SpecContext) {
		namespace := fmt.Sprintf("test-%s", rand.String(5))
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
		Expect(env.Client.Create(ctx, ns)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, ns)).To(Succeed())
		})

		rgdName := fmt.Sprintf("gv-resolve-%s", rand.String(5))
		kind := fmt.Sprintf("GvResolve%s", rand.String(5))
		rgd := simpleDeploymentRGD(rgdName, kind)

		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})

		waitForRGDActive(ctx, rgdName)

		// Create an instance
		instanceName := fmt.Sprintf("inst-%s", rand.String(5))
		instance := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
				"kind":       kind,
				"metadata": map[string]interface{}{
					"name":      instanceName,
					"namespace": namespace,
				},
				"spec": map[string]interface{}{
					"image":    "nginx:1.25",
					"replicas": 2,
				},
			},
		}
		Expect(env.Client.Create(ctx, instance)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, instance)).To(Succeed())
		})

		// Verify the deployment was created — proving the instance controller
		// successfully resolved the compiled graph from the registry
		deployment := &appsv1.Deployment{}
		Eventually(func(g Gomega) {
			err := env.Client.Get(ctx, types.NamespacedName{
				Name:      fmt.Sprintf("deploy-%s", instanceName),
				Namespace: namespace,
			}, deployment)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(*deployment.Spec.Replicas).To(Equal(int32(2)))
			g.Expect(deployment.Spec.Template.Spec.Containers[0].Image).To(Equal("nginx:1.25"))
		}, 20*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
	})

	It("should serve updated graph to instances after RGD spec change", func(ctx SpecContext) {
		namespace := fmt.Sprintf("test-%s", rand.String(5))
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
		Expect(env.Client.Create(ctx, ns)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, ns)).To(Succeed())
		})

		rgdName := fmt.Sprintf("gv-update-%s", rand.String(5))
		kind := fmt.Sprintf("GvUpdate%s", rand.String(5))

		// Start with a deployment-only RGD
		rgd := simpleDeploymentRGD(rgdName, kind)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})

		waitForRGDActive(ctx, rgdName)

		// Create instance
		instanceName := fmt.Sprintf("inst-%s", rand.String(5))
		instance := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
				"kind":       kind,
				"metadata": map[string]interface{}{
					"name":      instanceName,
					"namespace": namespace,
				},
				"spec": map[string]interface{}{
					"image":    "nginx:1.25",
					"replicas": 1,
				},
			},
		}
		Expect(env.Client.Create(ctx, instance)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, instance)).To(Succeed())
		})

		// Verify initial deployment is created
		deployment := &appsv1.Deployment{}
		Eventually(func(g Gomega) {
			err := env.Client.Get(ctx, types.NamespacedName{
				Name:      fmt.Sprintf("deploy-%s", instanceName),
				Namespace: namespace,
			}, deployment)
			g.Expect(err).ToNot(HaveOccurred())
		}, 20*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		// Update the RGD spec: change the default image
		Eventually(func(g Gomega) {
			fresh := &krov1alpha1.ResourceGraphDefinition{}
			err := env.Client.Get(ctx, types.NamespacedName{Name: rgdName}, fresh)
			g.Expect(err).ToNot(HaveOccurred())

			fresh.Spec.Schema.Spec.Raw = []byte(
				`{"replicas":"integer | default=1","image":"string | default=nginx:1.26-updated"}`,
			)
			err = env.Client.Update(ctx, fresh)
			g.Expect(err).ToNot(HaveOccurred())
		}, 10*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		// Wait for revision 2 to be issued.
		Eventually(func(g Gomega) {
			fresh := &krov1alpha1.ResourceGraphDefinition{}
			err := env.Client.Get(ctx, types.NamespacedName{Name: rgdName}, fresh)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(fresh.Status.State).To(Equal(krov1alpha1.ResourceGraphDefinitionStateActive))
			g.Expect(fresh.Status.LastIssuedRevision).To(BeNumerically(">=", int64(2)))
		}, 20*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		// Update the instance to use the new default image
		Eventually(func(g Gomega) {
			err := env.Client.Get(ctx, types.NamespacedName{
				Name:      instanceName,
				Namespace: namespace,
			}, instance)
			g.Expect(err).ToNot(HaveOccurred())

			instance.Object["spec"] = map[string]interface{}{
				"image":    "nginx:1.26-updated",
				"replicas": int64(1),
			}
			err = env.Client.Update(ctx, instance)
			g.Expect(err).ToNot(HaveOccurred())
		}, 10*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		// Verify deployment is updated with new image
		Eventually(func(g Gomega) {
			err := env.Client.Get(ctx, types.NamespacedName{
				Name:      fmt.Sprintf("deploy-%s", instanceName),
				Namespace: namespace,
			}, deployment)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(deployment.Spec.Template.Spec.Containers[0].Image).To(Equal("nginx:1.26-updated"))
		}, 60*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
	})
})

var _ = Describe("GraphRevision Multiple RGDs", func() {

})

var _ = Describe("GraphRevision Spec Immutability", func() {

	It("should reject updates to an existing GraphRevision spec", func(ctx SpecContext) {
		rgdName := fmt.Sprintf("gv-immut-%s", rand.String(5))
		kind := fmt.Sprintf("GvImmut%s", rand.String(5))
		rgd := configmapRGD(rgdName, kind)

		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})

		waitForRGDActive(ctx, rgdName)

		var gv internalv1alpha1.GraphRevision
		Eventually(func(g Gomega) {
			gvs := listGraphRevisions(ctx, rgdName)
			g.Expect(gvs).To(HaveLen(1))
			gv = gvs[0]
		}, 10*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		// Try to mutate the GraphRevision spec
		gv.Spec.Snapshot.Name = "tampered-name"
		err := env.Client.Update(ctx, &gv)
		Expect(err).To(HaveOccurred(), "GraphRevision spec should be immutable")
		Expect(err.Error()).To(ContainSubstring("immutable"),
			"error message should mention immutability")
	})

	//nolint:dupl // Intentionally separate — valid vs invalid injected spec exercise different GR controller paths.

	//nolint:dupl // See above — mirror test with invalid spec.

	It("should serve instances using compiled graph from current revision", func(ctx SpecContext) {
		rgdName := fmt.Sprintf("gr-serve-%s", rand.String(5))
		kind := fmt.Sprintf("GrServe%s", rand.String(5))
		namespace := fmt.Sprintf("test-%s", rand.String(5))

		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
		Expect(env.Client.Create(ctx, ns)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, ns)).To(Succeed())
		})

		rgd := configmapRGD(rgdName, kind)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})

		// Wait for active
		Eventually(func(g Gomega) {
			fresh := &krov1alpha1.ResourceGraphDefinition{}
			err := env.Client.Get(ctx, types.NamespacedName{Name: rgdName}, fresh)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(fresh.Status.State).To(Equal(krov1alpha1.ResourceGraphDefinitionStateActive))
		}, 20*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		// Create an instance
		instanceName := fmt.Sprintf("inst-%s", rand.String(5))
		instance := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
				"kind":       kind,
				"metadata": map[string]interface{}{
					"name":      instanceName,
					"namespace": namespace,
				},
				"spec": map[string]interface{}{
					"data": "v1-value",
				},
			},
		}
		Expect(env.Client.Create(ctx, instance)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, instance)).To(Succeed())
		})

		// Wait for ConfigMap with v1 data
		cmName := fmt.Sprintf("cm-%s", instanceName)
		Eventually(func(g Gomega) {
			cm := &corev1.ConfigMap{}
			err := env.Client.Get(ctx, types.NamespacedName{Name: cmName, Namespace: namespace}, cm)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(cm.Data).To(HaveKeyWithValue("key", "v1-value"))
		}, 20*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		// Verify revision 1 exists
		Eventually(func(g Gomega) {
			grs := listGraphRevisions(ctx, rgdName)
			g.Expect(grs).To(HaveLen(1))
			g.Expect(grs[0].Spec.Revision).To(Equal(int64(1)))
		}, 10*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		// Update instance data — instance controller should use the compiled
		// graph from the current revision to reconcile the change.
		Eventually(func(g Gomega) {
			current := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
					"kind":       kind,
				},
			}
			err := env.Client.Get(ctx, types.NamespacedName{Name: instanceName, Namespace: namespace}, current)
			g.Expect(err).ToNot(HaveOccurred())
			current.Object["spec"] = map[string]interface{}{"data": "v2-value"}
			g.Expect(env.Client.Update(ctx, current)).To(Succeed())
		}, 10*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		Eventually(func(g Gomega) {
			cm := &corev1.ConfigMap{}
			err := env.Client.Get(ctx, types.NamespacedName{Name: cmName, Namespace: namespace}, cm)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(cm.Data).To(HaveKeyWithValue("key", "v2-value"))
		}, 20*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
	})
})

func listGraphRevisions(ctx SpecContext, rgdName string) []internalv1alpha1.GraphRevision {
	list := &internalv1alpha1.GraphRevisionList{}
	sel := labels.SelectorFromSet(map[string]string{
		metadata.ResourceGraphDefinitionNameLabel: rgdName,
	})
	ExpectWithOffset(1, env.Client.List(ctx, list, &client.ListOptions{LabelSelector: sel})).To(Succeed())
	return list.Items
}

func findGRCondition(conditions []krov1alpha1.Condition, t krov1alpha1.ConditionType) *krov1alpha1.Condition {
	for i := range conditions {
		if conditions[i].Type == t {
			return &conditions[i]
		}
	}
	return nil
}

func findRGDCondition(conditions []krov1alpha1.Condition, t krov1alpha1.ConditionType) *krov1alpha1.Condition {
	for i := range conditions {
		if conditions[i].Type == t {
			return &conditions[i]
		}
	}
	return nil
}

func simpleDeploymentRGD(name, kind string) *krov1alpha1.ResourceGraphDefinition {
	return generator.NewResourceGraphDefinition(name,
		generator.WithSchema(
			kind, "v1alpha1",
			map[string]interface{}{
				"replicas": "integer | default=1",
				"image":    "string | default=nginx:latest",
			},
			nil,
		),
		generator.WithResource("deployment", map[string]interface{}{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]interface{}{
				"name": "deploy-${schema.metadata.name}",
			},
			"spec": map[string]interface{}{
				"replicas": "${schema.spec.replicas}",
				"selector": map[string]interface{}{
					"matchLabels": map[string]interface{}{
						"app": "test",
					},
				},
				"template": map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "test",
						},
					},
					"spec": map[string]interface{}{
						"containers": []interface{}{
							map[string]interface{}{
								"name":  "app",
								"image": "${schema.spec.image}",
							},
						},
					},
				},
			},
		}, nil, nil),
	)
}

func configmapRGD(name, kind string) *krov1alpha1.ResourceGraphDefinition {
	return generator.NewResourceGraphDefinition(name,
		generator.WithSchema(
			kind, "v1alpha1",
			map[string]interface{}{
				"data": "string | default=hello",
			},
			nil,
		),
		generator.WithResource("configmap", map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]interface{}{
				"name": "cm-${schema.metadata.name}",
			},
			"data": map[string]interface{}{
				"key": "${schema.spec.data}",
			},
		}, nil, nil),
	)
}

// createRGDWithRevisions creates a configmap RGD, waits for it to become active,
// then mutates the spec n-1 additional times to produce exactly n revisions.
// Returns the RGD name and kind for further operations.
//
//nolint:unparam // Keep the helper generic for tests that vary retained revision counts.
func createRGDWithRevisions(ctx SpecContext, rgdName, kind string, n int) {
	rgd := configmapRGD(rgdName, kind)
	ExpectWithOffset(1, env.Client.Create(ctx, rgd)).To(Succeed())

	// Wait for active + revision 1
	EventuallyWithOffset(1, func(g Gomega) {
		fresh := &krov1alpha1.ResourceGraphDefinition{}
		err := env.Client.Get(ctx, types.NamespacedName{Name: rgdName}, fresh)
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(fresh.Status.State).To(Equal(krov1alpha1.ResourceGraphDefinitionStateActive))
		g.Expect(listGraphRevisions(ctx, rgdName)).To(HaveLen(1))
	}, 20*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

	// Mutate resource template n-1 times to produce revisions 2..n
	for i := 2; i <= n; i++ {
		updateRGDTemplate(ctx, rgdName, fmt.Sprintf("rev-%d", i))
		expectedRevision := int64(i)
		EventuallyWithOffset(1, func(g Gomega) {
			fresh := &krov1alpha1.ResourceGraphDefinition{}
			err := env.Client.Get(ctx, types.NamespacedName{Name: rgdName}, fresh)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(fresh.Status.LastIssuedRevision).To(BeNumerically(">=", expectedRevision))
		}, 20*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
	}

	// Wait for all n revisions to exist
	EventuallyWithOffset(1, func(g Gomega) {
		g.Expect(listGraphRevisions(ctx, rgdName)).To(HaveLen(n))
	}, 20*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
}

// updateRGDTemplate updates the configmap RGD's resource template to trigger a
// new revision. Each call adds a unique label to the ConfigMap template so the
// spec hash changes without touching the schema.
func updateRGDTemplate(ctx SpecContext, rgdName, label string) {
	EventuallyWithOffset(1, func(g Gomega) {
		fresh := &krov1alpha1.ResourceGraphDefinition{}
		err := env.Client.Get(ctx, types.NamespacedName{Name: rgdName}, fresh)
		g.Expect(err).ToNot(HaveOccurred())
		template := `{"apiVersion":"v1","kind":"ConfigMap",` +
			`"metadata":{"name":"cm-${schema.metadata.name}",` +
			`"labels":{"revision":"%s"}},"data":{"key":"${schema.spec.data}"}}`
		fresh.Spec.Resources[0].Template.Raw = []byte(fmt.Sprintf(template, label))
		err = env.Client.Update(ctx, fresh)
		g.Expect(err).ToNot(HaveOccurred())
	}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
}

// emulateGCForRGD manually deletes GraphRevisions that have a deletionTimestamp,
// emulating what the Kubernetes garbage collector would do on cascade delete.
// envtest does not run a GC controller, so ownerReference-based cascade
// deletion doesn't work. Only revisions already marked for deletion are removed.
func emulateGCForRGD(ctx SpecContext, rgdName string) {
	list := &internalv1alpha1.GraphRevisionList{}
	sel := labels.SelectorFromSet(map[string]string{
		metadata.ResourceGraphDefinitionNameLabel: rgdName,
	})
	ExpectWithOffset(1, env.Client.List(ctx, list, &client.ListOptions{LabelSelector: sel})).To(Succeed())

	for i := range list.Items {
		gr := &list.Items[i]
		if !gr.DeletionTimestamp.IsZero() {
			// Remove finalizer so the API server can complete deletion
			gr.Finalizers = nil
			ExpectWithOffset(1, env.Client.Update(ctx, gr)).To(Succeed())
		} else {
			// Not marked for deletion — delete it explicitly (emulate GC marking + deletion)
			ExpectWithOffset(1, env.Client.Delete(ctx, gr)).To(Succeed())
		}
	}

	EventuallyWithOffset(1, func(g Gomega) {
		remaining := &internalv1alpha1.GraphRevisionList{}
		g.Expect(env.Client.List(ctx, remaining, &client.ListOptions{LabelSelector: sel})).To(Succeed())
		g.Expect(remaining.Items).To(BeEmpty())
	}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
}

// emulateOrphanDeleteForRGD emulates DeletePropagationOrphan in envtest by
// stripping ownerReferences from all GraphRevisions first, then deleting the
// RGD. Without ownerReferences, envtest won't cascade-delete the GRs.
// This matches the end state of a real orphan deletion: RGD gone, GRs alive
// with no owner.
func emulateOrphanDeleteForRGD(ctx SpecContext, rgdName string) {
	// Strip ownerReferences from all GRs
	for _, gr := range listGraphRevisions(ctx, rgdName) {
		gr.OwnerReferences = nil
		ExpectWithOffset(1, env.Client.Update(ctx, &gr)).To(Succeed())
	}

	// Now delete the RGD — GRs have no ownerRef so they won't be cascade-deleted
	rgd := &krov1alpha1.ResourceGraphDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rgdName},
	}
	ExpectWithOffset(1, env.Client.Delete(ctx, rgd)).To(Succeed())

	// Wait for RGD to be gone
	EventuallyWithOffset(1, func(g Gomega) {
		err := env.Client.Get(ctx, types.NamespacedName{Name: rgdName}, &krov1alpha1.ResourceGraphDefinition{})
		g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
	}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
}

var _ = Describe("GraphRevision Adoption", func() {

})

func invalidConfigmapRGD(name, kind string) *krov1alpha1.ResourceGraphDefinition {
	return generator.NewResourceGraphDefinition(name,
		generator.WithSchema(
			kind, "v1alpha1",
			map[string]interface{}{
				"data": "string | default=hello",
			},
			nil,
		),
		generator.WithResource("vpc", map[string]interface{}{
			"apiVersion": "ec2.services.k8s.aws/v1alpha1",
			"kind":       "VPC",
			"metadata": map[string]interface{}{
				"name": "${subnet.status.subnetID}",
			},
			"spec": map[string]interface{}{
				"cidrBlocks": []interface{}{"192.168.0.0/16"},
			},
		}, nil, nil),
		generator.WithResource("subnet", map[string]interface{}{
			"apiVersion": "ec2.services.k8s.aws/v1alpha1",
			"kind":       "Subnet",
			"metadata": map[string]interface{}{
				"name": "test-subnet",
			},
			"spec": map[string]interface{}{
				"vpcID":     "${vpc.status.vpcID}",
				"cidrBlock": "192.168.1.0/24",
			},
		}, nil, nil),
	)
}
