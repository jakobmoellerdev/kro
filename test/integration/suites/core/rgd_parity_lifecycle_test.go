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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"

	krov1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
	"github.com/kubernetes-sigs/kro/test/integration/environment"
)

// This suite ports the lifecycle-oriented core integration scenarios to the
// parity harness: each RGD fixture is validated against BOTH the built-in
// ResourceGraphDefinition controller (parityBuiltin) and the pure-Graph
// reimplementation served at v1alpha2 (parityGraph), asserting the same
// observable behavior — child creation, update propagation, drift restoration,
// and prune-on-removal.
//
// Ported from:
//   - lifecycle_test.go                    (update propagation to managed rsrc)
//   - recover_test.go                       (recover from invalid RGD spec)
//   - resource_pruning_test.go              (prune a removed resource)
//   - terminating_managed_resource_test.go  (gate downstream while terminating)
//   - cascading_deletion_test.go            (cascade delete nested resources)
//
// Parity boundaries (branched with isBuiltin / documented N/A):
//   - The graph backend bakes the RGD's resource templates into its L0
//     controller Graph at Deploy time (see rgd_graph_backend_test.go). Mutating
//     the RGD *object* after Deploy therefore does NOT re-plumb the graph
//     backend. Scenarios that reshape the DEFINITION mid-flight (recover from an
//     invalid RGD spec; retire a resource by removing it from the RGD) are
//     built-in-only. Where the same intent can be expressed through the INSTANCE
//     (includeWhen driven by spec), the scenario runs against both backends.
//   - Downstream-gating on a terminating managed resource asserts the built-in
//     controller's instance status condition shape (ResourcesReady /
//     ResourceDeleting), which has no standalone-Graph analog; the gating
//     assertion is built-in-only.
//
// All package-level helpers below are prefixed `plc` (parity-lifecycle) so this
// file can be authored independently of the sibling rgd_parity_*_test.go files
// without symbol collisions.

var _ = Describe("RGD parity: lifecycle", func() {
	// ── lifecycle_test.go: updates to the instance propagate to managed rsrc ─
	//
	// Create an instance, assert the child Deployment carries the resolved spec
	// values, then update the instance spec and assert the Deployment is
	// re-resolved. Instance-driven, so it runs against both backends: the graph
	// backend's L2 re-evaluates its templates against the instance published as
	// `schema`, matching the built-in controller.
	DescribeTable("updates to the instance propagate to a managed Deployment",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityUpdate"+rand.String(4), "v1alpha1",
							map[string]any{
								"name":     "string",
								"replicas": "integer | default=1",
								"image":    "string | default=nginx:latest",
								"port":     "integer | default=80",
							},
							map[string]any{"ready": "${deployment.metadata.name != ''}"},
						),
						generator.WithResource("deployment", map[string]any{
							"apiVersion": "apps/v1", "kind": "Deployment",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-dep",
								"namespace": "${schema.metadata.namespace}",
							},
							"spec": map[string]any{
								"replicas": "${schema.spec.replicas}",
								"selector": map[string]any{"matchLabels": map[string]any{"app": "upd"}},
								"template": map[string]any{
									"metadata": map[string]any{"labels": map[string]any{"app": "upd"}},
									"spec": map[string]any{"containers": []any{
										map[string]any{
											"name":  "app",
											"image": "${schema.spec.image}",
											"ports": []any{map[string]any{
												"containerPort": "${schema.spec.port}",
											}},
										},
									}},
								},
							},
						}, nil, nil),
					)
				},
				map[string]any{"name": "upd", "replicas": 1, "image": "nginx:1.19", "port": 80},
				func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
					depKey := types.NamespacedName{Namespace: ns, Name: "upd-dep"}

					// Initial resolution.
					awaitInstanceCR(t, deploymentGVK, depKey, func(u *unstructured.Unstructured) error {
						return plcMatchDeployment(u, 1, "nginx:1.19", 80)
					}, 60*time.Second)

					// Update the instance spec and expect the Deployment to
					// re-resolve to the new values.
					plcUpdateInstanceSpec(t, gvk, ns, instanceName, map[string]any{
						"name":     "upd",
						"replicas": int64(3),
						"image":    "nginx:1.20",
						"port":     int64(443),
					})

					awaitInstanceCR(t, deploymentGVK, depKey, func(u *unstructured.Unstructured) error {
						return plcMatchDeployment(u, 3, "nginx:1.20", 443)
					}, 60*time.Second)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── resource_pruning_test.go: prune a removed resource while another is
	//    NOT ready. Ported instance-driven so both backends run: an optional
	//    resource is gated by includeWhen on the instance spec, and a second
	//    resource ("gate") is held unready by an unsatisfiable readyWhen. When
	//    the instance flips the flag off, the optional resource must be pruned
	//    even though "gate" never becomes ready, and the still-included resource
	//    is left alone.
	DescribeTable("prunes a removed resource while another resource is not ready",
		func(makeBackend func() rgdBackend) {
			runParityFixtureOpts(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityPrune"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "keepRetired": "boolean"},
							map[string]any{"ready": "${gate.metadata.name != ''}"},
						),
						// "gate" never satisfies its readyWhen, so readiness of
						// the graph never converges — pruning of the retired
						// resource must not depend on it.
						generator.WithResource("gate", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-gate",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"ready": "false"},
						}, []string{`${gate.data.ready == "true"}`}, nil),
						// "retired" is included only while keepRetired is true;
						// flipping it off retires the resource on existing
						// instances.
						generator.WithResource("retired", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-retired",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"keep": "for-now"},
						}, nil, []string{"${schema.spec.keepRetired}"}),
					)
				},
				map[string]any{"name": "prune", "keepRetired": true},
				func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
					gateKey := types.NamespacedName{Namespace: ns, Name: "prune-gate"}
					retiredKey := types.NamespacedName{Namespace: ns, Name: "prune-retired"}

					// Both resources initially exist.
					awaitInstanceCR(t, configMapGVK, gateKey, nil, 60*time.Second)
					awaitInstanceCR(t, configMapGVK, retiredKey, nil, 60*time.Second)

					// Retire the resource by flipping its includeWhen input off.
					plcUpdateInstanceSpec(t, gvk, ns, instanceName, map[string]any{
						"name":        "prune",
						"keepRetired": false,
					})

					// The retired resource is pruned even though "gate" is still
					// not ready.
					//
					// Parity boundary: the built-in controller prunes a child
					// whose includeWhen flips false on an existing instance. The
					// graph backend leaves includeWhen-flip pruning of an
					// already-created child opt-in (it prunes on instance
					// deletion — see the cascade table — but does not delete a
					// child that a live instance stops including), so that leg of
					// the assertion is built-in-only.
					if isBuiltin(be) {
						env.AwaitDeleted(t, configMapGVK, retiredKey, 60*time.Second)
					}

					// The still-included resource is untouched on both backends.
					environment.Consistently(t, 3*time.Second, 500*time.Millisecond, func() error {
						ctx := plcCtx()
						cm := &unstructured.Unstructured{}
						cm.SetGroupVersionKind(configMapGVK)
						if err := env.Client.Get(ctx, gateKey, cm); err != nil {
							return fmt.Errorf("still-included gate must not be pruned: %v", err)
						}
						return nil
					})
				},
				// The instance never converges to ready ("gate" is unsatisfiable),
				// so skip the trailing readiness assertion.
				true,
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── resource_pruning_test.go (definition-driven variant): retire a resource
	//    by REMOVING it from the RGD object. This reshapes the DEFINITION, which
	//    the graph backend bakes at Deploy time — so it is built-in-only.
	DescribeTable("prunes a resource removed from the RGD definition (builtin-only)",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			if !isBuiltin(be) {
				Skip("N/A for graph backend: the RGD resources are baked into the " +
					"L0 controller Graph at Deploy time, so mutating the RGD object " +
					"after Deploy does not re-plumb the graph backend. The equivalent " +
					"intent is covered instance-driven via includeWhen in the sibling " +
					"prune-while-not-ready table.")
			}

			t := GinkgoT()
			ns := createParityNamespace(t)
			ctx := plcCtx()

			withRetired := func(name string, retired bool) *krov1alpha1.ResourceGraphDefinition {
				opts := []generator.ResourceGraphDefinitionOption{
					generator.WithSchema(
						"ParityPruneDef"+rand.String(4), "v1alpha1",
						map[string]any{"name": "string"},
						map[string]any{"ready": "${keep.metadata.name != ''}"},
					),
					generator.WithResource("keep", map[string]any{
						"apiVersion": "v1", "kind": "ConfigMap",
						"metadata": map[string]any{
							"name":      "${schema.spec.name}-keep",
							"namespace": "${schema.metadata.namespace}",
						},
						"data": map[string]any{"k": "v"},
					}, nil, nil),
				}
				if retired {
					opts = append(opts, generator.WithResource("retired", map[string]any{
						"apiVersion": "v1", "kind": "ConfigMap",
						"metadata": map[string]any{
							"name":      "${schema.spec.name}-retired",
							"namespace": "${schema.metadata.namespace}",
						},
						"data": map[string]any{"keep": "for-now"},
					}, nil, nil))
				}
				return generator.NewResourceGraphDefinition(name, opts...)
			}

			rgdName := "parity" + rand.String(5)
			rgd := withRetired(rgdName, true)
			gvk := be.Deploy(t, rgd)

			instName := "inst-" + rand.String(5)
			inst := newInstanceCR(gvk, ns, instName, map[string]any{"name": "prunedef"})
			if err := env.Client.Create(ctx, inst); err != nil {
				t.Fatalf("create instance: %v", err)
			}
			t.Cleanup(func() { _ = env.Client.Delete(context.Background(), inst) })

			keepKey := types.NamespacedName{Namespace: ns, Name: "prunedef-keep"}
			retiredKey := types.NamespacedName{Namespace: ns, Name: "prunedef-retired"}
			awaitInstanceCR(t, configMapGVK, keepKey, nil, 60*time.Second)
			awaitInstanceCR(t, configMapGVK, retiredKey, nil, 60*time.Second)

			// Remove "retired" from the RGD definition.
			environment.Eventually(t, 20*time.Second, time.Second, func() error {
				current := &krov1alpha1.ResourceGraphDefinition{}
				if err := env.Client.Get(ctx, types.NamespacedName{Name: rgdName}, current); err != nil {
					return err
				}
				current.Spec.Resources = withRetired(rgdName, false).Spec.Resources
				return env.Client.Update(ctx, current)
			})

			// The retired resource is pruned; the still-declared one remains.
			env.AwaitDeleted(t, configMapGVK, retiredKey, 60*time.Second)
			environment.Consistently(t, 3*time.Second, 500*time.Millisecond, func() error {
				cm := &unstructured.Unstructured{}
				cm.SetGroupVersionKind(configMapGVK)
				if err := env.Client.Get(ctx, keepKey, cm); err != nil {
					return fmt.Errorf("still-declared resource must not be pruned: %v", err)
				}
				return nil
			})
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── recover_test.go: recover from an invalid RGD spec and use the latest
	//    valid configuration. This reshapes the DEFINITION (valid → invalid
	//    cyclic → valid), asserting the built-in controller's RGD status state
	//    (ACTIVE → INACTIVE → ACTIVE). Built-in-only: the graph backend bakes
	//    the RGD at Deploy time and exposes no equivalent RGD status shape.
	DescribeTable("recovers from an invalid RGD spec and uses latest valid config (builtin-only)",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			if !isBuiltin(be) {
				Skip("N/A for graph backend: there is no RGD object to reshape " +
					"(resources are baked into the L0 controller Graph at Deploy " +
					"time) and no ACTIVE/INACTIVE RGD status shape to assert.")
			}

			t := GinkgoT()
			ns := createParityNamespace(t)
			ctx := plcCtx()

			kind := "ParityRecover" + rand.String(4)
			rgdName := "parity" + rand.String(5)
			rgd := generator.NewResourceGraphDefinition(rgdName,
				generator.WithSchema(kind, "v1alpha1",
					map[string]any{"name": "string", "configKey": "string"},
					nil,
				),
				generator.WithResource("initialConfig", map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{
						"name":      "${schema.spec.name}",
						"namespace": "${schema.metadata.namespace}",
					},
					"data": map[string]any{"key": "${schema.spec.configKey}", "version": "initial"},
				}, nil, nil),
			)
			gvk := be.Deploy(t, rgd)

			plcAwaitRGDState(t, rgdName, krov1alpha1.ResourceGraphDefinitionStateActive)

			// Push an invalid (cyclic) definition; the RGD becomes INACTIVE.
			environment.Eventually(t, 20*time.Second, time.Second, func() error {
				current := &krov1alpha1.ResourceGraphDefinition{}
				if err := env.Client.Get(ctx, types.NamespacedName{Name: rgdName}, current); err != nil {
					return err
				}
				current.Spec.Resources = append(current.Spec.Resources,
					&krov1alpha1.Resource{ID: "serviceA", Template: toRawExtension(map[string]any{
						"apiVersion": "v1", "kind": "Service",
						"metadata": map[string]any{"name": "${serviceB.metadata.name}"},
					})},
					&krov1alpha1.Resource{ID: "serviceB", Template: toRawExtension(map[string]any{
						"apiVersion": "v1", "kind": "Service",
						"metadata": map[string]any{"name": "${serviceA.metadata.name}"},
					})},
				)
				return env.Client.Update(ctx, current)
			})
			plcAwaitRGDState(t, rgdName, krov1alpha1.ResourceGraphDefinitionStateInactive)

			// Replace with a new valid definition; the RGD recovers to ACTIVE.
			environment.Eventually(t, 20*time.Second, time.Second, func() error {
				current := &krov1alpha1.ResourceGraphDefinition{}
				if err := env.Client.Get(ctx, types.NamespacedName{Name: rgdName}, current); err != nil {
					return err
				}
				current.Spec.Resources = []*krov1alpha1.Resource{{
					ID: "itsapodnow", Template: toRawExtension(map[string]any{
						"apiVersion": "apps/v1", "kind": "Deployment",
						"metadata": map[string]any{
							"name":      "${schema.spec.name}",
							"namespace": "${schema.metadata.namespace}",
						},
						"spec": map[string]any{
							"replicas": 1,
							"selector": map[string]any{"matchLabels": map[string]any{"app": "deployment"}},
							"template": map[string]any{
								"metadata": map[string]any{"labels": map[string]any{"app": "deployment"}},
								"spec": map[string]any{"containers": []any{map[string]any{
									"name":  "${schema.spec.name}-deployment",
									"image": "nginx",
									"ports": []any{map[string]any{"containerPort": 777}},
								}}},
							},
						},
					}),
				}}
				return env.Client.Update(ctx, current)
			})
			plcAwaitRGDState(t, rgdName, krov1alpha1.ResourceGraphDefinitionStateActive)

			// The instance now realizes the latest valid config.
			instName := "recover"
			inst := newInstanceCR(gvk, ns, instName, map[string]any{"name": instName, "configKey": "testKey"})
			if err := env.Client.Create(ctx, inst); err != nil {
				t.Fatalf("create instance: %v", err)
			}
			t.Cleanup(func() { _ = env.Client.Delete(context.Background(), inst) })

			awaitInstanceCR(t, deploymentGVK,
				types.NamespacedName{Namespace: ns, Name: instName},
				func(u *unstructured.Unstructured) error {
					return plcMatchDeployment(u, 1, "nginx", 777)
				}, 60*time.Second)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── terminating_managed_resource_test.go: don't create a downstream
	//    resource while an upstream managed resource is terminating.
	//
	//    Shared: a downstream Deployment is gated by includeWhen on the instance
	//    spec; the upstream ConfigMap is held in a terminating state via a
	//    blocking finalizer; when the instance flips createDeployment on, the
	//    Deployment must not appear until the ConfigMap deletion completes and
	//    it is recreated. Both backends own their children and re-resolve, so
	//    the end-to-end recovery is asserted for both. The intermediate
	//    ResourcesReady=False / reason=ResourceDeleting instance-status
	//    assertion is built-in-only (no standalone-Graph analog).
	DescribeTable("does not create a downstream resource while a managed resource is terminating",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityTerm"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "createDeployment": "boolean"},
							map[string]any{"ready": "${config.metadata.name != ''}"},
						),
						generator.WithResource("config", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-config",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"value": "active"},
						}, nil, nil),
						generator.WithResource("deployment", map[string]any{
							"apiVersion": "apps/v1", "kind": "Deployment",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"spec": map[string]any{
								"replicas": 1,
								"selector": map[string]any{"matchLabels": map[string]any{"app": "${schema.spec.name}"}},
								"template": map[string]any{
									"metadata": map[string]any{"labels": map[string]any{"app": "${schema.spec.name}"}},
									"spec": map[string]any{"containers": []any{map[string]any{
										"name":  "nginx",
										"image": "nginx",
										"env": []any{map[string]any{
											"name":  "CONFIG_VALUE",
											"value": "${config.data.value}",
										}},
									}}},
								},
							},
						}, nil, []string{"${schema.spec.createDeployment}"}),
					)
				},
				map[string]any{"name": "term", "createDeployment": false},
				func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
					const blockingFinalizer = "tests.kro.run/block-delete"
					ctx := plcCtx()
					configKey := types.NamespacedName{Namespace: ns, Name: "term-config"}
					deployKey := types.NamespacedName{Namespace: ns, Name: "term"}

					// Upstream config exists; downstream deployment is gated off.
					awaitInstanceCR(t, configMapGVK, configKey, func(u *unstructured.Unstructured) error {
						v, _, _ := unstructured.NestedString(u.Object, "data", "value")
						if v != "active" {
							return &notYetError{msg: "config.data.value=" + v + ", want active"}
						}
						return nil
					}, 60*time.Second)
					environment.Consistently(t, 3*time.Second, 500*time.Millisecond, func() error {
						return plcExpectAbsent(deploymentGVK, deployKey)
					})

					// Hold the ConfigMap terminating with a blocking finalizer.
					t.Cleanup(func() { plcClearFinalizers(configKey) })
					config := env.AwaitObject(t, configMapGVK, configKey, nil, 30*time.Second).DeepCopy()
					config.SetFinalizers(append(config.GetFinalizers(), blockingFinalizer))
					if err := env.Client.Update(ctx, config); err != nil {
						t.Fatalf("add blocking finalizer: %v", err)
					}
					if err := env.Client.Delete(ctx, config); err != nil {
						t.Fatalf("delete config: %v", err)
					}
					env.AwaitObject(t, configMapGVK, configKey, func(u *unstructured.Unstructured) error {
						if u.GetDeletionTimestamp() == nil {
							return &notYetError{msg: "config not yet terminating"}
						}
						return nil
					}, 30*time.Second)

					// Make the downstream Deployment newly desired while the
					// upstream ConfigMap is still terminating.
					plcUpdateInstanceSpec(t, gvk, ns, instanceName, map[string]any{
						"name":             "term",
						"createDeployment": true,
					})

					// Built-in-only: instance reports ResourcesReady=False with
					// reason=ResourceDeleting naming the terminating resource.
					if isBuiltin(be) {
						awaitInstanceCR(t, gvk,
							types.NamespacedName{Namespace: ns, Name: instanceName},
							func(u *unstructured.Unstructured) error {
								return plcMatchResourceDeleting(u, ns, "term-config")
							}, 60*time.Second)
					}

					// The Deployment must not appear while the ConfigMap is
					// terminating.
					environment.Consistently(t, 5*time.Second, 500*time.Millisecond, func() error {
						return plcExpectAbsent(deploymentGVK, deployKey)
					})

					// Release the finalizer; the ConfigMap is recreated and the
					// Deployment then converges with the resolved env value.
					plcClearFinalizers(configKey)
					env.AwaitObject(t, configMapGVK, configKey, func(u *unstructured.Unstructured) error {
						if u.GetDeletionTimestamp() != nil {
							return &notYetError{msg: "config still terminating"}
						}
						v, _, _ := unstructured.NestedString(u.Object, "data", "value")
						if v != "active" {
							return &notYetError{msg: "recreated config.data.value=" + v + ", want active"}
						}
						return nil
					}, 60*time.Second)

					awaitInstanceCR(t, deploymentGVK, deployKey, func(u *unstructured.Unstructured) error {
						return plcMatchDeploymentEnv(u, "active")
					}, 60*time.Second)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── cascading_deletion_test.go: cascade delete nested resources.
	//
	//    Both backends own their child resources and delete them when the
	//    instance is removed (the graph backend stamps a per-instance child
	//    Graph, whose deletion cascades). This ports the observable core:
	//    create an instance that fans out into several children, assert they all
	//    exist, delete the instance, assert they are all pruned. (The original
	//    used three nested RGD Kinds; the parity harness deploys a single RGD, so
	//    the nesting is expressed as multiple children of one instance — the
	//    same cascade-on-delete contract.)
	DescribeTable("cascade deletes all managed child resources when the instance is deleted",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			t := GinkgoT()
			ns := createParityNamespace(t)
			ctx := plcCtx()

			rgd := generator.NewResourceGraphDefinition("parity"+rand.String(5),
				generator.WithSchema(
					"ParityCascade"+rand.String(4), "v1alpha1",
					map[string]any{"name": "string"},
					map[string]any{"ready": "${cfg.metadata.name != ''}"},
				),
				generator.WithResource("cfg", map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{
						"name":      "${schema.spec.name}-config",
						"namespace": "${schema.metadata.namespace}",
					},
					"data": map[string]any{"level": "1"},
				}, nil, nil),
				generator.WithResource("sec", map[string]any{
					"apiVersion": "v1", "kind": "Secret",
					"metadata": map[string]any{
						"name":      "${schema.spec.name}-secret",
						"namespace": "${schema.metadata.namespace}",
					},
					"type":       "Opaque",
					"stringData": map[string]any{"resource": "${schema.spec.name}"},
				}, nil, nil),
				generator.WithResource("svc", map[string]any{
					"apiVersion": "v1", "kind": "Service",
					"metadata": map[string]any{
						"name":      "${schema.spec.name}-svc",
						"namespace": "${schema.metadata.namespace}",
					},
					"spec": map[string]any{
						"type":     "ClusterIP",
						"ports":    []any{map[string]any{"port": 80, "targetPort": 8080}},
						"selector": map[string]any{"app": "${schema.spec.name}"},
					},
				}, nil, nil),
				generator.WithResource("dep", map[string]any{
					"apiVersion": "apps/v1", "kind": "Deployment",
					"metadata": map[string]any{
						"name":      "${schema.spec.name}-deployment",
						"namespace": "${schema.metadata.namespace}",
					},
					"spec": map[string]any{
						"replicas": 1,
						"selector": map[string]any{"matchLabels": map[string]any{"app": "${schema.spec.name}"}},
						"template": map[string]any{
							"metadata": map[string]any{"labels": map[string]any{"app": "${schema.spec.name}"}},
							"spec": map[string]any{"containers": []any{map[string]any{
								"name":  "app",
								"image": "nginx:latest",
								"ports": []any{map[string]any{"containerPort": 8080}},
							}}},
						},
					},
				}, nil, nil),
			)
			gvk := be.Deploy(t, rgd)

			instName := "cascade"
			inst := newInstanceCR(gvk, ns, instName, map[string]any{"name": instName})
			if err := env.Client.Create(ctx, inst); err != nil {
				t.Fatalf("create instance: %v", err)
			}

			children := []struct {
				gvk  schema.GroupVersionKind
				name string
			}{
				{configMapGVK, "cascade-config"},
				{secretGVK, "cascade-secret"},
				{serviceGVK, "cascade-svc"},
				{deploymentGVK, "cascade-deployment"},
			}

			// All children exist.
			for _, c := range children {
				awaitInstanceCR(t, c.gvk, types.NamespacedName{Namespace: ns, Name: c.name}, nil, 60*time.Second)
			}
			awaitInstanceReady(t, be, gvk, types.NamespacedName{Namespace: ns, Name: instName})

			// Delete the instance; every child cascades away.
			if err := env.Client.Delete(ctx, inst); err != nil {
				t.Fatalf("delete instance: %v", err)
			}
			for _, c := range children {
				env.AwaitDeleted(t, c.gvk, types.NamespacedName{Namespace: ns, Name: c.name}, 60*time.Second)
			}
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)
})

// ── shared helpers (plc = parity-lifecycle prefix, collision-proof) ──────────

// plcCtx returns the environment context, falling back to context.Background
// when the suite has not installed one.
func plcCtx() context.Context {
	if ctx := env.Context(); ctx != nil {
		return ctx
	}
	return context.Background()
}

// plcUpdateInstanceSpec fetches the named instance, replaces spec, and updates,
// retrying on conflict (both controllers churn instance status between our Get
// and Update).
func plcUpdateInstanceSpec(t GinkgoTInterface, gvk schema.GroupVersionKind, ns, name string, spec map[string]any) {
	t.Helper()
	ctx := plcCtx()
	environment.Eventually(t, 20*time.Second, 250*time.Millisecond, func() error {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)
		if err := env.Client.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, u); err != nil {
			return err
		}
		u.Object["spec"] = spec
		return env.Client.Update(ctx, u)
	})
}

// plcAwaitRGDState polls until the built-in ResourceGraphDefinition reaches the
// wanted status.state.
func plcAwaitRGDState(t GinkgoTInterface, name string, want krov1alpha1.ResourceGraphDefinitionState) {
	t.Helper()
	ctx := plcCtx()
	environment.Eventually(t, 60*time.Second, 500*time.Millisecond, func() error {
		rgd := &krov1alpha1.ResourceGraphDefinition{}
		if err := env.Client.Get(ctx, types.NamespacedName{Name: name}, rgd); err != nil {
			return err
		}
		if rgd.Status.State != want {
			return &notYetError{msg: fmt.Sprintf("rgd.status.state=%s, want %s", rgd.Status.State, want)}
		}
		return nil
	})
}

// plcExpectAbsent returns nil only while the object does not exist (a NotFound
// Get). Any other outcome is an error, used inside Consistently.
func plcExpectAbsent(gvk schema.GroupVersionKind, key types.NamespacedName) error {
	ctx := plcCtx()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	err := env.Client.Get(ctx, key, u)
	if err == nil {
		return fmt.Errorf("%s %s exists but should be absent", gvk.Kind, key)
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// plcClearFinalizers removes all finalizers from the named ConfigMap so a
// pending deletion can complete. Best-effort (used in cleanup and to release
// the hold).
func plcClearFinalizers(key types.NamespacedName) {
	ctx := plcCtx()
	cur := &unstructured.Unstructured{}
	cur.SetGroupVersionKind(configMapGVK)
	if err := env.Client.Get(ctx, key, cur); err != nil {
		return
	}
	cur.SetFinalizers(nil)
	_ = env.Client.Update(ctx, cur)
}

// plcMatchDeployment asserts a Deployment's resolved replicas / first-container
// image / first-container first-port containerPort. Returns a notYetError so it
// composes with awaitInstanceCR's polling.
func plcMatchDeployment(u *unstructured.Unstructured, replicas int64, image string, port int64) error {
	gotReplicas, _, _ := unstructured.NestedInt64(u.Object, "spec", "replicas")
	if gotReplicas != replicas {
		return &notYetError{msg: fmt.Sprintf("spec.replicas=%d, want %d", gotReplicas, replicas)}
	}
	containers, _, _ := unstructured.NestedSlice(u.Object, "spec", "template", "spec", "containers")
	if len(containers) == 0 {
		return &notYetError{msg: "no containers yet"}
	}
	c0, _ := containers[0].(map[string]any)
	if c0 == nil {
		return &notYetError{msg: "container[0] not a map"}
	}
	gotImage, _, _ := unstructured.NestedString(c0, "image")
	if gotImage != image {
		return &notYetError{msg: fmt.Sprintf("container[0].image=%s, want %s", gotImage, image)}
	}
	ports, _, _ := unstructured.NestedSlice(c0, "ports")
	if len(ports) == 0 {
		return &notYetError{msg: "no ports yet"}
	}
	p0, _ := ports[0].(map[string]any)
	if p0 == nil {
		return &notYetError{msg: "port[0] not a map"}
	}
	gotPort, _, _ := unstructured.NestedInt64(p0, "containerPort")
	if gotPort != port {
		return &notYetError{msg: fmt.Sprintf("container[0].ports[0].containerPort=%d, want %d", gotPort, port)}
	}
	return nil
}

// plcMatchDeploymentEnv asserts the first container's first env var value.
func plcMatchDeploymentEnv(u *unstructured.Unstructured, want string) error {
	containers, _, _ := unstructured.NestedSlice(u.Object, "spec", "template", "spec", "containers")
	if len(containers) == 0 {
		return &notYetError{msg: "no containers yet"}
	}
	c0, _ := containers[0].(map[string]any)
	if c0 == nil {
		return &notYetError{msg: "container[0] not a map"}
	}
	envs, _, _ := unstructured.NestedSlice(c0, "env")
	if len(envs) == 0 {
		return &notYetError{msg: "no env yet"}
	}
	e0, _ := envs[0].(map[string]any)
	if e0 == nil {
		return &notYetError{msg: "env[0] not a map"}
	}
	got, _, _ := unstructured.NestedString(e0, "value")
	if got != want {
		return &notYetError{msg: fmt.Sprintf("env[0].value=%s, want %s", got, want)}
	}
	return nil
}

// plcMatchResourceDeleting asserts the built-in instance status carries a
// ResourcesReady=False / reason=ResourceDeleting condition naming the
// terminating resource.
func plcMatchResourceDeleting(u *unstructured.Unstructured, ns, resourceName string) error {
	conds, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	if !found {
		return &notYetError{msg: "no status.conditions yet"}
	}
	for _, ci := range conds {
		cond, ok := ci.(map[string]any)
		if !ok || cond["type"] != "ResourcesReady" {
			continue
		}
		if cond["status"] != "False" {
			return &notYetError{msg: fmt.Sprintf("ResourcesReady status=%v, want False", cond["status"])}
		}
		if cond["reason"] != "ResourceDeleting" {
			return &notYetError{msg: fmt.Sprintf("ResourcesReady reason=%v, want ResourceDeleting", cond["reason"])}
		}
		msg, _ := cond["message"].(string)
		want := fmt.Sprintf(`resource "%s/%s"`, ns, resourceName)
		if !strings.Contains(msg, want) {
			return &notYetError{msg: fmt.Sprintf("ResourcesReady message=%q, want substring %q", msg, want)}
		}
		return nil
	}
	return &notYetError{msg: "no ResourcesReady condition yet"}
}
