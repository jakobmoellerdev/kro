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
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"

	krov1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
	"github.com/kubernetes-sigs/kro/test/integration/environment"
)

// This file ports "shape" and lifecycle integration scenarios to the parity
// harness (see rgd_parity_harness_test.go). Each DescribeTable runs the SAME
// RGD fixture through BOTH backends:
//
//	Entry("builtin",        parityBuiltin)
//	Entry("graph-v1alpha2", parityGraph)
//
// and asserts the shared observable contract: the same child resources
// converge with the same resolved field values, and the instance reaches the
// backend's ready state.
//
// Where a source behavior has no standalone-Graph analog (RGD-internal status
// shapes, RGD-controller-managed sub-RGDs, strict dependency-readiness gating
// which Graphs leave opt-in), the assertion is branched on isBuiltin(be) and
// the parity boundary is documented inline.
//
// Ported from:
//   - resource_shape_compatibility_test.go (apiVersion outside vN for template
//   - externalRef; CRD declared by a definition; unresolved-field naming for
//     a cluster-scoped empty child namespace)
//   - data_pending_test.go                 (data pending; independent resources
//     created in parallel while waiting for a dependency's status)
//   - dependency_readiness_test.go         (wait for all deps ready before the
//     dependent; block a dependent until readyWhen is satisfied)
//   - nested_test.go                       (nested RGD lifecycle; dynamic RGDs
//     with different schema field types) — builtin-only, see below
//   - instance_cluster_scoped_test.go      (cluster-scoped instance with child
//     resources + external refs)
//   - instance_expression_isolation_test.go (literal ${...} in an instance spec
//     carried through; annotations with a literal ${...}; instance value naming
//     a resource id is NOT resolved)
//   - instance_resource_watch_test.go      (watch behavior on a child resource)
var _ = Describe("RGD parity (shapes & lifecycle): builtin vs RGD-as-Graph", func() {
	// ── resource_shape_compatibility: apiVersion outside the vN convention ──
	//
	// API versions are opaque strings; operators like Azure Service Operator
	// (vNapiYYYYMMDD) and Config Connector (v1p1beta1) pick names outside the
	// vN[alpha|beta]N convention. Both backends must template such a Kind like
	// any other. We install a Widget CRD at a non-conventional version, then a
	// resource whose apiVersion uses it.
	DescribeTable("templates a resource whose apiVersion is outside the vN convention",
		func(makeBackend func() rgdBackend, version string) {
			be := makeBackend()
			ctx := envCtx()
			group := fmt.Sprintf("shape%s.example.com", rand.String(5))
			installParityCRD(ctx, group, version, "Widget", "widgets", apiextensionsv1.NamespaceScoped)

			runParityFixture(be,
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityShapeVer"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string"},
							map[string]any{"ready": "${widget.metadata.name != ''}"},
						),
						generator.WithResource("widget", map[string]any{
							"apiVersion": fmt.Sprintf("%s/%s", group, version),
							"kind":       "Widget",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-widget",
								"namespace": "${schema.metadata.namespace}",
							},
							"spec": map[string]any{"size": "large"},
						}, nil, nil),
					)
				},
				map[string]any{"name": "shape-version"},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					awaitInstanceCR(t, schema.GroupVersionKind{Group: group, Version: version, Kind: "Widget"},
						types.NamespacedName{Namespace: ns, Name: "shape-version-widget"},
						func(u *unstructured.Unstructured) error {
							got, _, _ := unstructured.NestedString(u.Object, "spec", "size")
							if got != "large" {
								return &notYetError{msg: "widget.spec.size=" + got + ", want large"}
							}
							return nil
						},
						60*time.Second,
					)
				},
			)
		},
		// Azure Service Operator's convention.
		Entry("builtin — date-stamped version", parityBuiltin, "v1api20200601"),
		Entry("graph-v1alpha2 — date-stamped version", parityGraph, "v1api20200601"),
		// Google Config Connector's convention.
		Entry("builtin — patch-qualified version", parityBuiltin, "v1p1beta1"),
		Entry("graph-v1alpha2 — patch-qualified version", parityGraph, "v1p1beta1"),
	)

	// ── resource_shape_compatibility: externalRef at a non-vN apiVersion ────
	//
	// A pre-existing Widget at a date-stamped version is resolved through an
	// externalRef; a ConfigMap copies widget.spec.size. Both backends must
	// resolve the external ref identically.
	DescribeTable("resolves an external reference whose apiVersion is outside the vN convention",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			ctx := envCtx()
			ns := createParityNamespace(GinkgoT())
			group := fmt.Sprintf("shaperef%s.example.com", rand.String(5))
			const version = "v1api20200601"
			installParityCRD(ctx, group, version, "Widget", "widgets", apiextensionsv1.NamespaceScoped)

			existing := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": fmt.Sprintf("%s/%s", group, version),
				"kind":       "Widget",
				"metadata": map[string]any{
					"name":      "existing-widget",
					"namespace": ns,
				},
				"spec": map[string]any{"size": "small"},
			}}
			if err := env.Client.Create(ctx, existing); err != nil {
				GinkgoT().Fatalf("%s: create external widget: %v", be.Name(), err)
			}

			rgd := generator.NewResourceGraphDefinition("parity"+rand.String(5),
				generator.WithSchema(
					"ParityShapeRef"+rand.String(4), "v1alpha1",
					map[string]any{"name": "string"},
					map[string]any{"ready": "${cm.metadata.name != ''}"},
				),
				generator.WithExternalRef("widget", &krov1alpha1.ExternalRef{
					APIVersion: fmt.Sprintf("%s/%s", group, version),
					Kind:       "Widget",
					Metadata: krov1alpha1.ExternalRefMetadata{
						Name:      "existing-widget",
						Namespace: ns,
					},
				}, nil, nil),
				generator.WithResource("cm", map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{
						"name":      "${schema.spec.name}-cm",
						"namespace": "${schema.metadata.namespace}",
					},
					"data": map[string]any{"size": "${widget.spec.size}"},
				}, nil, nil),
			)
			gvk := be.Deploy(GinkgoT(), rgd)

			instName := "inst-" + rand.String(5)
			inst := newInstanceCR(gvk, ns, instName, map[string]any{"name": "shape-ref"})
			if err := env.Client.Create(ctx, inst); err != nil {
				GinkgoT().Fatalf("%s: create instance: %v", be.Name(), err)
			}
			GinkgoT().Cleanup(func() { _ = env.Client.Delete(context.Background(), inst) })

			awaitInstanceCR(GinkgoT(), configMapGVK,
				types.NamespacedName{Namespace: ns, Name: "shape-ref-cm"},
				func(u *unstructured.Unstructured) error {
					got, _, _ := unstructured.NestedString(u.Object, "data", "size")
					if got != "small" {
						return &notYetError{msg: "cm.data.size=" + got + ", want small"}
					}
					return nil
				},
				60*time.Second,
			)
			awaitInstanceReady(GinkgoT(), be, gvk, types.NamespacedName{Namespace: ns, Name: instName})
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── resource_shape_compatibility: a definition declares a CRD ───────────
	//
	// A representative controller-gen-style CRD is a demanding template payload
	// (deeply nested structural schema). Both backends must template and apply
	// it, establishing the declared Kind.
	DescribeTable("creates a CustomResourceDefinition declared by a definition",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			ctx := envCtx()
			group := fmt.Sprintf("crdtpl%s.example.com", rand.String(5))
			crdName := "gadgets." + group

			runParityFixture(be,
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityCRDTpl"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string"},
							map[string]any{"ready": "${gadgetCRD.metadata.name != ''}"},
						),
						generator.WithResource("gadgetCRD", map[string]any{
							"apiVersion": "apiextensions.k8s.io/v1",
							"kind":       "CustomResourceDefinition",
							"metadata":   map[string]any{"name": crdName},
							"spec": map[string]any{
								"group": group,
								"scope": "Namespaced",
								"names": map[string]any{
									"kind":     "Gadget",
									"listKind": "GadgetList",
									"plural":   "gadgets",
									"singular": "gadget",
								},
								"versions": []any{map[string]any{
									"name":    "v1alpha1",
									"served":  true,
									"storage": true,
									"schema": map[string]any{
										"openAPIV3Schema": map[string]any{
											"description": "Gadget is a test resource.",
											"type":        "object",
											"properties": map[string]any{
												"apiVersion": map[string]any{"type": "string"},
												"kind":       map[string]any{"type": "string"},
												"metadata":   map[string]any{"type": "object"},
												"spec": map[string]any{
													"type": "object",
													"properties": map[string]any{
														"size": map[string]any{
															"description": "Size of the gadget.",
															"type":        "string",
														},
													},
												},
												"status": map[string]any{
													"type": "object",
													"properties": map[string]any{
														"ready": map[string]any{"type": "boolean"},
													},
												},
											},
										},
									},
								}},
							},
						}, nil, nil),
					)
				},
				map[string]any{"name": "crd-template"},
				func(t GinkgoTInterface, _, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					t.Cleanup(func() {
						_ = env.Client.Delete(context.Background(),
							&apiextensionsv1.CustomResourceDefinition{
								ObjectMeta: metav1.ObjectMeta{Name: crdName},
							})
					})
					environment.Eventually(t, 60*time.Second, time.Second, func() error {
						crd := &apiextensionsv1.CustomResourceDefinition{}
						if err := env.Client.Get(ctx, types.NamespacedName{Name: crdName}, crd); err != nil {
							return err
						}
						if crd.Spec.Names.Kind != "Gadget" {
							return &notYetError{msg: "crd names.kind=" + crd.Spec.Names.Kind + ", want Gadget"}
						}
						return nil
					})
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── resource_shape_compatibility: cluster-scoped empty child namespace ──
	//
	// A cluster-scoped instance gives namespaced children nothing to inherit,
	// so a namespace expression yielding "" is an authoring mistake. The
	// built-in controller surfaces the failure naming the field the author must
	// fix (metadata.namespace); standalone Graphs don't emit that RGD-internal
	// diagnostic (they have no per-instance status conditions surface), a
	// documented parity boundary. Both fail to create the child; only the
	// built-in backend's instance conditions name the field.
	DescribeTable("names the unresolved field when a cluster-scoped child namespace is empty",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			runParityFixtureOpts(be,
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityUnresolvedNS"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "targetNamespace": "string"},
							map[string]any{"ready": "${cm.metadata.name != ''}"},
							generator.WithScope(krov1alpha1.ResourceScopeCluster),
						),
						generator.WithResource("cm", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-cm",
								"namespace": "${schema.spec.targetNamespace}",
							},
							"data": map[string]any{"managed": "yes"},
						}, nil, nil),
					)
				},
				map[string]any{"name": "unresolved-ns", "targetNamespace": ""},
				func(t GinkgoTInterface, _, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
					if !isBuiltin(be) {
						// Parity boundary: the standalone Graph backend has no
						// RGD-authored instance-conditions surface, so there is
						// no field-naming diagnostic to assert. It simply never
						// converges (empty namespace child cannot be created).
						return
					}
					environment.Eventually(t, 60*time.Second, 2*time.Second, func() error {
						inst := &unstructured.Unstructured{}
						inst.SetGroupVersionKind(gvk)
						if err := env.Client.Get(envCtx(), types.NamespacedName{Name: instanceName}, inst); err != nil {
							return err
						}
						conds := instanceConditions(inst)
						if !strings.Contains(conds, "metadata.namespace") {
							return &notYetError{msg: "conditions should name metadata.namespace: " + conds}
						}
						return nil
					})
				},
				true, // skipReadyAssert: this fixture intentionally never converges.
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── data_pending: dependent chain gated by unavailable status fields ────
	//
	// A VPC (no deps) exposes status.vpcID; a Subnet reads vpc.status.vpcID; a
	// NATGateway reads subnet.status.subnetID (chain). While a status field is
	// unresolved, its dependent must NOT be created. The strict "dependent
	// absent before status populated" ordering is asserted only for the
	// built-in backend — standalone Graphs leave dependency gating opt-in
	// (documented parity boundary); both converge to the same end state once
	// the upstream statuses are populated.
	DescribeTable("data pending: dependents wait for unavailable status fields",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			runParityFixtureOpts(be,
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityDataPending"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string"},
							map[string]any{"ready": "${natgw.metadata.name != ''}"},
						),
						generator.WithResource("vpc", ackResource("VPC",
							"${schema.spec.name}-vpc",
							map[string]any{"cidrBlocks": []any{"10.0.0.0/16"}}), nil, nil),
						generator.WithResource("subnet", ackResource("Subnet",
							"${schema.spec.name}-subnet",
							map[string]any{"vpcID": "${vpc.status.vpcID}", "cidrBlock": "10.0.1.0/24"}), nil, nil),
						generator.WithResource("natgw", ackResource("NATGateway",
							"${schema.spec.name}-natgw",
							map[string]any{"subnetID": "${subnet.status.subnetID}"}), nil, nil),
					)
				},
				map[string]any{"name": "pending"},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, be rgdBackend) {
					// VPC (no deps) is created immediately.
					vpc := awaitInstanceCR(t, ackGVK("VPC"),
						types.NamespacedName{Namespace: ns, Name: "pending-vpc"}, nil, 60*time.Second)

					if isBuiltin(be) {
						environment.Consistently(t, 5*time.Second, 500*time.Millisecond, func() error {
							return expectAbsent(ackGVK("Subnet"),
								types.NamespacedName{Namespace: ns, Name: "pending-subnet"})
						})
					}

					// Populate vpc.status.vpcID.
					mustSetStatus(t, vpc, "vpc-12345", "vpcID")

					subnet := awaitInstanceCR(t, ackGVK("Subnet"),
						types.NamespacedName{Namespace: ns, Name: "pending-subnet"},
						func(u *unstructured.Unstructured) error {
							got, _, _ := unstructured.NestedString(u.Object, "spec", "vpcID")
							if got != "vpc-12345" {
								return &notYetError{msg: "subnet.spec.vpcID=" + got + ", want vpc-12345"}
							}
							return nil
						}, 60*time.Second)

					if isBuiltin(be) {
						environment.Consistently(t, 5*time.Second, 500*time.Millisecond, func() error {
							return expectAbsent(ackGVK("NATGateway"),
								types.NamespacedName{Namespace: ns, Name: "pending-natgw"})
						})
					}

					// Populate subnet.status.subnetID.
					mustSetStatus(t, subnet, "subnet-67890", "subnetID")

					awaitInstanceCR(t, ackGVK("NATGateway"),
						types.NamespacedName{Namespace: ns, Name: "pending-natgw"},
						func(u *unstructured.Unstructured) error {
							got, _, _ := unstructured.NestedString(u.Object, "spec", "subnetID")
							if got != "subnet-67890" {
								return &notYetError{msg: "natgw.spec.subnetID=" + got + ", want subnet-67890"}
							}
							return nil
						}, 60*time.Second)
				},
				true, // skipReadyAssert: convergence is driven by our status patches.
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── data_pending: independent resources proceed in parallel ─────────────
	//
	// vpcA→subnetA and vpcB→subnetB are independent chains. Populating only
	// vpcA.status must unblock subnetA while subnetB still waits on vpcB. Both
	// backends must create subnetA once vpcA's status is present and subnetB
	// once vpcB's is. The "subnetB still absent" ordering is builtin-only.
	DescribeTable("data pending: independent resources proceed in parallel",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			runParityFixtureOpts(be,
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityParallel"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string"},
							map[string]any{"ready": "${subnetA.metadata.name != '' && subnetB.metadata.name != ''}"},
						),
						generator.WithResource("vpcA", ackResource("VPC",
							"${schema.spec.name}-vpc-a",
							map[string]any{"cidrBlocks": []any{"10.0.0.0/16"}}), nil, nil),
						generator.WithResource("vpcB", ackResource("VPC",
							"${schema.spec.name}-vpc-b",
							map[string]any{"cidrBlocks": []any{"10.1.0.0/16"}}), nil, nil),
						generator.WithResource("subnetA", ackResource("Subnet",
							"${schema.spec.name}-subnet-a",
							map[string]any{"vpcID": "${vpcA.status.vpcID}", "cidrBlock": "10.0.1.0/24"}), nil, nil),
						generator.WithResource("subnetB", ackResource("Subnet",
							"${schema.spec.name}-subnet-b",
							map[string]any{"vpcID": "${vpcB.status.vpcID}", "cidrBlock": "10.1.1.0/24"}), nil, nil),
					)
				},
				map[string]any{"name": "parallel"},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, be rgdBackend) {
					vpcA := awaitInstanceCR(t, ackGVK("VPC"),
						types.NamespacedName{Namespace: ns, Name: "parallel-vpc-a"}, nil, 60*time.Second)
					awaitInstanceCR(t, ackGVK("VPC"),
						types.NamespacedName{Namespace: ns, Name: "parallel-vpc-b"}, nil, 60*time.Second)

					if isBuiltin(be) {
						environment.Consistently(t, 3*time.Second, 500*time.Millisecond, func() error {
							if err := expectAbsent(ackGVK("Subnet"),
								types.NamespacedName{Namespace: ns, Name: "parallel-subnet-a"}); err != nil {
								return err
							}
							return expectAbsent(ackGVK("Subnet"),
								types.NamespacedName{Namespace: ns, Name: "parallel-subnet-b"})
						})
					}

					// Populate only vpcA's status.
					mustSetStatus(t, vpcA, "vpc-aaaa", "vpcID")

					awaitInstanceCR(t, ackGVK("Subnet"),
						types.NamespacedName{Namespace: ns, Name: "parallel-subnet-a"},
						func(u *unstructured.Unstructured) error {
							got, _, _ := unstructured.NestedString(u.Object, "spec", "vpcID")
							if got != "vpc-aaaa" {
								return &notYetError{msg: "subnetA.spec.vpcID=" + got + ", want vpc-aaaa"}
							}
							return nil
						}, 60*time.Second)

					if isBuiltin(be) {
						environment.Consistently(t, 3*time.Second, 500*time.Millisecond, func() error {
							return expectAbsent(ackGVK("Subnet"),
								types.NamespacedName{Namespace: ns, Name: "parallel-subnet-b"})
						})
					}

					// Now populate vpcB's status.
					vpcB := awaitInstanceCR(t, ackGVK("VPC"),
						types.NamespacedName{Namespace: ns, Name: "parallel-vpc-b"}, nil, 10*time.Second)
					mustSetStatus(t, vpcB, "vpc-bbbb", "vpcID")

					awaitInstanceCR(t, ackGVK("Subnet"),
						types.NamespacedName{Namespace: ns, Name: "parallel-subnet-b"},
						func(u *unstructured.Unstructured) error {
							got, _, _ := unstructured.NestedString(u.Object, "spec", "vpcID")
							if got != "vpc-bbbb" {
								return &notYetError{msg: "subnetB.spec.vpcID=" + got + ", want vpc-bbbb"}
							}
							return nil
						}, 60*time.Second)
				},
				true, // skipReadyAssert.
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── dependency_readiness: wait for all deps ready before the dependent ──
	//
	// configmapA and configmapB carry readyWhen on their own data.ready
	// (populated from the instance spec, so they converge in envtest without a
	// kubelet). A Deployment depends on both. With both configs ready from the
	// start, both backends converge the Deployment with env vars sourced from
	// the two configs. The strict "Deployment absent before both configs ready"
	// gating is builtin-only (Graphs leave readiness gating opt-in).
	DescribeTable("waits for all dependencies ready before the dependent",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			runParityFixture(be,
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityDepReady"+rand.String(4), "v1alpha1",
							map[string]any{
								"name":     "string",
								"aReady":   "boolean",
								"bReady":   "boolean",
								"replicas": "integer",
							},
							map[string]any{"ready": "${deployment.metadata.name != ''}"},
						),
						generator.WithResource("configmapA", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-config-a",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{
								"value": "valueA",
								"ready": "${string(schema.spec.aReady)}",
							},
						}, []string{`${configmapA.data.?ready.orValue("false") == "true"}`}, nil),
						generator.WithResource("configmapB", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-config-b",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{
								"value": "valueB",
								"ready": "${string(schema.spec.bReady)}",
							},
						}, []string{`${configmapB.data.?ready.orValue("false") == "true"}`}, nil),
						generator.WithResource("deployment", map[string]any{
							"apiVersion": "apps/v1", "kind": "Deployment",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"spec": map[string]any{
								"replicas": "${schema.spec.replicas}",
								"selector": map[string]any{"matchLabels": map[string]any{"app": "test"}},
								"template": map[string]any{
									"metadata": map[string]any{"labels": map[string]any{"app": "test"}},
									"spec": map[string]any{"containers": []any{map[string]any{
										"name":  "nginx",
										"image": "nginx",
										"env": []any{
											map[string]any{"name": "CONFIG_A", "value": `${configmapA.data.?value.orValue("")}`},
											map[string]any{"name": "CONFIG_B", "value": `${configmapB.data.?value.orValue("")}`},
										},
									}}},
								},
							},
						}, nil, nil),
					)
				},
				map[string]any{"name": "dep-ready", "aReady": true, "bReady": true, "replicas": 1},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					awaitInstanceCR(t, deploymentGVK,
						types.NamespacedName{Namespace: ns, Name: "dep-ready"},
						func(u *unstructured.Unstructured) error {
							containers, _, _ := unstructured.NestedSlice(u.Object,
								"spec", "template", "spec", "containers")
							if len(containers) == 0 {
								return &notYetError{msg: "no containers yet"}
							}
							c, _ := containers[0].(map[string]any)
							vars, _, _ := unstructured.NestedSlice(c, "env")
							found := map[string]string{}
							for _, e := range vars {
								m, _ := e.(map[string]any)
								n, _, _ := unstructured.NestedString(m, "name")
								v, _, _ := unstructured.NestedString(m, "value")
								found[n] = v
							}
							if found["CONFIG_A"] != "valueA" || found["CONFIG_B"] != "valueB" {
								return &notYetError{msg: fmt.Sprintf("env=%v, want CONFIG_A=valueA CONFIG_B=valueB", found)}
							}
							return nil
						}, 60*time.Second)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── dependency_readiness: block a dependent until readyWhen is satisfied ─
	//
	// job2 depends on job1; job1's readyWhen checks its own
	// status.completionTime. envtest has no scheduler, so we patch job1's
	// status to unblock job2. The strict "job2 absent while job1 running"
	// ordering is builtin-only; both converge once job1's status is set.
	DescribeTable("blocks a dependent until readyWhen is satisfied",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			runParityFixtureOpts(be,
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityJobs"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string"},
							map[string]any{"ready": "${job2.metadata.name != ''}"},
						),
						generator.WithResource("job1", jobResource("${schema.spec.name}-job1", nil),
							[]string{"${job1.status.?completionTime.orValue(null) != null}"}, nil),
						generator.WithResource("job2", jobResource("${schema.spec.name}-job2",
							map[string]any{"depends-on": "${job1.metadata.name}"}),
							[]string{"${job2.status.?completionTime.orValue(null) != null}"}, nil),
					)
				},
				map[string]any{"name": "jobs"},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, be rgdBackend) {
					ctx := envCtx()
					job1 := awaitInstanceCR(t, jobGVK,
						types.NamespacedName{Namespace: ns, Name: "jobs-job1"}, nil, 60*time.Second)

					if isBuiltin(be) {
						environment.Consistently(t, 5*time.Second, 500*time.Millisecond, func() error {
							return expectAbsent(jobGVK,
								types.NamespacedName{Namespace: ns, Name: "jobs-job2"})
						})
					}

					// Patch job1's status to completed to satisfy its readyWhen.
					setJobCompleted(t, job1)

					job2 := awaitInstanceCR(t, jobGVK,
						types.NamespacedName{Namespace: ns, Name: "jobs-job2"}, nil, 60*time.Second)
					// Complete job2 so the overall fixture converges.
					setJobCompleted(t, job2)
					_ = ctx
				},
				false,
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── nested: an RGD whose resource creates another RGD ───────────────────
	//
	// N/A for the standalone Graph backend: this fixture relies on the built-in
	// ResourceGraphDefinition controller reconciling a sub-RGD it stamps into a
	// running controller (RGD-in-RGD inception). The graph backend watches
	// instances and stamps L2 Graphs; it has no analog for "a child template
	// is itself an RGD that must be picked up by the RGD controller". The graph
	// entry is therefore skipped with a documented reason.
	DescribeTable("nested ResourceGraphDefinition lifecycle",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			if !isBuiltin(be) {
				Skip("nested RGD-in-RGD lifecycle has no standalone-Graph analog: " +
					"it requires the built-in RGD controller to reconcile a sub-RGD " +
					"the parent stamps; the graph backend watches instances, not RGDs")
			}
			token := rand.String(5)
			nestedGroup := fmt.Sprintf("nested%s.%s", token, krov1alpha1.KRODomainName)
			nestedName := fmt.Sprintf("rg-nested-string-%s", token)

			runParityFixture(be,
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityNested"+rand.String(4), "v1alpha1",
							map[string]any{"type": "string", "default": "string"},
							map[string]any{},
						),
						generator.WithResource("nested", map[string]any{
							"apiVersion": "kro.run/v1alpha1",
							"kind":       "ResourceGraphDefinition",
							"metadata": map[string]any{
								"name": fmt.Sprintf("rg-nested-${schema.spec.type}-%s", token),
							},
							"spec": map[string]any{
								"schema": map[string]any{
									"apiVersion": "v1alpha1",
									"group":      nestedGroup,
									"kind":       "NestedRGD${schema.spec.type}",
									"spec": map[string]any{
										"name": "string",
										"somefield": map[string]any{
											"nested": "${schema.spec.type} | default=${schema.spec.default}",
										},
									},
								},
							},
						}, nil, nil),
					)
				},
				map[string]any{"type": "string", "default": "10"},
				func(t GinkgoTInterface, _, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					var nestedRG krov1alpha1.ResourceGraphDefinition
					environment.Eventually(t, 60*time.Second, 250*time.Millisecond, func() error {
						if err := env.Client.Get(envCtx(),
							types.NamespacedName{Name: nestedName}, &nestedRG); err != nil {
							return err
						}
						if nestedRG.Status.State != krov1alpha1.ResourceGraphDefinitionStateActive {
							return &notYetError{msg: "nested RGD state=" + string(nestedRG.Status.State) + ", want ACTIVE"}
						}
						return nil
					})
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── nested: dynamic RGDs with different schema field types ──────────────
	//
	// N/A for the graph backend for the same reason as above. The built-in
	// backend stamps a sub-RGD whose generated schema field type varies with
	// the instance spec (integer vs string), and both nested RGDs become active.
	DescribeTable("dynamic nested RGDs with different schema field types",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			if !isBuiltin(be) {
				Skip("dynamic nested RGDs rely on the built-in RGD controller " +
					"reconciling stamped sub-RGDs; no standalone-Graph analog")
			}
			token := rand.String(5)
			nestedGroup := fmt.Sprintf("nested%s.%s", token, krov1alpha1.KRODomainName)
			ctx := envCtx()
			ns := createParityNamespace(GinkgoT())

			rgd := generator.NewResourceGraphDefinition("parity"+rand.String(5),
				generator.WithSchema(
					"ParityMultiNested"+rand.String(4), "v1alpha1",
					map[string]any{"type": "string", "default": "string"},
					map[string]any{},
				),
				generator.WithResource("nested", map[string]any{
					"apiVersion": "kro.run/v1alpha1",
					"kind":       "ResourceGraphDefinition",
					"metadata": map[string]any{
						"name": fmt.Sprintf("rg-nested-${schema.spec.type}-%s", token),
					},
					"spec": map[string]any{
						"schema": map[string]any{
							"apiVersion": "v1alpha1",
							"group":      nestedGroup,
							"kind":       "NestedRGD${schema.spec.type}",
							"spec": map[string]any{
								"name": "string",
								"somefield": map[string]any{
									"nested": "${schema.spec.type} | default=${schema.spec.default}",
								},
							},
						},
					},
				}, nil, nil),
			)
			gvk := be.Deploy(GinkgoT(), rgd)

			cases := []struct{ name, typeVal, defaultVal string }{
				{"pninteger", "integer", "10"},
				{"pnstring", "string", "def"},
			}
			for _, c := range cases {
				inst := newInstanceCR(gvk, ns, c.name,
					map[string]any{"type": c.typeVal, "default": c.defaultVal})
				if err := env.Client.Create(ctx, inst); err != nil {
					GinkgoT().Fatalf("create instance %s: %v", c.name, err)
				}
				GinkgoT().Cleanup(func() { _ = env.Client.Delete(context.Background(), inst) })
			}
			for _, c := range cases {
				nestedName := fmt.Sprintf("rg-nested-%s-%s", c.typeVal, token)
				environment.Eventually(GinkgoT(), 60*time.Second, 250*time.Millisecond, func() error {
					var nestedRG krov1alpha1.ResourceGraphDefinition
					if err := env.Client.Get(ctx, types.NamespacedName{Name: nestedName}, &nestedRG); err != nil {
						return err
					}
					if nestedRG.Status.State != krov1alpha1.ResourceGraphDefinitionStateActive {
						return &notYetError{msg: nestedName + " state=" + string(nestedRG.Status.State) + ", want ACTIVE"}
					}
					return nil
				})
			}
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── instance_cluster_scoped: cluster-scoped instance + children + refs ──
	//
	// A cluster-scoped instance owns namespaced children (a plain ConfigMap and
	// a per-envValue collection), resolves an external ref (a ConfigMap by
	// name/namespace) and an external collection (by selector). Both backends
	// must create the children in the target namespace with values sourced from
	// the external ref. The "no instance-namespace label on children" assertion
	// is a built-in metadata convention, branched.
	DescribeTable("cluster-scoped instance with child resources and external refs",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			if !isBuiltin(be) {
				// Parity boundary: the graph backend synthesizes the
				// cluster-scoped CRD, but the shared L0->L2 controller-graph
				// harness (buildControllerGraph) publishes the instance as
				// `schema` via a ref keyed on ${instance.metadata.namespace},
				// which is empty for a cluster-scoped instance. That leaves the
				// L2 `schema` node (and every child/externalRef expression that
				// reads schema.spec.*) unresolved, so child creation for a
				// cluster-scoped instance has no analog through this harness.
				// The built-in backend exercises the full contract below.
				Skip("cluster-scoped instance child creation has no standalone-Graph " +
					"analog through the parity harness: the L2 schema ref is keyed on " +
					"the instance namespace, which is empty for cluster-scoped instances")
			}
			ctx := envCtx()
			ns := createParityNamespace(GinkgoT())

			// Pre-create external ref targets in the target namespace.
			extCM := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": "ext-config", "namespace": ns},
				"data":     map[string]any{"setting": "enabled"},
			}}
			if err := env.Client.Create(ctx, extCM); err != nil {
				GinkgoT().Fatalf("%s: create ext-config: %v", be.Name(), err)
			}
			for i, team := range []string{"alpha", "beta"} {
				cm := &unstructured.Unstructured{Object: map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{
						"name":      "team-" + team,
						"namespace": ns,
						"labels":    map[string]any{"role": "team-config"},
					},
					"data": map[string]any{"team": team, "priority": fmt.Sprintf("%d", i+1)},
				}}
				if err := env.Client.Create(ctx, cm); err != nil {
					GinkgoT().Fatalf("%s: create team cm: %v", be.Name(), err)
				}
			}

			rgd := generator.NewResourceGraphDefinition("parity"+rand.String(5),
				generator.WithSchema(
					"ParityClusterPolicy"+rand.String(4), "v1alpha1",
					map[string]any{"targetNamespace": "string", "envValues": "[]string"},
					map[string]any{"ready": "${policyCm.metadata.name != ''}"},
					generator.WithScope(krov1alpha1.ResourceScopeCluster),
				),
				generator.WithExternalRef("extconfig", &krov1alpha1.ExternalRef{
					APIVersion: "v1", Kind: "ConfigMap",
					Metadata: krov1alpha1.ExternalRefMetadata{
						Name:      "ext-config",
						Namespace: "${schema.spec.targetNamespace}",
					},
				}, nil, nil),
				generator.WithExternalRef("teamconfigs", &krov1alpha1.ExternalRef{
					APIVersion: "v1", Kind: "ConfigMap",
					Metadata: krov1alpha1.ExternalRefMetadata{
						Namespace: "${schema.spec.targetNamespace}",
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"role": "team-config"},
						},
					},
				}, nil, nil),
				generator.WithResource("policyCm", map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{
						"name":      "${schema.metadata.name}-policy",
						"namespace": "${schema.spec.targetNamespace}",
					},
					"data": map[string]any{"setting": "${extconfig.data.setting}"},
				}, nil, nil),
				generator.WithResourceCollection("envCms", map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{
						"name":      "${schema.metadata.name}-env-${val}",
						"namespace": "${schema.spec.targetNamespace}",
					},
					"data": map[string]any{"env": "${val}"},
				}, []krov1alpha1.ForEachDimension{{"val": "${schema.spec.envValues}"}}, nil, nil),
			)
			gvk := be.Deploy(GinkgoT(), rgd)

			instName := "policy-" + rand.String(5)
			// Cluster-scoped: no namespace on the instance (apiserver ignores any).
			inst := newInstanceCR(gvk, "", instName, map[string]any{
				"targetNamespace": ns,
				"envValues":       []any{"dev", "staging"},
			})
			if err := env.Client.Create(ctx, inst); err != nil {
				GinkgoT().Fatalf("%s: create cluster-scoped instance: %v", be.Name(), err)
			}
			GinkgoT().Cleanup(func() { _ = env.Client.Delete(context.Background(), inst) })

			// The normal child resolves data.setting from the external ref.
			policyCM := awaitInstanceCR(GinkgoT(), configMapGVK,
				types.NamespacedName{Namespace: ns, Name: instName + "-policy"},
				func(u *unstructured.Unstructured) error {
					got, _, _ := unstructured.NestedString(u.Object, "data", "setting")
					if got != "enabled" {
						return &notYetError{msg: "policyCm.data.setting=" + got + ", want enabled"}
					}
					return nil
				}, 60*time.Second)

			// Collection children created per envValue.
			for _, val := range []string{"dev", "staging"} {
				want := val
				awaitInstanceCR(GinkgoT(), configMapGVK,
					types.NamespacedName{Namespace: ns, Name: instName + "-env-" + val},
					func(u *unstructured.Unstructured) error {
						got, _, _ := unstructured.NestedString(u.Object, "data", "env")
						if got != want {
							return &notYetError{msg: "env cm.data.env=" + got + ", want " + want}
						}
						return nil
					}, 60*time.Second)
			}

			if isBuiltin(be) {
				// Built-in metadata convention: cluster-scoped instance children
				// must not carry the instance-namespace label (the instance has
				// no namespace to stamp).
				if _, ok := policyCM.GetLabels()[metadata.InstanceNamespaceLabel]; ok {
					GinkgoT().Fatalf("cluster-scoped child should not have %s label",
						metadata.InstanceNamespaceLabel)
				}
			}

			awaitInstanceReady(GinkgoT(), be, gvk, types.NamespacedName{Name: instName})
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── instance_expression_isolation: literal ${...} in a spec field ───────
	//
	// Instance values are data, not templates. A shell snippet containing
	// "${...}" placed in a spec field must survive a round trip into a managed
	// resource unchanged. Both backends resolve ${schema.spec.note} to the
	// literal value the instance carries, without re-interpolating it.
	DescribeTable("carries a literal ${...} in an instance spec field through to a resource",
		func(makeBackend func() rgdBackend) {
			const literal = "echo ${HOME} && echo ${NOT_A_RESOURCE.field}"
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityLiteralExpr"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "note": "string"},
							map[string]any{"ready": "${cm.metadata.name != ''}"},
						),
						generator.WithResource("cm", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-cm",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"note": "${schema.spec.note}"},
						}, nil, nil),
					)
				},
				map[string]any{"name": "literal-expr", "note": literal},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "literal-expr-cm"},
						func(u *unstructured.Unstructured) error {
							got, _, _ := unstructured.NestedString(u.Object, "data", "note")
							if got != literal {
								return &notYetError{msg: "cm.data.note=" + got + ", want literal snippet"}
							}
							return nil
						}, 60*time.Second)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── instance_expression_isolation: annotations with a literal ${...} ────
	//
	// Annotations are unconstrained by the generated CRD's schema, so a "${...}"
	// there is the least constrained case. An instance carrying one must still
	// reconcile normally and produce its child.
	DescribeTable("reconciles an instance whose annotations contain a literal ${...}",
		func(makeBackend func() rgdBackend) {
			runParityFixtureAnnotated(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityAnnotationExpr"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string"},
							map[string]any{"ready": "${cm.metadata.name != ''}"},
						),
						generator.WithResource("cm", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-cm",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"name": "${schema.spec.name}"},
						}, nil, nil),
					)
				},
				map[string]any{"name": "annotation-expr"},
				map[string]string{"example.com/command": "run ${SOME_VAR}"},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "annotation-expr-cm"},
						func(u *unstructured.Unstructured) error {
							got, _, _ := unstructured.NestedString(u.Object, "data", "name")
							if got != "annotation-expr" {
								return &notYetError{msg: "cm.data.name=" + got + ", want annotation-expr"}
							}
							return nil
						}, 60*time.Second)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── instance_expression_isolation: value naming a resource is NOT resolved ─
	//
	// "${secretCarrier.data.token}" is a valid template expression when an
	// author writes it. Supplied as instance data it must stay a plain string:
	// the instance's author must not read a resource's contents by naming it.
	// Both backends must carry the literal through and NOT substitute the
	// carrier's value.
	DescribeTable("does not resolve an instance value that names a definition resource",
		func(makeBackend func() rgdBackend) {
			const literal = "${secretCarrier.data.token}"
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityScopeIso"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "note": "string"},
							map[string]any{"ready": "${echo.metadata.name != ''}"},
						),
						generator.WithResource("secretCarrier", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-carrier",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"token": "carrier-token-value"},
						}, nil, nil),
						generator.WithResource("echo", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-echo",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"note": "${schema.spec.note}"},
						}, nil, nil),
					)
				},
				map[string]any{"name": "scope-iso", "note": literal},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "scope-iso-echo"},
						func(u *unstructured.Unstructured) error {
							got, _, _ := unstructured.NestedString(u.Object, "data", "note")
							if got == "carrier-token-value" {
								return &notYetError{msg: "echo.data.note leaked the carrier's value"}
							}
							if got != literal {
								return &notYetError{msg: "echo.data.note=" + got + ", want literal expression"}
							}
							return nil
						}, 60*time.Second)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── instance_resource_watch: reactive reconcile on a child resource ─────
	//
	// The controller watches its managed children; an out-of-band update to a
	// child's data is observed and the value the update wrote is durable (the
	// controller does not fight a data-only change it would rewrite). Both
	// backends observe the update.
	DescribeTable("watch behavior on a child resource",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityWatch"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "field1": "string"},
							map[string]any{"ready": "${res1.metadata.name != ''}"},
						),
						generator.WithResource("res1", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-cm",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"key": "${schema.spec.field1}"},
						}, nil, nil),
					)
				},
				map[string]any{"name": "watch", "field1": "foo"},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					ctx := envCtx()
					key := types.NamespacedName{Namespace: ns, Name: "watch-cm"}
					// The child converges with the templated value.
					awaitInstanceCR(t, configMapGVK, key,
						func(u *unstructured.Unstructured) error {
							got, _, _ := unstructured.NestedString(u.Object, "data", "key")
							if got != "foo" {
								return &notYetError{msg: "cm.data.key=" + got + ", want foo"}
							}
							return nil
						}, 60*time.Second)

					// Out-of-band update to the child's data.
					environment.Eventually(t, 10*time.Second, 250*time.Millisecond, func() error {
						latest := &unstructured.Unstructured{}
						latest.SetGroupVersionKind(configMapGVK)
						if err := env.Client.Get(ctx, key, latest); err != nil {
							return err
						}
						_ = unstructured.SetNestedField(latest.Object, "updated", "data", "key")
						return env.Client.Update(ctx, latest)
					})

					// The written value is durable and observed by the watch.
					awaitInstanceCR(t, configMapGVK, key,
						func(u *unstructured.Unstructured) error {
							got, _, _ := unstructured.NestedString(u.Object, "data", "key")
							if got != "updated" {
								return &notYetError{msg: "cm.data.key=" + got + ", want updated"}
							}
							return nil
						}, 15*time.Second)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)
})

// ── local helpers ─────────────────────────────────────────────────────────

// envCtx returns the environment's context, falling back to Background.
func envCtx() context.Context {
	if ctx := env.Context(); ctx != nil {
		return ctx
	}
	return context.Background()
}

// installParityCRD creates a minimal CRD (with a status subresource) at an
// arbitrary group/version for use as a template or external-ref target, waits
// for it to be Established, and registers cleanup.
func installParityCRD(
	ctx context.Context,
	group, version, kind, plural string,
	scope apiextensionsv1.ResourceScope,
) {
	t := GinkgoT()
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: plural + "." + group},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: group,
			Scope: scope,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Kind:     kind,
				ListKind: kind + "List",
				Plural:   plural,
				Singular: strings.ToLower(kind),
			},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name:    version,
				Served:  true,
				Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"spec": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"size": {Type: "string"},
								},
							},
							"status": {Type: "object", XPreserveUnknownFields: ptrTrue()},
						},
					},
				},
				Subresources: &apiextensionsv1.CustomResourceSubresources{
					Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
				},
			}},
		},
	}
	if err := env.Client.Create(ctx, crd); err != nil {
		t.Fatalf("create parity CRD %s: %v", crd.Name, err)
	}
	t.Cleanup(func() {
		_ = env.Client.Delete(context.Background(), &apiextensionsv1.CustomResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: crd.Name},
		})
	})
	awaitCRDEstablished(t, env, crd.Name)
}

// ackGVK returns the GVK for an ACK ec2 kind (installed in the core envtest).
func ackGVK(kind string) schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: "ec2.services.k8s.aws", Version: "v1alpha1", Kind: kind}
}

// ackResource builds an ACK ec2 resource template of the given kind, with a
// templated metadata.name and the provided spec.
func ackResource(kind, name string, spec map[string]any) map[string]any {
	return map[string]any{
		"apiVersion": "ec2.services.k8s.aws/v1alpha1",
		"kind":       kind,
		"metadata": map[string]any{
			"name":      name,
			"namespace": "${schema.metadata.namespace}",
		},
		"spec": spec,
	}
}

// mustSetStatus sets a single string field under .status on obj and updates the
// status subresource, failing the test on error.
func mustSetStatus(t GinkgoTInterface, obj *unstructured.Unstructured, value string, path ...string) {
	t.Helper()
	ctx := envCtx()
	// Re-fetch to avoid a stale resourceVersion.
	latest := &unstructured.Unstructured{}
	latest.SetGroupVersionKind(obj.GroupVersionKind())
	key := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
	if err := env.Client.Get(ctx, key, latest); err != nil {
		t.Fatalf("get %s for status update: %v", key, err)
	}
	if err := unstructured.SetNestedField(latest.Object, value, append([]string{"status"}, path...)...); err != nil {
		t.Fatalf("set status field: %v", err)
	}
	if err := env.Client.Status().Update(ctx, latest); err != nil {
		t.Fatalf("update status of %s: %v", key, err)
	}
}

// expectAbsent returns an error if the object exists (used inside Consistently
// to assert a resource has not yet been created).
func expectAbsent(gvk schema.GroupVersionKind, key types.NamespacedName) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	err := env.Client.Get(envCtx(), key, obj)
	if err == nil {
		return fmt.Errorf("%s %s exists but should be absent", gvk.Kind, key)
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// jobGVK is the batch/v1 Job GVK.
var jobGVK = schema.GroupVersionKind{Group: "batch", Version: "v1", Kind: "Job"}

// jobResource builds a batch/v1 Job template with a busybox container and the
// given (optional) annotations.
func jobResource(name string, annotations map[string]any) map[string]any {
	meta := map[string]any{
		"name":      name,
		"namespace": "${schema.metadata.namespace}",
	}
	if annotations != nil {
		meta["annotations"] = annotations
	}
	return map[string]any{
		"apiVersion": "batch/v1", "kind": "Job",
		"metadata": meta,
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"restartPolicy": "Never",
					"containers": []any{map[string]any{
						"name":    "sleeper",
						"image":   "busybox",
						"command": []any{"sh", "-c", "true"},
					}},
				},
			},
		},
	}
}

// setJobCompleted patches a Job's status so its completionTime is set,
// satisfying a readyWhen on status.completionTime (envtest has no scheduler).
func setJobCompleted(t GinkgoTInterface, job *unstructured.Unstructured) {
	t.Helper()
	ctx := envCtx()
	latest := &unstructured.Unstructured{}
	latest.SetGroupVersionKind(jobGVK)
	key := types.NamespacedName{Namespace: job.GetNamespace(), Name: job.GetName()}
	if err := env.Client.Get(ctx, key, latest); err != nil {
		t.Fatalf("get job %s: %v", key, err)
	}
	now := metav1.Now().Format(time.RFC3339)
	_ = unstructured.SetNestedField(latest.Object, now, "status", "startTime")
	_ = unstructured.SetNestedField(latest.Object, now, "status", "completionTime")
	_ = unstructured.SetNestedField(latest.Object, int64(1), "status", "succeeded")
	// The Job status validator requires SuccessCriteriaMet=true before
	// Complete=true; set both in order. The readyWhen only reads
	// status.completionTime, but the apiserver rejects a Complete condition
	// without its precursor.
	_ = unstructured.SetNestedSlice(latest.Object, []any{
		map[string]any{
			"type":               "SuccessCriteriaMet",
			"status":             "True",
			"lastProbeTime":      now,
			"lastTransitionTime": now,
			"reason":             "SuccessCriteriaMet",
			"message":            "Job success criteria met",
		},
		map[string]any{
			"type":               "Complete",
			"status":             "True",
			"lastProbeTime":      now,
			"lastTransitionTime": now,
			"reason":             "CompletionsReached",
			"message":            "Job completed",
		},
	}, "status", "conditions")
	if err := env.Client.Status().Update(ctx, latest); err != nil {
		t.Fatalf("update job status %s: %v", key, err)
	}
}

// runParityFixtureAnnotated is runParityFixture with instance annotations. It
// mirrors the harness's flow but sets metadata.annotations on the created
// instance (annotations are the least-constrained place a literal ${...} can
// appear).
func runParityFixtureAnnotated(
	be rgdBackend,
	makeRGD func(name string) *krov1alpha1.ResourceGraphDefinition,
	instanceSpec map[string]any,
	annotations map[string]string,
	assertChildren func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend),
) {
	t := GinkgoT()
	ns := createParityNamespace(t)
	rgd := makeRGD("parity" + rand.String(5))
	gvk := be.Deploy(t, rgd)

	instName := "inst-" + rand.String(5)
	inst := newInstanceCR(gvk, ns, instName, instanceSpec)
	if len(annotations) > 0 {
		inst.SetAnnotations(annotations)
	}
	ctx := envCtx()
	if err := env.Client.Create(ctx, inst); err != nil {
		t.Fatalf("%s: create instance: %v", be.Name(), err)
	}
	t.Cleanup(func() { _ = env.Client.Delete(context.Background(), inst) })

	if assertChildren != nil {
		assertChildren(t, ns, instName, gvk, be)
	}
	awaitInstanceReady(t, be, gvk, types.NamespacedName{Namespace: ns, Name: instName})
}
