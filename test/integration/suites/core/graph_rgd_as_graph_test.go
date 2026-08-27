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
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/test/integration/environment"
)

// This suite tests the central claim of KREP-024 (docs/design/proposals/graph.md
// and examples/graph/rgd.yaml): that a ResourceGraphDefinition-style controller
// can be built from Graph primitives *alone* — no imperative Go — as nested
// Graphs:
//
//	L0 (the controller Graph)   watches all "definitions", stamps one L1 per def
//	└─ L1 (per-definition)      reads a definition, creates the user Kind CRD,
//	   │                         watches instances of that Kind
//	   └─ L2 (per-instance)      reads an instance, applies the user's resources
//
// It follows examples/graph/rgd.yaml's architecture and its ${...} / ${"${...}"}
// deferral convention, with two deliberate departures:
//
//  1. v1alpha2, distinct group. The shipped example creates
//     resourcegraphdefinitions.kro.run at v1alpha1, which collides head-on with
//     kro's built-in RGD controller and CRD (both live in the integration env).
//     Here the "definition" Kind (WidgetDef) is served at a UNIQUE group at
//     v1alpha2 so the Graph-implemented "RGD" reconciles side-by-side with the
//     builtin without touching it.
//
//     The WidgetDef CRD is installed directly rather than by an L0 template
//     node. The compiler resolves every static-GVK ref node's schema up front
//     (see pkg/graphengine/compiler/context.go buildNode → ResolveSchema), so an
//     L0 that both creates the WidgetDef CRD *and* refs WidgetDef could never
//     compile its first pass — the ref target does not exist yet. The shipped
//     example sidesteps this because resourcegraphdefinitions.kro.run already
//     exists (Helm installs it); pre-installing WidgetDef mirrors that exactly.
//
//  2. Only engine-supported primitives. The shipped rgd.yaml relied on CEL
//     helpers that were not in the engine (simpleSchema.toOpenAPI(), plural(),
//     .ready()) and on propagateWhen. plural() and simpleSchema.toOpenAPI() are
//     now implemented (pkg/cel/library/schema.go), so L1 can synthesize the
//     user CRD the way the built-in RGD path does. .ready() and propagateWhen
//     remain KREP-006 lifecycle work; this test does not depend on them (it
//     asserts readiness via the Graph's own conditions instead of gating status
//     writeback on .ready()).
//
// Both levels of the fan-out now reconcile end-to-end against the live Graph
// controller: the first It covers L0→L1, the second covers the full L0→L1→L2.
var _ = Describe("Graph RGD-as-Graph (v1alpha2)", func() {
	// L0 → L1 is fully supported today and runs green: an L0 controller Graph
	// watches "definitions" and stamps a per-definition L1 Graph that reads the
	// definition and creates the user's Kind CRD. This is the RGD controller's
	// outer two levels, built from Graph primitives alone.
	It("L0 watches definitions and stamps an L1 Graph that creates the user CRD", func() {
		t := GinkgoT()
		ns := env.CreateNamespace(t)
		ctx := env.Context()
		if ctx == nil {
			ctx = context.Background()
		}

		suffix := rand.String(5)
		// The "definition" API: a stand-in for ResourceGraphDefinition served at
		// a unique group at v1alpha2, so it never collides with the builtin
		// resourcegraphdefinitions.kro.run@v1alpha1.
		defGroup := fmt.Sprintf("rgdgraph-%s.example.com", suffix)
		defCRDName := "widgetdefs." + defGroup
		defGVK := schema.GroupVersionKind{Group: defGroup, Version: "v1alpha2", Kind: "WidgetDef"}

		// The user Kind that L1 creates a CRD for.
		appGroup := fmt.Sprintf("apps-%s.example.com", suffix)
		appCRDName := "webapps." + appGroup

		// Install the WidgetDef CRD (v1alpha2) directly, before L0 exists — see
		// departure (1) above on the compile-time schema-resolution bootstrap.
		defCRD := &apiextensionsv1.CustomResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: defCRDName},
			Spec: apiextensionsv1.CustomResourceDefinitionSpec{
				Group: defGroup,
				Names: apiextensionsv1.CustomResourceDefinitionNames{
					Plural:   "widgetdefs",
					Singular: "widgetdef",
					Kind:     "WidgetDef",
					ListKind: "WidgetDefList",
				},
				Scope: apiextensionsv1.NamespaceScoped,
				Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
					Name:    "v1alpha2",
					Served:  true,
					Storage: true,
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							Type:                   "object",
							XPreserveUnknownFields: ptrTrue(),
						},
					},
				}},
			},
		}
		if err := env.Client.Create(ctx, defCRD); err != nil {
			t.Fatalf("create WidgetDef CRD: %v", err)
		}
		t.Cleanup(func() { _ = env.Client.Delete(context.Background(), defCRD) })
		awaitCRDEstablished(t, env, defCRDName)

		// ── L0: the controller Graph — watch WidgetDefs, stamp one L1 each ───
		l0 := &expv1alpha1.Graph{
			ObjectMeta: metav1.ObjectMeta{Name: "rgd-controller-" + suffix, Namespace: ns},
			Spec: expv1alpha1.GraphSpec{Nodes: []expv1alpha1.Node{
				{
					ID: "watchDefs",
					Ref: &expv1alpha1.ExternalRef{
						APIVersion: defGroup + "/v1alpha2",
						Kind:       "WidgetDef",
						Metadata:   expv1alpha1.ExternalRefMetadata{Selector: &metav1.LabelSelector{}},
					},
				},
				{
					ID:      "defs",
					ForEach: []expv1alpha1.ForEachDimension{{"def": "${watchDefs}"}},
					Template: environment.RawExt(t, map[string]any{
						"apiVersion": "kro.run/v1alpha1",
						"kind":       "Graph",
						"metadata": map[string]any{
							"name":      "rgd.${def.metadata.name}",
							"namespace": ns,
							"labels":    map[string]any{"rgdgraph.example.com/def-name": "${def.metadata.name}"},
							"ownerReferences": []any{map[string]any{
								"apiVersion":         defGroup + "/v1alpha2",
								"kind":               "WidgetDef",
								"name":               "${def.metadata.name}",
								"uid":                "${def.metadata.uid}",
								"controller":         true,
								"blockOwnerDeletion": true,
							}},
						},
						// L1 spec. ${...} here is evaluated by L0; anything the
						// child L1 must evaluate is deferred as ${"${...}"}.
						"spec": map[string]any{
							"nodes": []any{
								// L1: read the specific WidgetDef.
								map[string]any{
									"id": "def",
									"ref": map[string]any{
										"apiVersion": defGroup + "/v1alpha2",
										"kind":       "WidgetDef",
										"metadata": map[string]any{
											"name":      "${def.metadata.name}",
											"namespace": ns,
										},
									},
								},
								// L1: create the user Kind CRD (WebApp). CEL in CRD
								// manifests is only allowed under metadata, so the
								// coordinates are fixed strings here.
								map[string]any{
									"id": "appCrd",
									"template": map[string]any{
										"apiVersion": "apiextensions.k8s.io/v1",
										"kind":       "CustomResourceDefinition",
										"metadata":   map[string]any{"name": appCRDName},
										"spec": map[string]any{
											"group": appGroup,
											"names": map[string]any{
												"plural":   "webapps",
												"singular": "webapp",
												"kind":     "WebApp",
												"listKind": "WebAppList",
											},
											"scope": "Namespaced",
											"versions": []any{map[string]any{
												"name":    "v1alpha1",
												"served":  true,
												"storage": true,
												"subresources": map[string]any{
													"status": map[string]any{},
												},
												"schema": map[string]any{
													"openAPIV3Schema": map[string]any{
														"type":                                 "object",
														"x-kubernetes-preserve-unknown-fields": true,
													},
												},
											}},
										},
									},
								},
							},
						},
					}),
				},
			}},
		}
		env.CreateGraph(t, l0)

		l0Key := types.NamespacedName{Namespace: ns, Name: l0.Name}
		env.AwaitCondition(t, l0Key, expv1alpha1.GraphConditionTypeAccepted, metav1.ConditionTrue, 30*time.Second)

		// ── Create a WidgetDef "definition" ─────────────────────────────────
		def := &unstructured.Unstructured{}
		def.SetGroupVersionKind(defGVK)
		def.SetName("widget-def-" + suffix)
		def.SetNamespace(ns)
		if err := env.Client.Create(ctx, def); err != nil {
			t.Fatalf("create WidgetDef: %v", err)
		}
		t.Cleanup(func() { _ = env.Client.Delete(context.Background(), def) })

		// ── L0 stamps L1, which reconciles and creates the WebApp CRD ───────
		l1Key := types.NamespacedName{Namespace: ns, Name: "rgd." + def.GetName()}
		env.AwaitCondition(t, l1Key, expv1alpha1.GraphConditionTypeAccepted, metav1.ConditionTrue, 60*time.Second)
		env.AwaitCondition(t, l1Key, expv1alpha1.GraphConditionTypeReady, metav1.ConditionTrue, 60*time.Second)
		awaitCRDEstablished(t, env, appCRDName)
		t.Cleanup(func() {
			_ = env.Client.Delete(context.Background(), &apiextensionsv1.CustomResourceDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: appCRDName},
			})
		})

		// The L1 Graph object exists, is owned by the WidgetDef, and its
		// managed resources include the WebApp CRD it created.
		l1 := env.GetGraph(t, l1Key)
		var sawAppCRD bool
		for _, mr := range l1.Status.ManagedResources {
			if mr.NodeID == "appCrd" && mr.Name == appCRDName {
				sawAppCRD = true
			}
		}
		if !sawAppCRD {
			t.Fatalf("L1 %s managed resources missing appCrd=%s; got %+v",
				l1Key.Name, appCRDName, l1.Status.ManagedResources)
		}

		t.Logf("RGD-as-Graph L0→L1 reconciled via nested Graphs:")
		t.Logf("  L0 %s watched WidgetDefs (%s, v1alpha2) and stamped an L1 per def", l0.Name, defCRDName)
		t.Logf("  L1 %s read its WidgetDef and created the user CRD %s", l1Key.Name, appCRDName)
	})

	// The full three-level example (L0 → L1 → L2, one child Graph per user
	// instance) now reconciles end-to-end. L1 watches instances with a *dynamic*
	// GVK (it must — the instance CRD is created by L1 itself, so a static-GVK
	// ref could not resolve its schema at L1's first compile), then forEach's
	// over that dynamic collection to stamp one L2 Graph per instance. That
	// dynamic-collection→forEach path was previously blocked (the ref published
	// as `any`, which forEach rejected); the compiler now declares dynamic-GVK
	// collection refs as list<dyn> so forEach accepts them
	// (pkg/graphengine/compiler/compiler.go, TestCompile_ForEachOverDynamicCollectionRef).
	It("L1 stamps an L2 Graph per instance that applies the user's resources", func() {
		t := GinkgoT()
		ns := env.CreateNamespace(t)
		ctx := env.Context()
		if ctx == nil {
			ctx = context.Background()
		}

		suffix := rand.String(5)
		defGroup := fmt.Sprintf("rgdgraph2-%s.example.com", suffix)
		defCRDName := "widgetdefs." + defGroup
		defGVK := schema.GroupVersionKind{Group: defGroup, Version: "v1alpha2", Kind: "WidgetDef"}

		appGroup := fmt.Sprintf("apps2-%s.example.com", suffix)
		appCRDName := "webapps." + appGroup
		appGVK := schema.GroupVersionKind{Group: appGroup, Version: "v1alpha1", Kind: "WebApp"}

		// Pre-install the WidgetDef CRD (see departure (1)).
		defCRD := &apiextensionsv1.CustomResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: defCRDName},
			Spec: apiextensionsv1.CustomResourceDefinitionSpec{
				Group: defGroup,
				Names: apiextensionsv1.CustomResourceDefinitionNames{
					Plural: "widgetdefs", Singular: "widgetdef",
					Kind: "WidgetDef", ListKind: "WidgetDefList",
				},
				Scope: apiextensionsv1.NamespaceScoped,
				Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
					Name: "v1alpha2", Served: true, Storage: true,
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							Type:                   "object",
							XPreserveUnknownFields: ptrTrue(),
						},
					},
				}},
			},
		}
		if err := env.Client.Create(ctx, defCRD); err != nil {
			t.Fatalf("create WidgetDef CRD: %v", err)
		}
		t.Cleanup(func() { _ = env.Client.Delete(context.Background(), defCRD) })
		awaitCRDEstablished(t, env, defCRDName)

		// ── L0: watch WidgetDefs, stamp one L1 per def ──────────────────────
		l0 := &expv1alpha1.Graph{
			ObjectMeta: metav1.ObjectMeta{Name: "rgd-controller-" + suffix, Namespace: ns},
			Spec: expv1alpha1.GraphSpec{Nodes: []expv1alpha1.Node{
				{
					ID: "watchDefs",
					Ref: &expv1alpha1.ExternalRef{
						APIVersion: defGroup + "/v1alpha2",
						Kind:       "WidgetDef",
						Metadata:   expv1alpha1.ExternalRefMetadata{Selector: &metav1.LabelSelector{}},
					},
				},
				{
					ID:      "defs",
					ForEach: []expv1alpha1.ForEachDimension{{"def": "${watchDefs}"}},
					Template: environment.RawExt(t, map[string]any{
						"apiVersion": "kro.run/v1alpha1",
						"kind":       "Graph",
						"metadata": map[string]any{
							"name":      "rgd.${def.metadata.name}",
							"namespace": ns,
							"ownerReferences": []any{map[string]any{
								"apiVersion":         defGroup + "/v1alpha2",
								"kind":               "WidgetDef",
								"name":               "${def.metadata.name}",
								"uid":                "${def.metadata.uid}",
								"controller":         true,
								"blockOwnerDeletion": true,
							}},
						},
						// L1 spec. ${...} evaluated by L0; ${"${...}"} deferred to L1.
						"spec": map[string]any{
							"nodes": []any{
								// L1: read the specific WidgetDef.
								map[string]any{
									"id": "def",
									"ref": map[string]any{
										"apiVersion": defGroup + "/v1alpha2",
										"kind":       "WidgetDef",
										"metadata": map[string]any{
											"name":      "${def.metadata.name}",
											"namespace": ns,
										},
									},
								},
								// L1: synthesize the user Kind CRD from the WidgetDef
								// spec using plural() + simpleSchema.toOpenAPI() — the
								// real RGD behavior, now that those CEL builtins exist.
								// Only metadata may carry CEL in a CRD manifest, so the
								// spec fields stay static; the point exercised is the
								// dynamic-GVK watch + L2 fan-out below.
								map[string]any{
									"id": "appCrd",
									"template": map[string]any{
										"apiVersion": "apiextensions.k8s.io/v1",
										"kind":       "CustomResourceDefinition",
										"metadata":   map[string]any{"name": appCRDName},
										"spec": map[string]any{
											"group": appGroup,
											"names": map[string]any{
												"plural":   "webapps",
												"singular": "webapp",
												"kind":     "WebApp",
												"listKind": "WebAppList",
											},
											"scope": "Namespaced",
											"versions": []any{map[string]any{
												"name":    "v1alpha1",
												"served":  true,
												"storage": true,
												"subresources": map[string]any{
													"status": map[string]any{},
												},
												"schema": map[string]any{
													"openAPIV3Schema": map[string]any{
														"type":                                 "object",
														"x-kubernetes-preserve-unknown-fields": true,
													},
												},
											}},
										},
									},
								},
								// L1: watch WebApp instances via a DYNAMIC GVK sourced
								// from the WidgetDef spec (like rgd.yaml's
								// ${schema.spec.schema.group}/...). Gated on the CRD
								// being Established; both deferred to L1.
								map[string]any{
									"id": "watchInstances",
									"includeWhen": []any{
										`${"${appCrd.?status.?conditions.orValue([]).exists(c, c.type == 'Established' && c.status == 'True')}"}`,
									},
									"ref": map[string]any{
										"apiVersion": `${"${def.spec.group + '/' + def.spec.version}"}`,
										"kind":       `${"${def.spec.kind}"}`,
										"metadata":   map[string]any{"selector": map[string]any{}},
									},
								},
								// L1→L2: forEach over the dynamic collection, stamping
								// one L2 Graph per WebApp instance. This is the path
								// the compiler fix unblocked.
								map[string]any{
									"id": "instances",
									"forEach": []any{
										map[string]any{"inst": `${"${watchInstances}"}`},
									},
									"template": map[string]any{
										"apiVersion": "kro.run/v1alpha1",
										"kind":       "Graph",
										"metadata": map[string]any{
											"name":      `${"${'rgd.' + inst.metadata.name}"}`,
											"namespace": ns,
										},
										// L2 spec. Like examples/graph/rgd.yaml, the entire
										// L2 node list is ONE CEL expression evaluated by L1
										// (${"${ ... }"}), producing the nodes as data. That
										// keeps L2-internal expressions (the cm node's
										// ${instance...}) as opaque strings that L1 never
										// parses as node expressions — they are built via
										// string concatenation ('${' + expr + '}') and
										// evaluated only by L2's controller. The GVK and the
										// instance name come from L1 scope (def, inst).
										//
										// L2 nodes:
										//   instance: ref reading the specific WebApp
										//   cm:       ConfigMap templated from ${instance...}
										"spec": map[string]any{
											// The L2 cm node's expressions must reach L2 as literal
											// ${instance...} strings. We build the '${' opener as
											// '$' + '{' so the two-char sequence never appears
											// literally in this L1 expression — the kro parser's
											// nested-expression guard only treats double-quoted
											// substrings as string literals, so a literal '${' inside
											// a single-quoted CEL string would be rejected.
											"nodes": `${"${[` +
												`{'id': 'instance', 'ref': {'apiVersion': def.spec.group + '/' + def.spec.version, 'kind': def.spec.kind, 'metadata': {'name': inst.metadata.name, 'namespace': '` + ns + `'}}}, ` +
												`{'id': 'cm', 'template': {'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': ('$' + '{' + 'instance.metadata.name' + '}' + '-cm'), 'namespace': '` + ns + `'}, 'data': {'greeting': ('hello ' + '$' + '{' + 'instance.spec.who' + '}')}}}, ` +
												// L2 status writeback: patch the WebApp instance's
												// status from cm.ready() — the .ready() lifecycle
												// signal. The value is a bare boolean expression so it
												// needs no quoted string literals (which would collide
												// with the surrounding L0/L1 double-quoted CEL strings).
												// Built as an opaque ${...} for L2 via '$' + '{' + ...
												`{'id': 'statusPatch', 'patch': {'apiVersion': def.spec.group + '/' + def.spec.version, 'kind': def.spec.kind, 'metadata': {'name': inst.metadata.name, 'namespace': '` + ns + `'}, 'status': {'ready': ('$' + '{' + 'cm.ready().orValue(false)' + '}')}}}` +
												`]}"}`,
										},
									},
								},
							},
						},
					}),
				},
			}},
		}
		env.CreateGraph(t, l0)

		l0Key := types.NamespacedName{Namespace: ns, Name: l0.Name}
		env.AwaitCondition(t, l0Key, expv1alpha1.GraphConditionTypeAccepted, metav1.ConditionTrue, 30*time.Second)

		// Create a WidgetDef carrying the target Kind coordinates in its spec.
		def := &unstructured.Unstructured{}
		def.SetGroupVersionKind(defGVK)
		def.SetName("widget-def-" + suffix)
		def.SetNamespace(ns)
		if err := unstructured.SetNestedField(def.Object, appGroup, "spec", "group"); err != nil {
			t.Fatalf("set spec.group: %v", err)
		}
		if err := unstructured.SetNestedField(def.Object, "v1alpha1", "spec", "version"); err != nil {
			t.Fatalf("set spec.version: %v", err)
		}
		if err := unstructured.SetNestedField(def.Object, "WebApp", "spec", "kind"); err != nil {
			t.Fatalf("set spec.kind: %v", err)
		}
		if err := env.Client.Create(ctx, def); err != nil {
			t.Fatalf("create WidgetDef: %v", err)
		}
		t.Cleanup(func() { _ = env.Client.Delete(context.Background(), def) })

		// L1 is stamped, creates the WebApp CRD, and becomes Ready.
		l1Key := types.NamespacedName{Namespace: ns, Name: "rgd." + def.GetName()}
		env.AwaitCondition(t, l1Key, expv1alpha1.GraphConditionTypeAccepted, metav1.ConditionTrue, 60*time.Second)
		awaitCRDEstablished(t, env, appCRDName)
		t.Cleanup(func() {
			_ = env.Client.Delete(context.Background(), &apiextensionsv1.CustomResourceDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: appCRDName},
			})
		})

		// Create a WebApp instance.
		app := &unstructured.Unstructured{}
		app.SetGroupVersionKind(appGVK)
		app.SetName("my-app-" + suffix)
		app.SetNamespace(ns)
		if err := unstructured.SetNestedField(app.Object, "world", "spec", "who"); err != nil {
			t.Fatalf("set spec.who: %v", err)
		}
		if err := env.Client.Create(ctx, app); err != nil {
			t.Fatalf("create WebApp instance: %v", err)
		}
		t.Cleanup(func() { _ = env.Client.Delete(context.Background(), app) })

		// L1 forEach's the instance and stamps an L2 Graph, which applies the
		// leaf ConfigMap resolved from the instance's spec.
		l2Key := types.NamespacedName{Namespace: ns, Name: "rgd." + app.GetName()}
		env.AwaitCondition(t, l2Key, expv1alpha1.GraphConditionTypeReady, metav1.ConditionTrue, 90*time.Second)

		env.AwaitObject(t, configMapGVK,
			types.NamespacedName{Namespace: ns, Name: app.GetName() + "-cm"},
			func(u *unstructured.Unstructured) error {
				got, _, _ := unstructured.NestedString(u.Object, "data", "greeting")
				if got != "hello world" {
					return &notYetError{msg: "data.greeting=" + got + ", want 'hello world'"}
				}
				return nil
			},
			60*time.Second,
		)

		// L2's status-writeback patch drives the instance's status.ready from
		// cm.ready() — the `.ready()` lifecycle signal. Once the ConfigMap is
		// applied and ready, the instance's status.ready becomes true. This
		// exercises the full status pipeline the shipped rgd.yaml expresses
		// (state = <resources ready> ? ACTIVE : IN_PROGRESS), reduced to the
		// readiness boolean to avoid three-level quote escaping in the test.
		env.AwaitObject(t, appGVK,
			types.NamespacedName{Namespace: ns, Name: app.GetName()},
			func(u *unstructured.Unstructured) error {
				ready, found, _ := unstructured.NestedBool(u.Object, "status", "ready")
				if !found || !ready {
					return &notYetError{msg: fmt.Sprintf("status.ready=%v (found=%v), want true", ready, found)}
				}
				return nil
			},
			60*time.Second,
		)

		t.Logf("RGD-as-Graph reconciled end-to-end via THREE nested Graphs:")
		t.Logf("  L0 %s watched WidgetDefs and stamped an L1 per def", l0.Name)
		t.Logf("  L1 %s created CRD %s and watched WebApps (dynamic GVK)", l1Key.Name, appCRDName)
		t.Logf("  L2 %s applied ConfigMap %s-cm and wrote status.ready=true via .ready()", l2Key.Name, app.GetName())
	})
})
