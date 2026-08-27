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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"

	krov1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
)

// Parity ports of the status-projection suites:
//   - status_test.go
//   - status_schema_ref_test.go
//   - status_array_projection_test.go
//   - kubernetes_time_test.go
//
// ── Parity boundary shared by every scenario in this file ──────────────────
//
// These source specs assert the instance's own user-defined status.* fields
// (configmapRef, echoHost, url, endpoints, creationTimestamp, …). Those fields
// are produced by the built-in RGD controller's status projection, which
// evaluates the RGD schema's `status` block against the resolved resource
// graph and writes it back onto the instance.
//
// The graph backend (rgd_graph_backend_test.go) does NOT reproduce that
// projection: its L2 statusWriteback node writes only `status.ready` (bool)
// from an AND-fold of resource readiness. It carries the RGD's *resource*
// templates through verbatim, so every child resource converges identically —
// that is the real shared contract these ports exercise on both backends — but
// the schema's user status block is never evaluated for the graph backend.
//
// Therefore each scenario:
//   - asserts child-resource convergence with the correct resolved field
//     values for BOTH backends (the genuine parity), and
//   - asserts the user-defined status.* field ONLY for the built-in backend,
//     branched on isBuiltin(be) and documented here as the parity boundary.
//
// The harness itself already asserts readiness under each backend's own
// convention (builtin: status.state=ACTIVE; graph: status.ready=true).

var _ = Describe("RGD parity: status projection", func() {
	// ── status_test.go: string-template interpolation in instance status ────
	//
	// A ConfigMap named from spec.name, with a status field built from a
	// multi-expression string template. Shared parity: the ConfigMap converges
	// named "my-configmap". Built-in only: status.configmapRef is interpolated
	// to "my-configmap-in-<namespace>".
	DescribeTable("string templates interpolated in instance status",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityStatusInterp"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string"},
							map[string]any{
								"configmapRef": "${configmap.metadata.name}-in-${configmap.metadata.namespace}",
							},
						),
						generator.WithResource("configmap", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"key": "value"},
						}, nil, nil),
					)
				},
				map[string]any{"name": "my-configmap"},
				func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
					// Shared: the ConfigMap converges named from spec.
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "my-configmap"},
						nil, 60*time.Second)

					// Parity boundary: only the built-in controller projects the
					// user status block; the graph backend writes only
					// status.ready.
					if isBuiltin(be) {
						want := fmt.Sprintf("my-configmap-in-%s", ns)
						awaitInstanceCR(t, gvk,
							types.NamespacedName{Namespace: ns, Name: instanceName},
							func(u *unstructured.Unstructured) error {
								got, found, _ := unstructured.NestedString(u.Object, "status", "configmapRef")
								if !found || got != want {
									return &notYetError{msg: fmt.Sprintf("status.configmapRef=%q found=%v, want %q", got, found, want)}
								}
								return nil
							},
							60*time.Second)
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── status_test.go: partial status resolution via includeWhen ───────────
	//
	// Three ConfigMaps, each gated by an includeWhen from the spec, plus three
	// cascading status fields. The source spec drives five spec-update states
	// asserting status fields appear/disappear with their dependencies.
	//
	// Shared parity: includeWhen inclusion/exclusion of the child ConfigMaps.
	// We assert both backends realize the enabled children and omit the
	// disabled ones for the initial spec (cm1 on, cm2/cm3 off). Built-in only:
	// the cascading status.fieldN presence/values (user status projection).
	//
	// The full multi-state spec-mutation walk from the source is exercised
	// only against the built-in backend (it is entirely about user-status
	// presence semantics, which the graph backend does not reproduce); both
	// backends are still driven through the shared includeWhen child contract.
	DescribeTable("partial status resolution follows includeWhen dependencies",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityStatusPartial"+rand.String(4), "v1alpha1",
							map[string]any{
								"name":       "string",
								"includeCm1": "boolean",
								"includeCm2": "boolean",
								"includeCm3": "boolean",
							},
							map[string]any{
								"field1": "${cm1.data.value}",
								"field2": "${cm1.data.value}-${cm2.data.value}",
								"field3": "${cm1.data.value}-${cm2.data.value}-${cm3.data.value}",
							},
						),
						generator.WithResource("cm1", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-cm1",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"value": "one"},
						}, nil, []string{"${schema.spec.includeCm1}"}),
						generator.WithResource("cm2", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-cm2",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"value": "two"},
						}, nil, []string{"${schema.spec.includeCm2}"}),
						generator.WithResource("cm3", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-cm3",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"value": "three"},
						}, nil, []string{"${schema.spec.includeCm3}"}),
					)
				},
				// cm1 enabled, cm2/cm3 disabled: a stable, converging state
				// (unlike the source's initial all-off state, which never
				// reaches ready and so would not satisfy the harness's ready
				// assertion). field1 resolves; field2/field3 stay absent.
				map[string]any{"name": "partial", "includeCm1": true, "includeCm2": false, "includeCm3": false},
				func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
					ctx := env.Context()
					if ctx == nil {
						ctx = context.Background()
					}

					// Shared: cm1 is included; cm2/cm3 are excluded.
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "partial-cm1"},
						nil, 60*time.Second)
					for _, absent := range []string{"partial-cm2", "partial-cm3"} {
						cm := &unstructured.Unstructured{}
						cm.SetGroupVersionKind(configMapGVK)
						err := env.Client.Get(ctx, types.NamespacedName{Namespace: ns, Name: absent}, cm)
						if err == nil {
							t.Fatalf("%s: %s should be excluded by includeWhen", be.Name(), absent)
						}
					}

					// Parity boundary: cascading user status fields are built-in
					// only. field1 present with cm1's value; field2/field3 absent.
					if isBuiltin(be) {
						awaitInstanceCR(t, gvk,
							types.NamespacedName{Namespace: ns, Name: instanceName},
							func(u *unstructured.Unstructured) error {
								status, _, _ := unstructured.NestedMap(u.Object, "status")
								field1, has1 := status["field1"]
								_, has2 := status["field2"]
								_, has3 := status["field3"]
								if !has1 || field1 != "one" {
									return &notYetError{msg: fmt.Sprintf("status.field1=%v has=%v, want one", field1, has1)}
								}
								if has2 || has3 {
									return &notYetError{msg: fmt.Sprintf("field2/field3 should be absent (has2=%v has3=%v)", has2, has3)}
								}
								return nil
							},
							60*time.Second)
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── status_schema_ref_test.go: status from a schema spec field only ─────
	//
	// status.echoHost references schema.spec.host directly (no resource in the
	// expression). Shared parity: a ConfigMap named from the host converges.
	// Built-in only: status.echoHost echoes the host.
	DescribeTable("status populated from a schema spec field only",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityStatusSchemaOnly"+rand.String(4), "v1alpha1",
							map[string]any{"host": "string"},
							map[string]any{"echoHost": "${schema.spec.host}"},
						),
						generator.WithResource("cm", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.host}",
								"namespace": "${schema.metadata.namespace}",
							},
						}, nil, nil),
					)
				},
				map[string]any{"host": "example.com"},
				func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "example.com"},
						nil, 60*time.Second)

					if isBuiltin(be) {
						awaitInstanceCR(t, gvk,
							types.NamespacedName{Namespace: ns, Name: instanceName},
							func(u *unstructured.Unstructured) error {
								got, found, _ := unstructured.NestedString(u.Object, "status", "echoHost")
								if !found || got != "example.com" {
									return &notYetError{msg: fmt.Sprintf("status.echoHost=%q found=%v, want example.com", got, found)}
								}
								return nil
							},
							60*time.Second)
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── status_schema_ref_test.go: status mixing resource + schema fields ────
	//
	// status.url combines a resource field with a schema spec field.
	// Shared parity: the ConfigMap converges. Built-in only: status.url.
	DescribeTable("status mixing a resource field and a schema spec field",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityStatusSchemaMixed"+rand.String(4), "v1alpha1",
							map[string]any{"path": "string"},
							map[string]any{
								"url": "${configmap.metadata.name + '/' + schema.spec.path}",
							},
						),
						generator.WithResource("configmap", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "my-configmap",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"key": "value"},
						}, nil, nil),
					)
				},
				map[string]any{"path": "api/v1"},
				func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "my-configmap"},
						nil, 60*time.Second)

					if isBuiltin(be) {
						awaitInstanceCR(t, gvk,
							types.NamespacedName{Namespace: ns, Name: instanceName},
							func(u *unstructured.Unstructured) error {
								got, found, _ := unstructured.NestedString(u.Object, "status", "url")
								if !found || got != "my-configmap/api/v1" {
									return &notYetError{msg: fmt.Sprintf("status.url=%q found=%v, want my-configmap/api/v1", got, found)}
								}
								return nil
							},
							60*time.Second)
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── status_schema_ref_test.go: schema-referencing status tracks spec ─────
	//
	// After a spec.label change, the schema-referencing status field updates.
	// Shared parity: a static ConfigMap converges. Built-in only: status.echoLabel
	// tracks spec.label ("first" then "second" after an update).
	DescribeTable("schema-referencing status field tracks a spec update",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityStatusSchemaUpdate"+rand.String(4), "v1alpha1",
							map[string]any{"label": "string"},
							map[string]any{"echoLabel": "${schema.spec.label}"},
						),
						generator.WithResource("cm", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "static-cm",
								"namespace": "${schema.metadata.namespace}",
							},
						}, nil, nil),
					)
				},
				map[string]any{"label": "first"},
				func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
					ctx := env.Context()
					if ctx == nil {
						ctx = context.Background()
					}

					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "static-cm"},
						nil, 60*time.Second)

					if !isBuiltin(be) {
						return // user status projection + its update are built-in only
					}

					instKey := types.NamespacedName{Namespace: ns, Name: instanceName}
					awaitInstanceCR(t, gvk, instKey,
						func(u *unstructured.Unstructured) error {
							got, found, _ := unstructured.NestedString(u.Object, "status", "echoLabel")
							if !found || got != "first" {
								return &notYetError{msg: fmt.Sprintf("status.echoLabel=%q found=%v, want first", got, found)}
							}
							return nil
						},
						60*time.Second)

					// Update spec.label and verify status tracks it.
					cur := &unstructured.Unstructured{}
					cur.SetGroupVersionKind(gvk)
					if err := env.Client.Get(ctx, instKey, cur); err != nil {
						t.Fatalf("get instance for update: %v", err)
					}
					if err := unstructured.SetNestedField(cur.Object, "second", "spec", "label"); err != nil {
						t.Fatalf("set spec.label: %v", err)
					}
					if err := env.Client.Update(ctx, cur); err != nil {
						t.Fatalf("update instance: %v", err)
					}

					awaitInstanceCR(t, gvk, instKey,
						func(u *unstructured.Unstructured) error {
							got, found, _ := unstructured.NestedString(u.Object, "status", "echoLabel")
							if !found || got != "second" {
								return &notYetError{msg: fmt.Sprintf("status.echoLabel=%q found=%v, want second", got, found)}
							}
							return nil
						},
						60*time.Second)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── status_array_projection_test.go: top-level array of expressions ──────
	//
	// status.endpoints is a list of two resource-name expressions and must be
	// projected as an array (not scalar keys). Shared parity: both ConfigMaps
	// converge. Built-in only: status.endpoints is an array of the two names.
	DescribeTable("status top-level array of expressions is projected as an array",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityStatusArray"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string"},
							map[string]any{
								"endpoints": []any{
									"${primary.metadata.name}",
									"${secondary.metadata.name}",
								},
							},
						),
						generator.WithResource("primary", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-primary",
								"namespace": "${schema.metadata.namespace}",
							},
						}, nil, nil),
						generator.WithResource("secondary", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-secondary",
								"namespace": "${schema.metadata.namespace}",
							},
						}, nil, nil),
					)
				},
				map[string]any{"name": "status-array"},
				func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "status-array-primary"}, nil, 60*time.Second)
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "status-array-secondary"}, nil, 60*time.Second)

					if isBuiltin(be) {
						awaitInstanceCR(t, gvk,
							types.NamespacedName{Namespace: ns, Name: instanceName},
							func(u *unstructured.Unstructured) error {
								eps, found, err := unstructured.NestedStringSlice(u.Object, "status", "endpoints")
								if err != nil || !found {
									return &notYetError{msg: fmt.Sprintf("status.endpoints not an array (found=%v err=%v)", found, err)}
								}
								want := map[string]bool{"status-array-primary": false, "status-array-secondary": false}
								for _, e := range eps {
									if _, ok := want[e]; !ok {
										return &notYetError{msg: "unexpected endpoint " + e}
									}
									want[e] = true
								}
								for name, seen := range want {
									if !seen {
										return &notYetError{msg: "missing endpoint " + name}
									}
								}
								return nil
							},
							60*time.Second)
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── status_array_projection_test.go: array nested under an object ────────
	//
	// status.network.names is an array nested under an object and must be
	// projected as an array. Shared parity: the ConfigMap converges. Built-in
	// only: status.network.names is an array of the one name.
	DescribeTable("status array nested under an object is projected as an array",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityStatusNestedArray"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string"},
							map[string]any{
								"network": map[string]any{
									"names": []any{"${primary.metadata.name}"},
								},
							},
						),
						generator.WithResource("primary", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-primary",
								"namespace": "${schema.metadata.namespace}",
							},
						}, nil, nil),
					)
				},
				map[string]any{"name": "status-nested-array"},
				func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "status-nested-array-primary"}, nil, 60*time.Second)

					if isBuiltin(be) {
						awaitInstanceCR(t, gvk,
							types.NamespacedName{Namespace: ns, Name: instanceName},
							func(u *unstructured.Unstructured) error {
								names, found, err := unstructured.NestedStringSlice(u.Object, "status", "network", "names")
								if err != nil || !found {
									return &notYetError{msg: fmt.Sprintf("status.network.names not an array (found=%v err=%v)", found, err)}
								}
								if len(names) != 1 || names[0] != "status-nested-array-primary" {
									return &notYetError{msg: fmt.Sprintf("status.network.names=%v, want [status-nested-array-primary]", names)}
								}
								return nil
							},
							60*time.Second)
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── kubernetes_time_test.go: status.creationTimestamp from a Secret ──────
	//
	// status.creationTimestamp echoes secret.metadata.creationTimestamp
	// (RFC3339). Shared parity: the Secret converges named from spec.name.
	// Built-in only: status.creationTimestamp equals the Secret's RFC3339
	// creationTimestamp.
	DescribeTable("status.creationTimestamp projected from a resource timestamp",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityTimestamp"+rand.String(4), "v1alpha1",
							map[string]any{"name": `string | default="ts-secret"`},
							map[string]any{
								"creationTimestamp": "${secret.metadata.creationTimestamp}",
							},
						),
						generator.WithResource("secret", map[string]any{
							"apiVersion": "v1", "kind": "Secret",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"stringData": map[string]any{"hello": "world"},
						}, nil, nil),
					)
				},
				map[string]any{"name": "ts-secret"},
				func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
					secretKey := types.NamespacedName{Namespace: ns, Name: "ts-secret"}
					sec := awaitInstanceCR(t, secretGVK, secretKey, nil, 60*time.Second)

					if !isBuiltin(be) {
						return // status.creationTimestamp projection is built-in only
					}

					// Reproduce the source's RFC3339-in-UTC formatting from the
					// Secret's own creationTimestamp.
					created := sec.GetCreationTimestamp()
					wantTS := created.Time.UTC().Format(time.RFC3339)

					awaitInstanceCR(t, gvk,
						types.NamespacedName{Namespace: ns, Name: instanceName},
						func(u *unstructured.Unstructured) error {
							got, found, _ := unstructured.NestedString(u.Object, "status", "creationTimestamp")
							if !found || got != wantTS {
								return &notYetError{msg: fmt.Sprintf("status.creationTimestamp=%q found=%v, want %q", got, found, wantTS)}
							}
							return nil
						},
						60*time.Second)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)
})

// N/A notes (scenarios/aspects intentionally not ported as separate specs):
//
//   - status_test.go "should only show status fields when all referenced
//     resources are available" — the source drives FIVE spec-mutation states
//     (all-off → cm1 → cm1+cm2 → all → cm2-off) asserting user-status field
//     appearance/disappearance at each step. That is purely built-in RGD
//     status-projection semantics with no graph-backend analog (the graph
//     backend never projects user status fields), and its all-off initial
//     state never converges to ready (incompatible with the harness's ready
//     assertion). The ported "partial status resolution follows includeWhen
//     dependencies" scenario captures the shared, converging slice: includeWhen
//     child inclusion/exclusion on both backends, plus the built-in cascading
//     status.field values for the stable cm1-on state. The full multi-state
//     walk is left to the original built-in-only spec.
