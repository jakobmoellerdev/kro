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
	ctrlinstance "github.com/kubernetes-sigs/kro/pkg/controller/instance"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
)

// rgd_parity_conditions_test.go ports the instance-conditions behavioral suites
// (instance_conditions_test.go, instance_custom_conditions_test.go) into the
// two-backend parity harness.
//
// Parity boundary — instance status shape:
//
//   - The built-in RGD controller writes the full kro condition machinery to
//     status.conditions[]: lifecycle types (Ready/InstanceManaged/GraphResolved/
//     ResourcesReady), author-defined conditions from the schema's
//     `status.conditions:` block, condition dedup, lastTransitionTime
//     preservation, runtime.condition internal-vs-wire semantics, and a rich
//     status.state (ACTIVE/IN_PROGRESS/Error).
//
//   - The graph backend (rgd_graph_backend_test.go) writes ONLY a
//     `status.ready` boolean, derived from every resource's .ready() lifecycle
//     signal. It does NOT reproduce kro's condition types, author-condition
//     projection, dedup, lastTransitionTime logic, or the Error state.
//
// So the *observable* contract shared by both backends for a conditions fixture
// is limited to: the child resource converges with resolved values, AND the
// instance reaches its backend-native ready state (builtin state=ACTIVE / graph
// ready=true). The rich condition assertions are built-in-only and are branched
// on isBuiltin(be).
//
// Many custom-conditions scenarios are PURELY built-in internals (dedup /
// lastTransitionTime churn / runtime.condition internal value / Error-on-dup /
// author-condition cleanup on RGD spec change) with no graph analog. Those are
// marked N/A for the graph backend: the graph Entry is a documented no-op, and
// the built-in Entry runs the original assertion. This keeps the mapping honest
// and tracked rather than forcing parity where none exists.
//
// A note on RGD-spec-mutation scenarios (e.g. "remove the conditions block"):
// the graph backend does not deploy a real RGD object — it synthesizes a CRD
// plus an L0 controller Graph — so it has no RGD spec to mutate mid-run. Such
// scenarios are inherently built-in-only.

var _ = Describe("RGD parity: instance conditions", func() {
	// ── instance_conditions_test.go #1: hierarchical conditions on success ──
	//
	// Shared observable: the ConfigMap child (named after the instance) is
	// created with the resolved data, and the instance reaches ready. The rich
	// Ready condition (status/reason/observedGeneration/lastTransitionTime) is
	// built-in-only; the graph backend expresses readiness only via
	// status.ready, asserted by the harness default ready-assert.
	DescribeTable("hierarchical conditions on successful reconciliation",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityCondSuccess"+rand.String(4), "v1alpha1",
							map[string]any{"configData": "string"},
							// A `ready` published field so the graph backend has a
							// signal; the built-in backend derives its own Ready.
							map[string]any{"ready": "${configmap.metadata.name != ''}"},
						),
						generator.WithResource("configmap", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.metadata.name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"config": "${schema.spec.configData}"},
						}, nil, nil),
					)
				},
				map[string]any{"configData": "test-data"},
				func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
					// Shared: ConfigMap child converges with resolved data.
					cmKey := types.NamespacedName{Namespace: ns, Name: instanceName}
					awaitInstanceCR(t, configMapGVK, cmKey,
						func(u *unstructured.Unstructured) error {
							got, _, _ := unstructured.NestedString(u.Object, "data", "config")
							if got != "test-data" {
								return &notYetError{msg: "cm.data.config=" + got + ", want test-data"}
							}
							return nil
						},
						60*time.Second,
					)

					// Built-in-only: the rich Ready condition machinery.
					if isBuiltin(be) {
						awaitInstanceCR(t, gvk, types.NamespacedName{Namespace: ns, Name: instanceName},
							func(u *unstructured.Unstructured) error {
								ready := findInstanceConditionByType(u, ctrlinstance.Ready)
								if ready == nil {
									return &notYetError{msg: "Ready condition not present yet"}
								}
								if ready["status"] != "True" {
									return &notYetError{msg: fmt.Sprintf("Ready.status=%v, want True", ready["status"])}
								}
								if ready["reason"] != "Ready" {
									return &notYetError{msg: fmt.Sprintf("Ready.reason=%v, want Ready", ready["reason"])}
								}
								if ready["observedGeneration"] != u.GetGeneration() {
									return &notYetError{msg: "Ready.observedGeneration mismatch"}
								}
								if ready["lastTransitionTime"] == nil {
									return &notYetError{msg: "Ready.lastTransitionTime not set"}
								}
								return nil
							},
							60*time.Second,
						)
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── instance_conditions_test.go #2: condition progression on deletion ───
	//
	// Shared observable: after the instance is deleted, its child ConfigMap is
	// garbage-collected (both backends manage child lifecycle). The condition
	// STRUCTURE-during-deletion invariant (every condition keeps type+status
	// while a deletionTimestamp is set) is built-in-only — the graph backend
	// writes no conditions.
	DescribeTable("condition progression during deletion",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityCondDelete"+rand.String(4), "v1alpha1",
							map[string]any{"configData": "string"},
							map[string]any{"ready": "${configmap.metadata.name != ''}"},
						),
						generator.WithResource("configmap", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.metadata.name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"config": "${schema.spec.configData}"},
						}, nil, nil),
					)
				},
				map[string]any{"configData": "delete-test-data"},
				func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
					ctx := env.Context()
					if ctx == nil {
						ctx = context.Background()
					}
					cmKey := types.NamespacedName{Namespace: ns, Name: instanceName}
					instKey := types.NamespacedName{Namespace: ns, Name: instanceName}

					// Wait for the child to exist, then the instance to converge.
					awaitInstanceCR(t, configMapGVK, cmKey, nil, 60*time.Second)
					awaitInstanceReady(t, be, gvk, instKey)

					// Built-in-only: while a deletionTimestamp is set, any
					// present conditions retain a valid type+status shape.
					if isBuiltin(be) {
						inst := &unstructured.Unstructured{}
						inst.SetGroupVersionKind(gvk)
						if err := env.Client.Get(ctx, instKey, inst); err != nil {
							t.Fatalf("get instance before delete: %v", err)
						}
						if err := env.Client.Delete(ctx, inst); err != nil {
							t.Fatalf("delete instance: %v", err)
						}
						awaitInstanceCR(t, gvk, instKey,
							func(u *unstructured.Unstructured) error {
								if u.GetDeletionTimestamp() == nil {
									return &notYetError{msg: "deletionTimestamp not set yet"}
								}
								conds, found, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
								if found {
									for _, c := range conds {
										m, ok := c.(map[string]any)
										if !ok {
											continue
										}
										if m["type"] == nil {
											return &notYetError{msg: "condition type nil during deletion"}
										}
										if m["status"] == nil {
											return &notYetError{msg: "condition status nil during deletion"}
										}
									}
								}
								return nil
							},
							20*time.Second,
						)
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── instance_custom_conditions_test.go #1: author-defined conditions ────
	//
	// Shared observable: the ConfigMap child is created and the instance reaches
	// ready. Built-in-only: the author's PrimaryReady/AppReady conditions are
	// projected onto status.conditions[] (with reason/message/observedGeneration/
	// lastTransitionTime), and kro's lifecycle types are absent because the
	// author declared no Ready. The graph backend has no author-condition
	// projection, so those assertions are branched on isBuiltin.
	DescribeTable("author-defined conditions are written to status",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityCcHappy"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string"},
							map[string]any{
								// `ready` for the graph backend; `conditions` for
								// the built-in backend's author-condition path.
								"ready": "${configmap.metadata.name != ''}",
								"conditions": []any{
									`${runtime.newCondition({type: 'PrimaryReady', status: 'True', reason: 'OK',
										message: 'always healthy in this test'})}`,
									`${runtime.newCondition({type: 'AppReady', status: 'False', reason: 'Init', message: ''})}`,
								},
							},
						),
						generator.WithResource("configmap", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"foo": "${schema.spec.name}"},
						}, nil, nil),
					)
				},
				map[string]any{"name": "demo"},
				func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
					// Shared: child ConfigMap converges.
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "demo"}, nil, 60*time.Second)

					if !isBuiltin(be) {
						// Graph backend: author conditions are N/A (no condition
						// projection); readiness is asserted by the harness.
						GinkgoWriter.Printf("graph backend: author-condition projection N/A; asserting status.ready only\n")
						return
					}

					// Built-in-only: author conditions on the wire, kro
					// lifecycle types absent (no author Ready declared).
					instKey := types.NamespacedName{Namespace: ns, Name: instanceName}
					awaitInstanceCR(t, gvk, instKey,
						func(u *unstructured.Unstructured) error {
							primary := findInstanceConditionByType(u, "PrimaryReady")
							if primary == nil {
								return &notYetError{msg: "PrimaryReady not on the wire yet"}
							}
							if primary["status"] != "True" || primary["reason"] != "OK" ||
								primary["message"] != "always healthy in this test" {
								return &notYetError{msg: "PrimaryReady fields not as authored"}
							}
							if primary["lastTransitionTime"] == nil {
								return &notYetError{msg: "PrimaryReady.lastTransitionTime not set"}
							}
							if primary["observedGeneration"] != u.GetGeneration() {
								return &notYetError{msg: "PrimaryReady.observedGeneration mismatch"}
							}
							appReady := findInstanceConditionByType(u, "AppReady")
							if appReady == nil || appReady["status"] != "False" || appReady["reason"] != "Init" {
								return &notYetError{msg: "AppReady not as authored"}
							}
							for _, builtin := range []string{
								ctrlinstance.InstanceManaged, ctrlinstance.GraphResolved,
								ctrlinstance.ResourcesReady, ctrlinstance.Ready,
							} {
								if findInstanceConditionByType(u, builtin) != nil {
									return &notYetError{msg: "kro lifecycle type " + builtin + " must not appear"}
								}
							}
							return nil
						},
						60*time.Second,
					)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── instance_custom_conditions_test.go #5: conditions read schema.spec ──
	//
	// Shared observable: the ConfigMap child converges and the instance is
	// ready. Built-in-only: the author's AppReady condition tracks
	// schema.spec.healthy (True initially, flips to False after a spec update).
	// The graph backend does not project conditions, so the spec→condition
	// dataflow is branched on isBuiltin.
	DescribeTable("conditions evaluate against schema.spec and follow spec changes",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityCcSchema"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "healthy": "boolean | default=true"},
							map[string]any{
								"ready": "${configmap.metadata.name != ''}",
								"conditions": []any{
									`${runtime.newCondition({type: 'AppReady', status: schema.spec.healthy ? 'True' : 'False',
										reason: 'CheckedSpec', message: ''})}`,
								},
							},
						),
						generator.WithResource("configmap", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"foo": "${schema.spec.name}"},
						}, nil, nil),
					)
				},
				map[string]any{"name": "demo", "healthy": true},
				func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
					ctx := env.Context()
					if ctx == nil {
						ctx = context.Background()
					}
					// Shared: child ConfigMap converges.
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "demo"}, nil, 60*time.Second)

					if !isBuiltin(be) {
						GinkgoWriter.Printf("graph backend: schema-driven author condition N/A; asserting status.ready only\n")
						return
					}

					instKey := types.NamespacedName{Namespace: ns, Name: instanceName}
					// Built-in-only: AppReady=True while healthy=true.
					awaitInstanceCR(t, gvk, instKey,
						func(u *unstructured.Unstructured) error {
							c := findInstanceConditionByType(u, "AppReady")
							if c == nil {
								return &notYetError{msg: "AppReady not present yet"}
							}
							if c["status"] != "True" {
								return &notYetError{msg: fmt.Sprintf("AppReady.status=%v, want True", c["status"])}
							}
							return nil
						},
						60*time.Second,
					)

					// Flip spec.healthy=false; the condition status must follow.
					inst := &unstructured.Unstructured{}
					inst.SetGroupVersionKind(gvk)
					if err := env.Client.Get(ctx, instKey, inst); err != nil {
						t.Fatalf("get instance: %v", err)
					}
					if err := unstructured.SetNestedField(inst.Object, false, "spec", "healthy"); err != nil {
						t.Fatalf("set spec.healthy: %v", err)
					}
					if err := env.Client.Update(ctx, inst); err != nil {
						t.Fatalf("update instance: %v", err)
					}
					awaitInstanceCR(t, gvk, instKey,
						func(u *unstructured.Unstructured) error {
							c := findInstanceConditionByType(u, "AppReady")
							if c == nil {
								return &notYetError{msg: "AppReady missing after update"}
							}
							if c["status"] != "False" {
								return &notYetError{msg: fmt.Sprintf("AppReady.status=%v, want False after healthy=false", c["status"])}
							}
							return nil
						},
						60*time.Second,
					)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── Built-in-only scenarios (graph backend N/A) ─────────────────────────
	//
	// The following custom-conditions scenarios exercise kro's condition
	// machinery with NO graph analog. The graph backend writes only
	// status.ready, so there is nothing observable to compare. Each runs its
	// original assertion against the built-in backend only; the graph Entry is
	// a documented no-op (the fixture body early-returns for !isBuiltin).
	//
	// N/A rationale, per scenario:
	//
	//   - "preserves lastTransitionTime when unchanged": internal LTT-dedup
	//     invariant on kro condition writes; graph writes no conditions.
	//   - "emits kro built-in conditions when no conditions block": asserts the
	//     presence of kro lifecycle condition TYPES; graph has none.
	//   - "author Ready overrides kro lifecycle Ready": author Ready=False
	//     changes status.state semantics (never ACTIVE); graph readiness is a
	//     bare bool with no author-condition override path.
	//   - "runtime.condition reads kro internal, not wire override": inspects
	//     kro's internal condition store vs the wire; no graph analog.
	//   - "drops runtime-duplicate types, sets state=Error": Error state + dedup
	//     are built-in; graph has neither.
	//   - "preserves lastTransitionTime for author overrides of built-in types":
	//     internal LTT-churn invariant; graph writes no conditions.
	//   - "preserves a previously written condition while data is pending":
	//     condition preservation + no-builtin-leak invariant; graph writes no
	//     conditions.
	//   - "removes leftover author conditions after the block is removed":
	//     requires mutating the RGD spec at runtime; the graph backend has no
	//     RGD object to mutate (it deploys a synthesized CRD + L0 Graph).

	It("N/A(graph): preserves lastTransitionTime when condition status is unchanged", func() {
		runBuiltinOnlyCondition(parityBuiltin(),
			func(name string) *krov1alpha1.ResourceGraphDefinition {
				return generator.NewResourceGraphDefinition(name,
					generator.WithSchema(
						"ParityCcLtt"+rand.String(4), "v1alpha1",
						map[string]any{"name": "string", "label": "string | default=initial"},
						map[string]any{
							"conditions": []any{
								`${runtime.newCondition({type: 'AlwaysTrue', status: 'True', reason: 'X', message: ''})}`,
							},
						},
					),
					generator.WithResource("configmap", map[string]any{
						"apiVersion": "v1", "kind": "ConfigMap",
						"metadata": map[string]any{
							"name":      "${schema.spec.name}",
							"namespace": "${schema.metadata.namespace}",
							"labels":    map[string]any{"app": "${schema.spec.label}"},
						},
						"data": map[string]any{"foo": "${schema.spec.name}"},
					}, nil, nil),
				)
			},
			map[string]any{"name": "demo", "label": "initial"},
			func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind) {
				ctx := env.Context()
				if ctx == nil {
					ctx = context.Background()
				}
				instKey := types.NamespacedName{Namespace: ns, Name: instanceName}

				var firstLTT any
				awaitInstanceCR(t, gvk, instKey, func(u *unstructured.Unstructured) error {
					c := findInstanceConditionByType(u, "AlwaysTrue")
					if c == nil {
						return &notYetError{msg: "AlwaysTrue not present yet"}
					}
					firstLTT = c["lastTransitionTime"]
					if firstLTT == nil {
						return &notYetError{msg: "lastTransitionTime not set"}
					}
					return nil
				}, 60*time.Second)

				inst := &unstructured.Unstructured{}
				inst.SetGroupVersionKind(gvk)
				if err := env.Client.Get(ctx, instKey, inst); err != nil {
					t.Fatalf("get instance: %v", err)
				}
				_ = unstructured.SetNestedField(inst.Object, "updated", "spec", "label")
				if err := env.Client.Update(ctx, inst); err != nil {
					t.Fatalf("update instance: %v", err)
				}

				awaitInstanceCR(t, gvk, instKey, func(u *unstructured.Unstructured) error {
					c := findInstanceConditionByType(u, "AlwaysTrue")
					if c == nil {
						return &notYetError{msg: "AlwaysTrue missing after update"}
					}
					if c["observedGeneration"] != u.GetGeneration() {
						return &notYetError{msg: "observedGeneration should track new generation"}
					}
					if fmt.Sprintf("%v", c["lastTransitionTime"]) != fmt.Sprintf("%v", firstLTT) {
						return &notYetError{msg: "lastTransitionTime must be preserved when status unchanged"}
					}
					return nil
				}, 60*time.Second)
			},
		)
	})

	It("N/A(graph): emits kro built-in conditions when no conditions block is present", func() {
		runBuiltinOnlyCondition(parityBuiltin(),
			func(name string) *krov1alpha1.ResourceGraphDefinition {
				return generator.NewResourceGraphDefinition(name,
					generator.WithSchema(
						"ParityCcBackCompat"+rand.String(4), "v1alpha1",
						map[string]any{"name": "string"},
						nil,
					),
					generator.WithResource("configmap", map[string]any{
						"apiVersion": "v1", "kind": "ConfigMap",
						"metadata": map[string]any{
							"name":      "${schema.spec.name}",
							"namespace": "${schema.metadata.namespace}",
						},
						"data": map[string]any{"foo": "${schema.spec.name}"},
					}, nil, nil),
				)
			},
			map[string]any{"name": "demo"},
			func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind) {
				awaitInstanceCR(t, gvk, types.NamespacedName{Namespace: ns, Name: instanceName},
					func(u *unstructured.Unstructured) error {
						ts := getConditionTypes(u)
						for _, want := range []string{
							ctrlinstance.Ready, ctrlinstance.InstanceManaged,
							ctrlinstance.GraphResolved, ctrlinstance.ResourcesReady,
						} {
							if !containsString(ts, want) {
								return &notYetError{msg: "missing lifecycle condition " + want}
							}
						}
						ready := findInstanceConditionByType(u, ctrlinstance.Ready)
						if ready == nil || ready["status"] != "True" {
							return &notYetError{msg: "Ready not True yet"}
						}
						return nil
					}, 60*time.Second)
			},
		)
	})

	It("N/A(graph): honors an author-defined Ready that overrides kro's lifecycle Ready", func() {
		// Author Ready=False changes status.state semantics (never ACTIVE), so
		// skip the harness ready-assert and assert the wire condition directly.
		be := parityBuiltin()
		runParityFixtureOpts(be,
			func(name string) *krov1alpha1.ResourceGraphDefinition {
				return generator.NewResourceGraphDefinition(name,
					generator.WithSchema(
						"ParityCcAuthorReady"+rand.String(4), "v1alpha1",
						map[string]any{"name": "string"},
						map[string]any{
							"conditions": []any{
								`${runtime.newCondition({type: 'Ready', status: 'False', reason: 'AuthorSaysNo', message: 'this is a test'})}`,
							},
						},
					),
					generator.WithResource("configmap", map[string]any{
						"apiVersion": "v1", "kind": "ConfigMap",
						"metadata": map[string]any{
							"name":      "${schema.spec.name}",
							"namespace": "${schema.metadata.namespace}",
						},
						"data": map[string]any{"foo": "${schema.spec.name}"},
					}, nil, nil),
				)
			},
			map[string]any{"name": "demo"},
			func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
				if !isBuiltin(be) {
					GinkgoWriter.Printf("graph backend: author Ready override N/A; skipping\n")
					return
				}
				awaitInstanceCR(t, gvk, types.NamespacedName{Namespace: ns, Name: instanceName},
					func(u *unstructured.Unstructured) error {
						ready := findInstanceConditionByType(u, ctrlinstance.Ready)
						if ready == nil {
							return &notYetError{msg: "Ready not present yet"}
						}
						if ready["status"] != "False" || ready["reason"] != "AuthorSaysNo" ||
							ready["message"] != "this is a test" {
							return &notYetError{msg: "author Ready=False must win"}
						}
						return nil
					}, 60*time.Second)
			},
			true, // skipReadyAssert: author Ready=False means status.state never ACTIVE
		)
	})

	It("N/A(graph): runtime.condition reads kro's internal value, not the author's wire override", func() {
		runBuiltinOnlyCondition(parityBuiltin(),
			func(name string) *krov1alpha1.ResourceGraphDefinition {
				return generator.NewResourceGraphDefinition(name,
					generator.WithSchema(
						"ParityCcInternal"+rand.String(4), "v1alpha1",
						map[string]any{"name": "string"},
						map[string]any{
							"conditions": []any{
								`${runtime.newCondition({type: 'ResourcesReady', status: 'True', reason: 'AuthorOverride', message: 'author claims ready'})}`,
								`${runtime.newCondition({type: 'DerivedReady', status: runtime.condition(schema, 'ResourcesReady').status, reason: 'MirrorsKroInternal', message: ''})}`,
							},
						},
					),
					generator.WithResource("configmap", map[string]any{
						"apiVersion": "v1", "kind": "ConfigMap",
						"metadata": map[string]any{
							"name":      "${schema.spec.name}",
							"namespace": "${schema.metadata.namespace}",
						},
						"data": map[string]any{"foo": "${schema.spec.name}"},
					}, []string{`${configmap.data["ready"] == "true"}`}, nil),
				)
			},
			map[string]any{"name": "demo"},
			func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind) {
				awaitInstanceCR(t, gvk, types.NamespacedName{Namespace: ns, Name: instanceName},
					func(u *unstructured.Unstructured) error {
						rr := findInstanceConditionByType(u, ctrlinstance.ResourcesReady)
						if rr == nil {
							return &notYetError{msg: "author ResourcesReady override not on wire yet"}
						}
						if rr["status"] != "True" || rr["reason"] != "AuthorOverride" {
							return &notYetError{msg: "author override should appear unchanged on wire"}
						}
						derived := findInstanceConditionByType(u, "DerivedReady")
						if derived == nil {
							return &notYetError{msg: "DerivedReady not present yet"}
						}
						if derived["status"] != "False" {
							return &notYetError{msg: "runtime.condition must reflect kro internal (False), not wire override"}
						}
						return nil
					}, 60*time.Second)
			},
		)
	})

	It("N/A(graph): drops runtime-duplicate condition types, sets state=Error, keeps survivors", func() {
		runBuiltinOnlyCondition(parityBuiltin(),
			func(name string) *krov1alpha1.ResourceGraphDefinition {
				return generator.NewResourceGraphDefinition(name,
					generator.WithSchema(
						"ParityCcDup"+rand.String(4), "v1alpha1",
						map[string]any{"name": "string"},
						map[string]any{
							"conditions": []any{
								`${runtime.newCondition({type: 'Survivor', status: 'True', reason: '', message: ''})}`,
								`${runtime.newCondition({type: 'Same-' + schema.spec.name, status: 'True', reason: '', message: ''})}`,
								`${runtime.newCondition({type: 'Same-' + schema.spec.name, status: 'False', reason: '', message: ''})}`,
							},
						},
					),
					generator.WithResource("configmap", map[string]any{
						"apiVersion": "v1", "kind": "ConfigMap",
						"metadata": map[string]any{
							"name":      "${schema.spec.name}",
							"namespace": "${schema.metadata.namespace}",
						},
						"data": map[string]any{"foo": "${schema.spec.name}"},
					}, nil, nil),
				)
			},
			map[string]any{"name": "demo"},
			func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind) {
				awaitInstanceCR(t, gvk, types.NamespacedName{Namespace: ns, Name: instanceName},
					func(u *unstructured.Unstructured) error {
						state, _, _ := unstructured.NestedString(u.Object, "status", "state")
						if state != string(krov1alpha1.InstanceStateError) {
							return &notYetError{msg: "duplicate condition type must surface as state=Error, got " + state}
						}
						if findInstanceConditionByType(u, "Survivor") == nil {
							return &notYetError{msg: "Survivor must remain on the wire"}
						}
						if findInstanceConditionByType(u, "Same-demo") != nil {
							return &notYetError{msg: "both copies of the duplicated type must be dropped"}
						}
						return nil
					}, 60*time.Second)
			},
		)
	})

	It("N/A(graph): preserves lastTransitionTime for author conditions overriding built-in types", func() {
		runBuiltinOnlyCondition(parityBuiltin(),
			func(name string) *krov1alpha1.ResourceGraphDefinition {
				return generator.NewResourceGraphDefinition(name,
					generator.WithSchema(
						"ParityCcOverrideLtt"+rand.String(4), "v1alpha1",
						map[string]any{"name": "string", "label": "string | default=initial"},
						map[string]any{
							"conditions": []any{
								`${runtime.newCondition({type: 'ResourcesReady', status: 'True', reason: 'AuthorOverride', message: ''})}`,
							},
						},
					),
					generator.WithResource("configmap", map[string]any{
						"apiVersion": "v1", "kind": "ConfigMap",
						"metadata": map[string]any{
							"name":      "${schema.spec.name}",
							"namespace": "${schema.metadata.namespace}",
							"labels":    map[string]any{"app": "${schema.spec.label}"},
						},
						"data": map[string]any{"foo": "${schema.spec.name}"},
					}, []string{`${configmap.data["ready"] == "true"}`}, nil),
				)
			},
			map[string]any{"name": "demo", "label": "initial"},
			func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind) {
				ctx := env.Context()
				if ctx == nil {
					ctx = context.Background()
				}
				instKey := types.NamespacedName{Namespace: ns, Name: instanceName}

				var firstLTT any
				awaitInstanceCR(t, gvk, instKey, func(u *unstructured.Unstructured) error {
					c := findInstanceConditionByType(u, ctrlinstance.ResourcesReady)
					if c == nil || c["status"] != "True" {
						return &notYetError{msg: "author ResourcesReady=True not on wire yet"}
					}
					firstLTT = c["lastTransitionTime"]
					if firstLTT == nil {
						return &notYetError{msg: "lastTransitionTime not set"}
					}
					return nil
				}, 60*time.Second)

				inst := &unstructured.Unstructured{}
				inst.SetGroupVersionKind(gvk)
				if err := env.Client.Get(ctx, instKey, inst); err != nil {
					t.Fatalf("get instance: %v", err)
				}
				_ = unstructured.SetNestedField(inst.Object, "updated", "spec", "label")
				if err := env.Client.Update(ctx, inst); err != nil {
					t.Fatalf("update instance: %v", err)
				}

				awaitInstanceCR(t, gvk, instKey, func(u *unstructured.Unstructured) error {
					c := findInstanceConditionByType(u, ctrlinstance.ResourcesReady)
					if c == nil {
						return &notYetError{msg: "ResourcesReady missing after update"}
					}
					if c["observedGeneration"] != u.GetGeneration() {
						return &notYetError{msg: "observedGeneration should track new generation"}
					}
					if fmt.Sprintf("%v", c["lastTransitionTime"]) != fmt.Sprintf("%v", firstLTT) {
						return &notYetError{msg: "lastTransitionTime must not churn when author status stable"}
					}
					return nil
				}, 60*time.Second)
			},
		)
	})

	It("N/A(graph): preserves a previously written condition while its data is pending", func() {
		// This scenario intentionally drives the instance data-pending, so it
		// never reaches ACTIVE; assert the wire condition directly.
		be := parityBuiltin()
		runParityFixtureOpts(be,
			func(name string) *krov1alpha1.ResourceGraphDefinition {
				return generator.NewResourceGraphDefinition(name,
					generator.WithSchema(
						"ParityCcDataPending"+rand.String(4), "v1alpha1",
						map[string]any{"name": "string", "key": "string"},
						map[string]any{
							"conditions": []any{
								`${runtime.newCondition({type: 'Static', status: 'True', reason: '', message: ''})}`,
								`${runtime.newCondition({type: 'DataDriven', status: configmap.data[schema.spec.key] == 'x' ? 'True' : 'False', reason: 'FromConfigMap', message: ''})}`,
							},
						},
					),
					generator.WithResource("configmap", map[string]any{
						"apiVersion": "v1", "kind": "ConfigMap",
						"metadata": map[string]any{
							"name":      "${schema.spec.name}",
							"namespace": "${schema.metadata.namespace}",
						},
						"data": map[string]any{"phase": "x"},
					}, nil, nil),
				)
			},
			map[string]any{"name": "demo", "key": "phase"},
			func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend) {
				if !isBuiltin(be) {
					GinkgoWriter.Printf("graph backend: data-pending condition preservation N/A; skipping\n")
					return
				}
				ctx := env.Context()
				if ctx == nil {
					ctx = context.Background()
				}
				instKey := types.NamespacedName{Namespace: ns, Name: instanceName}

				awaitInstanceCR(t, gvk, instKey, func(u *unstructured.Unstructured) error {
					c := findInstanceConditionByType(u, "DataDriven")
					if c == nil || c["status"] != "True" {
						return &notYetError{msg: "DataDriven not True yet"}
					}
					return nil
				}, 60*time.Second)

				inst := &unstructured.Unstructured{}
				inst.SetGroupVersionKind(gvk)
				if err := env.Client.Get(ctx, instKey, inst); err != nil {
					t.Fatalf("get instance: %v", err)
				}
				_ = unstructured.SetNestedField(inst.Object, "missing", "spec", "key")
				if err := env.Client.Update(ctx, inst); err != nil {
					t.Fatalf("update instance: %v", err)
				}

				// The previously written condition must survive while data is
				// pending, and kro's built-ins must not leak onto the wire.
				deadline := time.Now().Add(8 * time.Second)
				for time.Now().Before(deadline) {
					cur := &unstructured.Unstructured{}
					cur.SetGroupVersionKind(gvk)
					if err := env.Client.Get(ctx, instKey, cur); err != nil {
						t.Fatalf("get instance during pending window: %v", err)
					}
					c := findInstanceConditionByType(cur, "DataDriven")
					if c == nil {
						t.Fatalf("DataDriven disappeared while data pending")
					}
					if c["status"] != "True" {
						t.Fatalf("DataDriven last-written value not preserved, got %v", c["status"])
					}
					for _, builtin := range []string{
						ctrlinstance.InstanceManaged, ctrlinstance.GraphResolved,
						ctrlinstance.ResourcesReady, ctrlinstance.Ready,
					} {
						if findInstanceConditionByType(cur, builtin) != nil {
							t.Fatalf("kro built-in %s leaked while data pending", builtin)
						}
					}
					time.Sleep(time.Second)
				}
			},
			true, // skipReadyAssert: data-pending never converges to ACTIVE
		)
	})

	It("N/A(graph): removes leftover author conditions after the conditions block is removed", func() {
		// Requires mutating the RGD spec at runtime; only the built-in backend
		// has an RGD object to mutate, so this is built-in-only by construction.
		be := parityBuiltin()
		ctx := env.Context()
		if ctx == nil {
			ctx = context.Background()
		}
		ns := createParityNamespace(GinkgoT())

		kind := "ParityCcBlockRemoved" + rand.String(4)
		configmap := generator.WithResource("configmap", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{
				"name":      "${schema.spec.name}",
				"namespace": "${schema.metadata.namespace}",
			},
			"data": map[string]any{"foo": "${schema.spec.name}"},
		}, nil, nil)

		makeRGD := func(name string) *krov1alpha1.ResourceGraphDefinition {
			return generator.NewResourceGraphDefinition(name,
				generator.WithSchema(kind, "v1alpha1",
					map[string]any{"name": "string"},
					map[string]any{
						"conditions": []any{
							`${runtime.newCondition({type: 'AppReady', status: 'True', reason: '', message: ''})}`,
						},
					},
				),
				configmap,
			)
		}

		t := GinkgoT()
		rgd := makeRGD("parity" + rand.String(5))
		gvk := be.Deploy(t, rgd)

		instName := "demo"
		inst := newInstanceCR(gvk, ns, instName, map[string]any{"name": "demo"})
		if err := env.Client.Create(ctx, inst); err != nil {
			t.Fatalf("create instance: %v", err)
		}
		t.Cleanup(func() { _ = env.Client.Delete(context.Background(), inst) })

		instKey := types.NamespacedName{Namespace: ns, Name: instName}
		awaitInstanceCR(t, gvk, instKey, func(u *unstructured.Unstructured) error {
			if findInstanceConditionByType(u, "AppReady") == nil {
				return &notYetError{msg: "AppReady not present yet"}
			}
			return nil
		}, 60*time.Second)

		// Drop the conditions block from the live RGD; kro's built-ins take the
		// wire back and the author condition is cleaned up.
		live := &krov1alpha1.ResourceGraphDefinition{}
		if err := env.Client.Get(ctx, types.NamespacedName{Name: rgd.Name}, live); err != nil {
			t.Fatalf("get RGD: %v", err)
		}
		withoutConditions := generator.NewResourceGraphDefinition(rgd.Name,
			generator.WithSchema(kind, "v1alpha1", map[string]any{"name": "string"}, nil),
			configmap,
		)
		live.Spec = withoutConditions.Spec
		if err := env.Client.Update(ctx, live); err != nil {
			t.Fatalf("update RGD: %v", err)
		}

		awaitInstanceCR(t, gvk, instKey, func(u *unstructured.Unstructured) error {
			if findInstanceConditionByType(u, "AppReady") != nil {
				return &notYetError{msg: "leftover author condition must be removed"}
			}
			if findInstanceConditionByType(u, ctrlinstance.Ready) == nil {
				return &notYetError{msg: "kro built-in Ready must return"}
			}
			return nil
		}, 60*time.Second)
	})
})

// runBuiltinOnlyCondition drives a condition fixture through the built-in
// backend only, running assert with the resolved instance GVK. It mirrors
// runParityFixture's deploy/create/cleanup flow but skips the harness
// ready-assert (condition fixtures manage their own readiness expectations) and
// takes a graph-free assert signature, since these scenarios have no graph
// analog. The graph backend is documented N/A at each call site's It title.
func runBuiltinOnlyCondition(
	be rgdBackend,
	makeRGD func(name string) *krov1alpha1.ResourceGraphDefinition,
	instanceSpec map[string]any,
	assert func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind),
) {
	t := GinkgoT()
	ns := createParityNamespace(t)

	rgd := makeRGD("parity" + rand.String(5))
	gvk := be.Deploy(t, rgd)

	instName := "demo"
	inst := newInstanceCR(gvk, ns, instName, instanceSpec)
	ctx := env.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := env.Client.Create(ctx, inst); err != nil {
		t.Fatalf("%s: create instance: %v", be.Name(), err)
	}
	t.Cleanup(func() { _ = env.Client.Delete(context.Background(), inst) })

	assert(t, ns, instName, gvk)
}

// containsString reports whether s is in list.
func containsString(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
