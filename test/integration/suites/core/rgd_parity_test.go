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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"

	krov1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
)

// This suite validates that an RGD fixture produces the same observable
// behavior whether it is realized by the built-in ResourceGraphDefinition
// controller or by the RGD-as-Graph controller (examples/graph/rgd.yaml, served
// at v1alpha2 under a distinct group). Each fixture is run against BOTH backends
// via the rgdBackend abstraction (see rgd_backend_test.go); the assertions cover
// only the shared contract — the user CR's child resources converge with the
// correct resolved field values, and the instance's status.ready flips true —
// never backend-internal status shapes.
//
// This is the concrete answer to "test the RGD tests against the generated one
// from rgd v1alpha2": the same fixtures, the same instances, the same
// assertions, exercised on both the imperative-Go controller and the pure-Graph
// one built from the primitives unblocked earlier (plural(),
// simpleSchema.toOpenAPI(), forEach over a dynamic collection ref, .ready()).
var _ = Describe("RGD parity: builtin vs RGD-as-Graph", func() {
	// backends returns a fresh backend pair per fixture. The graph backend
	// carries per-run state (its isolating group), so it is constructed anew
	// each time rather than shared.
	backends := func() []rgdBackend {
		return []rgdBackend{
			builtinRGDBackend{},
			newGraphRGDBackend(),
		}
	}

	// runFixture drives one RGD fixture through one backend end to end: deploy
	// the definition, create an instance, and assert the child resource and the
	// instance status converge.
	runFixture := func(
		be rgdBackend,
		makeRGD func(name string) *krov1alpha1.ResourceGraphDefinition,
		instanceSpec map[string]any,
		assertChildren func(t GinkgoTInterface, ns, instanceName string),
	) {
		t := GinkgoT()
		ns := createParityNamespace(t)

		// Unique RGD/Kind names per (backend, run) so the two backends' CRDs
		// never collide on the shared cluster-scoped API surface.
		rgd := makeRGD("parity" + rand.String(5))
		gvk := be.Deploy(t, rgd)

		instName := "inst-" + rand.String(5)
		inst := newInstanceCR(gvk, ns, instName, instanceSpec)
		ctx := env.Context()
		if ctx == nil {
			ctx = context.Background()
		}
		if err := env.Client.Create(ctx, inst); err != nil {
			t.Fatalf("%s: create instance: %v", be.Name(), err)
		}
		t.Cleanup(func() { _ = env.Client.Delete(context.Background(), inst) })

		assertChildren(t, ns, instName)

		// Shared status contract: the instance eventually reports ready.
		// Built-in RGD writes status.state=ACTIVE + Ready condition; the graph
		// backend writes status.ready=true via .ready(). Assert the lowest
		// common denominator each backend guarantees.
		awaitInstanceReady(t, be, gvk, types.NamespacedName{Namespace: ns, Name: instName})
	}

	// ── Fixture: a single ConfigMap resolved from the instance spec ─────────
	//
	// The classic smoke shape: one resource whose data is templated from
	// ${schema.spec.*}. Proves schema→resource value resolution is identical.
	DescribeTable("a ConfigMap resolved from the instance spec",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			runFixture(be,
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityCM"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "greeting": "string"},
							map[string]any{"ready": "${cm.metadata.name != ''}"},
						),
						generator.WithResource("cm", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"greeting": "${schema.spec.greeting}"},
						}, nil, nil),
					)
				},
				map[string]any{"name": "resolved-cm", "greeting": "hello"},
				func(t GinkgoTInterface, ns, _ string) {
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "resolved-cm"},
						func(u *unstructured.Unstructured) error {
							got, _, _ := unstructured.NestedString(u.Object, "data", "greeting")
							if got != "hello" {
								return &notYetError{msg: "cm.data.greeting=" + got + ", want hello"}
							}
							return nil
						},
						60*time.Second,
					)
				},
			)
		},
		Entry("builtin", func() rgdBackend { return builtinRGDBackend{} }),
		Entry("graph-v1alpha2", func() rgdBackend { return newGraphRGDBackend() }),
	)

	// ── Fixture: two resources with a cross-resource reference ──────────────
	//
	// cm2 reads cm1's name, exercising dependency ordering + cross-node CEL.
	DescribeTable("two ConfigMaps with a cross-resource reference",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			runFixture(be,
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityChain"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string"},
							map[string]any{"ready": "${cm2.metadata.name != ''}"},
						),
						generator.WithResource("cm1", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-1",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"role": "first"},
						}, nil, nil),
						generator.WithResource("cm2", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-2",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"refs": "${cm1.metadata.name}"},
						}, nil, nil),
					)
				},
				map[string]any{"name": "chain"},
				func(t GinkgoTInterface, ns, _ string) {
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "chain-2"},
						func(u *unstructured.Unstructured) error {
							got, _, _ := unstructured.NestedString(u.Object, "data", "refs")
							if got != "chain-1" {
								return &notYetError{msg: "cm2.data.refs=" + got + ", want chain-1"}
							}
							return nil
						},
						60*time.Second,
					)
				},
			)
		},
		Entry("builtin", func() rgdBackend { return builtinRGDBackend{} }),
		Entry("graph-v1alpha2", func() rgdBackend { return newGraphRGDBackend() }),
	)

	// ── Fixture: readyWhen gating drives dependency ordering + status ──────
	//
	// gate is a ConfigMap whose readyWhen reads its own data.ready (populated
	// from the instance spec, so it converges in envtest without a kubelet).
	// dependent references gate.data.value, so it is gated until gate is ready.
	// This exercises the whole readiness pipeline through the graph backend:
	// readyWhen → .ready() → dependency gating → status.ready writeback — the
	// features unblocked earlier. Both backends must converge identically.
	DescribeTable("readyWhen gates a dependent resource",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			runFixture(be,
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityGate"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "gateReady": "boolean"},
							map[string]any{"ready": "${dependent.metadata.name != ''}"},
						),
						// gate: ready only when data.ready == "true" (from spec).
						generator.WithResource("gate", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-gate",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"ready": `${string(schema.spec.gateReady)}`},
						}, []string{`${gate.data.?ready.orValue("false") == "true"}`}, nil),
						// dependent: references gate, so it applies only after gate is ready.
						generator.WithResource("dependent", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-dependent",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"gatedOn": "${gate.metadata.name}"},
						}, nil, nil),
					)
				},
				map[string]any{"name": "gated", "gateReady": true},
				func(t GinkgoTInterface, ns, _ string) {
					// The dependent only exists once gate satisfied its readyWhen.
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "gated-dependent"},
						func(u *unstructured.Unstructured) error {
							got, _, _ := unstructured.NestedString(u.Object, "data", "gatedOn")
							if got != "gated-gate" {
								return &notYetError{msg: "dependent.data.gatedOn=" + got + ", want gated-gate"}
							}
							return nil
						},
						60*time.Second,
					)
				},
			)
		},
		Entry("builtin", func() rgdBackend { return builtinRGDBackend{} }),
		Entry("graph-v1alpha2", func() rgdBackend { return newGraphRGDBackend() }),
	)

	// ── Fixture: includeWhen conditionally includes a resource ────────────
	//
	// optional is included only when schema.spec.enabled is true. With enabled
	// true, both backends must create it; the always resource is unconditional.
	DescribeTable("includeWhen conditionally includes a resource",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			runFixture(be,
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityInc"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "enabled": "boolean"},
							map[string]any{"ready": "${always.metadata.name != ''}"},
						),
						generator.WithResource("always", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-always",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"k": "v"},
						}, nil, nil),
						generator.WithResource("optional", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-optional",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"k": "v"},
						}, nil, []string{"${schema.spec.enabled}"}),
					)
				},
				map[string]any{"name": "cond", "enabled": true},
				func(t GinkgoTInterface, ns, _ string) {
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "cond-always"},
						nil, 60*time.Second)
					// enabled=true → optional is included by both backends.
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "cond-optional"},
						nil, 60*time.Second)
				},
			)
		},
		Entry("builtin", func() rgdBackend { return builtinRGDBackend{} }),
		Entry("graph-v1alpha2", func() rgdBackend { return newGraphRGDBackend() }),
	)

	// ── Fixture: forEach expands a collection resource per list element ────
	//
	// cm is a collection: one ConfigMap per entry in schema.spec.regions, named
	// after each region. This exercises the graph backend's forEach carry-
	// through (rgdResourceToL2Node) and the engine's collection expansion,
	// which must match the built-in controller's per-element stamping.
	DescribeTable("forEach expands a collection resource",
		func(makeBackend func() rgdBackend) {
			be := makeBackend()
			runFixture(be,
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityColl"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "regions": "[]string"},
							map[string]any{"ready": "${cm.map(c, c.metadata.name).size() >= 0}"},
						),
						generator.WithResourceCollection("cm", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-${region}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"region": "${region}"},
						},
							[]krov1alpha1.ForEachDimension{{"region": "${schema.spec.regions}"}},
							nil, nil),
					)
				},
				map[string]any{"name": "fleet", "regions": []any{"us", "eu"}},
				func(t GinkgoTInterface, ns, _ string) {
					// One ConfigMap per region, each carrying its region value.
					for _, region := range []string{"us", "eu"} {
						want := region
						awaitInstanceCR(t, configMapGVK,
							types.NamespacedName{Namespace: ns, Name: "fleet-" + region},
							func(u *unstructured.Unstructured) error {
								got, _, _ := unstructured.NestedString(u.Object, "data", "region")
								if got != want {
									return &notYetError{msg: "cm.data.region=" + got + ", want " + want}
								}
								return nil
							},
							60*time.Second,
						)
					}
				},
			)
		},
		Entry("builtin", func() rgdBackend { return builtinRGDBackend{} }),
		Entry("graph-v1alpha2", func() rgdBackend { return newGraphRGDBackend() }),
	)

	_ = backends
})

// createParityNamespace makes a throwaway namespace for a parity spec and
// registers cleanup.
func createParityNamespace(t GinkgoTInterface) string {
	t.Helper()
	name := fmt.Sprintf("parity-%s", rand.String(6))
	ctx := env.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := env.Client.Create(ctx, ns); err != nil {
		t.Fatalf("create namespace %s: %v", name, err)
	}
	t.Cleanup(func() { _ = env.Client.Delete(context.Background(), ns) })
	return name
}

// awaitInstanceReady asserts the instance converges to a ready state under the
// backend's own status convention: the built-in controller sets
// status.state=ACTIVE, the graph backend sets status.ready=true.
func awaitInstanceReady(t GinkgoTInterface, be rgdBackend, gvk schema.GroupVersionKind, key types.NamespacedName) {
	t.Helper()
	awaitInstanceCR(t, gvk, key, func(u *unstructured.Unstructured) error {
		if be.Name() == (builtinRGDBackend{}).Name() {
			state, _, _ := unstructured.NestedString(u.Object, "status", "state")
			if state != string(krov1alpha1.ResourceGraphDefinitionStateActive) && state != "ACTIVE" {
				return &notYetError{msg: "status.state=" + state + ", want ACTIVE"}
			}
			return nil
		}
		ready, found, _ := unstructured.NestedBool(u.Object, "status", "ready")
		if !found || !ready {
			return &notYetError{msg: fmt.Sprintf("status.ready=%v found=%v, want true", ready, found)}
		}
		return nil
	}, 60*time.Second)
}
