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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"

	krov1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
	"github.com/kubernetes-sigs/kro/test/integration/environment"
)

// This suite validates that an RGD fixture produces the same observable
// behavior whether it is realized by the built-in ResourceGraphDefinition
// controller or by the RGD-as-Graph controller (examples/graph/rgd.yaml, served
// at v1alpha2 under a distinct group). Each fixture is run against BOTH backends
// via the rgdBackend abstraction (see rgd_backend_test.go) and the package-level
// runParityFixture harness (see rgd_parity_harness_test.go); the assertions
// cover the shared contract — the user CR's child resources converge with the
// correct resolved field values, and the instance reaches ready — with
// backend-specific behavior branched on the backend and documented as a parity
// boundary.
//
// This is the "core" set of ported scenarios; feature-specific ported scenarios
// live in the sibling rgd_parity_*_test.go files.
var _ = Describe("RGD parity: builtin vs RGD-as-Graph", func() {
	// ── Fixture: a single ConfigMap resolved from the instance spec ─────────
	//
	// The classic smoke shape: one resource whose data is templated from
	// ${schema.spec.*}. Proves schema→resource value resolution is identical.
	DescribeTable("a ConfigMap resolved from the instance spec",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
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
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
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
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── Fixture: two resources with a cross-resource reference ──────────────
	//
	// cm2 reads cm1's name, exercising dependency ordering + cross-node CEL.
	DescribeTable("two ConfigMaps with a cross-resource reference",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
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
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
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
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── Fixture: readyWhen gating (data-driven) drives dependency ordering ──
	//
	// gate is a ConfigMap whose readyWhen reads its own data.ready (populated
	// from the instance spec, so it converges in envtest without a kubelet).
	// dependent references gate, so it is gated until gate is ready. Exercises
	// readyWhen → .ready() → status writeback through the graph backend.
	DescribeTable("readyWhen gates a dependent resource",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityGate"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "gateReady": "boolean"},
							map[string]any{"ready": "${dependent.metadata.name != ''}"},
						),
						generator.WithResource("gate", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-gate",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"ready": `${string(schema.spec.gateReady)}`},
						}, []string{`${gate.data.?ready.orValue("false") == "true"}`}, nil),
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
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
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
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── Fixture: includeWhen conditionally includes a resource ────────────
	DescribeTable("includeWhen conditionally includes a resource",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
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
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "cond-always"},
						nil, 60*time.Second)
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "cond-optional"},
						nil, 60*time.Second)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── Fixture: forEach expands a collection resource per list element ────
	DescribeTable("forEach expands a collection resource",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
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
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
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
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── Fixture: observed-status readyWhen gates a dependent (readiness_test) ─
	//
	// Mirrors readiness_test.go: a Deployment whose readyWhen checks
	// spec.replicas == status.availableReplicas, and a Service that must wait.
	// envtest has no kubelet, so the test patches the Deployment status
	// mid-fixture. The strict "Service absent before Deployment ready" ordering
	// is asserted only for the built-in backend — standalone Graphs leave
	// dependency-readiness gating opt-in (GateReadiness), a documented parity
	// boundary; both converge to the same end state.
	DescribeTable("readyWhen on observed status gates a dependent (readiness parity)",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityRollout"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "replicas": "integer"},
							map[string]any{"ready": "${service.metadata.name != ''}"},
						),
						generator.WithResource("deployment", map[string]any{
							"apiVersion": "apps/v1", "kind": "Deployment",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"spec": map[string]any{
								"replicas": "${schema.spec.replicas}",
								"selector": map[string]any{"matchLabels": map[string]any{"app": "rollout"}},
								"template": map[string]any{
									"metadata": map[string]any{"labels": map[string]any{"app": "rollout"}},
									"spec": map[string]any{"containers": []any{
										map[string]any{"name": "app", "image": "nginx"},
									}},
								},
							},
						}, []string{"${deployment.spec.replicas == deployment.status.availableReplicas}"}, nil),
						generator.WithResource("service", map[string]any{
							"apiVersion": "v1", "kind": "Service",
							"metadata": map[string]any{
								"name":      "${deployment.metadata.name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"spec": map[string]any{
								"selector": map[string]any{"app": "rollout"},
								"ports":    []any{map[string]any{"port": 8080, "targetPort": 8080}},
							},
						}, nil, nil),
					)
				},
				map[string]any{"name": "rollout", "replicas": 3},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, be rgdBackend) {
					ctx := env.Context()
					if ctx == nil {
						ctx = context.Background()
					}
					depKey := types.NamespacedName{Namespace: ns, Name: "rollout"}

					awaitInstanceCR(t, deploymentGVK, depKey, nil, 60*time.Second)

					if isBuiltin(be) {
						environment.Consistently(t, 2*time.Second, 250*time.Millisecond, func() error {
							svc := &corev1.Service{}
							if err := env.Client.Get(ctx, depKey, svc); err == nil {
								return fmt.Errorf("service created before deployment ready")
							}
							return nil
						})
					}

					dep := &appsv1.Deployment{}
					if err := env.Client.Get(ctx, depKey, dep); err != nil {
						t.Fatalf("get deployment: %v", err)
					}
					dep.Status.Replicas = 3
					dep.Status.ReadyReplicas = 3
					dep.Status.AvailableReplicas = 3
					dep.Status.Conditions = []appsv1.DeploymentCondition{{
						Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue,
						Reason: "MinimumReplicasAvailable",
					}}
					if err := env.Client.Status().Update(ctx, dep); err != nil {
						t.Fatalf("patch deployment status: %v", err)
					}

					awaitInstanceCR(t, serviceGVK, depKey, nil, 60*time.Second)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)
})
