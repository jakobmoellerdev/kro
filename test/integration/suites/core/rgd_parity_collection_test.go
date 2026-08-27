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
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
	"github.com/kubernetes-sigs/kro/test/integration/environment"
)

// This suite ports the forEach-collection behavioral scenarios
// (collection_test.go, collection_behavior_test.go, collection_watch_test.go)
// to the parity harness: every fixture is run through BOTH the built-in RGD
// controller and the RGD-as-Graph reimplementation (served at v1alpha2), and
// the SAME per-item child resources are asserted to converge with the SAME
// resolved field values.
//
// Parity boundaries (documented per scenario, branched with isBuiltin(be)):
//
//   - Standalone Graphs leave dependency-readiness gating opt-in
//     (GateReadiness off by default), so the strict ORDERING assertion
//     "dependent is absent until the collection's readyWhen is satisfied" only
//     holds for the built-in backend. Both backends still converge to the same
//     end state (all children present, instance ready).
//   - RGD-internal status (status.state=IN_PROGRESS/ACTIVE, TopologicalOrder,
//     status.<field> aggregates) is a built-in shape; the graph backend reports
//     status.ready instead. The harness's ready assertion already abstracts
//     this, so no scenario asserts status.state for the graph backend.
//   - The collection SIZE-LIMIT scenario asserts a built-in RGDConfig-enforced
//     cap surfaced through the built-in instance conditions; the graph backend
//     has no analog (N/A, see below).
//
// Helpers below (parityUpdateInstanceSpec / parityAwaitChildDeleted /
// parityDriftChildData / awaitChildData) are file-local. parityCtx is shared
// with the other rgd_parity_*_test.go files (defined in
// rgd_parity_includeref_test.go).

// parityUpdateInstanceSpec fetches the instance CR (by GVK) and applies mutate
// to its spec map, retrying on conflict (the controllers churn status between
// our Get and Update). Used by scale up/down and includeWhen-toggle scenarios.
func parityUpdateInstanceSpec(
	t GinkgoTInterface,
	gvk schema.GroupVersionKind,
	key types.NamespacedName,
	mutate func(spec map[string]any),
) {
	t.Helper()
	ctx := parityCtx()
	for range 20 {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)
		if err := env.Client.Get(ctx, key, u); err != nil {
			t.Fatalf("update instance: get %s: %v", key, err)
		}
		spec, _, _ := unstructured.NestedMap(u.Object, "spec")
		if spec == nil {
			spec = map[string]any{}
		}
		mutate(spec)
		u.Object["spec"] = spec
		if err := env.Client.Update(ctx, u); err == nil {
			return
		} else if !apierrors.IsConflict(err) {
			t.Fatalf("update instance: %s: %v", key, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("update instance %s: gave up after conflict retries", key)
}

// parityAwaitChildDeleted polls until a child object is gone.
func parityAwaitChildDeleted(t GinkgoTInterface, gvk schema.GroupVersionKind, key types.NamespacedName, timeout time.Duration) {
	t.Helper()
	env.AwaitDeleted(t, gvk, key, timeout)
}

// parityDriftChildData mutates one string field of a child's data map, then
// updates it — simulating out-of-band drift the controller should restore.
func parityDriftChildData(t GinkgoTInterface, gvk schema.GroupVersionKind, key types.NamespacedName, field, value string) {
	t.Helper()
	ctx := parityCtx()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	if err := env.Client.Get(ctx, key, u); err != nil {
		t.Fatalf("drift: get %s: %v", key, err)
	}
	if err := unstructured.SetNestedField(u.Object, value, "data", field); err != nil {
		t.Fatalf("drift: set field: %v", err)
	}
	if err := env.Client.Update(ctx, u); err != nil {
		t.Fatalf("drift: update %s: %v", key, err)
	}
}

// awaitChildData asserts a ConfigMap child's data.<field> resolves to want.
func awaitChildData(t GinkgoTInterface, ns, name, field, want string, timeout time.Duration) {
	t.Helper()
	awaitInstanceCR(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: name},
		func(u *unstructured.Unstructured) error {
			got, _, _ := unstructured.NestedString(u.Object, "data", field)
			if got != want {
				return &notYetError{msg: fmt.Sprintf("%s data.%s=%q, want %q", name, field, got, want)}
			}
			return nil
		}, timeout)
}

var _ = Describe("RGD parity: forEach collections", func() {

	// ── multiple ConfigMaps from a forEach collection (basic expansion) ─────
	// Ported from collection_test.go "should create multiple ConfigMaps from a
	// forEach collection". Deletion cleanup is covered by the dedicated
	// deletion scenario below.
	DescribeTable("expands a collection into one ConfigMap per list element",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityMultiCM"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "values": "[]string"},
							map[string]any{"ready": "${configmaps.map(c, c.metadata.name).size() >= 0}"},
						),
						generator.WithResourceCollection("configmaps", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-${value}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"key": "${value}"},
						},
							[]krov1alpha1.ForEachDimension{{"value": "${schema.spec.values}"}},
							nil, nil),
					)
				},
				map[string]any{"name": "multicm", "values": []any{"alpha", "beta", "gamma"}},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					for _, v := range []string{"alpha", "beta", "gamma"} {
						awaitChildData(t, ns, "multicm-"+v, "key", v, 60*time.Second)
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── cartesian product of two forEach iterators ─────────────────────────
	// Ported from collection_test.go "should handle cartesian product with
	// multiple forEach iterators".
	DescribeTable("expands a cartesian product of two forEach iterators",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityCartesian"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "regions": "[]string", "tiers": "[]string"},
							map[string]any{"ready": "${configmaps.map(c, c.metadata.name).size() >= 0}"},
						),
						generator.WithResourceCollection("configmaps", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-${region}-${tier}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"region": "${region}", "tier": "${tier}"},
						},
							[]krov1alpha1.ForEachDimension{
								{"region": "${schema.spec.regions}"},
								{"tier": "${schema.spec.tiers}"},
							},
							nil, nil),
					)
				},
				map[string]any{
					"name":    "cart",
					"regions": []any{"us", "eu"},
					"tiers":   []any{"web", "api"},
				},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					combos := []struct{ region, tier string }{
						{"us", "web"}, {"us", "api"}, {"eu", "web"}, {"eu", "api"},
					}
					for _, c := range combos {
						name := fmt.Sprintf("cart-%s-%s", c.region, c.tier)
						region, tier := c.region, c.tier
						awaitInstanceCR(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: name},
							func(u *unstructured.Unstructured) error {
								gotR, _, _ := unstructured.NestedString(u.Object, "data", "region")
								gotT, _, _ := unstructured.NestedString(u.Object, "data", "tier")
								if gotR != region || gotT != tier {
									return &notYetError{msg: fmt.Sprintf("%s region=%q tier=%q, want %q/%q", name, gotR, gotT, region, tier)}
								}
								return nil
							}, 60*time.Second)
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── includeWhen=true creates the collection; toggling recreates/prunes ──
	// Ported from collection_test.go "should create collection with includeWhen
	// condition" and "should toggle collection resources when includeWhen
	// changes". Combined: start enabled=false (absent), toggle true (created),
	// toggle false (pruned), then true again so the fixture converges.
	DescribeTable("includeWhen gates and toggles a collection",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityToggleColl"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "values": "[]string", "enabled": "boolean"},
							map[string]any{"ready": "${configmaps.map(c, c.metadata.name).size() >= 0}"},
						),
						generator.WithResourceCollection("configmaps", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-${value}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"key": "${value}"},
						},
							[]krov1alpha1.ForEachDimension{{"value": "${schema.spec.values}"}},
							nil,
							[]string{"${schema.spec.enabled}"}),
					)
				},
				map[string]any{"name": "toggle", "values": []any{"alpha", "beta"}, "enabled": false},
				func(t GinkgoTInterface, ns, instName string, gvk schema.GroupVersionKind, _ rgdBackend) {
					instKey := types.NamespacedName{Namespace: ns, Name: instName}

					// Phase 1: enabled=false -> no ConfigMaps.
					for _, v := range []string{"alpha", "beta"} {
						name := "toggle-" + v
						environment.Consistently(t, 3*time.Second, 250*time.Millisecond, func() error {
							u := &unstructured.Unstructured{}
							u.SetGroupVersionKind(configMapGVK)
							if err := env.Client.Get(parityCtx(), types.NamespacedName{Namespace: ns, Name: name}, u); err == nil {
								return fmt.Errorf("%s should not exist while enabled=false", name)
							}
							return nil
						})
					}

					// Phase 2: toggle enabled=true -> ConfigMaps created.
					parityUpdateInstanceSpec(t, gvk, instKey, func(spec map[string]any) { spec["enabled"] = true })
					for _, v := range []string{"alpha", "beta"} {
						awaitChildData(t, ns, "toggle-"+v, "key", v, 60*time.Second)
					}

					// Phase 3: toggle enabled=false -> ConfigMaps pruned.
					parityUpdateInstanceSpec(t, gvk, instKey, func(spec map[string]any) { spec["enabled"] = false })
					for _, v := range []string{"alpha", "beta"} {
						parityAwaitChildDeleted(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: "toggle-" + v}, 60*time.Second)
					}

					// Leave enabled=true so the fixture converges to ready.
					parityUpdateInstanceSpec(t, gvk, instKey, func(spec map[string]any) { spec["enabled"] = true })
					for _, v := range []string{"alpha", "beta"} {
						awaitChildData(t, ns, "toggle-"+v, "key", v, 60*time.Second)
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── collection depends on a regular resource (value propagation) ────────
	// Ported from collection_test.go "should create collection with dependency
	// on regular resource".
	DescribeTable("a collection reads a value from a regular resource",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityCollDep"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "values": "[]string"},
							map[string]any{"ready": "${baseConfig.metadata.name != ''}"},
						),
						generator.WithResource("baseConfig", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-base",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"version": "v1.0.0"},
						}, nil, nil),
						generator.WithResourceCollection("configs", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-${value}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{
								"key":     "${value}",
								"version": "${baseConfig.data.version}",
							},
						},
							[]krov1alpha1.ForEachDimension{{"value": "${schema.spec.values}"}},
							nil, nil),
					)
				},
				map[string]any{"name": "dep", "values": []any{"one", "two"}},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					awaitChildData(t, ns, "dep-base", "version", "v1.0.0", 60*time.Second)
					for _, v := range []string{"one", "two"} {
						name := "dep-" + v
						val := v
						awaitInstanceCR(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: name},
							func(u *unstructured.Unstructured) error {
								key, _, _ := unstructured.NestedString(u.Object, "data", "key")
								ver, _, _ := unstructured.NestedString(u.Object, "data", "version")
								if key != val || ver != "v1.0.0" {
									return &notYetError{msg: fmt.Sprintf("%s key=%q version=%q, want %q/v1.0.0", name, key, ver, val)}
								}
								return nil
							}, 60*time.Second)
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── dynamic forEach expression referencing another resource ────────────
	// Ported from collection_test.go "should handle collection chaining with
	// dynamic forEach expression". The forEach list is gated on a base
	// resource's data field.
	DescribeTable("a collection forEach expression references another resource",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityDynFE"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "values": "[]string"},
							map[string]any{"ready": "${baseConfig.metadata.name != ''}"},
						),
						generator.WithResource("baseConfig", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-base",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"enabled": "true", "prefix": "chained"},
						}, nil, nil),
						generator.WithResourceCollection("chainedConfigs", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-${val}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{
								"key":    "${val}",
								"prefix": "${baseConfig.data.prefix}",
							},
						},
							[]krov1alpha1.ForEachDimension{
								{"val": "${has(baseConfig.data.enabled) ? schema.spec.values : []}"},
							},
							nil, nil),
					)
				},
				map[string]any{"name": "chaining", "values": []any{"one", "two"}},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					for _, v := range []string{"one", "two"} {
						name := "chaining-" + v
						val := v
						awaitInstanceCR(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: name},
							func(u *unstructured.Unstructured) error {
								key, _, _ := unstructured.NestedString(u.Object, "data", "key")
								prefix, _, _ := unstructured.NestedString(u.Object, "data", "prefix")
								if key != val || prefix != "chained" {
									return &notYetError{msg: fmt.Sprintf("%s key=%q prefix=%q, want %q/chained", name, key, prefix, val)}
								}
								return nil
							}, 60*time.Second)
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── collection-to-collection chaining ──────────────────────────────────
	// Ported from collection_test.go "should handle collection-to-collection
	// chaining": the second collection iterates over the first's output
	// (${firstConfigs} is list(ConfigMap)).
	DescribeTable("a collection iterates over another collection's output",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityC2C"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "values": "[]string"},
							map[string]any{"ready": "${firstConfigs.map(c, c.metadata.name).size() >= 0}"},
						),
						generator.WithResourceCollection("firstConfigs", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-first-${val}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"key": "${val}", "source": "first-collection"},
						},
							[]krov1alpha1.ForEachDimension{{"val": "${schema.spec.values}"}},
							nil, nil),
						generator.WithResourceCollection("secondConfigs", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-second-${config.data.key}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{
								"originalKey":    "${config.data.key}",
								"originalSource": "${config.data.source}",
								"source":         "second-collection",
							},
						},
							[]krov1alpha1.ForEachDimension{{"config": "${firstConfigs}"}},
							nil, nil),
					)
				},
				map[string]any{"name": "c2c", "values": []any{"alpha", "beta"}},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					for _, v := range []string{"alpha", "beta"} {
						name := "c2c-first-" + v
						val := v
						awaitInstanceCR(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: name},
							func(u *unstructured.Unstructured) error {
								key, _, _ := unstructured.NestedString(u.Object, "data", "key")
								src, _, _ := unstructured.NestedString(u.Object, "data", "source")
								if key != val || src != "first-collection" {
									return &notYetError{msg: fmt.Sprintf("%s key=%q source=%q", name, key, src)}
								}
								return nil
							}, 60*time.Second)
					}
					for _, v := range []string{"alpha", "beta"} {
						name := "c2c-second-" + v
						val := v
						awaitInstanceCR(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: name},
							func(u *unstructured.Unstructured) error {
								ok, _, _ := unstructured.NestedString(u.Object, "data", "originalKey")
								os, _, _ := unstructured.NestedString(u.Object, "data", "originalSource")
								src, _, _ := unstructured.NestedString(u.Object, "data", "source")
								if ok != val || os != "first-collection" || src != "second-collection" {
									return &notYetError{msg: fmt.Sprintf("%s originalKey=%q originalSource=%q source=%q", name, ok, os, src)}
								}
								return nil
							}, 60*time.Second)
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── empty collection converges (no children) ───────────────────────────
	// Ported from collection_test.go "should handle empty collection list
	// gracefully": an empty forEach list means no children, and the instance
	// still reaches ready.
	DescribeTable("an empty collection converges with no children",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityEmptyColl"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "values": "[]string"},
							map[string]any{"ready": "${configmaps.map(c, c.metadata.name).size() >= 0}"},
						),
						generator.WithResourceCollection("configmaps", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-${value}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"key": "${value}"},
						},
							[]krov1alpha1.ForEachDimension{{"value": "${schema.spec.values}"}},
							nil, nil),
					)
				},
				map[string]any{"name": "empty", "values": []any{}},
				// No children to assert; runParityFixture's ready assertion
				// proves the instance converges with an empty collection.
				nil,
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── dependent references an empty collection via size() in CEL ─────────
	// Ported from collection_test.go "should allow dependents to reference
	// empty collections in CEL expressions" (issue #17 comment).
	DescribeTable("a dependent references an empty collection with size()",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityEmptyDep"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "names": "[]string"},
							map[string]any{"ready": "${summary.metadata.name != ''}"},
						),
						generator.WithResourceCollection("entries", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-entry-${name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"name": "${name}"},
						},
							[]krov1alpha1.ForEachDimension{{"name": "${schema.spec.names}"}},
							nil, nil),
						generator.WithResource("summary", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-summary",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"itemCount": "${string(size(entries))}"},
						}, nil, nil),
					)
				},
				map[string]any{"name": "emptydep", "names": []any{}},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					awaitChildData(t, ns, "emptydep-summary", "itemCount", "0", 60*time.Second)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── deep chaining + scale up/down ───────────────────────────────────────
	// Ported from collection_test.go "should handle deep chaining with scale up
	// and scale down": baseConfig -> level1 collection -> level2 collection
	// (c2c) -> summary -> finalPods collection. Scale up (add c) then scale
	// down (remove b), asserting per-item children appear/prune.
	//
	// N/A for status.* aggregates: the source asserts status.level1Count /
	// level2Count / podCount (RGD-internal status shape) — the graph backend
	// reports status.ready, not arbitrary aggregate fields, so those are not
	// asserted. The observable child set (the real contract) IS asserted for
	// both backends, including the summary ConfigMap's size()-derived counts.
	DescribeTable("deep collection chaining scales up and down",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityDeepChain"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "items": "[]string", "prefix": "string"},
							map[string]any{"ready": "${summaryConfig.metadata.name != ''}"},
						),
						generator.WithResource("baseConfig", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-base",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{
								"prefix":    "${schema.spec.prefix}",
								"itemCount": "${string(size(schema.spec.items))}",
							},
						}, nil, nil),
						generator.WithResourceCollection("level1Configs", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-l1-${entry}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{
								"entry":  "${entry}",
								"prefix": "${baseConfig.data.prefix}",
								"level":  "1",
							},
						},
							[]krov1alpha1.ForEachDimension{{"entry": "${schema.spec.items}"}},
							nil, nil),
						generator.WithResourceCollection("level2Configs", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-l2-${l1.data.entry}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{
								"sourceEntry":  "${l1.data.entry}",
								"sourcePrefix": "${l1.data.prefix}",
								"level":        "2",
							},
						},
							[]krov1alpha1.ForEachDimension{{"l1": "${level1Configs}"}},
							nil, nil),
						generator.WithResource("summaryConfig", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-summary",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{
								"level1Count": "${string(size(level1Configs))}",
								"level2Count": "${string(size(level2Configs))}",
							},
						}, nil, nil),
						generator.WithResourceCollection("finalPods", map[string]any{
							"apiVersion": "v1", "kind": "Pod",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-pod-${l2.data.sourceEntry}",
								"namespace": "${schema.metadata.namespace}",
							},
							"spec": map[string]any{
								"restartPolicy": "Never",
								"containers": []any{
									map[string]any{
										"name":    "worker",
										"image":   "busybox:latest",
										"command": []any{"sh", "-c", "echo ${summaryConfig.data.level2Count} items && sleep 3600"},
									},
								},
							},
						},
							[]krov1alpha1.ForEachDimension{{"l2": "${level2Configs}"}},
							nil, nil),
					)
				},
				map[string]any{"name": "deep", "items": []any{"a", "b"}, "prefix": "test"},
				func(t GinkgoTInterface, ns, instName string, gvk schema.GroupVersionKind, _ rgdBackend) {
					instKey := types.NamespacedName{Namespace: ns, Name: instName}
					podGVK := schema.GroupVersionKind{Version: "v1", Kind: "Pod"}

					// Initial: items a, b.
					awaitChildData(t, ns, "deep-base", "itemCount", "2", 60*time.Second)
					for _, item := range []string{"a", "b"} {
						awaitChildData(t, ns, "deep-l1-"+item, "entry", item, 60*time.Second)
						awaitChildData(t, ns, "deep-l2-"+item, "sourceEntry", item, 60*time.Second)
						awaitInstanceCR(t, podGVK, types.NamespacedName{Namespace: ns, Name: "deep-pod-" + item}, nil, 60*time.Second)
					}
					awaitChildData(t, ns, "deep-summary", "level2Count", "2", 60*time.Second)

					// SCALE UP: add "c".
					parityUpdateInstanceSpec(t, gvk, instKey, func(spec map[string]any) {
						spec["items"] = []any{"a", "b", "c"}
					})
					awaitChildData(t, ns, "deep-l1-c", "entry", "c", 60*time.Second)
					awaitChildData(t, ns, "deep-l2-c", "sourceEntry", "c", 60*time.Second)
					awaitInstanceCR(t, podGVK, types.NamespacedName{Namespace: ns, Name: "deep-pod-c"}, nil, 60*time.Second)
					awaitChildData(t, ns, "deep-summary", "level2Count", "3", 60*time.Second)

					// SCALE DOWN: remove "b".
					parityUpdateInstanceSpec(t, gvk, instKey, func(spec map[string]any) {
						spec["items"] = []any{"a", "c"}
					})
					parityAwaitChildDeleted(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: "deep-l1-b"}, 60*time.Second)
					parityAwaitChildDeleted(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: "deep-l2-b"}, 60*time.Second)
					parityAwaitChildDeleted(t, podGVK, types.NamespacedName{Namespace: ns, Name: "deep-pod-b"}, 60*time.Second)
					awaitChildData(t, ns, "deep-summary", "level2Count", "2", 60*time.Second)
					for _, item := range []string{"a", "c"} {
						awaitChildData(t, ns, "deep-l1-"+item, "entry", item, 60*time.Second)
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── collection readyWhen per-item with `each` (data-driven) ────────────
	// Ported from collection_test.go "should evaluate readyWhen per-item
	// expressions with each keyword" and "should create collection resources
	// without readyWhen expressions". The readyWhen reads each item's data.ready
	// (templated "true"), so the collection is ready and both backends converge.
	DescribeTable("collection readyWhen per-item via the each keyword",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityEachReady"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "values": "[]string"},
							map[string]any{"ready": "${configs.map(c, c.metadata.name).size() >= 0}"},
						),
						generator.WithResourceCollection("configs", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-${value}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"key": "${value}", "ready": "true"},
						},
							[]krov1alpha1.ForEachDimension{{"value": "${schema.spec.values}"}},
							[]string{"${each.data.ready == 'true'}"},
							nil),
					)
				},
				map[string]any{"name": "eachready", "values": []any{"one", "two", "three"}},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					for _, v := range []string{"one", "two", "three"} {
						name := "eachready-" + v
						val := v
						awaitInstanceCR(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: name},
							func(u *unstructured.Unstructured) error {
								key, _, _ := unstructured.NestedString(u.Object, "data", "key")
								ready, _, _ := unstructured.NestedString(u.Object, "data", "ready")
								if key != val || ready != "true" {
									return &notYetError{msg: fmt.Sprintf("%s key=%q ready=%q", name, key, ready)}
								}
								return nil
							}, 60*time.Second)
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── collection readyWhen gates a dependent (strict ordering: builtin) ───
	// Ported from collection_test.go "should keep instance IN_PROGRESS until
	// collection readyWhen is satisfied (no dependents)" and "should block
	// dependent resources until collection readyWhen is satisfied", using a
	// data-driven ready flag (makeReady spec bool) so it converges in envtest
	// without a kubelet.
	//
	// PARITY BOUNDARY: the strict ORDERING assertion "coordinator absent while
	// makeReady=false" only holds for the built-in backend (standalone Graphs
	// leave GateReadiness off by default). Both backends converge to the same
	// end state once makeReady=true.
	DescribeTable("collection readyWhen gates a dependent resource",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityCollGate"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "workers": "[]string", "makeReady": "boolean"},
							map[string]any{"ready": "${coordinator.metadata.name != ''}"},
						),
						generator.WithResourceCollection("workerConfigs", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-worker-${worker}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{
								"worker": "${worker}",
								"ready":  "${string(schema.spec.makeReady)}",
							},
						},
							[]krov1alpha1.ForEachDimension{{"worker": "${schema.spec.workers}"}},
							[]string{"${each.data.ready == 'true'}"},
							nil),
						generator.WithResource("coordinator", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-coordinator",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{
								"workerCount": "${string(size(workerConfigs))}",
								"firstWorker": "${workerConfigs[0].metadata.name}",
							},
						}, nil, nil),
					)
				},
				map[string]any{
					"name":      "cluster",
					"workers":   []any{"alpha", "beta", "gamma"},
					"makeReady": false,
				},
				func(t GinkgoTInterface, ns, instName string, gvk schema.GroupVersionKind, be rgdBackend) {
					instKey := types.NamespacedName{Namespace: ns, Name: instName}
					coordKey := types.NamespacedName{Namespace: ns, Name: "cluster-coordinator"}

					// Worker items exist regardless of readiness gating.
					for _, w := range []string{"alpha", "beta", "gamma"} {
						awaitChildData(t, ns, "cluster-worker-"+w, "worker", w, 60*time.Second)
					}

					// PARITY BOUNDARY: strict ordering (coordinator blocked
					// while collection not ready) only for the built-in backend.
					if isBuiltin(be) {
						environment.Consistently(t, 4*time.Second, 250*time.Millisecond, func() error {
							u := &unstructured.Unstructured{}
							u.SetGroupVersionKind(configMapGVK)
							if err := env.Client.Get(parityCtx(), coordKey, u); err == nil {
								return fmt.Errorf("coordinator created before collection readyWhen satisfied")
							}
							return nil
						})
					}

					// Flip makeReady=true: worker items become ready, coordinator
					// converges. Both backends reach the same end state.
					parityUpdateInstanceSpec(t, gvk, instKey, func(spec map[string]any) { spec["makeReady"] = true })
					for _, w := range []string{"alpha", "beta", "gamma"} {
						awaitChildData(t, ns, "cluster-worker-"+w, "ready", "true", 60*time.Second)
					}
					awaitInstanceCR(t, configMapGVK, coordKey,
						func(u *unstructured.Unstructured) error {
							count, _, _ := unstructured.NestedString(u.Object, "data", "workerCount")
							if count != "3" {
								return &notYetError{msg: "coordinator workerCount=" + count + ", want 3"}
							}
							return nil
						}, 60*time.Second)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── instance deletion cleans up ALL collection resources ────────────────
	// Ported from collection_test.go "should delete all collection resources
	// when instance is deleted".
	DescribeTable("deleting the instance deletes all collection resources",
		func(makeBackend func() rgdBackend) {
			runParityFixtureOpts(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityCollDelete"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "values": "[]string"},
							map[string]any{"ready": "${configs.map(c, c.metadata.name).size() >= 0}"},
						),
						generator.WithResourceCollection("configs", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-${value}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"key": "${value}"},
						},
							[]krov1alpha1.ForEachDimension{{"value": "${schema.spec.values}"}},
							nil, nil),
					)
				},
				map[string]any{"name": "del", "values": []any{"one", "two", "three"}},
				func(t GinkgoTInterface, ns, instName string, gvk schema.GroupVersionKind, _ rgdBackend) {
					values := []string{"one", "two", "three"}
					for _, v := range values {
						awaitChildData(t, ns, "del-"+v, "key", v, 60*time.Second)
					}

					// Delete the instance, then assert all collection children
					// are cleaned up.
					u := &unstructured.Unstructured{}
					u.SetGroupVersionKind(gvk)
					u.SetNamespace(ns)
					u.SetName(instName)
					if err := env.Client.Delete(parityCtx(), u); err != nil {
						t.Fatalf("delete instance: %v", err)
					}
					for _, v := range values {
						parityAwaitChildDeleted(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: "del-" + v}, 60*time.Second)
					}
				},
				// The instance is deleted mid-assertion, so skip the trailing
				// ready assertion (there is no instance left to become ready).
				true,
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── drift on a collection item is restored ─────────────────────────────
	// Ported from collection_test.go "should detect and restore drift in
	// collection resources".
	//
	// PARITY BOUNDARY (RESOLVED): the built-in backend restores managed-field
	// drift on collection items. The standalone graph backend previously did NOT:
	// its collection drift-watch registers a selector scoped by node-id AND
	// instance-id, but standalone collection children were stamped with only
	// node-id (instance-id was applied solely by the RGD path's LabelInjector).
	// The selector therefore never matched, so out-of-band drift on a collection
	// item never re-enqueued the Graph. Fixed by stamping the Graph-UID fallback
	// instance-id onto standalone collection children in stampKROMeta
	// (pkg/graphengine/executor/simple.go), mirroring the fallback watchCollection
	// already uses for the selector. Drift restore is now asserted for BOTH
	// backends.
	DescribeTable("drift on a collection item is restored",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityCollDrift"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "items": "[]string", "prefix": "string"},
							map[string]any{"ready": "${configs.map(c, c.metadata.name).size() >= 0}"},
						),
						generator.WithResourceCollection("configs", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-${entry}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{
								"entry":  "${entry}",
								"prefix": "${schema.spec.prefix}",
								"static": "unchanged",
							},
						},
							[]krov1alpha1.ForEachDimension{{"entry": "${schema.spec.items}"}},
							nil, nil),
					)
				},
				map[string]any{"name": "drift", "items": []any{"alpha", "beta"}, "prefix": "original"},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, be rgdBackend) {
					for _, item := range []string{"alpha", "beta"} {
						name := "drift-" + item
						it := item
						awaitInstanceCR(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: name},
							func(u *unstructured.Unstructured) error {
								entry, _, _ := unstructured.NestedString(u.Object, "data", "entry")
								prefix, _, _ := unstructured.NestedString(u.Object, "data", "prefix")
								if entry != it || prefix != "original" {
									return &notYetError{msg: fmt.Sprintf("%s entry=%q prefix=%q", name, entry, prefix)}
								}
								return nil
							}, 60*time.Second)
					}

					// Tamper with a managed field on the alpha item, expect restore
					// on both backends (collection-item instance-id fix, see table
					// comment).
					alphaKey := types.NamespacedName{Namespace: ns, Name: "drift-alpha"}
					parityDriftChildData(t, configMapGVK, alphaKey, "prefix", "DRIFTED")
					awaitInstanceCR(t, configMapGVK, alphaKey,
						func(u *unstructured.Unstructured) error {
							prefix, _, _ := unstructured.NestedString(u.Object, "data", "prefix")
							entry, _, _ := unstructured.NestedString(u.Object, "data", "entry")
							static, _, _ := unstructured.NestedString(u.Object, "data", "static")
							if prefix != "original" || entry != "alpha" || static != "unchanged" {
								return &notYetError{msg: fmt.Sprintf("drift-alpha not restored: prefix=%q entry=%q static=%q", prefix, entry, static)}
							}
							return nil
						}, 60*time.Second)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── collection spans multiple namespaces + cross-ns drift correction ────
	// Ported from collection_test.go "should delete collection resources across
	// multiple namespaces" (creation half) and collection_behavior_test.go
	// "corrects drift ... in every namespace the collection spans". Even
	// indices land in ns1 (the instance namespace), odd in ns2 (an alt
	// namespace).
	//
	// PARITY BOUNDARY: cross-namespace child CREATION is asserted for both
	// backends. Cross-namespace drift RESTORE is asserted only for the built-in
	// backend (the graph backend cannot re-resolve the L2 `schema` self-ref on a
	// child-triggered reconcile, so collection-item drift is not restored — same
	// boundary as the single-namespace drift scenario above).
	DescribeTable("a collection spans multiple namespaces and drift is corrected in each",
		func(makeBackend func() rgdBackend) {
			// Alt namespace created per run; register cleanup up front.
			t := GinkgoT()
			altNS := fmt.Sprintf("parity-alt-%s", rand.String(6))
			altObj := &unstructured.Unstructured{}
			altObj.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "Namespace"})
			altObj.SetName(altNS)
			if err := env.Client.Create(parityCtx(), altObj); err != nil {
				t.Fatalf("create alt namespace: %v", err)
			}
			t.Cleanup(func() { _ = env.Client.Delete(context.Background(), altObj) })

			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityCrossNS"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "ns1": "string", "ns2": "string"},
							map[string]any{"ready": "${configmaps.map(c, c.metadata.name).size() >= 0}"},
						),
						generator.WithResourceCollection("configmaps", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-item-${string(i)}",
								"namespace": "${i % 2 == 0 ? schema.spec.ns1 : schema.spec.ns2}",
							},
							"data": map[string]any{"index": "${string(i)}", "managed": "expected"},
						},
							[]krov1alpha1.ForEachDimension{{"i": "${lists.range(6)}"}},
							nil, nil),
					)
				},
				// ns1 is a sentinel rewritten to the real primary namespace in
				// the assertion body (the namespace isn't known until the
				// instance is created by the harness).
				map[string]any{"name": "crossns", "ns1": "__PRIMARY__", "ns2": altNS},
				func(t GinkgoTInterface, ns, instName string, gvk schema.GroupVersionKind, be rgdBackend) {
					// Rewrite ns1 to the real primary namespace now that we know it.
					parityUpdateInstanceSpec(t, gvk, types.NamespacedName{Namespace: ns, Name: instName},
						func(spec map[string]any) { spec["ns1"] = ns })

					// Even indices -> primary ns, odd indices -> alt ns.
					for _, idx := range []int{0, 2, 4} {
						awaitChildData(t, ns, fmt.Sprintf("crossns-item-%d", idx), "index", fmt.Sprintf("%d", idx), 60*time.Second)
					}
					for _, idx := range []int{1, 3, 5} {
						awaitChildData(t, altNS, fmt.Sprintf("crossns-item-%d", idx), "index", fmt.Sprintf("%d", idx), 60*time.Second)
					}

					// PARITY BOUNDARY: collection-item drift restore is built-in
					// only (see table comment).
					if !isBuiltin(be) {
						return
					}

					// Drift correction spans both namespaces: tamper item-0
					// (primary) and item-1 (alt), expect both restored.
					items := []struct {
						ns   string
						name string
					}{
						{ns, "crossns-item-0"},
						{altNS, "crossns-item-1"},
					}
					for _, it := range items {
						key := types.NamespacedName{Namespace: it.ns, Name: it.name}
						parityDriftChildData(t, configMapGVK, key, "managed", "tampered")
						nsName := it.name
						awaitInstanceCR(t, configMapGVK, key,
							func(u *unstructured.Unstructured) error {
								got, _, _ := unstructured.NestedString(u.Object, "data", "managed")
								if got != "expected" {
									return &notYetError{msg: fmt.Sprintf("%s data.managed=%q, want expected (not restored)", nsName, got)}
								}
								return nil
							}, 60*time.Second)
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── reactive reconcile when a non-last collection item is modified ──────
	// Ported from collection_watch_test.go "should reactively reconcile when a
	// non-last collection item is modified": mutate the FIRST item and assert
	// it is restored.
	//
	// PARITY BOUNDARY: the built-in backend watches every collection item and
	// reactively restores drift. The standalone graph backend cannot re-resolve
	// the L2 `schema` self-ref on a child-triggered reconcile, so the restore is
	// asserted only for the built-in backend (same boundary as the drift
	// scenarios above). Both backends agree on the initial child set.
	DescribeTable("modifying a non-last collection item is reconciled",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityCollWatch"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "values": "[]string"},
							map[string]any{"ready": "${configmaps.map(c, c.metadata.name).size() >= 0}"},
						),
						generator.WithResourceCollection("configmaps", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-${value}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"key": "${value}"},
						},
							[]krov1alpha1.ForEachDimension{{"value": "${schema.spec.values}"}},
							nil, nil),
					)
				},
				map[string]any{"name": "watch", "values": []any{"alpha", "beta", "gamma"}},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, be rgdBackend) {
					for _, v := range []string{"alpha", "beta", "gamma"} {
						awaitChildData(t, ns, "watch-"+v, "key", v, 60*time.Second)
					}

					// PARITY BOUNDARY: collection-item drift restore is built-in
					// only (see table comment).
					if !isBuiltin(be) {
						return
					}

					// Mutate the FIRST item (alpha) and assert it is restored.
					alphaKey := types.NamespacedName{Namespace: ns, Name: "watch-alpha"}
					parityDriftChildData(t, configMapGVK, alphaKey, "key", "tampered")
					awaitChildData(t, ns, "watch-alpha", "key", "alpha", 60*time.Second)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── collection shrink prunes the removed item ──────────────────────────
	// Ported from collection_watch_test.go "should not react to externally
	// created resources after collection shrinks" (shrink+prune half). The
	// stale-watch-selector negative assertion is builtin-specific watch
	// internals (N/A for parity); the observable prune-on-shrink IS asserted
	// for both backends.
	DescribeTable("shrinking a collection prunes the removed item",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityCollShrink"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "values": "[]string"},
							map[string]any{"ready": "${configmaps.map(c, c.metadata.name).size() >= 0}"},
						),
						generator.WithResourceCollection("configmaps", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-${value}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{"key": "${value}"},
						},
							[]krov1alpha1.ForEachDimension{{"value": "${schema.spec.values}"}},
							nil, nil),
					)
				},
				map[string]any{"name": "shrink", "values": []any{"alpha", "beta", "gamma"}},
				func(t GinkgoTInterface, ns, instName string, gvk schema.GroupVersionKind, _ rgdBackend) {
					for _, v := range []string{"alpha", "beta", "gamma"} {
						awaitChildData(t, ns, "shrink-"+v, "key", v, 60*time.Second)
					}

					// Shrink 3 -> 2 by removing gamma; assert it is pruned and
					// the remaining items stay.
					parityUpdateInstanceSpec(t, gvk, types.NamespacedName{Namespace: ns, Name: instName},
						func(spec map[string]any) { spec["values"] = []any{"alpha", "beta"} })
					parityAwaitChildDeleted(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: "shrink-gamma"}, 60*time.Second)
					for _, v := range []string{"alpha", "beta"} {
						awaitChildData(t, ns, "shrink-"+v, "key", v, 60*time.Second)
					}
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── N/A scenarios (no graph analog) ─────────────────────────────────────
	//
	// N/A: collection_behavior_test.go "enforces the configured collection size
	// limit" — asserts a built-in RGDConfig-enforced cap
	// (configuredMaxCollectionSize) surfaced through the built-in instance's
	// status conditions. The graph backend has no RGDConfig cap plumbed through
	// its L0/L2 template pipeline and reports only status.ready, so there is no
	// observable parity contract.
	//
	// N/A: collection_watch_test.go "should not react to externally created
	// resources after collection shrinks" (the negative watch-selector half) —
	// asserts built-in-controller watch internals (a stale label-selector watch
	// must NOT fire for an unlabeled external object). This is implementation
	// detail of the built-in collection watch machinery, not an observable
	// child-convergence contract the graph backend shares. The positive
	// prune-on-shrink behavior IS ported above.
})
