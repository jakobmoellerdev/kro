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

package executor

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/testutil/generator"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/watchrouter"
)

// Patch field-manager semantics (contribute without ownership, release
// without delete, status-subresource routing) depend on the real API
// server's server-side-apply managed-field tracking, which the fake client
// does not model. These tests run against envtest and skip when
// KUBEBUILDER_ASSETS is unset.

var (
	patchEnvOnce sync.Once
	patchTestEnv *envtest.Environment
	patchEnvCfg  *rest.Config
	patchEnvErr  error
)

// TestMain owns the envtest lifecycle for the executor package: it boots
// lazily (only the patch tests need it) and is torn down once after the run.
func TestMain(m *testing.M) {
	code := m.Run()
	if patchTestEnv != nil {
		_ = patchTestEnv.Stop()
	}
	os.Exit(code)
}

// patchEnvClient boots (once) an envtest control plane and returns a typed
// client. Skips the calling test when KUBEBUILDER_ASSETS is not configured.
func patchEnvClient(t *testing.T) client.Client {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest patch tests")
	}
	patchEnvOnce.Do(func() {
		patchTestEnv = &envtest.Environment{}
		patchEnvCfg, patchEnvErr = patchTestEnv.Start()
	})
	if patchEnvErr != nil {
		t.Fatalf("start envtest: %v", patchEnvErr)
	}
	cl, err := client.New(patchEnvCfg, client.Options{Scheme: clientgoscheme.Scheme})
	require.NoError(t, err)
	return cl
}

var podGVK = schema.GroupVersionKind{Version: "v1", Kind: "Pod"}

// mustCreateConfigMap seeds a ConfigMap that a patch node will contribute to.
func mustCreateConfigMap(t *testing.T, cl client.Client, ns, name string, data map[string]any) {
	t.Helper()
	cm := &unstructured.Unstructured{}
	cm.SetGroupVersionKind(configMapGVK)
	cm.SetNamespace(ns)
	cm.SetName(name)
	if data != nil {
		require.NoError(t, unstructured.SetNestedMap(cm.Object, data, "data"))
	}
	require.NoError(t, cl.Create(context.Background(), cm))
}

func getConfigMap(t *testing.T, cl client.Client, ns, name string) *unstructured.Unstructured {
	t.Helper()
	cm := &unstructured.Unstructured{}
	cm.SetGroupVersionKind(configMapGVK)
	require.NoError(t, cl.Get(context.Background(),
		types.NamespacedName{Namespace: ns, Name: name}, cm))
	return cm
}

func hasFieldManager(obj *unstructured.Unstructured, manager string) bool {
	for _, mf := range obj.GetManagedFields() {
		if mf.Manager == manager {
			return true
		}
	}
	return false
}

// TestPatch_ContributesFields verifies a patch node adds fields to a
// pre-existing object under its own per-node field manager, records a
// Contribution, and never records the target as an owned managed resource.
func TestPatch_ContributesFields(t *testing.T) {
	cl := patchEnvClient(t)
	ns := "default"
	mustCreateConfigMap(t, cl, ns, "existing", map[string]any{"orig": "kept"})

	g := generator.NewGraph("g",
		generator.WithNamespace(ns),
		generator.WithDef("cfg", map[string]any{"v": "contributed"}),
		generator.WithPatch("p", "v1", "ConfigMap", "existing", map[string]any{
			"data": map[string]any{"added": "${cfg.v}"},
		}),
	)
	g.SetUID("uid-contributes")

	rt := compileAndBuild(t, g)
	res, err := NewSimple(cl).Apply(context.Background(), rt, watchrouter.NoopWatcher{})
	require.NoError(t, err)

	// Patch nodes never enter the owned inventory; they are tracked as
	// contributions instead.
	assert.Empty(t, res.Applied, "patch must not be recorded as an owned resource")
	require.Len(t, res.Contributions, 1)
	c := res.Contributions[0]
	assert.Equal(t, "ConfigMap", c.Kind)
	assert.Equal(t, "existing", c.Name)
	assert.Equal(t, patchFieldManager(g.GetUID(), "p"), c.FieldManager)

	cm := getConfigMap(t, cl, ns, "existing")
	data, _, _ := unstructured.NestedStringMap(cm.Object, "data")
	assert.Equal(t, "contributed", data["added"], "contributed field is present")
	assert.Equal(t, "kept", data["orig"], "pre-existing field survives")
	assert.True(t, hasFieldManager(cm, c.FieldManager), "contribution is owned by the per-node field manager")
}

// TestPatch_TargetAbsentSoftRequeue verifies that a patch whose target does
// not exist is a soft requeue: ErrNotReady, the node is Unresolved, and no
// contribution is recorded.
func TestPatch_TargetAbsentSoftRequeue(t *testing.T) {
	cl := patchEnvClient(t)

	g := generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithPatch("p", "v1", "ConfigMap", "missing", map[string]any{
			"data": map[string]any{"k": "v"},
		}),
	)
	g.SetUID("uid-absent")

	rt := compileAndBuild(t, g)
	res, err := NewSimple(cl).Apply(context.Background(), rt, watchrouter.NoopWatcher{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNotReady)
	assert.Contains(t, res.Unresolved, "p")
	assert.Empty(t, res.Contributions)
}

// TestPatch_DisjointContributionsCoexist verifies two patch nodes each
// contribute different fields to the same object and both survive, each under
// its own field manager.
func TestPatch_DisjointContributionsCoexist(t *testing.T) {
	cl := patchEnvClient(t)
	ns := "default"
	mustCreateConfigMap(t, cl, ns, "shared", nil)

	g := generator.NewGraph("g",
		generator.WithNamespace(ns),
		generator.WithPatch("p1", "v1", "ConfigMap", "shared", map[string]any{
			"data": map[string]any{"one": "from-p1"},
		}),
		generator.WithPatch("p2", "v1", "ConfigMap", "shared", map[string]any{
			"data": map[string]any{"two": "from-p2"},
		}),
	)
	g.SetUID("uid-disjoint")

	rt := compileAndBuild(t, g)
	res, err := NewSimple(cl).Apply(context.Background(), rt, watchrouter.NoopWatcher{})
	require.NoError(t, err)
	require.Len(t, res.Contributions, 2)

	cm := getConfigMap(t, cl, ns, "shared")
	data, _, _ := unstructured.NestedStringMap(cm.Object, "data")
	assert.Equal(t, "from-p1", data["one"])
	assert.Equal(t, "from-p2", data["two"])
	assert.True(t, hasFieldManager(cm, patchFieldManager(g.GetUID(), "p1")))
	assert.True(t, hasFieldManager(cm, patchFieldManager(g.GetUID(), "p2")))
}

// TestPatch_ReleaseDropsFieldsKeepsObject verifies release-on-prune: the
// contributed fields are dropped when the contribution is released, while the
// target object and other fields survive.
func TestPatch_ReleaseDropsFieldsKeepsObject(t *testing.T) {
	cl := patchEnvClient(t)
	ns := "default"
	mustCreateConfigMap(t, cl, ns, "releasable", map[string]any{"orig": "kept"})

	g := generator.NewGraph("g",
		generator.WithNamespace(ns),
		generator.WithPatch("p", "v1", "ConfigMap", "releasable", map[string]any{
			"data": map[string]any{"added": "gone-later"},
		}),
	)
	g.SetUID("uid-release")

	rt := compileAndBuild(t, g)
	ex := NewSimple(cl)
	res, err := ex.Apply(context.Background(), rt, watchrouter.NoopWatcher{})
	require.NoError(t, err)
	require.Len(t, res.Contributions, 1)

	cm := getConfigMap(t, cl, ns, "releasable")
	data, _, _ := unstructured.NestedStringMap(cm.Object, "data")
	require.Equal(t, "gone-later", data["added"])

	require.NoError(t, ex.Release(context.Background(), res.Contributions))

	cm = getConfigMap(t, cl, ns, "releasable")
	data, _, _ = unstructured.NestedStringMap(cm.Object, "data")
	_, stillThere := data["added"]
	assert.False(t, stillThere, "released field is dropped")
	assert.Equal(t, "kept", data["orig"], "unrelated field survives")
	assert.False(t, hasFieldManager(cm, res.Contributions[0].FieldManager), "field manager relinquished its fields")
}

// TestPatch_StatusSubresourceRouting verifies a patch with subresource=status
// contributes to the status subresource.
func TestPatch_StatusSubresourceRouting(t *testing.T) {
	cl := patchEnvClient(t)
	ns := "default"

	// A minimal Pod so the status subresource exists to contribute to.
	pod := &unstructured.Unstructured{}
	pod.SetGroupVersionKind(podGVK)
	pod.SetNamespace(ns)
	pod.SetName("statuspod")
	require.NoError(t, unstructured.SetNestedSlice(pod.Object, []any{
		map[string]any{"name": "c", "image": "nginx"},
	}, "spec", "containers"))
	require.NoError(t, cl.Create(context.Background(), pod))

	g := generator.NewGraph("g",
		generator.WithNamespace(ns),
		generator.WithPatchSpec("p", &expv1alpha1.PatchSpec{
			APIVersion:  "v1",
			Kind:        "Pod",
			Metadata:    expv1alpha1.PatchMetadata{Name: "statuspod"},
			Subresource: "status",
			Body: generator.RawExtFromMap(map[string]any{
				"status": map[string]any{"phase": "Running"},
			}),
		}),
	)
	g.SetUID("uid-status")

	rt := compileAndBuild(t, g)
	res, err := NewSimple(cl).Apply(context.Background(), rt, watchrouter.NoopWatcher{})
	require.NoError(t, err)
	require.Len(t, res.Contributions, 1)
	assert.Equal(t, "status", res.Contributions[0].Subresource)

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(podGVK)
	require.NoError(t, cl.Get(context.Background(),
		types.NamespacedName{Namespace: ns, Name: "statuspod"}, got))
	phase, _, _ := unstructured.NestedString(got.Object, "status", "phase")
	assert.Equal(t, "Running", phase)
}
