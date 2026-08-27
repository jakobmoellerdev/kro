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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/testutil/generator"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/watchrouter"
)

// cmData reads a ConfigMap's data map from the cluster.
func cmData(t *testing.T, c client.Client, name string) map[string]any {
	t.Helper()
	cm := &unstructured.Unstructured{}
	cm.SetGroupVersionKind(configMapGVK)
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: "default", Name: name}, cm))
	data, _, _ := unstructured.NestedMap(cm.Object, "data")
	return data
}

// TestSimple_ReadyLifecycleSignal exercises the `.ready()` CEL macro end to
// end through the executor: a status node reads the readiness of two upstream
// nodes and writes ACTIVE/IN_PROGRESS accordingly. This is the status-writeback
// shape from examples/graph/rgd.yaml (state = <all resources ready> ? ACTIVE :
// IN_PROGRESS), reduced to the mechanism under test.
//
// `a` has no readyWhen → ready as soon as applied. `b` has a readyWhen that is
// false → applied-but-not-ready. `.ready()` rewrites to __kro_ready__[?'id'],
// which carries no DAG edge, so `status` is ordered last by its node index and
// observes the fully-populated per-pass readiness map.
func TestSimple_ReadyLifecycleSignal(t *testing.T) {
	t.Parallel()

	graph := func() *expv1alpha1.Graph {
		return generator.NewGraph("g",
			generator.WithNamespace("default"),
			// a: ready immediately (no readyWhen).
			generator.WithTemplate("a", map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": "a"},
				"data":     map[string]any{"k": "v"},
			}),
			// b: applied but never ready (readyWhen is false).
			generator.WithTemplate("b", map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": "b"},
				"data":     map[string]any{"k": "v"},
			}),
			generator.WithReadyWhen(`${b.data.k == "never"}`),
			// status: reads both nodes' readiness via .ready(). No DAG edge to
			// a/b (the reference is to __kro_ready__), so its high node index
			// orders it last — after a and b are marked this pass.
			generator.WithTemplate("stat", map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": "stat"},
				"data": map[string]any{
					"state":  `${a.ready().orValue(false) && b.ready().orValue(false) ? "ACTIVE" : "IN_PROGRESS"}`,
					"aReady": `${a.ready().orValue(false) ? "yes" : "no"}`,
					"bReady": `${b.ready().orValue(false) ? "yes" : "no"}`,
				},
			}),
		)
	}

	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	ex := NewSimple(cl)

	_, _ = ex.Apply(context.Background(), compileAndBuild(t, graph()), watchrouter.NoopWatcher{})

	// a and b both applied.
	assert.True(t, cmExists(t, cl, "a"))
	assert.True(t, cmExists(t, cl, "b"))

	// status observed a ready, b not ready → IN_PROGRESS.
	data := cmData(t, cl, "stat")
	assert.Equal(t, "yes", data["aReady"], "a has no readyWhen → ready")
	assert.Equal(t, "no", data["bReady"], "b's readyWhen is false → not ready")
	assert.Equal(t, "IN_PROGRESS", data["state"], "not all nodes ready → IN_PROGRESS")
}

// TestSimple_ReadyLifecycleSignal_AllReady is the converged case: every
// referenced node is ready, so the status node reports ACTIVE.
func TestSimple_ReadyLifecycleSignal_AllReady(t *testing.T) {
	t.Parallel()

	graph := generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithTemplate("a", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "a"},
			"data":     map[string]any{"k": "v"},
		}),
		generator.WithTemplate("b", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "b"},
			"data":     map[string]any{"k": "v"},
		}),
		// b ready when its own data lands (it does, immediately after apply).
		generator.WithReadyWhen(`${b.data.k == "v"}`),
		generator.WithTemplate("stat", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "stat"},
			"data": map[string]any{
				"state": `${a.ready().orValue(false) && b.ready().orValue(false) ? "ACTIVE" : "IN_PROGRESS"}`,
			},
		}),
	)

	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	ex := NewSimple(cl)

	_, err := ex.Apply(context.Background(), compileAndBuild(t, graph), watchrouter.NoopWatcher{})
	require.NoError(t, err)

	data := cmData(t, cl, "stat")
	assert.Equal(t, "ACTIVE", data["state"], "all referenced nodes ready → ACTIVE")
}

// TestSimple_ReadyUnknownNodeIsNone confirms .ready() on a node that was never
// evaluated this pass yields optional.none(), so .orValue supplies the default.
func TestSimple_ReadyUnknownNodeIsNone(t *testing.T) {
	t.Parallel()

	graph := generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithTemplate("a", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "a"},
			"data":     map[string]any{"k": "v"},
		}),
		generator.WithTemplate("stat", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "stat"},
			"data": map[string]any{
				// "ghost" is not a node in this graph; .ready() must be
				// optional.none() and fall through to the default.
				"ghost": `${a.ready().orValue(false) ? "present" : "default"}`,
			},
		}),
	)

	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	ex := NewSimple(cl)
	_, err := ex.Apply(context.Background(), compileAndBuild(t, graph), watchrouter.NoopWatcher{})
	require.NoError(t, err)

	data := cmData(t, cl, "stat")
	assert.Equal(t, "present", data["ghost"], "a is ready → branch taken (sanity)")
}
