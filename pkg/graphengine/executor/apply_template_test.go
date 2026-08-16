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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/testutil/generator"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/watchrouter"
)

// patchFailClient delegates everything to the embedded client except SSA
// apply, so a test can distinguish create-vs-update failure handling: Get
// still reports whether the object exists.
type patchFailClient struct {
	client.Client
	err error
}

func (p *patchFailClient) Patch(
	context.Context, client.Object, client.Patch, ...client.PatchOption,
) error {
	return p.err
}

// failWatcher rejects every watch declaration.
type failWatcher struct{ err error }

func (f failWatcher) Watch(watchrouter.WatchRequest) error { return f.err }
func (f failWatcher) Done(bool)                            {}

// terminatingCM builds a ConfigMap that is mid-deletion. The fake client
// requires a finalizer alongside deletionTimestamp, which mirrors reality: an
// object only lingers in a terminating state because something is finalizing it.
func terminatingCM(name string) *unstructured.Unstructured {
	now := metav1.Now()
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": name, "namespace": "default"},
		"data":     map[string]any{"k": "old"},
	}}
	obj.SetFinalizers([]string{"example.com/hold"})
	obj.SetDeletionTimestamp(&now)
	return obj
}

func liveCM(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": name, "namespace": "default"},
		"data":     map[string]any{"k": "old"},
	}}
}

func scalarCMGraph(name string) *expv1alpha1.Graph {
	return generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithTemplate("cm", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": name},
			"data":     map[string]any{"k": "new"},
		}),
	)
}

// collectionCMGraph expands to cm-alpha and cm-beta via forEach.
func collectionCMGraph() *expv1alpha1.Graph {
	return generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithDef("src", map[string]any{"names": []any{"alpha", "beta"}}),
		generator.WithTemplate("cm", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "${'cm-' + n}"},
			"data":     map[string]any{"k": "new"},
		}, generator.ForEachDim("n", "${src.names}")),
	)
}

// kro must not re-apply over an object that is being deleted: the write would
// either fail or resurrect fields on a doomed object. A terminating scalar
// template surfaces the distinguishable ResourceDeleting signal, which also
// satisfies ErrNotReady so the reconciler requeues and gates dependents
// instead of failing.
func TestSimple_ApplyTemplate_TerminatingScalar(t *testing.T) {
	t.Parallel()
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(terminatingCM("doomed")).Build()

	res, err := NewSimple(cl).Apply(context.Background(),
		compileAndBuild(t, scalarCMGraph("doomed")), watchrouter.NoopWatcher{})

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrResourceDeleting),
		"a terminating live object must be distinguishable, got %v", err)
	assert.True(t, errors.Is(err, ErrNotReady),
		"and must requeue rather than fail the reconcile")
	assert.Contains(t, res.Unresolved, "cm",
		"an unresolved identity must withhold prune for this node")

	var target *ResourceDeletingError
	require.True(t, errors.As(err, &target))
	assert.Equal(t, "doomed", target.Name)
}

// In a collection, one terminating item gates the whole node but must not stop
// its siblings from applying — otherwise a single stuck item would starve every
// other member of its watches and identities.
func TestSimple_ApplyTemplate_TerminatingCollectionItemStillAppliesSiblings(t *testing.T) {
	t.Parallel()
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(terminatingCM("cm-alpha")).Build()

	res, err := NewSimple(cl).Apply(context.Background(),
		compileAndBuild(t, collectionCMGraph()), watchrouter.NoopWatcher{})

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrResourceDeleting))
	assert.True(t, errors.Is(err, ErrNotReady))

	assert.True(t, cmExists(t, cl, "cm-beta"),
		"a sibling of a terminating item must still be applied")

	names := make([]string, 0, len(res.Applied))
	for _, a := range res.Applied {
		names = append(names, a.Name)
	}
	assert.Contains(t, names, "cm-beta", "the applied sibling must be tracked")
	assert.NotContains(t, names, "cm-alpha",
		"a terminating item must not be advertised as applied")
}

// A watch that cannot be registered is a hard error: the executor's drift
// detection depends on the watch existing, so continuing would silently lose
// events for that resource.
func TestSimple_ApplyTemplate_WatchRegistrationFailureIsHard(t *testing.T) {
	t.Parallel()

	t.Run("scalar", func(t *testing.T) {
		t.Parallel()
		cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
		_, err := NewSimple(cl).Apply(context.Background(),
			compileAndBuild(t, scalarCMGraph("cm")),
			failWatcher{err: errors.New("informer unavailable")})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "register watch")
		assert.False(t, errors.Is(err, ErrNotReady),
			"a watch registration failure must not be softened into a requeue")
		assert.False(t, cmExists(t, cl, "cm"),
			"nothing may be applied once the watch could not be declared")
	})

	t.Run("collection", func(t *testing.T) {
		t.Parallel()
		cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
		_, err := NewSimple(cl).Apply(context.Background(),
			compileAndBuild(t, collectionCMGraph()),
			failWatcher{err: errors.New("informer unavailable")})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "collection watch")
	})
}

// Collection per-item apply tolerance. The two failure shapes are treated
// differently on purpose, and the distinction is what keeps a collection from
// wedging on one bad member.
func TestSimple_ApplyTemplate_CollectionApplyTolerance(t *testing.T) {
	t.Parallel()

	t.Run("a rejected update on an existing object records the live identity", func(t *testing.T) {
		t.Parallel()
		// Both members already exist, so every SSA is an update. A rejected
		// update (an immutable field, say) must not block the node forever:
		// the objects are in the cluster, so their identities are recorded and
		// the node converges.
		base := fake.NewClientBuilder().WithScheme(newScheme(t)).
			WithObjects(liveCM("cm-alpha"), liveCM("cm-beta")).Build()
		cl := &patchFailClient{Client: base, err: errors.New("field is immutable")}

		res, err := NewSimple(cl).Apply(context.Background(),
			compileAndBuild(t, collectionCMGraph()), watchrouter.NoopWatcher{})

		require.NoError(t, err,
			"an unfixable update on objects that exist must not hold the node not-ready")
		names := make([]string, 0, len(res.Applied))
		for _, a := range res.Applied {
			names = append(names, a.Name)
		}
		assert.ElementsMatch(t, []string{"cm-alpha", "cm-beta"}, names,
			"existing objects keep their tracked identities")
	})

	t.Run("a failed create holds the collection soft not-ready", func(t *testing.T) {
		t.Parallel()
		// Neither member exists, so every SSA is a create. A failed create
		// means the resource is absent: record the failure, keep going, and
		// hold the node not-ready so the reconcile requeues rather than
		// hard-failing.
		base := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
		cl := &patchFailClient{Client: base, err: errors.New("quota exceeded")}

		res, err := NewSimple(cl).Apply(context.Background(),
			compileAndBuild(t, collectionCMGraph()), watchrouter.NoopWatcher{})

		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrNotReady),
			"a failed create must requeue, never hard-abort the walk, got %v", err)
		assert.Contains(t, err.Error(), "quota exceeded",
			"the underlying cause must reach the condition message")
		assert.Contains(t, err.Error(), "2 item(s)",
			"every failing item is reported, not just the first")
		assert.Empty(t, res.Applied,
			"nothing landed, so nothing may be advertised as applied")
	})
}
