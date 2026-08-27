// Copyright 2025 The Kube Resource Orchestrator Authors
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
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/testutil/generator"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
)

// collectionGraphWithUID builds a Graph with a single collection node whose
// local id is always "res" (shared across Graphs on purpose) and a fixed UID,
// mirroring the standalone Graph path which has no LabelInjector.
func collectionGraphWithUID(name string, uid types.UID) *expv1alpha1.Graph {
	g := generator.NewGraph(name,
		generator.WithNamespace("default"),
		generator.WithDef("src", map[string]any{"names": []any{"x", "y"}}),
		generator.WithTemplate("res", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "${'" + name + "-' + n}"},
			"data":     map[string]any{"k": "v"},
		}, generator.ForEachDim("n", "${src.names}")),
	)
	g.SetUID(uid)
	return g
}

// collectionWatchSelector applies the Graph on the standalone path (no
// LabelInjector) and returns the single collection selector watch it
// registered.
func collectionWatchSelector(t *testing.T, g *expv1alpha1.Graph) labels.Selector {
	t.Helper()
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	w := &recordingWatcher{}
	// No WithLabelInjector: this is the standalone Graph path, which the RGD
	// path's instance labeler does not touch.
	_, err := NewSimple(cl).Apply(context.Background(), compileAndBuild(t, g), w)
	require.NoError(t, err)
	for i := range w.reqs {
		if w.reqs[i].Selector != nil { // the collection selector watch
			return w.reqs[i].Selector
		}
	}
	t.Fatal("no collection selector watch was registered")
	return nil
}

// TestSimple_CollectionWatch_GraphPathStampsInstanceID is the regression test
// for the finding that the standalone Graph path stamped NO instance-id label
// on collection-node watches: two different Graphs whose collection nodes share
// a node id ("res") registered byte-identical selectors and woke each other on
// every event.
//
// Before the fix the selector was {node-id} only, so both Graphs' selectors
// were identical and cross-matched. After the fix each Graph's watch also
// carries an instance-id derived from the Graph's own UID (mirroring how the
// RGD path scopes watches by instance), so the two selectors are distinct and
// neither matches the other Graph's items.
func TestSimple_CollectionWatch_GraphPathStampsInstanceID(t *testing.T) {
	t.Parallel()

	graphA := collectionGraphWithUID("graph-a", types.UID("uid-a"))
	graphB := collectionGraphWithUID("graph-b", types.UID("uid-b"))

	selA := collectionWatchSelector(t, graphA)
	selB := collectionWatchSelector(t, graphB)

	// The Graph path must stamp an instance-id on the watch label set, so the
	// two selectors are NOT identical even though both collection nodes share
	// the local id "res".
	assert.NotEqual(t, selA.String(), selB.String(),
		"two Graphs sharing a collection node id must register DISTINCT watch selectors")

	// Concretely, each selector must carry its own Graph UID as the
	// instance-id, and must not match the peer Graph's items.
	itemA := labels.Set{metadata.NodeIDLabel: "res", metadata.InstanceIDLabel: "uid-a"}
	itemB := labels.Set{metadata.NodeIDLabel: "res", metadata.InstanceIDLabel: "uid-b"}

	assert.True(t, selA.Matches(itemA), "graph-a's watch must match graph-a's own items")
	assert.False(t, selA.Matches(itemB),
		"graph-a's watch must NOT match graph-b's items (self-wake regression)")
	assert.True(t, selB.Matches(itemB), "graph-b's watch must match graph-b's own items")
	assert.False(t, selB.Matches(itemA),
		"graph-b's watch must NOT match graph-a's items (self-wake regression)")

	// The instance-id label must actually be present in the selector (the
	// pre-fix bug left it absent on the Graph path).
	itemNoInstance := labels.Set{metadata.NodeIDLabel: "res"}
	assert.False(t, selA.Matches(itemNoInstance),
		"the Graph-path watch must require an instance-id, not match node-id alone")
}

// TestSimple_CollectionChildStampsInstanceID is the symmetric other half of the
// selector test above: it verifies the executor actually STAMPS the instance-id
// label the collection watch selector matches on, onto the applied collection
// children. Before the fix the standalone Graph path stamped only node-id on
// collection items (instance-id came solely from the RGD path's LabelInjector),
// so the selector — which falls back to the Graph UID — could never match the
// children, and out-of-band drift on a collection item never re-enqueued the
// Graph. stampKROMeta now stamps instance-id = Graph UID on standalone
// collection children, making label and selector symmetric by construction.
func TestSimple_CollectionChildStampsInstanceID(t *testing.T) {
	t.Parallel()
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()

	g := collectionGraphWithUID("stamp", types.UID("uid-stamp"))
	_, err := NewSimple(cl).Apply(context.Background(),
		compileAndBuild(t, g), &recordingWatcher{})
	require.NoError(t, err)

	// The collection expands one ConfigMap per name in the def ("x", "y").
	for _, n := range []string{"x", "y"} {
		cm := getCM(t, cl, "stamp-"+n)
		lbls := cm.GetLabels()
		assert.Equal(t, "uid-stamp", lbls[metadata.InstanceIDLabel],
			"collection child %q must carry instance-id = Graph UID so the drift-watch selector matches it", "stamp-"+n)
		assert.Equal(t, "res", lbls[metadata.NodeIDLabel],
			"collection child %q must still carry its node-id", "stamp-"+n)

		// Prove label+selector are symmetric: the Graph's own watch selector
		// must match the child's actual label set.
		sel := collectionWatchSelector(t, g)
		assert.True(t, sel.Matches(labels.Set(lbls)),
			"the Graph's collection-watch selector must match its own child %q labels", "stamp-"+n)
	}
}
