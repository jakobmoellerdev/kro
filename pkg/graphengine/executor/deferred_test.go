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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	memory "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/restmapper"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/features"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/testutil/generator"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/watchrouter"
	testk8s "github.com/kubernetes-sigs/kro/pkg/testutil/k8s"
)

var widgetGVK = schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"}

// unknownWidgetMapper is a REST mapper that does NOT know the Widget GVK, so a
// mapping attempt returns a NoMatch error (the "CRD absent" state).
func unknownWidgetMapper(t *testing.T) *restmapper.DeferredDiscoveryRESTMapper {
	t.Helper()
	_, disco := testk8s.NewFakeResolver()
	return restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(disco))
}

// knownWidgetMapper is a REST mapper whose discovery advertises example.com/v1
// Widget, so a mapping succeeds (the "CRD present" state).
func knownWidgetMapper(t *testing.T) *restmapper.DeferredDiscoveryRESTMapper {
	t.Helper()
	_, disco := testk8s.NewFakeResolver()
	disco.Resources = append(disco.Resources, &metav1.APIResourceList{
		GroupVersion: "example.com/v1",
		APIResources: []metav1.APIResource{{
			Name:       "widgets",
			Namespaced: true,
			Kind:       "Widget",
			Verbs:      []string{"get", "list", "watch", "create", "update", "patch", "delete"},
		}},
	})
	return restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(disco))
}

// deferredGraph builds a Graph whose Widget Template targets a GVK absent from
// the compile-time REST mapper. With the DeferredSchemaResolution gate on and
// a non-empty includeWhen it compiles into a schema-less, lazily-resolved node
// (empty GVR). includeWhen is `${flag.enabled}` so a row can flip the flag to
// make the node included or ignored at apply time.
func deferredGraph(enabled bool) *expv1alpha1.Graph {
	return generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithDef("flag", map[string]any{"enabled": enabled}),
		generator.WithTemplate("widget", map[string]any{
			"apiVersion": "example.com/v1", "kind": "Widget",
			"metadata": map[string]any{"name": "w"},
			"spec":     map[string]any{"size": "large"},
		}),
		generator.WithIncludeWhen("${flag.enabled}"),
	)
}

// TestSimple_Apply_DeferredSchema exercises the executor half of deferred
// schema resolution: a compiled node with an empty GVR has its concrete GVK
// resolved through the live REST mapper at apply time. A GVK the cluster
// still can't map (CRD absent) is a soft requeue (Unresolved + ErrNotReady,
// nothing applied); once the CRD lands the same node applies cleanly.
//
// Gate-sensitive: the feature gate is a global, so these subtests must NOT run
// in parallel.
func TestSimple_Apply_DeferredSchema(t *testing.T) {
	t.Run("CRD still absent at apply is a soft requeue, nothing applied", func(t *testing.T) {
		require.NoError(t, features.FeatureGate.Set("DeferredSchemaResolution=true"))
		defer func() { _ = features.FeatureGate.Set("DeferredSchemaResolution=false") }()

		rt := compileAndBuild(t, deferredGraph(true))

		cl := fake.NewClientBuilder().
			WithScheme(newScheme(t)).
			WithRESTMapper(unknownWidgetMapper(t)).
			Build()

		res, err := NewSimple(cl).Apply(context.Background(), rt, watchrouter.NoopWatcher{})
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrNotReady),
			"an unmappable deferred GVK must surface as ErrNotReady, got %v", err)
		assert.Contains(t, res.Unresolved, "widget",
			"a deferred node whose CRD is absent must be recorded Unresolved so prune is skipped")
		for _, mr := range res.Applied {
			assert.NotEqual(t, "widget", mr.NodeID, "an unresolved deferred node must never be Applied")
		}
	})

	t.Run("CRD present at apply resolves, applies, and tracks the concrete GVK", func(t *testing.T) {
		require.NoError(t, features.FeatureGate.Set("DeferredSchemaResolution=true"))
		defer func() { _ = features.FeatureGate.Set("DeferredSchemaResolution=false") }()

		rt := compileAndBuild(t, deferredGraph(true))

		cl := fake.NewClientBuilder().
			WithScheme(newScheme(t)).
			WithRESTMapper(knownWidgetMapper(t)).
			Build()

		res, err := NewSimple(cl).Apply(context.Background(), rt, watchrouter.NoopWatcher{})
		require.NoError(t, err)

		assert.NotContains(t, res.Unresolved, "widget", "a mappable deferred node must not stay Unresolved")
		require.Len(t, res.Applied, 1)
		assert.Equal(t, "widget", res.Applied[0].NodeID)
		assert.Equal(t, "example.com/v1", res.Applied[0].APIVersion)
		assert.Equal(t, "Widget", res.Applied[0].Kind)
		assert.Equal(t, "default", res.Applied[0].Namespace, "namespaced scope resolved at apply time")

		w := &unstructured.Unstructured{}
		w.SetGroupVersionKind(widgetGVK)
		require.NoError(t, cl.Get(context.Background(),
			types.NamespacedName{Namespace: "default", Name: "w"}, w))
	})

	t.Run("deferred node with includeWhen false is ignored, never mapped", func(t *testing.T) {
		require.NoError(t, features.FeatureGate.Set("DeferredSchemaResolution=true"))
		defer func() { _ = features.FeatureGate.Set("DeferredSchemaResolution=false") }()

		rt := compileAndBuild(t, deferredGraph(false))

		// Use the unknown mapper: if the executor mistakenly attempted a
		// mapping for an ignored node it would surface errSchemaNotReady, so a
		// clean, empty result proves no mapping/apply happened.
		cl := fake.NewClientBuilder().
			WithScheme(newScheme(t)).
			WithRESTMapper(unknownWidgetMapper(t)).
			Build()

		res, err := NewSimple(cl).Apply(context.Background(), rt, watchrouter.NoopWatcher{})
		require.NoError(t, err)
		for _, mr := range res.Applied {
			assert.NotEqual(t, "widget", mr.NodeID, "an ignored node must not be Applied")
		}
		assert.NotContains(t, res.Unresolved, "widget", "an ignored node must not be Unresolved")

		w := &unstructured.Unstructured{}
		w.SetGroupVersionKind(widgetGVK)
		err = cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "w"}, w)
		require.Error(t, err, "no Widget should have been created for an ignored node")
	})
}
