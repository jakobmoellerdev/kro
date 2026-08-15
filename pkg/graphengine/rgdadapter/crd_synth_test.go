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

package rgdadapter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	memory2 "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/restmapper"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graph"
	"github.com/kubernetes-sigs/kro/pkg/graph/crd"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
	"github.com/kubernetes-sigs/kro/pkg/testutil/k8s"
)

func TestSynthesizeInstanceCRD_NamespacedShapeAndParity(t *testing.T) {
	rgd := newNamespacedWebAppRGD()

	got, err := SynthesizeInstanceCRD(rgd)
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Equal(t, "webapps.kro.run", got.Name)
	assert.Equal(t, "kro.run", got.Spec.Group)
	assert.Equal(t, "WebApp", got.Spec.Names.Kind)
	assert.Equal(t, "WebAppList", got.Spec.Names.ListKind)
	assert.Equal(t, "webapps", got.Spec.Names.Plural)
	assert.Equal(t, "webapp", got.Spec.Names.Singular)
	assert.Equal(t, extv1.NamespaceScoped, got.Spec.Scope)

	require.Len(t, got.Spec.Versions, 1)
	version := got.Spec.Versions[0]
	assert.Equal(t, "v1alpha1", version.Name)
	assert.True(t, version.Served)
	assert.True(t, version.Storage)
	require.NotNil(t, version.Subresources)
	require.NotNil(t, version.Subresources.Status)

	require.NotNil(t, version.Schema)
	require.NotNil(t, version.Schema.OpenAPIV3Schema)
	openAPI := version.Schema.OpenAPIV3Schema
	assert.Equal(t, "object", openAPI.Type)
	require.Contains(t, openAPI.Properties, "spec")
	require.Contains(t, openAPI.Properties, "status")
	require.Contains(t, openAPI.Properties, "apiVersion")
	require.Contains(t, openAPI.Properties, "kind")
	require.Contains(t, openAPI.Properties, "metadata")

	specProps := openAPI.Properties["spec"].Properties
	require.Contains(t, specProps, "name")
	assert.Equal(t, "string", specProps["name"].Type)
	require.Contains(t, specProps, "replicas")
	assert.Equal(t, "integer", specProps["replicas"].Type)
	require.Contains(t, specProps, "image")
	assert.Equal(t, "string", specProps["image"].Type)

	// Empty-status placeholder: no inferred/default status fields yet.
	assert.Empty(t, openAPI.Properties["status"].Properties)

	want, err := synthesizeViaSharedLeaves(rgd)
	require.NoError(t, err)
	assert.Equal(t, want, got, "adapter must reuse crd.SynthesizeCRD + BuildInstanceSpecSchema")

	// NewResourceGraphDefinition needs a schema resolver/REST mapper even
	// with no resources (CompileSource still builds a typed CEL env). A
	// fake pair is enough. The builder then calls SetCRDStatus, so Graph.CRD
	// differs in status; everything else (the pre-status synthesis) matches.
	fakeResolver, fakeDiscovery := k8s.NewFakeResolver()
	restMapper := restmapper.NewDeferredDiscoveryRESTMapper(memory2.NewMemCacheClient(fakeDiscovery))
	builder, err := graph.NewBuilder(nil, nil,
		graph.WithSchemaResolver(fakeResolver),
		graph.WithRESTMapper(restMapper),
	)
	require.NoError(t, err)

	built, err := builder.NewResourceGraphDefinition(rgd, graph.Config{})
	require.NoError(t, err)
	require.NotNil(t, built.CRD)

	assertEqualCRDMinusStatus(t, got, built.CRD)
}

func TestSynthesizeInstanceCRD_Errors(t *testing.T) {
	t.Run("nil rgd", func(t *testing.T) {
		_, err := SynthesizeInstanceCRD(nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "resourcegraphdefinition is required")
	})

	t.Run("nil schema", func(t *testing.T) {
		_, err := SynthesizeInstanceCRD(&v1alpha1.ResourceGraphDefinition{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "schema is required")
	})

	t.Run("invalid simpleschema", func(t *testing.T) {
		rgd := generator.NewResourceGraphDefinition("bad",
			generator.WithSchema("Bad", "v1alpha1", map[string]interface{}{
				"field": "not-a-type",
			}, map[string]interface{}{}),
		)
		rgd.Spec.Schema.Group = "kro.run"
		_, err := SynthesizeInstanceCRD(rgd)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to build instance spec schema")
	})
}

func newNamespacedWebAppRGD() *v1alpha1.ResourceGraphDefinition {
	rgd := generator.NewResourceGraphDefinition("webapp",
		generator.WithSchema(
			"WebApp", "v1alpha1",
			map[string]interface{}{
				"name":     "string",
				"replicas": "integer | default=1",
				"image":    "string",
			},
			map[string]interface{}{},
			generator.WithScope(v1alpha1.ResourceScopeNamespaced),
		),
	)
	rgd.Spec.Schema.Group = "kro.run"
	return rgd
}

// synthesizeViaSharedLeaves is the same call sequence NewResourceGraphDefinition
// uses before SetCRDStatus. Parity against this, rather than Graph.CRD, is the
// apples-to-apples comparison: the adapter stops at the empty-status placeholder.
func synthesizeViaSharedLeaves(rgd *v1alpha1.ResourceGraphDefinition) (*extv1.CustomResourceDefinition, error) {
	specSchema, err := graph.BuildInstanceSpecSchema(rgd.Spec.Schema)
	if err != nil {
		return nil, err
	}
	scope := extv1.NamespaceScoped
	if rgd.Spec.Schema.Scope == v1alpha1.ResourceScopeCluster {
		scope = extv1.ClusterScoped
	}
	return crd.SynthesizeCRD(
		rgd.Spec.Schema.Group,
		rgd.Spec.Schema.APIVersion,
		rgd.Spec.Schema.Kind,
		*specSchema,
		extv1.JSONSchemaProps{},
		false,
		scope,
		rgd.Spec.Schema,
	), nil
}

func assertEqualCRDMinusStatus(t *testing.T, adapterCRD, builderCRD *extv1.CustomResourceDefinition) {
	t.Helper()

	assert.Equal(t, adapterCRD.Name, builderCRD.Name)
	assert.Equal(t, adapterCRD.Labels, builderCRD.Labels)
	assert.Equal(t, adapterCRD.Annotations, builderCRD.Annotations)
	assert.Equal(t, adapterCRD.Spec.Group, builderCRD.Spec.Group)
	assert.Equal(t, adapterCRD.Spec.Names, builderCRD.Spec.Names)
	assert.Equal(t, adapterCRD.Spec.Scope, builderCRD.Spec.Scope)
	require.Len(t, builderCRD.Spec.Versions, 1)

	gotVer := adapterCRD.Spec.Versions[0]
	wantVer := builderCRD.Spec.Versions[0]
	assert.Equal(t, gotVer.Name, wantVer.Name)
	assert.Equal(t, gotVer.Served, wantVer.Served)
	assert.Equal(t, gotVer.Storage, wantVer.Storage)
	assert.Equal(t, gotVer.Subresources, wantVer.Subresources)
	assert.Equal(t, gotVer.AdditionalPrinterColumns, wantVer.AdditionalPrinterColumns)

	gotSchema := gotVer.Schema.OpenAPIV3Schema.DeepCopy()
	wantSchema := wantVer.Schema.OpenAPIV3Schema.DeepCopy()
	delete(gotSchema.Properties, "status")
	delete(wantSchema.Properties, "status")
	assert.Equal(t, gotSchema, wantSchema, "OpenAPI schema minus status must match NewResourceGraphDefinition")
}
