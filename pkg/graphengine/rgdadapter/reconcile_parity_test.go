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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	memory "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/restmapper"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/compiler"
	graphruntime "github.com/kubernetes-sigs/kro/pkg/graphengine/runtime"
	testk8s "github.com/kubernetes-sigs/kro/pkg/testutil/k8s"
)

func rawResource(m map[string]any) runtime.RawExtension {
	raw, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return runtime.RawExtension{Raw: raw}
}

// TestReconcileParity_SchemaAndCrossNode is the F1 seam proof: an RGD with two
// template resources — one reading the instance via ${schema.spec.*}, the other
// reading the first via cross-node CEL — is translated to a Graph, the instance
// is injected as a `schema` def node, and the Graph engine compiler+runtime
// resolve both references. This proves RGD composition (instance scope +
// inter-node wiring) runs on the Graph engine.
func TestReconcileParity_SchemaAndCrossNode(t *testing.T) {
	rgd := &v1alpha1.ResourceGraphDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "webapp"},
		Spec: v1alpha1.ResourceGraphDefinitionSpec{
			Resources: []*v1alpha1.Resource{
				{
					ID: "cm1",
					Template: rawResource(map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata":   map[string]any{"name": "cm1", "namespace": "default"},
						"data":       map[string]any{"value": "${schema.spec.value}"},
					}),
				},
				{
					ID: "cm2",
					Template: rawResource(map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata":   map[string]any{"name": "cm2", "namespace": "default"},
						"data":       map[string]any{"ref": "${cm1.metadata.name}"},
					}),
				},
			},
		},
	}

	// 1. Translate RGD resources -> Graph nodes.
	g, err := ResourceGraphDefinitionToGraph(rgd)
	require.NoError(t, err)
	require.Len(t, g.Spec.Nodes, 2)

	// 2. Inject the instance as a `schema` def node (the instance-scope seam).
	instance := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "example.com/v1alpha1",
		"kind":       "WebApp",
		"metadata":   map[string]any{"name": "demo", "namespace": "default"},
		"spec":       map[string]any{"value": "hello-from-instance"},
	}}
	schemaNode, err := InstanceSchemaNode(instance)
	require.NoError(t, err)
	g.Spec.Nodes = append([]v1alpha1.Node{schemaNode}, g.Spec.Nodes...)

	// 3. Compile through the Graph engine (fake resolver + REST mapper).
	fakeResolver, disco := testk8s.NewFakeResolver()
	rm := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(disco))
	prog, err := compiler.NewCompilerWithDependencies(fakeResolver, rm).Compile(g)
	require.NoError(t, err, "translated Graph (with schema def node) must compile")

	// 4. Instantiate the runtime and resolve in dependency order (mirroring the
	// executor's topological walk: publish each node's value before dependents).
	rt := graphruntime.New(prog, g)

	// Publish the schema def node first — this is the instance-scope injection.
	schemaObjs, err := rt.Node(SchemaNodeID).Resolve()
	require.NoError(t, err)
	require.Len(t, schemaObjs, 1)
	rt.Set(SchemaNodeID, schemaObjs[0].Object)

	cm1Objs, err := rt.Node("cm1").Resolve()
	require.NoError(t, err)
	require.Len(t, cm1Objs, 1)
	// ${schema.spec.value} resolved from the injected instance.
	assert.Equal(t, "hello-from-instance", nestedString(t, cm1Objs[0].Object, "data", "value"))

	// Publish cm1 so cm2's cross-node reference resolves.
	rt.Set("cm1", cm1Objs[0].Object)

	cm2Objs, err := rt.Node("cm2").Resolve()
	require.NoError(t, err)
	require.Len(t, cm2Objs, 1)
	// ${cm1.metadata.name} resolved from the published cm1.
	assert.Equal(t, "cm1", nestedString(t, cm2Objs[0].Object, "data", "ref"))
}

func nestedString(t *testing.T, obj map[string]any, path ...string) string {
	t.Helper()
	v, found, err := unstructured.NestedString(obj, path...)
	require.NoError(t, err)
	require.Truef(t, found, "path %v not found in %v", path, obj)
	return v
}
