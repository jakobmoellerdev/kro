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

// Package rgdadapter translates a ResourceGraphDefinition into a Graph so
// RGD composition can run on the Graph engine (compiler.Compile →
// runtime.New → executor.Apply). F1 covers template resources and
// named externalRef → ref; the instance spec is not a resource node and is
// injected as a `schema` def node via InstanceSchemaNode.
package rgdadapter

import (
	"encoding/json"
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
)

// ErrUnsupported is returned when an RGD resource shape has no F1 mapping.
// The error names the gap so later increments can pick it up instead of
// silently dropping the resource.
var ErrUnsupported = errors.New("rgdadapter: unsupported RGD shape")

// ResourceGraphDefinitionToGraph maps each RGD resource onto a Graph node:
//
//   - template          → Node.Template (same ID, raw manifest + ${...} CEL intact)
//   - named externalRef → Node.Ref
//
// forEach / readyWhen / includeWhen are copied through. The instance
// spec is not emitted as a resource node — callers prepend InstanceSchemaNode
// to expose it as `${schema.spec.*}`.
func ResourceGraphDefinitionToGraph(rgd *v1alpha1.ResourceGraphDefinition) (*v1alpha1.Graph, error) {
	if rgd == nil {
		return nil, fmt.Errorf("rgdadapter: resourcegraphdefinition is required")
	}
	if len(rgd.Spec.Resources) == 0 {
		return nil, fmt.Errorf("rgdadapter: resourcegraphdefinition %q: at least one resource is required", rgd.Name)
	}

	g := &v1alpha1.Graph{}
	g.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("Graph"))
	g.Name = rgd.Name
	g.Spec.Nodes = make([]v1alpha1.Node, 0, len(rgd.Spec.Resources))

	for i, res := range rgd.Spec.Resources {
		if res == nil {
			return nil, fmt.Errorf("rgdadapter: resource[%d]: resource is nil", i)
		}
		node, err := resourceToNode(res)
		if err != nil {
			return nil, err
		}
		g.Spec.Nodes = append(g.Spec.Nodes, node)
	}
	return g, nil
}

func resourceToNode(res *v1alpha1.Resource) (v1alpha1.Node, error) {
	hasTemplate := len(res.Template.Raw) > 0
	hasRef := res.ExternalRef != nil
	switch {
	case hasTemplate && hasRef:
		return v1alpha1.Node{}, fmt.Errorf("%w: resource %q: template and externalRef are both set", ErrUnsupported, res.ID)
	case hasTemplate:
		return v1alpha1.Node{
			ID:          res.ID,
			Template:    copyRaw(res.Template.Raw),
			ReadyWhen:   copyStrings(res.ReadyWhen),
			IncludeWhen: copyStrings(res.IncludeWhen),
			ForEach:     copyForEach(res.ForEach),
		}, nil
	case hasRef:
		if res.ExternalRef.Metadata.Selector != nil {
			return v1alpha1.Node{}, fmt.Errorf("%w: resource %q: externalRef collections (selector) are not mapped in F1", ErrUnsupported, res.ID)
		}
		if res.ExternalRef.Metadata.Name == "" {
			return v1alpha1.Node{}, fmt.Errorf("%w: resource %q: externalRef is missing metadata.name", ErrUnsupported, res.ID)
		}
		if len(res.ForEach) > 0 {
			return v1alpha1.Node{}, fmt.Errorf("%w: resource %q: forEach on externalRef is not mapped in F1", ErrUnsupported, res.ID)
		}
		return v1alpha1.Node{
			ID:          res.ID,
			Ref:         res.ExternalRef.DeepCopy(),
			ReadyWhen:   copyStrings(res.ReadyWhen),
			IncludeWhen: copyStrings(res.IncludeWhen),
		}, nil
	default:
		return v1alpha1.Node{}, fmt.Errorf("%w: resource %q: neither template nor externalRef is set", ErrUnsupported, res.ID)
	}
}

func copyRaw(raw []byte) *runtime.RawExtension {
	return &runtime.RawExtension{Raw: append([]byte(nil), raw...)}
}

// SchemaNodeID is the node ID under which an instance's spec/metadata/status is
// exposed in Graph scope, matching RGD's `schema` variable.
const SchemaNodeID = "schema"

// InstanceSchemaNode materialises an instance object as a Graph `def` node named
// `schema`, so RGD-style `${schema.spec.*}` references resolve in the Graph
// world. The Graph compiler rejects unknown top-level identifiers, so the
// instance must be a declared node (a def) rather than post-compile seeded
// scope. Prepend the returned node to a translated Graph for each instance
// reconcile.
func InstanceSchemaNode(instance *unstructured.Unstructured) (v1alpha1.Node, error) {
	if instance == nil {
		return v1alpha1.Node{}, fmt.Errorf("rgdadapter: instance is required")
	}
	val := map[string]any{
		"metadata": map[string]any{
			"name":              instance.GetName(),
			"namespace":         instance.GetNamespace(),
			"generateName":      instance.GetGenerateName(),
			"labels":            interfaceMap(instance.GetLabels()),
			"annotations":       interfaceMap(instance.GetAnnotations()),
			"resourceVersion":   instance.GetResourceVersion(),
			"uid":               string(instance.GetUID()),
			"generation":        instance.GetGeneration(),
			"creationTimestamp": instance.GetCreationTimestamp().Format("2006-01-02T15:04:05Z"),
			"finalizers":        interfaceSlice(instance.GetFinalizers()),
			"ownerReferences":   []any{},
		},
	}
	if spec, ok := instance.Object["spec"]; ok {
		val["spec"] = spec
	}
	if status, ok := instance.Object["status"]; ok {
		val["status"] = status
	}
	raw, err := json.Marshal(val)
	if err != nil {
		return v1alpha1.Node{}, fmt.Errorf("rgdadapter: marshal instance schema node: %w", err)
	}
	return v1alpha1.Node{ID: SchemaNodeID, Def: &runtime.RawExtension{Raw: raw}}, nil
}

func copyStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	return append([]string(nil), in...)
}

func copyForEach(in []v1alpha1.ForEachDimension) []v1alpha1.ForEachDimension {
	if len(in) == 0 {
		return nil
	}
	out := make([]v1alpha1.ForEachDimension, len(in))
	for i, dim := range in {
		copied := make(v1alpha1.ForEachDimension, len(dim))
		for k, v := range dim {
			copied[k] = v
		}
		out[i] = copied
	}
	return out
}

// interfaceMap converts a string map to a map[string]any so CEL sees it as an
// object (empty map when nil, so ${schema.metadata.labels} always resolves).
func interfaceMap(m map[string]string) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func interfaceSlice(in []string) []any {
	if in == nil {
		return []any{}
	}
	out := make([]any, len(in))
	for i, s := range in {
		out[i] = s
	}
	return out
}
