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
	// Zero resources is valid: a NoOp/arbitrary-object RGD manages no
	// children and only projects status from its schema. The instance's
	// `schema` def node (prepended by BuildRuntimeForInstance) keeps the
	// compiled Graph non-empty, so the MinItems=1 Node constraint still holds.

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
		// A selector externalRef is a read-only COLLECTION of external
		// objects: name is absent (mutually exclusive with selector at the
		// API level), and the compiler/executor treat the node as a
		// collection. A single-object externalRef requires metadata.name.
		isCollection := res.ExternalRef.Metadata.Selector != nil
		if !isCollection && res.ExternalRef.Metadata.Name == "" {
			return v1alpha1.Node{}, fmt.Errorf("%w: resource %q: externalRef is missing metadata.name", ErrUnsupported, res.ID)
		}
		if len(res.ForEach) > 0 {
			return v1alpha1.Node{}, fmt.Errorf("%w: resource %q: forEach on externalRef is not mapped in F1", ErrUnsupported, res.ID)
		}
		// The ExternalRef (including a *metav1.LabelSelector under
		// metadata.selector, with matchLabels + matchExpressions) is carried
		// through verbatim into the Ref node. projectPayload converts it to
		// unstructured, so CEL fragments inside matchExpressions[].values[]
		// are parsed and rendered, and the executor can read the selector
		// back off the resolved object.
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
	val := map[string]any{}
	// Expose the instance's full metadata (uid, labels, annotations,
	// generation, creationTimestamp, …) as ${schema.metadata.*}, matching
	// the classic RGD runtime. Falling back to name/namespace only would
	// break RGDs that reference e.g. ${schema.metadata.uid}.
	if md, ok := instance.Object["metadata"].(map[string]any); ok {
		val["metadata"] = md
	} else {
		val["metadata"] = map[string]any{
			"name":      instance.GetName(),
			"namespace": instance.GetNamespace(),
		}
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
