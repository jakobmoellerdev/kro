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
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graph"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/compiler"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/runtime"
)

// Compiler is the interface subset of compiler.Compiler that
// BuildRuntimeForInstance needs. Production callers pass *compiler.Compiler
// directly; tests may pass a narrower stub.
type Compiler interface {
	CompileWithOptions(g *v1alpha1.Graph, opts ...compiler.CompileOption) (*compiler.Program, error)
}

// BuildRuntimeForInstance is the single adapter entrypoint that F6's
// controller will call for every reconcile cycle:
//
//  1. Translate the RGD's resources into a Graph (ResourceGraphDefinitionToGraph).
//  2. Prepend an InstanceSchemaNode so ${schema.spec.*} references resolve.
//  3. Set Graph metadata (name + namespace) from the instance so the executor
//     can default namespaced resources to the correct namespace.
//  4. Compile the Graph via c.Compile.
//  5. Construct and return a runtime.Runtime ready for executor.Simple.Apply.
//
// On success the caller owns the returned Runtime and Graph; they are
// discarded after each reconcile cycle (Runtime is single-use by design).
func BuildRuntimeForInstance(
	rgd *v1alpha1.ResourceGraphDefinition,
	instance *unstructured.Unstructured,
	c Compiler,
) (*runtime.Runtime, *v1alpha1.Graph, error) {
	if rgd == nil {
		return nil, nil, fmt.Errorf("rgdadapter: rgd is required")
	}
	if instance == nil {
		return nil, nil, fmt.Errorf("rgdadapter: instance is required")
	}
	if c == nil {
		return nil, nil, fmt.Errorf("rgdadapter: compiler is required")
	}

	// Step 1: translate RGD resources → Graph nodes.
	g, err := ResourceGraphDefinitionToGraph(rgd)
	if err != nil {
		return nil, nil, fmt.Errorf("rgdadapter: translate: %w", err)
	}

	// Step 2: prepend the instance as a `schema` def node.
	schemaNode, err := InstanceSchemaNode(instance)
	if err != nil {
		return nil, nil, fmt.Errorf("rgdadapter: schema node: %w", err)
	}
	g.Spec.Nodes = append([]v1alpha1.Node{schemaNode}, g.Spec.Nodes...)

	// Step 3: stamp the Graph's metadata so the executor can namespace-default
	// namespaced resources correctly (executor reads rt.Graph().GetNamespace()).
	g.ObjectMeta = metav1.ObjectMeta{
		Name:      instance.GetName(),
		Namespace: instance.GetNamespace(),
	}

	// Step 4: compile. The `schema` def node is typed from the RGD's
	// declared SimpleSchema (override), not inferred from the current
	// instance value — a fresh instance missing fields must still compile,
	// and compile-time typing matches the classic builder.
	var compileOpts []compiler.CompileOption
	if rgd.Spec.Schema != nil {
		schemaVarSchema, err := graph.InstanceSchemaForCEL(rgd)
		if err != nil {
			return nil, nil, fmt.Errorf("rgdadapter: instance schema: %w", err)
		}
		compileOpts = append(compileOpts, compiler.WithNodeSchemaOverride(SchemaNodeID, schemaVarSchema))
	}
	prog, err := c.CompileWithOptions(g, compileOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("rgdadapter: compile: %w", err)
	}

	// Step 5: construct the runtime.
	rt := runtime.New(prog, g)
	return rt, g, nil
}
