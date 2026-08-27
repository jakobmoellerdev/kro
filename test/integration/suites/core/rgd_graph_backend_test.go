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

package core_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"

	krov1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	kgraph "github.com/kubernetes-sigs/kro/pkg/graph"
	"github.com/kubernetes-sigs/kro/pkg/graph/crd"
	"github.com/kubernetes-sigs/kro/test/integration/environment"
)

// graphRGDBackend realizes an RGD through the RGD-as-Graph controller
// (examples/graph/rgd.yaml), the pure-Graph reimplementation of the RGD
// controller. It is the "generated one from rgd v1alpha2": the definition Kind
// it watches is served at v1alpha2 under a UNIQUE per-run group so it never
// collides with the built-in kro.run ResourceGraphDefinition CRD/controller.
//
// Per RGD fixture it:
//
//   - installs the user's Kind CRD synthesized from spec.schema (the built-in
//     controller does the same via SynthesizeCRD; done directly here because
//     the compiler resolves a static ref's schema up front, so an L1 that both
//     creates the CRD and watches its Kind could not compile its first pass —
//     see graph_rgd_as_graph_test.go);
//   - deploys an L0 controller Graph that watches instances of the Kind
//     (dynamic GVK) and forEach-stamps one L2 Graph per instance;
//   - each L2 applies the RGD's resource templates (carried through as opaque
//     ${...} strings, evaluated by L2 against the instance published as
//     `schema` — matching RGD's ${schema.spec.*} convention) and writes back
//     status.ready via the `.ready()` lifecycle signal.
//
// This exercises the same watch → forEach → stamp → apply → status pipeline the
// shipped rgd.yaml expresses, against real RGD fixtures, entirely on the Graph
// engine.
type graphRGDBackend struct {
	group string
}

func newGraphRGDBackend() *graphRGDBackend {
	return &graphRGDBackend{group: fmt.Sprintf("graphrgd-%s.example.com", rand.String(5))}
}

func (graphRGDBackend) Name() string { return "graph-rgd-v1alpha2" }

// InstanceGVK: instances live at the RGD's declared group/version/kind — the
// graph backend does not rewrite the user Kind, only the definition Kind it
// watches. It matches the built-in backend so both apply the same instances.
func (graphRGDBackend) InstanceGVK(rgd *krov1alpha1.ResourceGraphDefinition) schema.GroupVersionKind {
	group := rgd.Spec.Schema.Group
	if group == "" {
		group = krov1alpha1.KRODomainName
	}
	return schema.GroupVersionKind{
		Group:   group,
		Version: rgd.Spec.Schema.APIVersion,
		Kind:    rgd.Spec.Schema.Kind,
	}
}

func (b *graphRGDBackend) Deploy(t environment.TestingT, rgd *krov1alpha1.ResourceGraphDefinition) schema.GroupVersionKind {
	t.Helper()
	ctx := env.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	gvk := b.InstanceGVK(rgd)

	// Ensure the controller namespace exists (envtest has no kro-system).
	ctrlNS := &unstructured.Unstructured{}
	ctrlNS.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "Namespace"})
	ctrlNS.SetName(graphRGDControllerNamespace)
	if err := env.Client.Create(ctx, ctrlNS); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("graph backend: ensure controller namespace: %v", err)
	}

	// 1. Synthesize + install the user's Kind CRD from spec.schema, exactly as
	//    the built-in controller does. (See ensureTestInstanceCRD.)
	specSchema, err := kgraph.BuildInstanceSpecSchema(rgd.Spec.Schema)
	if err != nil {
		t.Fatalf("graph backend: build instance spec schema: %v", err)
	}
	scope := apiextensionsv1.NamespaceScoped
	if rgd.Spec.Schema.Scope == krov1alpha1.ResourceScopeCluster {
		scope = apiextensionsv1.ClusterScoped
	}
	userCRD := crd.SynthesizeCRD(
		gvk.Group, gvk.Version, gvk.Kind,
		*specSchema,
		apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: ptrTrue()},
		false, scope, rgd.Spec.Schema,
	)
	if err := env.Client.Create(ctx, userCRD); err != nil {
		t.Fatalf("graph backend: create user CRD %q: %v", userCRD.Name, err)
	}
	t.Cleanup(func() { _ = env.Client.Delete(context.Background(), userCRD) })
	awaitCRDEstablished(t, env, userCRD.Name)

	// 2. Deploy the L0 controller Graph: watch instances of the user Kind and
	//    forEach-stamp one L2 Graph per instance. The L2 node list is built by
	//    CEL from the RGD's resources (carried in the L0 template as data),
	//    matching examples/graph/rgd.yaml's L1→L2 construction.
	l0 := b.buildControllerGraph(t, rgd, gvk)
	if err := env.Client.Create(ctx, l0); err != nil {
		t.Fatalf("graph backend: create L0 controller graph: %v", err)
	}
	t.Cleanup(func() { _ = env.Client.Delete(context.Background(), l0) })

	l0Key := types.NamespacedName{Namespace: l0.Namespace, Name: l0.Name}
	env.AwaitCondition(t, l0Key, krov1alpha1.GraphConditionTypeAccepted, metav1.ConditionTrue, 60*time.Second)
	return gvk
}

// buildControllerGraph constructs the L0 controller Graph for rgd. L0 watches
// all instances of the user Kind (static GVK — the CRD is pre-installed in
// Deploy) and stamps one L2 Graph per instance. The L2 spec.nodes is a single
// CEL expression evaluated by L0 that concatenates:
//
//	[{schema ref → the instance}] + [rgd resource templates...] + [status patch]
//
// so the instance is published as `schema` (RGD convention) and each resource's
// ${schema.spec.*} expressions survive as opaque strings evaluated by L2.
func (b *graphRGDBackend) buildControllerGraph(
	t environment.TestingT,
	rgd *krov1alpha1.ResourceGraphDefinition,
	gvk schema.GroupVersionKind,
) *krov1alpha1.Graph {
	t.Helper()

	// The L2 node list, embedded as STRUCTURED data in the L0 template (not a
	// stringified blob). L0 evaluates the template, so:
	//   - the schema ref's name/namespace are L0 expressions (${instance...}),
	//     baked in when L0 stamps the child;
	//   - resource-template strings are passed through passThrough() so their
	//     ${schema.spec.*} survive L0 evaluation verbatim and are evaluated by
	//     L2 against the instance published as `schema`;
	//   - the status.ready expression references L2 nodes (cm1.ready()), so it
	//     is also passed through to L2.
	l2Nodes := make([]any, 0, 1+len(rgd.Spec.Resources)+1)
	l2Nodes = append(l2Nodes, map[string]any{
		"id": "schema",
		"ref": map[string]any{
			"apiVersion": gvk.Group + "/" + gvk.Version,
			"kind":       gvk.Kind,
			"metadata": map[string]any{
				"name":      "${instance.metadata.name}",
				"namespace": "${instance.metadata.namespace}",
			},
		},
	})
	readyTerms := make([]string, 0, len(rgd.Spec.Resources))
	for _, res := range rgd.Spec.Resources {
		node, err := rgdResourceToL2Node(res)
		if err != nil {
			t.Fatalf("graph backend: resource %q: %v", res.ID, err)
		}
		l2Nodes = append(l2Nodes, node)
		readyTerms = append(readyTerms, res.ID+".ready().orValue(false)")
	}
	// status.ready = all resources ready (true when there are none).
	readyExpr := "true"
	if len(readyTerms) > 0 {
		readyExpr = joinAnd(readyTerms)
	}
	l2Nodes = append(l2Nodes, map[string]any{
		"id": "statusWriteback",
		"patch": map[string]any{
			"apiVersion": gvk.Group + "/" + gvk.Version,
			"kind":       gvk.Kind,
			"metadata": map[string]any{
				"name":      "${instance.metadata.name}",
				"namespace": "${instance.metadata.namespace}",
			},
			// Evaluated by L2 (references L2 nodes' .ready()); pass through L0.
			"status": map[string]any{
				"ready": deferExpr(readyExpr),
			},
		},
	})

	// L0 controller graph: watch instances (static GVK — CRD pre-installed in
	// Deploy) and forEach-stamp one L2 Graph per instance.
	name := "graphrgd-" + rgd.Spec.Schema.Kind + "-" + rand.String(4)
	return &krov1alpha1.Graph{
		ObjectMeta: metav1.ObjectMeta{Name: lowerASCII(name), Namespace: graphRGDControllerNamespace},
		Spec: krov1alpha1.GraphSpec{Nodes: []krov1alpha1.Node{
			{
				ID: "watchInstances",
				Ref: &krov1alpha1.ExternalRef{
					APIVersion: gvk.Group + "/" + gvk.Version,
					Kind:       gvk.Kind,
					Metadata:   krov1alpha1.ExternalRefMetadata{Selector: &metav1.LabelSelector{}},
				},
			},
			{
				ID:      "instances",
				ForEach: []krov1alpha1.ForEachDimension{{"instance": "${watchInstances}"}},
				Template: environment.RawExt(t, map[string]any{
					"apiVersion": "kro.run/v1alpha1",
					"kind":       "Graph",
					"metadata": map[string]any{
						"name":      "${'inst-' + instance.metadata.name}",
						"namespace": graphRGDControllerNamespace,
					},
					"spec": map[string]any{
						"nodes": l2Nodes,
					},
				}),
			},
		}},
	}
}

// graphRGDControllerNamespace is where the graph backend's L0 controller graphs
// (and the L2 children they stamp) live. kro-system mirrors rgd.yaml.
const graphRGDControllerNamespace = "kro-system"

// rgdResourceToL2Node maps one RGD resource to an L2 node map (template or
// externalRef), carrying readyWhen/includeWhen/forEach through. The template's
// ${schema.spec.*} expressions are preserved verbatim as opaque data for L2.
func rgdResourceToL2Node(res *krov1alpha1.Resource) (map[string]any, error) {
	node := map[string]any{"id": res.ID}
	switch {
	case len(res.Template.Raw) > 0:
		var tmpl map[string]any
		if err := json.Unmarshal(res.Template.Raw, &tmpl); err != nil {
			return nil, fmt.Errorf("unmarshal template: %w", err)
		}
		// The template's ${schema.spec.*} must survive L0 evaluation and be
		// evaluated by L2. Rewrite every string so its ${...} passes through
		// one CEL level unchanged.
		node["template"] = deferValue(tmpl)
	case res.ExternalRef != nil:
		raw, err := json.Marshal(res.ExternalRef)
		if err != nil {
			return nil, fmt.Errorf("marshal externalRef: %w", err)
		}
		var ref map[string]any
		if err := json.Unmarshal(raw, &ref); err != nil {
			return nil, fmt.Errorf("unmarshal externalRef: %w", err)
		}
		node["ref"] = deferValue(ref)
	default:
		return nil, fmt.Errorf("resource has neither template nor externalRef")
	}
	// readyWhen/includeWhen are L2 expressions (they reference L2 nodes), so
	// they pass through L0 unevaluated as well.
	if len(res.ReadyWhen) > 0 {
		terms := make([]any, len(res.ReadyWhen))
		for i, w := range res.ReadyWhen {
			terms[i] = deferExpr(unwrapExpr(w))
		}
		node["readyWhen"] = terms
	}
	if len(res.IncludeWhen) > 0 {
		terms := make([]any, len(res.IncludeWhen))
		for i, w := range res.IncludeWhen {
			terms[i] = deferExpr(unwrapExpr(w))
		}
		node["includeWhen"] = terms
	}
	// forEach dimensions bind an iterator name to a CEL list expression that
	// typically references the instance (${schema.spec.*}), so each dimension's
	// value is an L2 expression and passes through L0 unevaluated.
	if len(res.ForEach) > 0 {
		dims := make([]any, len(res.ForEach))
		for i, dim := range res.ForEach {
			out := make(map[string]any, len(dim))
			for iter, expr := range dim {
				out[iter] = deferExpr(unwrapExpr(expr))
			}
			dims[i] = out
		}
		node["forEach"] = dims
	}
	return node, nil
}

// deferValue recursively rewrites every string in v so any ${...} it contains
// survives one CEL evaluation (L0's) unchanged, to be evaluated at L2. Maps and
// slices are walked; scalars other than strings are returned as-is.
func deferValue(v any) any {
	switch x := v.(type) {
	case string:
		if !strings.Contains(x, "${") {
			return x // no expression: leave literal (L0 passes it through)
		}
		return "${" + passThrough(x) + "}"
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[k] = deferValue(val)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = deferValue(val)
		}
		return out
	default:
		return x
	}
}

// deferExpr wraps a bare CEL expression body (no ${}) so it survives L0
// evaluation and is evaluated at L2: L0 produces the literal ${<body>}.
func deferExpr(body string) string {
	return "${" + passThrough("${"+body+"}") + "}"
}

// unwrapExpr strips a single ${...} wrapper from s if present, returning the
// inner body; otherwise returns s unchanged. RGD readyWhen/includeWhen entries
// are authored as ${expr}; deferExpr re-wraps the body for L2.
func unwrapExpr(s string) string {
	if strings.HasPrefix(s, "${") && strings.HasSuffix(s, "}") {
		return s[2 : len(s)-1]
	}
	return s
}

// passThrough returns a CEL expression (to sit inside a ${...}) that evaluates
// to s verbatim, reconstructing any "${" as '$' + '{' so it is not treated as a
// nested interpolation at the evaluating level. See the unit-verified behavior
// in the design notes: passThrough("${schema.spec.name}-1") evaluates to the
// literal "${schema.spec.name}-1".
func passThrough(s string) string {
	parts := strings.Split(s, "${")
	expr := quoteCEL(parts[0])
	for _, p := range parts[1:] {
		expr += " + '$' + '{' + " + quoteCEL(p)
	}
	return expr
}

// quoteCEL renders s as a single-quoted CEL string literal, escaping
// backslashes and single quotes.
func quoteCEL(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

// joinAnd joins CEL boolean terms with &&.
func joinAnd(terms []string) string {
	out := ""
	for i, term := range terms {
		if i > 0 {
			out += " && "
		}
		out += term
	}
	return out
}
