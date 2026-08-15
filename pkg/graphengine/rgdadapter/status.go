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

// Package rgdadapter – instance status projection (F3b).
//
// After executor.Apply has reconciled the Graph nodes, the runtime scope
// holds each node's observed value.  ProjectInstanceStatus evaluates the
// RGD's spec.schema.status CEL expressions against that scope to produce
// the instance's status map.  ProjectInstanceConditions evaluates the
// status.conditions block that uses runtime.newCondition(…).
package rgdadapter

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/cel/openapi"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
	krocel "github.com/kubernetes-sigs/kro/pkg/cel"
	celunstructured "github.com/kubernetes-sigs/kro/pkg/cel/unstructured"
	"github.com/kubernetes-sigs/kro/pkg/cel/library"
	"github.com/kubernetes-sigs/kro/pkg/graph"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/runtime"
)

// ProjectInstanceStatus evaluates the RGD's spec.schema.status CEL
// expressions against the reconciled runtime scope and returns a flat
// map[string]any that callers can patch onto the instance's .status.
//
// The routine:
//  1. Unmarshals RGD.Spec.Schema.Status.Raw.
//  2. Calls graph.ParseStatusExpressions to separate condition fields from
//     regular fields (and strip the ${…} wrappers).
//  3. Builds a transient CEL environment containing every Graph-node ID
//     in the runtime scope as an untyped (dyn) variable.
//  4. Compiles and evaluates each field expression against rt.Scope().
//  5. Sets the result at the field's path in the output map.
//
// The function is intentionally schemaless (dyn) at projection time: we
// have already proved the expressions are valid at Graph-compile time; we
// only need their runtime values here.
//
// status.conditions entries are excluded — call ProjectInstanceConditions
// for those.
func ProjectInstanceStatus(
	rt *runtime.Runtime,
	rgd *v1alpha1.ResourceGraphDefinition,
) (map[string]any, error) {
	if rt == nil {
		return nil, fmt.Errorf("status projection: runtime is required")
	}
	if rgd == nil {
		return nil, fmt.Errorf("status projection: rgd is required")
	}

	statusMap, err := unmarshalStatusRaw(rgd)
	if err != nil {
		return nil, err
	}
	if statusMap == nil {
		// No status block defined.
		return map[string]any{}, nil
	}

	// Parse: separates conditions from plain fields; mutates statusMap to
	// remove the conditions key.
	fields, _, noExprFields, err := graph.ParseStatusExpressions(statusMap)
	if err != nil {
		return nil, fmt.Errorf("status projection: parse: %w", err)
	}
	if len(noExprFields) > 0 {
		return nil, fmt.Errorf("status projection: fields without expressions are not supported: %v", noExprFields)
	}
	if len(fields) == 0 {
		return map[string]any{}, nil
	}

	// Build a CEL env with every scope key declared as dyn.
	// runtime library is NOT needed for plain status fields (no newCondition).
	env, err := buildStatusEnv(rt.Scope(), false)
	if err != nil {
		return nil, fmt.Errorf("status projection: build env: %w", err)
	}

	// Use schema-aware scope so CEL sees correctly typed values
	// (e.g. Secret.data as bytes for string(bytes) conversions).
	saScope := schemaAwareScope(rt.Scope(), rt)

	result := make(map[string]any, len(fields))
	for _, f := range fields {
		val, err := evalStatusExpr(env, saScope, f.Expression)
		if err != nil {
			return nil, fmt.Errorf("status projection: field %q: %w", f.Path, err)
		}
		if err := setAtPath(result, f.Path, val); err != nil {
			return nil, fmt.Errorf("status projection: set %q: %w", f.Path, err)
		}
	}
	return result, nil
}

// ProjectInstanceConditions evaluates the status.conditions expressions
// (runtime.newCondition(…) calls) against the reconciled runtime scope and
// returns them as a []metav1.Condition slice.
//
// The Graph engine's compiler does NOT include library.Runtime() (it passes
// WithRuntimeLibrary(false)).  That means the compiled Graph Program has no
// compiled programs for condition expressions.  We therefore re-compile the
// condition expressions here against a fresh CEL environment that DOES
// include library.Runtime().
//
// Gap (scoreboard rows 15, 17): the Graph engine's node compiler does not
// compile author-condition expressions — they live in the RGD, not in a
// Graph node.  There is no cached cel.Program in the runtime for them.  We
// must compile them on every reconcile against the transient env built
// below.  Once F6 wires a persistent per-RGD CEL cache this overhead can be
// amortized, but correctness is unaffected.
func ProjectInstanceConditions(
	rt *runtime.Runtime,
	rgd *v1alpha1.ResourceGraphDefinition,
) ([]metav1.Condition, error) {
	if rt == nil {
		return nil, fmt.Errorf("condition projection: runtime is required")
	}
	if rgd == nil {
		return nil, fmt.Errorf("condition projection: rgd is required")
	}

	statusMap, err := unmarshalStatusRaw(rgd)
	if err != nil {
		return nil, err
	}
	if statusMap == nil {
		return nil, nil
	}

	// ParseStatusExpressions mutates statusMap to extract + remove the
	// conditions block.  We only care about conditionExprs here.
	_, conditionExprs, _, err := graph.ParseStatusExpressions(statusMap)
	if err != nil {
		return nil, fmt.Errorf("condition projection: parse: %w", err)
	}
	if len(conditionExprs) == 0 {
		return nil, nil
	}

	// Build a CEL env WITH library.Runtime() so runtime.newCondition compiles.
	env, err := buildStatusEnv(rt.Scope(), true)
	if err != nil {
		return nil, fmt.Errorf("condition projection: build env: %w", err)
	}

	// The evaluation scope must include the `runtime` singleton.
	saScope := schemaAwareScope(rt.Scope(), rt)
	scope := make(map[string]any, len(saScope)+1)
	for k, v := range saScope {
		scope[k] = v
	}
	scope[library.RuntimeVarName] = library.RuntimeSingleton

	conditions := make([]metav1.Condition, 0, len(conditionExprs))
	for i, rawExpr := range conditionExprs {
		inner := unwrapExpr(rawExpr)
		cond, err := evalConditionExpr(env, scope, inner)
		if err != nil {
			return nil, fmt.Errorf("condition projection: conditions[%d] %q: %w", i, rawExpr, err)
		}
		conditions = append(conditions, cond)
	}
	return conditions, nil
}

// ── helpers ──────────────────────────────────────────────────────────────────

// schemaAwareScope returns a copy of rawScope where each value that has a
// corresponding OpenAPI schema in prog.NodeSchemas is wrapped with
// celunstructured.UnstructuredToVal for schema-aware CEL type conversion
// (e.g. Secret.data values are typed as bytes so string(bytes) works).
// Values without a matching schema are passed through unchanged.
func schemaAwareScope(rawScope map[string]any, rt *runtime.Runtime) map[string]any {
	if rt == nil || rt.Program() == nil {
		return rawScope
	}
	nodeSchemas := rt.Program().NodeSchemas
	if len(nodeSchemas) == 0 {
		return rawScope
	}
	out := make(map[string]any, len(rawScope))
	for k, v := range rawScope {
		sc, ok := nodeSchemas[k]
		if !ok || sc == nil {
			out[k] = v
			continue
		}
		switch val := v.(type) {
		case map[string]any:
			out[k] = celunstructured.UnstructuredToVal(val, &openapi.Schema{Schema: sc})
		case []any:
			// Collection nodes: wrap each item.
			list := make([]any, len(val))
			for i, item := range val {
				if m, ok := item.(map[string]any); ok {
					list[i] = celunstructured.UnstructuredToVal(m, &openapi.Schema{Schema: sc})
				} else {
					list[i] = item
				}
			}
			out[k] = list
		default:
			out[k] = v
		}
	}
	return out
}

// unmarshalStatusRaw decodes RGD.Spec.Schema.Status.Raw into a map.
// Returns (nil, nil) when no status block is defined.
func unmarshalStatusRaw(rgd *v1alpha1.ResourceGraphDefinition) (map[string]interface{}, error) {
	if rgd.Spec.Schema == nil {
		return nil, nil
	}
	raw := rgd.Spec.Schema.Status.Raw
	if len(raw) == 0 {
		return nil, nil
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("status projection: unmarshal status: %w", err)
	}
	return m, nil
}

// buildStatusEnv constructs a transient CEL environment whose variable
// declarations cover every key currently in scope (declared as dyn).
// includeRuntime = true adds library.Runtime() for newCondition support.
func buildStatusEnv(scope map[string]any, includeRuntime bool) (*cel.Env, error) {
	ids := make([]string, 0, len(scope))
	for k := range scope {
		ids = append(ids, k)
	}
	opts := []krocel.EnvOption{
		krocel.WithResourceIDs(ids),
		krocel.WithRuntimeLibrary(includeRuntime),
	}
	return krocel.DefaultEnvironment(opts...)
}

// evalStatusExpr compiles and evaluates a plain CEL expression (no ${…}
// wrapper) against scope.  Returns the Go-native result.
func evalStatusExpr(env *cel.Env, scope map[string]any, expr string) (any, error) {
	parsed, issues := env.Parse(expr)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("parse %q: %w", expr, issues.Err())
	}
	checked, issues := env.Check(parsed)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("check %q: %w", expr, issues.Err())
	}
	prog, err := env.Program(checked)
	if err != nil {
		return nil, fmt.Errorf("program %q: %w", expr, err)
	}
	e := &krocel.Expression{Original: expr, Program: prog}
	return e.Eval(scope)
}

// evalConditionExpr compiles a runtime.newCondition(…) expression and returns
// a metav1.Condition from the library.Condition value.
//
// NOTE: we bypass krocel.Expression.Eval here because that path goes through
// conversion.GoNativeType, which does not know about the custom
// *library.Condition ref.Val type and returns ErrUnsupportedType.  Instead we
// call cel.Program.Eval directly and pattern-match the raw ref.Val.
func evalConditionExpr(env *cel.Env, scope map[string]any, expr string) (metav1.Condition, error) {
	parsed, issues := env.Parse(expr)
	if issues != nil && issues.Err() != nil {
		return metav1.Condition{}, fmt.Errorf("parse %q: %w", expr, issues.Err())
	}
	checked, issues := env.Check(parsed)
	if issues != nil && issues.Err() != nil {
		return metav1.Condition{}, fmt.Errorf("check %q: %w", expr, issues.Err())
	}
	prog, err := env.Program(checked)
	if err != nil {
		return metav1.Condition{}, fmt.Errorf("program %q: %w", expr, err)
	}

	out, _, err := prog.Eval(scope)
	if err != nil {
		return metav1.Condition{}, fmt.Errorf("eval %q: %w", expr, err)
	}

	// The eval result is a *library.Condition ref.Val.
	if c, ok := out.(*library.Condition); ok {
		return metav1.Condition{
			Type:    c.ConditionType,
			Status:  metav1.ConditionStatus(c.Status),
			Reason:  c.Reason,
			Message: c.Message,
		}, nil
	}
	return metav1.Condition{}, fmt.Errorf("expected *library.Condition ref.Val, got %T", out)
}

// unwrapExpr strips ${...} wrappers from a CEL expression string.
// Standalone ${expr} → expr; bare expression → unchanged.
func unwrapExpr(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "${") && strings.HasSuffix(s, "}") {
		s = strings.TrimPrefix(s, "${")
		s = strings.TrimSuffix(s, "}")
	}
	return s
}

// setAtPath writes val at the dotted path in m, creating intermediate maps
// as needed.  Only simple dot-separated paths (no array indices) are
// supported; status fields with array paths are not a current RGD pattern.
func setAtPath(m map[string]any, path string, val any) error {
	parts := strings.Split(path, ".")
	cur := m
	for i, part := range parts {
		if i == len(parts)-1 {
			cur[part] = val
			return nil
		}
		if next, ok := cur[part]; ok {
			nextMap, ok := next.(map[string]any)
			if !ok {
				return fmt.Errorf("path segment %q already exists with type %T", part, next)
			}
			cur = nextMap
		} else {
			nextMap := make(map[string]any)
			cur[part] = nextMap
			cur = nextMap
		}
	}
	return nil
}
