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

package library

import (
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common"
	"github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/parser"
)

// ReadyVarName is the reserved CEL variable kro injects at evaluation time
// holding each node's readiness: a map from node ID to bool (true when the
// node reached a terminal ready state this reconcile pass). The name uses
// underscores so it can never collide with a node ID, which the Graph API
// constrains to ^[A-Za-z][A-Za-z0-9]*$ (no underscores).
const ReadyVarName = "__kro_ready__"

// Lifecycle returns a CEL library implementing the `.ready()` lifecycle
// signal used by higher-level abstractions expressed as Graphs (see
// examples/graph/rgd.yaml, docs/design/proposals/graph.md KREP-006).
//
// `<node>.ready()` reports whether the referenced node reached a terminal
// ready state this reconcile pass, as an optional<bool>:
//
//   - optional.of(true)  — the node applied and satisfied its readyWhen
//   - optional.of(false) — the node was evaluated but is not ready yet
//   - optional.none()    — readiness is unknown this pass (not yet evaluated,
//     or skipped); combine with .orValue(...) to pick a default
//
// It is authored in member form on a bare node identifier:
//
//	deployment.ready()
//	crd.ready().orValue(false)
//
// Mechanically, `.ready()` is a parse-time macro that rewrites `id.ready()`
// into an optional index into the reserved readiness map,
// __kro_ready__[?'id']. That yields optional<bool> and composes with the
// standard optional helpers (orValue, has, etc.) without a custom binding,
// so the only runtime contribution is injecting __kro_ready__ into scope
// (see the graph engine's executor/runtime, which populates it from the
// per-pass readiness state).
func Lifecycle() cel.EnvOption {
	return cel.Lib(&lifecycleLibrary{})
}

type lifecycleLibrary struct{}

func (l *lifecycleLibrary) LibraryName() string {
	return "kro.lifecycle"
}

func (l *lifecycleLibrary) CompileOptions() []cel.EnvOption {
	return []cel.EnvOption{
		// The readiness map is a variable so the .ready() macro can rewrite
		// into an optional index against it. Declared map<string, bool>;
		// __kro_ready__[?'id'] then type-checks as optional<bool>.
		cel.Variable(ReadyVarName, cel.MapType(cel.StringType, cel.BoolType)),
		cel.Macros(parser.NewReceiverMacro("ready", 0, readyMacro)),
	}
}

func (l *lifecycleLibrary) ProgramOptions() []cel.ProgramOption {
	return nil
}

// readyMacro rewrites `<ident>.ready()` into `__kro_ready__[?'<ident>']`, an
// optional<bool> lookup keyed by the receiver's node ID. Only a bare
// identifier receiver is supported: readiness is a per-node property keyed by
// node ID, and a non-identifier receiver (a field access, index, or literal)
// has no node identity to key on. Such receivers are rejected with a
// source-position error rather than silently producing optional.none().
func readyMacro(eh parser.ExprHelper, target ast.Expr, args []ast.Expr) (ast.Expr, *common.Error) {
	// cel-go dispatches macros on (name, arg count, receiver-style) only, so
	// any `x.ready()` lands here. The receiver must be a bare identifier.
	if target == nil || target.Kind() != ast.IdentKind {
		if target != nil {
			return nil, eh.NewError(target.ID(),
				"ready(): receiver must be a bare node identifier (e.g. deployment.ready())")
		}
		return nil, eh.NewError(0, "ready(): receiver must be a bare node identifier")
	}
	nodeID := target.AsIdent()

	readyMap := eh.NewIdent(ReadyVarName)
	key := eh.NewLiteral(types.String(nodeID))
	// __kro_ready__[?'nodeID'] → optional<bool>
	return eh.NewCall(operators.OptIndex, readyMap, key), nil
}
