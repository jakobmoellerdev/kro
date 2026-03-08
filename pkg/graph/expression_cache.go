// Copyright 2025 The Kubernetes Authors.
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

package graph

import "github.com/google/cel-go/cel"

// expressionCache caches Parse+Check results (checked ASTs) for CEL expressions
// within a single RGD build. When the same expression string appears in multiple
// resources (e.g., "schema.spec.name"), the expensive Parse and Check steps are
// performed only once. Programs must still be compiled per-expression since each
// Expression needs its own Program instance.
//
// Not thread-safe — designed for use within a single build operation.
type expressionCache struct {
	entries map[string]*cel.Ast
}

func newExpressionCache() *expressionCache {
	return &expressionCache{entries: make(map[string]*cel.Ast)}
}

// lookup returns the cached checked AST for the given expression string, if any.
func (c *expressionCache) lookup(expr string) (*cel.Ast, bool) {
	if c == nil {
		return nil, false
	}
	ast, ok := c.entries[expr]
	return ast, ok
}

// store caches a checked AST for the given expression string.
func (c *expressionCache) store(expr string, ast *cel.Ast) {
	if c == nil {
		return
	}
	c.entries[expr] = ast
}
