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

import (
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	krocel "github.com/kubernetes-sigs/kro/pkg/cel"
)

func TestExpressionCache_HitOnDuplicate(t *testing.T) {
	env, err := krocel.DefaultEnvironment(
		krocel.WithResourceIDs([]string{"schema"}),
	)
	require.NoError(t, err)

	cache := newExpressionCache()

	expr1 := &krocel.Expression{Original: "schema"}
	expr2 := &krocel.Expression{Original: "schema"}

	ast1, err := parseCheckAndCompile(env, expr1, cache)
	require.NoError(t, err)

	ast2, err := parseCheckAndCompile(env, expr2, cache)
	require.NoError(t, err)

	// Both should return the same AST pointer (cache hit)
	assert.Same(t, ast1, ast2)
	// Both should have programs set
	assert.NotNil(t, expr1.Program)
	assert.NotNil(t, expr2.Program)
}

func TestExpressionCache_MissOnDifferent(t *testing.T) {
	env, err := krocel.DefaultEnvironment(
		krocel.WithResourceIDs([]string{"a", "b"}),
	)
	require.NoError(t, err)

	cache := newExpressionCache()

	expr1 := &krocel.Expression{Original: "a"}
	expr2 := &krocel.Expression{Original: "b"}

	ast1, err := parseCheckAndCompile(env, expr1, cache)
	require.NoError(t, err)

	ast2, err := parseCheckAndCompile(env, expr2, cache)
	require.NoError(t, err)

	// Different expressions should produce different ASTs
	assert.NotSame(t, ast1, ast2)
}

func TestExpressionCache_NilCacheSafe(t *testing.T) {
	env, err := krocel.DefaultEnvironment(
		krocel.WithResourceIDs([]string{"x"}),
	)
	require.NoError(t, err)

	expr := &krocel.Expression{Original: "x"}
	ast, err := parseCheckAndCompile(env, expr, nil)
	require.NoError(t, err)
	assert.NotNil(t, ast)
	assert.NotNil(t, expr.Program)
}

func TestExpressionCache_LookupAndStore(t *testing.T) {
	cache := newExpressionCache()

	// Empty cache returns miss
	_, ok := cache.lookup("test")
	assert.False(t, ok)

	// Store and retrieve
	fakeAST := &cel.Ast{}
	cache.store("test", fakeAST)

	got, ok := cache.lookup("test")
	assert.True(t, ok)
	assert.Same(t, fakeAST, got)
}

func TestExpressionCache_NilCacheMethods(t *testing.T) {
	var cache *expressionCache

	// nil cache should not panic
	_, ok := cache.lookup("test")
	assert.False(t, ok)

	cache.store("test", &cel.Ast{}) // should not panic
}
