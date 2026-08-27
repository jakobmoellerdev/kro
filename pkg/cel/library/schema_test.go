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
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlural(t *testing.T) {
	env, err := cel.NewEnv(Schema())
	require.NoError(t, err)

	cases := []struct {
		expr string
		want string
	}{
		{`plural('WebApp')`, "webapps"},
		{`plural('Gateway')`, "gateways"},
		{`plural('Ingress')`, "ingresses"},
		{`plural('Policy')`, "policies"},
		{`plural('resourcegraphdefinition')`, "resourcegraphdefinitions"},
	}
	for _, tc := range cases {
		t.Run(tc.expr, func(t *testing.T) {
			ast, issues := env.Compile(tc.expr)
			require.NoError(t, issues.Err())
			prg, err := env.Program(ast)
			require.NoError(t, err)
			out, _, err := prg.Eval(map[string]any{})
			require.NoError(t, err)
			assert.Equal(t, tc.want, out.Value())
		})
	}
}

func TestSimpleSchemaToOpenAPI(t *testing.T) {
	env, err := cel.NewEnv(Schema())
	require.NoError(t, err)

	t.Run("single-arg spec map converts to an OpenAPI object schema", func(t *testing.T) {
		ast, issues := env.Compile(`simpleSchema.toOpenAPI({'name': 'string', 'replicas': 'integer'})`)
		require.NoError(t, issues.Err())
		prg, err := env.Program(ast)
		require.NoError(t, err)
		out, _, err := prg.Eval(map[string]any{})
		require.NoError(t, err)

		result, ok := out.Value().(map[string]any)
		require.True(t, ok, "result must be a map, got %T", out.Value())
		assert.Equal(t, "object", result["type"])

		props, ok := result["properties"].(map[string]any)
		require.True(t, ok, "expected properties map, got %T", result["properties"])
		name, ok := props["name"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "string", name["type"])
		replicas, ok := props["replicas"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "integer", replicas["type"])
	})

	t.Run("result is usable as a nested CRD openAPIV3Schema", func(t *testing.T) {
		// The typical embedding: the converted schema sits under
		// versions[].schema.openAPIV3Schema. Verify field access composes.
		ast, issues := env.Compile(
			`simpleSchema.toOpenAPI({'name': 'string'}).properties.name.type`)
		require.NoError(t, issues.Err())
		prg, err := env.Program(ast)
		require.NoError(t, err)
		out, _, err := prg.Eval(map[string]any{})
		require.NoError(t, err)
		assert.Equal(t, "string", out.Value())
	})

	t.Run("invalid simpleschema type surfaces a CEL error", func(t *testing.T) {
		ast, issues := env.Compile(`simpleSchema.toOpenAPI({'bad': 'notatype'})`)
		require.NoError(t, issues.Err())
		prg, err := env.Program(ast)
		require.NoError(t, err)
		_, _, err = prg.Eval(map[string]any{})
		require.Error(t, err, "an unknown SimpleSchema type must error")
	})

	t.Run("non-map argument surfaces a CEL error", func(t *testing.T) {
		ast, issues := env.Compile(`simpleSchema.toOpenAPI('not a map')`)
		require.NoError(t, issues.Err())
		prg, err := env.Program(ast)
		require.NoError(t, err)
		_, _, err = prg.Eval(map[string]any{})
		require.Error(t, err)
	})
}
