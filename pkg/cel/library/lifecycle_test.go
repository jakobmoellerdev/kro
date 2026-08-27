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

func newLifecycleEnv(t *testing.T) *cel.Env {
	t.Helper()
	env, err := cel.NewEnv(cel.OptionalTypes(), Lifecycle())
	require.NoError(t, err)
	return env
}

func evalLifecycle(t *testing.T, env *cel.Env, expr string, ready map[string]bool) (any, error) {
	t.Helper()
	ast, issues := env.Compile(expr)
	require.NoError(t, issues.Err())
	prg, err := env.Program(ast)
	require.NoError(t, err)
	// __kro_ready__ is a map<string, bool>; the .ready() macro rewrote the
	// expression into an optional index against it.
	m := map[string]any{}
	for k, v := range ready {
		m[k] = v
	}
	out, _, err := prg.Eval(map[string]any{ReadyVarName: m})
	if err != nil {
		return nil, err
	}
	return out.Value(), nil
}

func TestReadyMacro(t *testing.T) {
	env := newLifecycleEnv(t)

	t.Run("ready node with orValue resolves true", func(t *testing.T) {
		v, err := evalLifecycle(t, env, `deployment.ready().orValue(false)`, map[string]bool{"deployment": true})
		require.NoError(t, err)
		assert.Equal(t, true, v)
	})

	t.Run("not-ready node with orValue resolves false", func(t *testing.T) {
		v, err := evalLifecycle(t, env, `deployment.ready().orValue(false)`, map[string]bool{"deployment": false})
		require.NoError(t, err)
		assert.Equal(t, false, v)
	})

	t.Run("unknown node yields optional.none, orValue picks default", func(t *testing.T) {
		// deployment absent from the readiness map → optional.none() →
		// orValue(false) == false.
		v, err := evalLifecycle(t, env, `deployment.ready().orValue(false)`, map[string]bool{"other": true})
		require.NoError(t, err)
		assert.Equal(t, false, v)
	})

	t.Run("has() distinguishes none from present", func(t *testing.T) {
		v, err := evalLifecycle(t, env, `deployment.ready().hasValue()`, map[string]bool{"other": true})
		require.NoError(t, err)
		assert.Equal(t, false, v, "absent node → optional.none → hasValue false")

		v, err = evalLifecycle(t, env, `deployment.ready().hasValue()`, map[string]bool{"deployment": false})
		require.NoError(t, err)
		assert.Equal(t, true, v, "present-but-false node → optional.of(false) → hasValue true")
	})

	t.Run("AND-fold across multiple nodes (rgd.yaml status shape)", func(t *testing.T) {
		expr := `deployment.ready().orValue(false) && service.ready().orValue(false)`
		v, err := evalLifecycle(t, env, expr, map[string]bool{"deployment": true, "service": true})
		require.NoError(t, err)
		assert.Equal(t, true, v)

		v, err = evalLifecycle(t, env, expr, map[string]bool{"deployment": true, "service": false})
		require.NoError(t, err)
		assert.Equal(t, false, v)
	})

	t.Run("ternary state selection (ACTIVE/IN_PROGRESS)", func(t *testing.T) {
		expr := `deployment.ready().orValue(false) ? 'ACTIVE' : 'IN_PROGRESS'`
		v, err := evalLifecycle(t, env, expr, map[string]bool{"deployment": true})
		require.NoError(t, err)
		assert.Equal(t, "ACTIVE", v)

		v, err = evalLifecycle(t, env, expr, map[string]bool{"deployment": false})
		require.NoError(t, err)
		assert.Equal(t, "IN_PROGRESS", v)
	})
}

func TestReadyMacroRejectsNonIdentifierReceiver(t *testing.T) {
	env := newLifecycleEnv(t)

	cases := []string{
		`foo.bar.ready()`,   // field access receiver
		`items[0].ready()`,  // index receiver
		`'literal'.ready()`, // literal receiver
	}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			_, issues := env.Compile(expr)
			require.Error(t, issues.Err(), "non-identifier receiver must be rejected at parse time")
		})
	}
}
