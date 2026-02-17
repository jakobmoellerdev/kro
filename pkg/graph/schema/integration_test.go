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

package schema

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime/schema"

	krocel "github.com/kubernetes-sigs/kro/pkg/cel"
)

// TestSecretDataCELIntegration verifies that the unified converter fixes the original issue:
// CEL expressions like "string(secret.data.clientId)" now work because Secret data is
// converted from base64 strings to []byte at runtime, matching the compile-time type.
func TestSecretDataCELIntegration(t *testing.T) {
	converter := NewObjectConverter(&mockRESTMapper{}, createTestResolver())

	// Simulate the original Secret data from the sample-rgd.yaml
	secret := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]interface{}{
			"name":      "sample-secret",
			"namespace": "default",
		},
		"data": map[string]interface{}{
			"clientId":     base64.StdEncoding.EncodeToString([]byte("max")),        // "bWF4"
			"clientSecret": base64.StdEncoding.EncodeToString([]byte("mustermann")), // "bXVzdGVybWFubg=="
		},
	}

	gvr := schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "secrets",
	}

	// Apply the unified converter
	converted := converter.ConvertForGVR(secret, gvr)

	// Verify that data fields are now bytes (raw base64 strings as bytes)
	data := converted["data"].(map[string]interface{})
	clientId := data["clientId"].([]byte)
	clientSecret := data["clientSecret"].([]byte)

	// The values are the raw base64 strings as bytes, not decoded
	expectedClientId := base64.StdEncoding.EncodeToString([]byte("max"))
	expectedClientSecret := base64.StdEncoding.EncodeToString([]byte("mustermann"))
	require.Equal(t, []byte(expectedClientId), clientId)
	require.Equal(t, []byte(expectedClientSecret), clientSecret)

	// Now simulate the CEL expression evaluation that was failing
	// This represents what happens in the runtime when CEL evaluates "string(secret.data.clientId)"

	// Create a minimal CEL environment for testing string conversion
	// We need to declare the 'secret' variable so it's recognized by CEL
	env, err := krocel.DefaultEnvironment(krocel.WithResourceIDs([]string{"secret"}))
	require.NoError(t, err)

	// Test the original failing expression: string(secret.data.clientId)
	// Before our fix: CEL saw secret.data.clientId as string, but string(string) doesn't exist
	// After our fix: CEL sees secret.data.clientId as bytes, and string(bytes) exists
	ast, issues := env.Compile("string(secret.data.clientId)")
	require.NoError(t, issues.Err())

	program, err := env.Program(ast)
	require.NoError(t, err)

	expr := &krocel.Expression{
		Original: "string(secret.data.clientId)",
		Program:  program,
	}

	// Create CEL context with our converted data
	celContext := map[string]any{
		"secret": converted,
	}

	result, err := expr.Eval(celContext)
	require.NoError(t, err)

	// Verify the result is the base64 string (since we're not decoding)
	assert.Equal(t, expectedClientId, result)

	// Test the second expression from sample-rgd.yaml
	ast2, issues2 := env.Compile("string(secret.data.clientSecret)")
	require.NoError(t, issues2.Err())

	program2, err := env.Program(ast2)
	require.NoError(t, err)

	expr2 := &krocel.Expression{
		Original: "string(secret.data.clientSecret)",
		Program:  program2,
	}

	result2, err := expr2.Eval(celContext)
	require.NoError(t, err)

	assert.Equal(t, expectedClientSecret, result2)
}
