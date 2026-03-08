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

package cel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

func TestSchemaCache_SamePointerCached(t *testing.T) {
	schema := &spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type: spec.StringOrArray{"object"},
			Properties: map[string]spec.Schema{
				"name": {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}},
			},
		},
	}

	cache := NewSchemaCache()

	result1 := cache.Convert(schema, false)
	require.NotNil(t, result1)

	result2 := cache.Convert(schema, false)
	require.NotNil(t, result2)

	// Same pointer input should return same pointer output
	assert.Same(t, result1, result2)
}

func TestSchemaCache_DifferentPointers(t *testing.T) {
	schema1 := &spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type: spec.StringOrArray{"object"},
			Properties: map[string]spec.Schema{
				"name": {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}},
			},
		},
	}
	schema2 := &spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type: spec.StringOrArray{"object"},
			Properties: map[string]spec.Schema{
				"name": {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}},
			},
		},
	}

	cache := NewSchemaCache()

	result1 := cache.Convert(schema1, false)
	result2 := cache.Convert(schema2, false)

	// Different pointers produce separate cache entries
	assert.NotSame(t, result1, result2)
}

func TestSchemaCache_NilSchema(t *testing.T) {
	cache := NewSchemaCache()
	result := cache.Convert(nil, false)
	assert.Nil(t, result)
}

func TestSchemaCache_NilResultCached(t *testing.T) {
	// A schema with no type produces nil DeclType
	schema := &spec.Schema{}

	cache := NewSchemaCache()

	result1 := cache.Convert(schema, false)
	assert.Nil(t, result1)

	// Second call should also return nil (from cache, not re-converting)
	result2 := cache.Convert(schema, false)
	assert.Nil(t, result2)

	// Verify it's in the cache
	assert.Equal(t, 1, len(cache.cache))
}

func TestSchemaCache_NilCacheSafe(t *testing.T) {
	var cache *SchemaCache

	schema := &spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type: spec.StringOrArray{"string"},
		},
	}

	// Should not panic with nil cache
	result := cache.Convert(schema, false)
	assert.NotNil(t, result)
}
