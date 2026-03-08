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
	celopenapi "k8s.io/apiserver/pkg/cel/openapi"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

func TestSchemaDeclTypeWithMetadata_FastPath(t *testing.T) {
	// Build a realistic object schema with nested properties
	schema := &spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type: spec.StringOrArray{"object"},
			Required: []string{"name"},
			Properties: map[string]spec.Schema{
				"name": {
					SchemaProps: spec.SchemaProps{
						Type: spec.StringOrArray{"string"},
					},
				},
				"replicas": {
					SchemaProps: spec.SchemaProps{
						Type: spec.StringOrArray{"integer"},
					},
				},
				"metadata": {
					SchemaProps: spec.SchemaProps{
						Type: spec.StringOrArray{"object"},
						Properties: map[string]spec.Schema{
							"labels": {
								SchemaProps: spec.SchemaProps{
									Type: spec.StringOrArray{"object"},
									AdditionalProperties: &spec.SchemaOrBool{
										Allows: true,
										Schema: &spec.Schema{
											SchemaProps: spec.SchemaProps{
												Type: spec.StringOrArray{"string"},
											},
										},
									},
								},
							},
						},
					},
				},
				"items": {
					SchemaProps: spec.SchemaProps{
						Type: spec.StringOrArray{"array"},
						Items: &spec.SchemaOrArray{
							Schema: &spec.Schema{
								SchemaProps: spec.SchemaProps{
									Type: spec.StringOrArray{"string"},
								},
							},
						},
					},
				},
			},
		},
	}

	// The fast path is exercised via *celopenapi.Schema
	result := SchemaDeclTypeWithMetadata(&celopenapi.Schema{Schema: schema}, false)
	require.NotNil(t, result)

	// Verify the result has the expected fields
	assert.Contains(t, result.Fields, "name")
	assert.Contains(t, result.Fields, "replicas")
	assert.Contains(t, result.Fields, "metadata")
	assert.Contains(t, result.Fields, "items")
}

func TestSchemaDeclTypeWithMetadata_NilSchema(t *testing.T) {
	result := SchemaDeclTypeWithMetadata(nil, false)
	assert.Nil(t, result)
}

func TestSchemaDeclTypeWithMetadata_PrimitiveTypes(t *testing.T) {
	tests := []struct {
		name     string
		typ      string
		wantName string
	}{
		{"string", "string", "string"},
		{"integer", "integer", "int"},
		{"number", "number", "double"},
		{"boolean", "boolean", "bool"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := &spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: spec.StringOrArray{tt.typ},
				},
			}
			result := SchemaDeclTypeWithMetadata(&celopenapi.Schema{Schema: schema}, false)
			require.NotNil(t, result)
			assert.Equal(t, tt.wantName, result.CelType().String())
		})
	}
}

func BenchmarkSchemaDeclTypeWithMetadata(b *testing.B) {
	// Build a schema resembling a K8s Pod spec
	schema := &spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:     spec.StringOrArray{"object"},
			Required: []string{"apiVersion", "kind"},
			Properties: map[string]spec.Schema{
				"apiVersion": {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}},
				"kind":       {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}},
				"metadata": {
					SchemaProps: spec.SchemaProps{
						Type: spec.StringOrArray{"object"},
						Properties: map[string]spec.Schema{
							"name":      {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}},
							"namespace": {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}},
							"labels": {
								SchemaProps: spec.SchemaProps{
									Type: spec.StringOrArray{"object"},
									AdditionalProperties: &spec.SchemaOrBool{
										Allows: true,
										Schema: &spec.Schema{SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}},
									},
								},
							},
						},
					},
				},
				"spec": {
					SchemaProps: spec.SchemaProps{
						Type: spec.StringOrArray{"object"},
						Properties: map[string]spec.Schema{
							"containers": {
								SchemaProps: spec.SchemaProps{
									Type: spec.StringOrArray{"array"},
									Items: &spec.SchemaOrArray{
										Schema: &spec.Schema{
											SchemaProps: spec.SchemaProps{
												Type: spec.StringOrArray{"object"},
												Properties: map[string]spec.Schema{
													"name":  {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}},
													"image": {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}},
													"ports": {
														SchemaProps: spec.SchemaProps{
															Type: spec.StringOrArray{"array"},
															Items: &spec.SchemaOrArray{
																Schema: &spec.Schema{
																	SchemaProps: spec.SchemaProps{
																		Type: spec.StringOrArray{"object"},
																		Properties: map[string]spec.Schema{
																			"containerPort": {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"integer"}}},
																			"protocol":      {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}},
																		},
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
							"restartPolicy": {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}},
						},
					},
				},
				"status": {
					SchemaProps: spec.SchemaProps{
						Type: spec.StringOrArray{"object"},
						Properties: map[string]spec.Schema{
							"phase":   {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}},
							"podIP":   {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}},
							"hostIP":  {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}},
							"message": {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}},
						},
					},
				},
			},
		},
	}

	wrapped := &celopenapi.Schema{Schema: schema}

	b.ResetTimer()
	for b.Loop() {
		SchemaDeclTypeWithMetadata(wrapped, false)
	}
}
