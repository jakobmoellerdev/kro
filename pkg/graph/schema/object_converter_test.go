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
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

type mockRESTMapper struct {
	meta.RESTMapper
}

type mockSchemaResolver struct {
	schemas map[schema.GroupVersionKind]*spec.Schema
}

func (m *mockSchemaResolver) ResolveSchema(gvk schema.GroupVersionKind) (*spec.Schema, error) {
	if m.schemas != nil {
		if schema, ok := m.schemas[gvk]; ok {
			return schema, nil
		}
	}
	return nil, nil
}

func (m *mockRESTMapper) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	if resource.Group == "" && resource.Version == "v1" && resource.Resource == "secrets" {
		return schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Secret"}, nil
	}
	if resource.Group == "" && resource.Version == "v1" && resource.Resource == "configmaps" {
		return schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}, nil
	}
	if resource.Group == "" && resource.Version == "v1" && resource.Resource == "services" {
		return schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Service"}, nil
	}
	if resource.Group == "" && resource.Version == "v1" && resource.Resource == "pods" {
		return schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}, nil
	}
	if resource.Group == "custom.io" && resource.Version == "v1" && resource.Resource == "customs" {
		return schema.GroupVersionKind{Group: "custom.io", Version: "v1", Kind: "Custom"}, nil
	}
	if resource.Group == "apps" && resource.Version == "v1" && resource.Resource == "deployments" {
		return schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}, nil
	}
	return schema.GroupVersionKind{}, nil
}

// createTestResolver creates a mock schema resolver with comprehensive test schemas
func createTestResolver() *mockSchemaResolver {
	return &mockSchemaResolver{schemas: createTestSchemas()}
}

// createTestSchemas creates comprehensive test schemas for all conversion scenarios
func createTestSchemas() map[schema.GroupVersionKind]*spec.Schema {
	schemas := make(map[schema.GroupVersionKind]*spec.Schema)

	// Secret schema with data field as map[string]bytes
	secretGVK := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Secret"}
	schemas[secretGVK] = &spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type: []string{"object"},
			Properties: map[string]spec.Schema{
				"data": {
					SchemaProps: spec.SchemaProps{
						Type: []string{"object"},
						AdditionalProperties: &spec.SchemaOrBool{
							Schema: &spec.Schema{
								SchemaProps: spec.SchemaProps{
									Type:   []string{"string"},
									Format: "byte", // base64-encoded bytes → []byte
								},
							},
						},
					},
				},
			},
		},
	}

	// ConfigMap schema with data field as map[string]string
	configMapGVK := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}
	schemas[configMapGVK] = &spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type: []string{"object"},
			Properties: map[string]spec.Schema{
				"data": {
					SchemaProps: spec.SchemaProps{
						Type: []string{"object"},
						AdditionalProperties: &spec.SchemaOrBool{
							Schema: &spec.Schema{
								SchemaProps: spec.SchemaProps{
									Type: []string{"string"}, // plain string
								},
							},
						},
					},
				},
			},
		},
	}

	// Service schema with x-kubernetes-int-or-string ports
	serviceGVK := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Service"}
	schemas[serviceGVK] = &spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type: []string{"object"},
			Properties: map[string]spec.Schema{
				"spec": {
					SchemaProps: spec.SchemaProps{
						Type: []string{"object"},
						Properties: map[string]spec.Schema{
							"ports": {
								SchemaProps: spec.SchemaProps{
									Type: []string{"array"},
									Items: &spec.SchemaOrArray{
										Schema: &spec.Schema{
											SchemaProps: spec.SchemaProps{
												Type: []string{"object"},
												Properties: map[string]spec.Schema{
													"port": {
														SchemaProps: spec.SchemaProps{
															Type: []string{"integer"},
														},
													},
													"targetPort": {
														SchemaProps: spec.SchemaProps{},
														VendorExtensible: spec.VendorExtensible{
															Extensions: map[string]interface{}{
																"x-kubernetes-int-or-string": true,
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
			},
		},
	}

	// Pod schema with timestamp and duration fields
	podGVK := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}
	schemas[podGVK] = &spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type: []string{"object"},
			Properties: map[string]spec.Schema{
				"metadata": {
					SchemaProps: spec.SchemaProps{
						Type: []string{"object"},
						Properties: map[string]spec.Schema{
							"creationTimestamp": {
								SchemaProps: spec.SchemaProps{
									Type:   []string{"string"},
									Format: "date-time", // RFC3339 → time.Time
								},
							},
						},
					},
				},
				"spec": {
					SchemaProps: spec.SchemaProps{
						Type: []string{"object"},
						Properties: map[string]spec.Schema{
							"activeDeadlineSeconds": {
								SchemaProps: spec.SchemaProps{
									Type:   []string{"string"},
									Format: "duration", // duration string → time.Duration
								},
							},
						},
					},
				},
			},
		},
	}

	// CustomResource with preserve-unknown-fields
	customGVK := schema.GroupVersionKind{Group: "custom.io", Version: "v1", Kind: "Custom"}
	schemas[customGVK] = &spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type: []string{"object"},
			Properties: map[string]spec.Schema{
				"spec": {
					SchemaProps: spec.SchemaProps{
						Type: []string{"object"},
					},
					VendorExtensible: spec.VendorExtensible{
						Extensions: map[string]interface{}{
							"x-kubernetes-preserve-unknown-fields": true,
						},
					},
				},
			},
		},
	}

	return schemas
}

func TestObjectConverter_ConvertSecret(t *testing.T) {
	converter := NewObjectConverter(&mockRESTMapper{}, createTestResolver())

	// Test Secret with base64-encoded string data
	secret := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]interface{}{
			"name": "test-secret",
		},
		"data": map[string]interface{}{
			"username": base64.StdEncoding.EncodeToString([]byte("admin")),
			"password": base64.StdEncoding.EncodeToString([]byte("secret123")),
		},
	}

	gvr := schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "secrets",
	}

	converted := converter.ConvertForGVR(secret, gvr)

	// Check that data fields are converted to byte arrays (raw base64 bytes, not decoded)
	// This matches the behavior where format:"byte" strings are converted to []byte without decoding
	data := converted["data"].(map[string]interface{})
	assert.IsType(t, []byte{}, data["username"])
	assert.IsType(t, []byte{}, data["password"])

	// The values should be the raw base64 strings as byte arrays, NOT decoded
	expectedUsername := base64.StdEncoding.EncodeToString([]byte("admin"))
	expectedPassword := base64.StdEncoding.EncodeToString([]byte("secret123"))
	assert.Equal(t, []byte(expectedUsername), data["username"])
	assert.Equal(t, []byte(expectedPassword), data["password"])
}

func TestObjectConverter_ConvertConfigMap(t *testing.T) {
	converter := NewObjectConverter(&mockRESTMapper{}, createTestResolver())

	// Test ConfigMap with string data (should remain as strings)
	configMap := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]interface{}{
			"name": "test-config",
		},
		"data": map[string]interface{}{
			"config.yaml": "key: value\nother: data",
			"app.conf":    "setting=123",
		},
	}

	gvr := schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "configmaps",
	}

	converted := converter.ConvertForGVR(configMap, gvr)

	// Check that data fields remain as strings
	// This follows the existing pattern where no format means plain string type
	data := converted["data"].(map[string]interface{})
	assert.IsType(t, "string", data["config.yaml"])
	assert.IsType(t, "string", data["app.conf"])
	assert.Equal(t, "key: value\nother: data", data["config.yaml"])
	assert.Equal(t, "setting=123", data["app.conf"])
}

func TestObjectConverter_NilInputs(t *testing.T) {
	converter := NewObjectConverter(&mockRESTMapper{}, createTestResolver())

	gvr := schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "secrets",
	}

	// Test with nil converter (should handle gracefully)
	var nilConverter *ObjectConverter
	result := nilConverter.ConvertForGVR(map[string]interface{}{"test": "value"}, gvr)
	assert.Equal(t, map[string]interface{}{"test": "value"}, result)

	// Test with nil object
	result = converter.ConvertForGVR(nil, gvr)
	assert.Nil(t, result)

	// Test with nil restMapper
	converterNoMapper := NewObjectConverter(nil, createTestResolver())
	result = converterNoMapper.ConvertForGVR(map[string]interface{}{"test": "value"}, gvr)
	assert.Equal(t, map[string]interface{}{"test": "value"}, result)

	// Test with nil resolver
	converterNoResolver := NewObjectConverter(&mockRESTMapper{}, nil)
	result = converterNoResolver.ConvertForGVR(map[string]interface{}{"test": "value"}, gvr)
	assert.Equal(t, map[string]interface{}{"test": "value"}, result)
}

func TestObjectConverter_InvalidBase64(t *testing.T) {
	converter := NewObjectConverter(&mockRESTMapper{}, createTestResolver())

	// Test Secret with invalid base64 data
	secret := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]interface{}{
			"name": "test-secret",
		},
		"data": map[string]interface{}{
			"invalid": "not-valid-base64!@#",
		},
	}

	gvr := schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "secrets",
	}

	converted := converter.ConvertForGVR(secret, gvr)
	data := converted["data"].(map[string]interface{})

	// The string is simply converted to bytes without base64 validation
	// This is the expected behavior when format:"byte" is specified
	assert.IsType(t, []byte{}, data["invalid"])
	assert.Equal(t, []byte("not-valid-base64!@#"), data["invalid"])
}

func TestObjectConverter_UnknownResource(t *testing.T) {
	converter := NewObjectConverter(&mockRESTMapper{}, createTestResolver())

	// Test with unknown resource type (should return shallow copied but unchanged)
	obj := map[string]interface{}{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]interface{}{
			"name": "test-deployment",
		},
		"spec": map[string]interface{}{
			"replicas": 3,
		},
	}

	gvr := schema.GroupVersionResource{
		Group:    "apps",
		Version:  "v1",
		Resource: "deployments",
	}

	converted := converter.ConvertForGVR(obj, gvr)

	// Should return the object unchanged (but shallow copied)
	assert.Equal(t, obj, converted)

	// Verify it's a shallow copy (maps.Clone behavior) - nested maps are shared
	obj["spec"].(map[string]interface{})["replicas"] = 5
	assert.Equal(t, 5, converted["spec"].(map[string]interface{})["replicas"])

	// But the top level is copied
	obj["newField"] = "added"
	_, exists := converted["newField"]
	assert.False(t, exists)
}

// Additional test cases for all the new compatibility features

func TestObjectConverter_IntOrString(t *testing.T) {
	converter := NewObjectConverter(&mockRESTMapper{}, createTestResolver())

	// Test Service with x-kubernetes-int-or-string targetPort conversion
	service := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]interface{}{
			"name": "test-service",
		},
		"spec": map[string]interface{}{
			"ports": []interface{}{
				map[string]interface{}{
					"port":       float64(8080), // JSON numbers come as float64
					"targetPort": float64(9000), // Should convert to int64 due to x-kubernetes-int-or-string
				},
				map[string]interface{}{
					"port":       float64(80),
					"targetPort": "http", // String value should remain as string
				},
			},
		},
	}

	gvr := schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "services",
	}

	converted := converter.ConvertForGVR(service, gvr)

	spec := converted["spec"].(map[string]interface{})
	ports := spec["ports"].([]interface{})

	// First port: both port and targetPort should be int64
	port1 := ports[0].(map[string]interface{})
	assert.IsType(t, int64(0), port1["port"])
	assert.IsType(t, int64(0), port1["targetPort"])
	assert.Equal(t, int64(8080), port1["port"])
	assert.Equal(t, int64(9000), port1["targetPort"])

	// Second port: port should be int64, targetPort should remain string
	port2 := ports[1].(map[string]interface{})
	assert.IsType(t, int64(0), port2["port"])
	assert.IsType(t, "string", port2["targetPort"])
	assert.Equal(t, int64(80), port2["port"])
	assert.Equal(t, "http", port2["targetPort"])
}

func TestObjectConverter_DateTimeFormats(t *testing.T) {
	converter := NewObjectConverter(&mockRESTMapper{}, createTestResolver())

	// Test Pod with timestamp and duration fields
	pod := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]interface{}{
			"name":              "test-pod",
			"creationTimestamp": "2024-01-15T10:30:00Z",
		},
		"spec": map[string]interface{}{
			"activeDeadlineSeconds": "30s",
		},
	}

	gvr := schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "pods",
	}

	converted := converter.ConvertForGVR(pod, gvr)

	// Check timestamp conversion
	metadata := converted["metadata"].(map[string]interface{})
	timestamp := metadata["creationTimestamp"]
	assert.IsType(t, time.Time{}, timestamp)
	expectedTime, _ := time.Parse(time.RFC3339, "2024-01-15T10:30:00Z")
	assert.Equal(t, expectedTime, timestamp)

	// Check duration conversion
	spec := converted["spec"].(map[string]interface{})
	duration := spec["activeDeadlineSeconds"]
	assert.IsType(t, time.Duration(0), duration)
	expectedDuration, _ := time.ParseDuration("30s")
	assert.Equal(t, expectedDuration, duration)
}

func TestObjectConverter_PreserveUnknownFields(t *testing.T) {
	converter := NewObjectConverter(&mockRESTMapper{}, createTestResolver())

	// Test Custom resource with x-kubernetes-preserve-unknown-fields
	custom := map[string]interface{}{
		"apiVersion": "custom.io/v1",
		"kind":       "Custom",
		"metadata": map[string]interface{}{
			"name": "test-custom",
		},
		"spec": map[string]interface{}{
			"knownField": "value",
			"unknownField": map[string]interface{}{
				"nested": "data",
				"number": float64(123),
			},
		},
	}

	gvr := schema.GroupVersionResource{
		Group:    "custom.io",
		Version:  "v1",
		Resource: "customs",
	}

	converted := converter.ConvertForGVR(custom, gvr)

	// Check that spec is preserved as-is due to x-kubernetes-preserve-unknown-fields
	spec := converted["spec"].(map[string]interface{})
	assert.Equal(t, "value", spec["knownField"])

	// Unknown fields should be preserved exactly
	unknownField := spec["unknownField"].(map[string]interface{})
	assert.Equal(t, "data", unknownField["nested"])
	assert.Equal(t, float64(123), unknownField["number"]) // Should remain as float64, not converted
}

func TestObjectConverter_InvalidDateTime(t *testing.T) {
	converter := NewObjectConverter(&mockRESTMapper{}, createTestResolver())

	// Test Pod with invalid timestamp (should remain as string)
	pod := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]interface{}{
			"name":              "test-pod",
			"creationTimestamp": "invalid-timestamp",
		},
		"spec": map[string]interface{}{
			"activeDeadlineSeconds": "invalid-duration",
		},
	}

	gvr := schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "pods",
	}

	converted := converter.ConvertForGVR(pod, gvr)

	// Invalid formats should remain as strings
	metadata := converted["metadata"].(map[string]interface{})
	assert.IsType(t, "string", metadata["creationTimestamp"])
	assert.Equal(t, "invalid-timestamp", metadata["creationTimestamp"])

	spec := converted["spec"].(map[string]interface{})
	assert.IsType(t, "string", spec["activeDeadlineSeconds"])
	assert.Equal(t, "invalid-duration", spec["activeDeadlineSeconds"])
}
