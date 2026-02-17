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
	"maps"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/cel/openapi/resolver"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// ObjectConverter converts unstructured objects to match their OpenAPI schema types.
// This fixes type mismatches like Secret data fields being base64 strings instead of bytes.
type ObjectConverter struct {
	restMapper     meta.RESTMapper
	schemaResolver resolver.SchemaResolver
}

// NewObjectConverter creates a new converter with the given mapper and resolver.
func NewObjectConverter(mapper meta.RESTMapper, resolver resolver.SchemaResolver) *ObjectConverter {
	return &ObjectConverter{
		restMapper:     mapper,
		schemaResolver: resolver,
	}
}

// ConvertForGVR converts an unstructured object to match its OpenAPI schema types.
func (c *ObjectConverter) ConvertForGVR(
	obj map[string]interface{},
	gvr schema.GroupVersionResource,
) map[string]interface{} {
	if c == nil || obj == nil {
		return obj
	}

	if c.restMapper == nil || c.schemaResolver == nil {
		return obj
	}

	// Convert GVR to GVK using RESTMapper
	gvk, err := c.restMapper.KindFor(gvr)
	if err != nil {
		// If we can't get the kind, return a shallow copy to avoid modifications
		return maps.Clone(obj)
	}

	// Get the OpenAPI schema
	schema, err := c.schemaResolver.ResolveSchema(gvk)
	if err != nil || schema == nil {
		// No schema available, return a shallow copy to avoid modifications
		return maps.Clone(obj)
	}

	// Clone to avoid modifying original and apply conversions
	result := maps.Clone(obj)
	c.convertObject(result, schema)
	return result
}

// hasKubernetesExtension checks if a schema has a specific Kubernetes extension enabled.
func hasKubernetesExtension(schema *spec.Schema, extension string) bool {
	if schema == nil {
		return false
	}
	// Check VendorExtensible.Extensions (the standard location)
	if schema.Extensions != nil {
		if val, exists := schema.Extensions[extension]; exists {
			if boolVal, ok := val.(bool); ok && boolVal {
				return true
			}
		}
	}
	return false
}

// convertObject recursively converts object properties according to the schema.
func (c *ObjectConverter) convertObject(obj map[string]interface{}, schema *spec.Schema) {
	if schema == nil {
		return
	}

	if schema.Properties != nil {
		for propName, propSchema := range schema.Properties {
			if val, exists := obj[propName]; exists {
				obj[propName] = c.convertValue(val, &propSchema)
			}
		}
	}
}

// convertValue converts a single value according to its schema type.
func (c *ObjectConverter) convertValue(val interface{}, schema *spec.Schema) interface{} {
	if schema == nil {
		return val
	}

	// Check for x-kubernetes-int-or-string extension first (can be on any schema type)
	isIntOrString := hasKubernetesExtension(schema, "x-kubernetes-int-or-string")
	if isIntOrString {
		// Handle int-or-string: convert float64 to int64 if whole number, preserve strings
		if f, ok := val.(float64); ok && f == float64(int64(f)) {
			return int64(f)
		}
		// Keep as string if not a whole number or already string/other type
		return val
	}

	// If schema has no type, return as-is
	if len(schema.Type) == 0 {
		return val
	}

	switch schema.Type[0] {
	case "object":
		if mapVal, ok := val.(map[string]interface{}); ok {
			// Check for x-kubernetes-preserve-unknown-fields extension
			if hasKubernetesExtension(schema, "x-kubernetes-preserve-unknown-fields") {
				// Return as-is to preserve all unknown fields
				return mapVal
			}

			if schema.AdditionalProperties != nil && schema.AdditionalProperties.Schema != nil {
				result := maps.Clone(mapVal)
				for key, mapItemVal := range result {
					result[key] = c.convertValue(mapItemVal, schema.AdditionalProperties.Schema)
				}
				return result
			}
			if schema.Properties != nil {
				result := maps.Clone(mapVal)
				c.convertObject(result, schema)
				return result
			}
		}
		return val

	case "string":
		if str, ok := val.(string); ok {
			switch schema.Format {
			case "byte", "binary":
				// Handle base64-encoded bytes
				if decoded, err := base64.StdEncoding.DecodeString(str); err == nil {
					return decoded
				}
				return []byte(str)
			case "date":
				// Handle RFC3339 date format (YYYY-MM-DD)
				if t, err := time.Parse("2006-01-02", str); err == nil {
					return t
				}
				return val
			case "date-time":
				// Handle RFC3339 date-time format
				if t, err := time.Parse(time.RFC3339, str); err == nil {
					return t
				}
				// Try parsing without nanoseconds
				if t, err := time.Parse(time.RFC3339Nano, str); err == nil {
					return t
				}
				return val
			case "duration":
				// Handle Go duration strings (e.g., "30s", "5m", "1h30m")
				if d, err := time.ParseDuration(str); err == nil {
					return d
				}
				return val
			}
		}
		return val

	case "integer":
		// Convert JSON float64 to int64 for proper integer types
		if f, ok := val.(float64); ok && f == float64(int64(f)) {
			return int64(f)
		}
		return val
	case "array":
		// Handle arrays with schema-based element conversion
		if arr, ok := val.([]interface{}); ok && schema.Items != nil && schema.Items.Schema != nil {
			result := make([]interface{}, len(arr))
			for i, item := range arr {
				result[i] = c.convertValue(item, schema.Items.Schema)
			}
			return result
		}
		return val

	default:
		return val
	}
}
