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
	apiservercel "k8s.io/apiserver/pkg/cel"
	celopenapi "k8s.io/apiserver/pkg/cel/openapi"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// SchemaCache memoizes SchemaDeclTypeWithMetadata results by pointer identity.
// When the same *spec.Schema pointer is converted multiple times (e.g., multiple
// resources sharing the same K8s type from the schema resolver), the expensive
// recursive conversion is performed only once.
//
// Not thread-safe — designed for use within a single build operation.
type SchemaCache struct {
	cache map[*spec.Schema]*apiservercel.DeclType
}

// NewSchemaCache creates a new schema conversion cache.
func NewSchemaCache() *SchemaCache {
	return &SchemaCache{cache: make(map[*spec.Schema]*apiservercel.DeclType)}
}

// Convert converts a *spec.Schema to a *DeclType, returning a cached result
// if the same pointer has been converted before. Nil results are also cached
// to avoid re-converting schemas that produce no DeclType.
func (c *SchemaCache) Convert(schema *spec.Schema, isResourceRoot bool) *apiservercel.DeclType {
	if schema == nil {
		return nil
	}
	if c != nil {
		if cached, ok := c.cache[schema]; ok {
			return cached
		}
	}
	result := SchemaDeclTypeWithMetadata(&celopenapi.Schema{Schema: schema}, isResourceRoot)
	if c != nil {
		c.cache[schema] = result
	}
	return result
}
