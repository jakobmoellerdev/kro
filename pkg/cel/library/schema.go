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
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/gobuffalo/flect"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"

	"github.com/kubernetes-sigs/kro/pkg/cel/conversion"
	"github.com/kubernetes-sigs/kro/pkg/simpleschema"
)

// Schema returns a CEL library with helpers for building Kubernetes API
// machinery from data available in scope. These exist so that higher-level
// abstractions (an RGD-style controller) can be expressed as a Graph — see
// examples/graph/rgd.yaml and docs/design/proposals/graph.md.
//
// Library functions:
//
// plural(kind) lowercases a Kind and pluralizes it, producing the resource
// name a CRD uses. It matches exactly how kro's own CRD synthesis derives the
// plural (flect.Pluralize(strings.ToLower(kind)), see pkg/graph/crd/crd.go), so
// a Graph that stamps CRDs computes the same names the built-in path does.
//
//	plural(string) -> string
//
// Examples:
//
//	plural('WebApp')   == 'webapps'
//	plural('Gateway')  == 'gateways'
//	plural('Ingress')  == 'ingresses'
//
// simpleSchema.toOpenAPI(spec) converts a kro SimpleSchema field-type map (the
// shape authored under an RGD's spec.schema.spec) into an OpenAPI v3
// JSONSchemaProps object, returned as a CEL map suitable for embedding under a
// CRD's versions[].schema.openAPIV3Schema. A second argument supplies custom
// type definitions.
//
//	simpleSchema.toOpenAPI(map) -> map
//	simpleSchema.toOpenAPI(map, map) -> map
//
// Examples:
//
//	simpleSchema.toOpenAPI({'name': 'string', 'replicas': 'integer'})
//	simpleSchema.toOpenAPI(schema.spec.schema.spec, schema.spec.schema.types)
func Schema(options ...SchemaOption) cel.EnvOption {
	lib := &schemaLibrary{version: math.MaxUint32}
	for _, o := range options {
		lib = o(lib)
	}
	return cel.Lib(lib)
}

type schemaLibrary struct {
	version uint32
}

// SchemaOption is a functional option for configuring the schema library.
type SchemaOption func(*schemaLibrary) *schemaLibrary

// SchemaVersion configures the version of the schema library. The version
// limits which functions are available; functions introduced above the given
// version are excluded. If unset, all functions are available.
func SchemaVersion(version uint32) SchemaOption {
	return func(lib *schemaLibrary) *schemaLibrary {
		lib.version = version
		return lib
	}
}

func (l *schemaLibrary) LibraryName() string {
	return "kro.schema"
}

func (l *schemaLibrary) CompileOptions() []cel.EnvOption {
	return []cel.EnvOption{
		cel.Function("plural",
			cel.Overload("plural_string",
				[]*cel.Type{cel.StringType},
				cel.StringType,
				cel.UnaryBinding(pluralize),
			),
		),
		cel.Function("simpleSchema.toOpenAPI",
			cel.Overload("simpleschema_toopenapi_map",
				[]*cel.Type{cel.DynType},
				cel.DynType,
				cel.UnaryBinding(func(spec ref.Val) ref.Val {
					return toOpenAPI(spec, nil)
				}),
			),
			cel.Overload("simpleschema_toopenapi_map_map",
				[]*cel.Type{cel.DynType, cel.DynType},
				cel.DynType,
				cel.BinaryBinding(toOpenAPI),
			),
		),
	}
}

func (l *schemaLibrary) ProgramOptions() []cel.ProgramOption {
	return nil
}

func pluralize(kind ref.Val) ref.Val {
	native, err := kind.ConvertToNative(reflect.TypeFor[string]())
	if err != nil {
		return types.NewErr("plural argument must be a string")
	}
	str, ok := native.(string)
	if !ok {
		return types.NewErr("plural argument must be a string")
	}
	return types.String(flect.Pluralize(strings.ToLower(str)))
}

// toOpenAPI converts a SimpleSchema field-type map (spec) plus optional custom
// types into an OpenAPI JSONSchemaProps, returned as a CEL map. Non-map inputs
// or conversion failures surface as CEL errors.
func toOpenAPI(spec, customTypes ref.Val) ref.Val {
	specMap, err := refValToStringMap(spec)
	if err != nil {
		return types.NewErr("simpleSchema.toOpenAPI: spec %s", err.Error())
	}

	var typesMap map[string]any
	if customTypes != nil {
		if _, isNull := customTypes.(types.Null); !isNull {
			typesMap, err = refValToStringMap(customTypes)
			if err != nil {
				return types.NewErr("simpleSchema.toOpenAPI: types %s", err.Error())
			}
		}
	}

	openAPI, err := simpleschema.ToOpenAPISpec(specMap, typesMap)
	if err != nil {
		return types.NewErr("simpleSchema.toOpenAPI: %s", err.Error())
	}

	// Round-trip through JSON so the JSONSchemaProps becomes a plain
	// map[string]any that the CEL type adapter can represent, dropping empty
	// fields (JSONSchemaProps has omitempty on its fields).
	raw, err := json.Marshal(openAPI)
	if err != nil {
		return types.NewErr("simpleSchema.toOpenAPI: marshal result: %s", err.Error())
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return types.NewErr("simpleSchema.toOpenAPI: unmarshal result: %s", err.Error())
	}
	return types.DefaultTypeAdapter.NativeToValue(out)
}

// refValToStringMap converts a CEL value to a map[string]any via the shared
// GoNativeType helper, validating that the result is a string-keyed map.
func refValToStringMap(v ref.Val) (map[string]any, error) {
	native, err := conversion.GoNativeType(v)
	if err != nil {
		return nil, err
	}
	m, ok := native.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("argument must be a map, got %T", native)
	}
	return m, nil
}
