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

package library

import (
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// Collections returns a CEL library that provides collection manipulation functions.
//
// Library functions:
//
// toMap(list) returns a function that can be called with key/value extraction logic.
// Due to CEL limitations with lambdas, this provides utility functions instead:
//
// - toMapByField(list, keyField, valueField): creates map using object fields
// - toMapByKey(list, keyField): creates map keyed by field, value is whole object
// - toSet(list): converts list to set (map with true values)
//
// Example usage:
//
//	toMapByField(items, "name", "port")  // {name1: port1, name2: port2, ...}
//	toMapByKey(items, "name")            // {name1: item1, name2: item2, ...}
//	toSet(names)                         // {name1: true, name2: true, ...}
func Collections() cel.EnvOption {
	return cel.Lib(&collectionsLibrary{})
}

type collectionsLibrary struct{}

func (l *collectionsLibrary) LibraryName() string {
	return "collections"
}

func (l *collectionsLibrary) CompileOptions() []cel.EnvOption {
	T := cel.TypeParamType("value")
	return []cel.EnvOption{
		cel.Function("toMapByField",
			cel.Overload("toMapByField_list_string_string",
				[]*cel.Type{cel.ListType(T), cel.StringType, cel.StringType},
				cel.MapType(cel.DynType, cel.DynType),
				cel.FunctionBinding(toMapByFieldImpl),
			),
		),
		cel.Function("toMapByKey",
			cel.Overload("toMapByKey_list_string",
				[]*cel.Type{cel.ListType(T), cel.StringType},
				cel.MapType(cel.DynType, T),
				cel.FunctionBinding(toMapByKeyImpl),
			),
		),
		cel.Function("toSet",
			cel.Overload("toSet_list",
				[]*cel.Type{cel.ListType(T)},
				cel.MapType(cel.DynType, cel.BoolType),
				cel.FunctionBinding(toSetImpl),
			),
		),
	}
}

func (l *collectionsLibrary) ProgramOptions() []cel.ProgramOption {
	return nil
}

func toMapByFieldImpl(vals ...ref.Val) ref.Val {
	if len(vals) != 3 {
		return types.NewErr("toMapByField expects exactly 3 arguments")
	}

	listVal := vals[0]
	keyField := vals[1]
	valueField := vals[2]

	if listVal.Type() != types.ListType {
		return types.NewErr("toMapByField first argument must be a list")
	}

	if keyField.Type() != types.StringType {
		return types.NewErr("toMapByField keyField must be a string")
	}

	if valueField.Type() != types.StringType {
		return types.NewErr("toMapByField valueField must be a string")
	}

	list := listVal.(traits.Lister)
	keyFieldStr := string(keyField.(types.String))
	valueFieldStr := string(valueField.(types.String))
	result := make(map[ref.Val]ref.Val)

	iterator := list.Iterator()
	for iterator.HasNext() == types.True {
		item := iterator.Next()

		if item.Type() != types.MapType {
			return types.NewErr("toMapByField requires list of objects")
		}

		itemMap := item.(traits.Mapper)

		keyVal := itemMap.Get(types.String(keyFieldStr))
		if types.IsError(keyVal) {
			continue // skip items without the key field
		}

		valueVal := itemMap.Get(types.String(valueFieldStr))
		if types.IsError(valueVal) {
			continue // skip items without the value field
		}

		result[keyVal] = valueVal
	}

	return types.NewRefValMap(types.DefaultTypeAdapter, result)
}

func toMapByKeyImpl(vals ...ref.Val) ref.Val {
	if len(vals) != 2 {
		return types.NewErr("toMapByKey expects exactly 2 arguments")
	}

	listVal := vals[0]
	keyField := vals[1]

	if listVal.Type() != types.ListType {
		return types.NewErr("toMapByKey first argument must be a list")
	}

	if keyField.Type() != types.StringType {
		return types.NewErr("toMapByKey keyField must be a string")
	}

	list := listVal.(traits.Lister)
	keyFieldStr := string(keyField.(types.String))
	result := make(map[ref.Val]ref.Val)

	iterator := list.Iterator()
	for iterator.HasNext() == types.True {
		item := iterator.Next()

		if item.Type() != types.MapType {
			return types.NewErr("toMapByKey requires list of objects")
		}

		itemMap := item.(traits.Mapper)

		keyVal := itemMap.Get(types.String(keyFieldStr))
		if types.IsError(keyVal) {
			continue // skip items without the key field
		}

		result[keyVal] = item
	}

	return types.NewRefValMap(types.DefaultTypeAdapter, result)
}

func toSetImpl(vals ...ref.Val) ref.Val {
	if len(vals) != 1 {
		return types.NewErr("toSet expects exactly 1 argument")
	}

	listVal := vals[0]

	if listVal.Type() != types.ListType {
		return types.NewErr("toSet argument must be a list")
	}

	list := listVal.(traits.Lister)
	result := make(map[ref.Val]ref.Val)

	iterator := list.Iterator()
	for iterator.HasNext() == types.True {
		item := iterator.Next()
		result[item] = types.True
	}

	return types.NewRefValMap(types.DefaultTypeAdapter, result)
}
