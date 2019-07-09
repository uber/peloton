// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package base

import (
	"reflect"
)

// OptionalString type can be used for primary key of type string
// to be evaluated as either nil or some string value
// different than empty string
type OptionalString struct {
	Value string
}

// OptionalUInt64 type can be used for primary key of type uint64
// to be evaluated as either nil or some uint64 value
// different than empty uint64
type OptionalUInt64 struct {
	Value uint64
}

// NewOptionalString returns either new *OptionalString or nil
func NewOptionalString(v interface{}) *OptionalString {
	s, ok := v.(string)
	if ok && len(s) > 0 {
		return &OptionalString{Value: s}
	}
	return nil
}

// String for *OptionalString type
func (s *OptionalString) String() string {
	return s.Value
}

// NewOptionalUInt64 returns either new *OptionalUInt64 or nil
func NewOptionalUInt64(v interface{}) *OptionalUInt64 {
	s, ok := v.(uint64)
	if ok {
		return &OptionalUInt64{Value: s}
	}
	return nil
}

// UInt64 for *OptionalUInt64 type
func (s *OptionalUInt64) UInt64() uint64 {
	return s.Value
}

// IsOfTypeOptional returns whether a value is of type custom optional
// Currently only support OptionalString, OptionalUInt64 type,
// but can be extended for additional optional types
func IsOfTypeOptional(value reflect.Value) bool {
	switch value.Type() {
	case reflect.TypeOf(&OptionalString{}):
		return true
	case reflect.TypeOf(&OptionalUInt64{}):
		return true
	default:
		return false
	}
}

// ConvertFromOptionalToRawType returns an interface of raw type
// understandable by the DB layer, extracted from a custom
// optional type
// Currently only support OptionalString, OptionalUInt64 type,
// but can be extended for additional optional types
func ConvertFromOptionalToRawType(value reflect.Value) interface{} {
	q := value.Type()
	switch q {
	case reflect.TypeOf(&OptionalString{}):
		return value.Interface().(*OptionalString).String()
	case reflect.TypeOf(&OptionalUInt64{}):
		return value.Interface().(*OptionalUInt64).UInt64()
	default:
		return value
	}
}

// ConvertFromRawToOptionalType returns a value representing an
// optional type built from the raw type fetched from DB
func ConvertFromRawToOptionalType(value reflect.Value,
	typ reflect.Type) reflect.Value {
	switch typ {
	case reflect.TypeOf(&OptionalString{}):
		return reflect.ValueOf(
			&OptionalString{
				Value: reflect.Indirect(value).String(),
			})
	case reflect.TypeOf(&OptionalUInt64{}):
		return reflect.ValueOf(
			&OptionalUInt64{
				Value: uint64(reflect.Indirect(value).Int()),
			})
	default:
		return value
	}

}
