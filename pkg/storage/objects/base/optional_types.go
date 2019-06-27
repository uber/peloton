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

// IsOfTypeOptional returns whether a value is of type custom optional
// Currently only support OptionalString type,
// but can be extended for additional optional types
func IsOfTypeOptional(value reflect.Value) bool {
	switch value.Type() {
	case reflect.TypeOf(&OptionalString{}):
		return true
	default:
		return false
	}
}

// ConvertFromOptionalToRawType returns an interface of raw type
// understandable by the DB layer, extracted from a custom
// optional type
// Currently only support OptionalString type,
// but can be extended for additional optional types
func ConvertFromOptionalToRawType(value reflect.Value) interface{} {
	return value.Interface().(*OptionalString).String()
}

// ConvertFromRawToOptionalType returns a value representing an
// optional type built from the raw type fetched from DB
func ConvertFromRawToOptionalType(value reflect.Value) reflect.Value {
	return reflect.ValueOf(
		&OptionalString{
			Value: reflect.Indirect(value).String(),
		})
}
