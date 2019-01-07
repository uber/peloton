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

package cached

import (
	"reflect"

	"go.uber.org/yarpc/yarpcerrors"
)

// patch patches diff onto entity inplace.
// entity is expected to be a pointer.
// diff is a kv map, which key is the field name to be updated,
// and v is the new value.
// It is the caller's responsibility to make sure the key in diff has
// a matching field in entity and the value is of correct type. Otherwise,
// the function panic.
func patch(entity interface{}, diff map[string]interface{}) error {
	entityType := reflect.TypeOf(entity)
	entityValue := reflect.ValueOf(entity)
	entityIndirectValue := reflect.Indirect(entityValue)

	if entityType.Kind() != reflect.Ptr {
		return yarpcerrors.InvalidArgumentErrorf("patch expects input of type Ptr")
	}

	if entityValue.IsNil() {
		return yarpcerrors.InvalidArgumentErrorf("patch expects non-nil entity")
	}

	for field, value := range diff {
		fieldValue := entityIndirectValue.FieldByName(field)
		// check if the field exists in entity
		if !fieldValue.IsValid() {
			return yarpcerrors.InvalidArgumentErrorf("field: %s does not exist", field)
		}

		if value == nil {
			// diff unsets the value
			fieldValue.Set(reflect.Zero(fieldValue.Type()))
		} else if reflect.TypeOf(value).AssignableTo(fieldValue.Type()) {
			// diff patches the value
			fieldValue.Set(reflect.ValueOf(value))
		} else {
			return yarpcerrors.InvalidArgumentErrorf(
				"field: %s in diff cannot be assigned to entity", field)
		}
	}
	return nil
}
