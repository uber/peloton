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

package orm

import (
	"reflect"
	"strings"

	"github.com/uber/peloton/pkg/storage/objects/base"

	"go.uber.org/yarpc/yarpcerrors"
)

// Table is an ORM internal representation of storage object. Storage
// object is translated into Definition that contains the primary key
// information as well as column to datatype map
// It also contains maps used to translate storage object fields into DB columns
// and viceversa and this is used during read and write operations
type Table struct {
	base.Definition

	// map from DB column name -> object field name
	ColToField map[string]string

	// map of base field name to DB column name
	FieldToCol map[string]string
}

// GetKeyRowFromObject is a helper for generating a row of partition and
// clustering key column values to be used in a select query.
func (t *Table) GetKeyRowFromObject(
	e base.Object) []base.Column {
	v := reflect.ValueOf(e).Elem()
	row := []base.Column{}

	// populate partition key values
	for _, pk := range t.Key.PartitionKeys {
		fieldName := t.ColToField[pk]
		value := v.FieldByName(fieldName)

		// Special case for optional type:
		// conversion needed from custom optional type
		// into raw type understandable by DB layer
		if base.IsOfTypeOptional(value) {
			// nil value of type Optional should not be accounted for
			if !value.IsNil() {
				row = append(row, base.Column{
					Name:  pk,
					Value: base.ConvertFromOptionalToRawType(value),
				})
			}
			continue
		}

		row = append(row, base.Column{
			Name:  pk,
			Value: value.Interface(),
		})
	}

	// populate clustering key values
	for _, ck := range t.Key.ClusteringKeys {
		fieldName := t.ColToField[ck.Name]
		value := v.FieldByName(fieldName)

		// Special case for optional type:
		// conversion needed from custom optional type
		// into raw type understandable by DB layer
		if base.IsOfTypeOptional(value) {
			// nil value of type optional should not be accounted for
			if !value.IsNil() {
				row = append(row, base.Column{
					Name:  ck.Name,
					Value: base.ConvertFromOptionalToRawType(value),
				})
			}
			continue
		}

		row = append(row, base.Column{
			Name:  ck.Name,
			Value: value.Interface(),
		})
	}

	return row
}

// GetPartitionKeyRowFromObject is a helper for generating a row of partition
// key column values to be used in a GetAll query.
func (t *Table) GetPartitionKeyRowFromObject(
	e base.Object) []base.Column {
	v := reflect.ValueOf(e).Elem()
	row := []base.Column{}

	// populate partition key values
	for _, pk := range t.Key.PartitionKeys {
		fieldName := t.ColToField[pk]
		value := v.FieldByName(fieldName)
		// Special case for optional types: only account non-nil values
		// A value of custom optional type is either
		// nil or poiting to a non-nil non-zero custom type value
		if base.IsOfTypeOptional(value) {
			// nil value of type optional should not be accounted for
			if !value.IsNil() {
				row = append(row, base.Column{
					Name:  pk,
					Value: base.ConvertFromOptionalToRawType(value),
				})
			}
			continue
		}
		row = append(row, base.Column{
			Name:  pk,
			Value: value.Interface(),
		})
	}
	return row
}

// GetRowFromObject is a helper for generating a row from the storage object
// selectedFields will be used to restrict the number of columns in that row
// This will be used to convert only select fields of an object to a row.
// selectedFields if empty will be ignored. It will be empty in case this
// function is called when handling Create operation since in that case, all
// fields of the object must be converted to a row. Update can be used to
// update specific fields of the object
func (t *Table) GetRowFromObject(
	e base.Object,
	selectedFields ...string,
) []base.Column {
	v := reflect.ValueOf(e).Elem()
	row := []base.Column{}
	selectedFieldMap := make(map[string]struct{})

	// create a map of select fields that we are interested in. For example,
	// caller will tell us to convert only fields A, B, C from object to the
	// []base.Column but ignore field D.
	for _, field := range selectedFields {
		selectedFieldMap[field] = struct{}{}
	}
	for columnName, fieldName := range t.ColToField {
		if len(selectedFields) > 0 {
			if _, ok := selectedFieldMap[fieldName]; !ok {
				// field is not selected, skip it
				continue
			}
		}
		value := v.FieldByName(fieldName)

		// Special case for optional type:
		// conversion needed from custom optional string type
		// into raw type understandable by DB layer
		if base.IsOfTypeOptional(value) {
			// nil value of type optional should not be accounted for
			if !value.IsNil() {
				row = append(row, base.Column{
					Name:  columnName,
					Value: base.ConvertFromOptionalToRawType(value),
				})
			}
			continue
		}

		row = append(row, base.Column{
			Name:  columnName,
			Value: value.Interface(),
		})
	}
	return row
}

// SetObjectFromRow is a helper for populating storage object from the
// given row
func (t *Table) SetObjectFromRow(e base.Object, row []base.Column) {
	columnsMap := make(map[string]interface{})
	for _, column := range row {
		columnsMap[column.Name] = column.Value
	}

	r := reflect.ValueOf(e).Elem()

	for columnName, fieldName := range t.ColToField {
		columnValue := columnsMap[columnName]
		val := r.FieldByName(fieldName)
		var fv reflect.Value
		if columnValue != nil {
			fv = reflect.ValueOf(columnValue)
			if !fv.IsValid() || fv.Kind() == reflect.Ptr && fv.IsNil() {
				val.Set(reflect.Zero(val.Type()))
			} else if base.IsOfTypeOptional(val) {
				// Special case for custom optional type:
				// Build a new value representing an optional type
				// built from the raw type fetched from DB
				val.Set(base.ConvertFromRawToOptionalType(fv, val.Type()))
			} else {
				val.Set(reflect.Indirect(fv).Convert(val.Type()))
			}
		}
	}
}

// TableFromObject creates a orm.Table from a storage.Object
// instance.
func TableFromObject(e base.Object) (*Table, error) {
	var err error
	elem := reflect.TypeOf(e).Elem()

	t := &Table{
		ColToField: map[string]string{},
		FieldToCol: map[string]string{},
		Definition: base.Definition{
			ColumnToType: map[string]reflect.Type{},
		},
	}
	for i := 0; i < elem.NumField(); i++ {
		structField := elem.Field(i)
		name := structField.Name

		if name == objectName {
			// Extract Object tags which have all the connector key information
			tag := strings.TrimSpace(structField.Tag.Get(connectorTag))

			// Parse cassandra specific tag to extract table name and primary
			// key information
			if t.Definition.Name, t.Key, err =
				parseCassandraObjectTag(tag); err != nil {
				return nil, err
			}
		} else {
			// For all other fields of this object, parse the column name tag
			tag := strings.TrimSpace(structField.Tag.Get(columnTag))
			columnName, err := parseNameTag(tag)
			if err != nil {
				return nil, err
			}
			// Keep a column name to data type mapping which will be used when
			// allocating row memory for DB queries.
			t.ColumnToType[columnName] = structField.Type

			// Keep a column name to field name and viceversa mapping so that
			// it is easy to convert table to object and viceversa
			t.ColToField[columnName] = name
			t.FieldToCol[name] = columnName
		}
	}

	if t.Key == nil {
		return nil, yarpcerrors.InternalErrorf(
			"cannot find orm.Object in object %v", e)
	}

	return t, nil
}

// BuildObjectIndex builds an index to map storage object type to its
// Table representation
func BuildObjectIndex(objects []base.Object) (
	map[reflect.Type]*Table, error) {
	objectIndex := make(map[reflect.Type]*Table)
	for _, o := range objects {
		// Map each storage object to its internal Table representation used to
		// translate operations on each object to corresponding UQL statements
		table, err := TableFromObject(o)
		if err != nil {
			return nil, err
		}

		// use base type as internal lookup key
		typ := reflect.TypeOf(o).Elem()

		objectIndex[typ] = table
	}
	return objectIndex, nil
}
