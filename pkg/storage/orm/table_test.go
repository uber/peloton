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

package orm_test

import (
	"github.com/uber/peloton/pkg/storage/objects/base"
	"github.com/uber/peloton/pkg/storage/orm"
)

// ValidObject is a representation of the orm annotations
type ValidObject struct {
	base.Object `cassandra:"name=valid_object, primaryKey=((id), name)"`
	ID          uint64 `column:"name=id"`
	Name        string `column:"name=name"`
	Data        string `column:"name=data"`
}

// ValidObjectWithOptString is a representation of the orm annotations
// with a primary key of type optional string
type ValidObjectWithOptString struct {
	base.Object `cassandra:"name=valid_object_opt_string, primaryKey=((name))"`
	Name        *base.OptionalString `column:"name=name"`
	Data        string               `column:"name=data"`
}

// InvalidObject1 has primary key as empty
type InvalidObject1 struct {
	base.Object `cassandra:"name=valid_object, primaryKey=()"`
	ID          uint64 `column:"name=id"`
	Name        string `column:"name=name"`
}

// InvalidObject2 has invalid orm tag
type InvalidObject2 struct {
	base.Object `randomstring:"name=valid_object, primaryKey=((id), name)"`
	ID          uint64 `column:"name=id"`
	Name        string `column:"name=name"`
}

// InvalidObject3 has invalid orm tag on ID field
type InvalidObject3 struct {
	base.Object `cassandra:"name=valid_object, primaryKey=((id), name)"`
	ID          uint64 `randomstring:"name=id"`
	Name        string `column:"name=name"`
}

// TestTableFromObject tests creating orm.Table from given base object
// This is meant to test that only entities annotated in a certain format will
// be successfully converted to orm tables
func (suite *ORMTestSuite) TestTableFromObject() {
	_, err := orm.TableFromObject(&ValidObject{})
	suite.NoError(err)

	tt := []base.Object{
		&InvalidObject1{}, &InvalidObject2{}, &InvalidObject3{}}
	for _, t := range tt {
		_, err := orm.TableFromObject(t)
		suite.Error(err)
	}
}

// TestGetRowFromObject tests building a row (list of base.Column) from base
// object
func (suite *ORMTestSuite) TestGetRowFromObject() {
	e := &ValidObject{
		ID:   uint64(1),
		Name: "test",
		Data: "testdata",
	}
	table, err := orm.TableFromObject(e)
	suite.NoError(err)

	row := table.GetRowFromObject(e)
	suite.ensureRowsEqual(row, testRow)

	fieldsToUpdate := []string{"ID", "Name"}
	selectedFieldsRow := table.GetRowFromObject(e, fieldsToUpdate...)
	suite.ensureRowsEqual(selectedFieldsRow, keyRow)
}

// TestGetRowFromObjectWithOptString tests building a row (list of base.Column) from base
// object, with PK of type custom optional string
func (suite *ORMTestSuite) TestGetRowFromObjectWithOptString() {
	e := &ValidObjectWithOptString{
		Name: &base.OptionalString{Value: "testname"},
		Data: "testdata",
	}
	table, err := orm.TableFromObject(e)
	suite.NoError(err)

	row := table.GetRowFromObject(e)
	suite.ensureRowsEqual(
		row,
		[]base.Column{
			{
				Name:  "name",
				Value: "testname",
			},
			{
				Name:  "data",
				Value: "testdata",
			},
		},
	)
}

// TestGetKeyRowFromObject tests getting primary key row (list of primary key
// base.Column) from base object
func (suite *ORMTestSuite) TestGetKeyRowFromObject() {
	e := &ValidObject{
		ID:   uint64(1),
		Name: "test",
		Data: "junk",
	}
	table, err := orm.TableFromObject(e)
	suite.NoError(err)

	keyRow := table.GetKeyRowFromObject(e)
	suite.Equal(e.ID, keyRow[0].Value)
	suite.Equal(e.Name, keyRow[1].Value)
	suite.Equal(len(keyRow), 2)
}

// TestGetKeyRowFromObjectWithOptString tests getting primary key row (list of primary key
// base.Column) from base object, with PK of type custom optional string
func (suite *ORMTestSuite) TestGetKeyRowFromObjectWithOptString() {
	e := &ValidObjectWithOptString{
		Name: &base.OptionalString{Value: "testname"},
		Data: "testdata",
	}
	table, err := orm.TableFromObject(e)
	suite.NoError(err)

	keyRow := table.GetKeyRowFromObject(e)
	suite.Equal(e.Name.String(), keyRow[0].Value)
	suite.Len(keyRow, 1)
}

// TestGetPartitionKeyRowFromObject tests getting partition key row
// (list of primary key base.Column) from base object
func (suite *ORMTestSuite) TestGetPartitionKeyRowFromObject() {
	e := &ValidObject{
		ID:   uint64(1),
		Name: "test",
		Data: "junk",
	}
	table, err := orm.TableFromObject(e)
	suite.NoError(err)

	keyRow := table.GetPartitionKeyRowFromObject(e)
	suite.Equal(e.ID, keyRow[0].Value)
	suite.Equal(len(keyRow), 1)
}

// TestGetPartitionKeyRowFromObjectWithOptString tests getting partition key row
// (list of primary key base.Column) from base object
// with PK of type custom optional string
func (suite *ORMTestSuite) TestGetPartitionKeyRowFromObjectWithOptString() {
	// If optional string type value is not nil
	e := &ValidObjectWithOptString{
		Name: &base.OptionalString{Value: "testname"},
		Data: "testdata",
	}
	table, err := orm.TableFromObject(e)
	suite.NoError(err)

	keyRow := table.GetPartitionKeyRowFromObject(e)
	suite.Equal(e.Name.String(), keyRow[0].Value)
	suite.Len(keyRow, 1)

	// If optional string type value is nil
	e = &ValidObjectWithOptString{
		Name: nil,
		Data: "testdata",
	}
	table, err = orm.TableFromObject(e)
	suite.NoError(err)

	keyRow = table.GetPartitionKeyRowFromObject(e)
	suite.Len(keyRow, 0)
}
