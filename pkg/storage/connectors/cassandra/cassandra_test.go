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

package cassandra

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/uber/peloton/pkg/storage/objects/base"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

// C* connector
var connector *cassandraConnector

// test table name to be created for this test
var testTableName1 string
var testTableName2 string

// testRow in DB representation looks like this:
//
// 	*	id	| 	 name	|  	 data
//  1.   1  	"test"	   "testdata"
var testRow = []base.Column{
	{
		Name:  "id",
		Value: uint64(1),
	},
	{
		Name:  "name",
		Value: "test",
	},
	{
		Name:  "data",
		Value: "testdata",
	},
}

// testRowWithCK in DB representation looks like this:
// here, primary key is "id" and clustering key is "ck"
// 	*	id	| 	 ck	   | 	name	|  	 data
//  1.   1  	 20	       "test"      "testdata20"
//  2.   1  	 10	       "test"      "testdata10"
var testRowsWithCK = [][]base.Column{
	{
		{
			Name:  "id",
			Value: uint64(1),
		},
		{
			Name:  "ck",
			Value: uint64(10),
		},
		{
			Name:  "name",
			Value: "test",
		},
		{
			Name:  "data",
			Value: "testdata20",
		},
	},
	{
		{
			Name:  "id",
			Value: uint64(1),
		},
		{
			Name:  "ck",
			Value: uint64(20),
		},
		{
			Name:  "name",
			Value: "test",
		},
		{
			Name:  "data",
			Value: "testdata10",
		},
	},
}

// keyRow defines primary key column list for test table
var keyRow = []base.Column{
	{
		Name:  "id",
		Value: uint64(1),
	},
}

// Initialize C* session and create a test table
func init() {
	rand.Seed(time.Now().UnixNano())
	config := &Config{
		CassandraConn: &CassandraConn{
			ContactPoints: []string{"127.0.0.1"},
			Port:          9043,
			CQLVersion:    "3.4.2",
			MaxGoRoutines: 1000,
		},
		StoreName: "peloton_test",
	}

	session, err := CreateStoreSession(
		config.CassandraConn, config.StoreName)
	if err != nil {
		log.Fatal(err)
	}

	testTableName1 = fmt.Sprintf("test_table_%d", rand.Intn(1000))
	testTableName2 = fmt.Sprintf("test_table_%d", rand.Intn(1000))

	// create a test table
	table1 := fmt.Sprintf("CREATE TABLE peloton_test.%s"+
		" (id int, name text, data text, PRIMARY KEY (id))", testTableName1)

	if err := session.Query(table1).Exec(); err != nil {
		log.Fatal(err)
	}

	// create a test table with partition key "id" and clustering key "ck"
	table2 := fmt.Sprintf("CREATE TABLE peloton_test.%s"+
		" (id int, ck int, name text, data text, PRIMARY KEY ((id), ck))",
		testTableName2)

	if err := session.Query(table2).Exec(); err != nil {
		log.Fatal(err)
	}

	testScope := tally.NewTestScope("", map[string]string{})
	conn, err := NewCassandraConnector(config, testScope)
	if err != nil {
		log.Fatal(err)
	}

	connector = conn.(*cassandraConnector)
}

// TestCreateGetDelete creates a row in the test table and reads it back
// Then it deletes it and verifies that row was deleted
func (suite *CassandraConnSuite) TestCreateGetDelete() {
	// Definition stores schema information about an Object
	obj := &base.Definition{
		Name: testTableName1,
		Key: &base.PrimaryKey{
			PartitionKeys: []string{"id"},
		},
		// Column name to data type mapping of the object
		ColumnToType: map[string]reflect.Type{
			"id":   reflect.TypeOf(1),
			"data": reflect.TypeOf("data"),
			"name": reflect.TypeOf("name"),
		},
	}
	// create the test row in C*
	err := connector.Create(context.Background(), obj, testRow)
	suite.NoError(err)

	// read the row from C* test table for given keys
	row, err := connector.Get(context.Background(), obj, keyRow)
	suite.NoError(err)
	suite.Len(row, 3)

	// delete the test row from C*
	err = connector.Delete(context.Background(), obj, keyRow)
	suite.NoError(err)

	// read the row from C* test table for given keys
	row, err = connector.Get(context.Background(), obj, keyRow)
	suite.Error(err)
	suite.True(yarpcerrors.IsNotFound(err))

	// delete this row again from C*. It is a noop for C*
	// this should not result in error.
	err = connector.Delete(context.Background(), obj, keyRow)
	suite.NoError(err)
}

func (suite *CassandraConnSuite) TestCreateUpdateGet() {
	// Definition stores schema information about an Object
	obj := &base.Definition{
		Name: testTableName1,
		Key: &base.PrimaryKey{
			PartitionKeys: []string{"id"},
		},
		// Column name to data type mapping of the object
		ColumnToType: map[string]reflect.Type{
			"id":   reflect.TypeOf(1),
			"data": reflect.TypeOf("data"),
			"name": reflect.TypeOf("name"),
		},
	}
	// create the test row in C*
	err := connector.Create(context.Background(), obj, testRow)
	suite.NoError(err)

	// update the test row in C* where we update only the "Name" field and not
	// "Data" field of the object
	// testUpdateRow just updates the Name field
	testUpdateRow := []base.Column{
		{
			Name:  "name",
			Value: "test-update",
		},
	}
	err = connector.Update(context.Background(), obj, testUpdateRow, keyRow)
	suite.NoError(err)

	// read the row from C* test table for given keys
	row, err := connector.Get(context.Background(), obj, keyRow)
	suite.NoError(err)
	suite.Len(row, 3)
	for _, col := range row {
		if col.Name == "name" {
			name := col.Value.(*string)
			suite.Equal("test-update", *name)
		}
	}

	// testUpdateRowWithPrimaryKey used for update operation, this should fail
	// because you cannot use SET with primary key
	testUpdateRowWithPrimaryKey := []base.Column{
		{
			Name:  "id",
			Value: uint64(1),
		},
		{
			Name:  "name",
			Value: "test-update",
		},
	}
	err = connector.Update(
		context.Background(), obj, testUpdateRowWithPrimaryKey, keyRow)
	suite.Error(err)
	suite.Equal(err.Error(), "PRIMARY KEY part id found in SET part")
}

// TestCreateGetAll tests the GetAll operation
func (suite *CassandraConnSuite) TestCreateGetAll() {
	// Definition stores schema information about an Object
	obj := &base.Definition{
		Name: testTableName2,
		Key: &base.PrimaryKey{
			PartitionKeys: []string{"id"},
			ClusteringKeys: []*base.ClusteringKey{
				{
					Name:       "ck",
					Descending: true,
				},
			},
		},
		// Column name to data type mapping of the object
		ColumnToType: map[string]reflect.Type{
			"id":   reflect.TypeOf(1),
			"ck":   reflect.TypeOf(1),
			"data": reflect.TypeOf("data"),
			"name": reflect.TypeOf("name"),
		},
	}

	// create the test rows in C*
	for _, row := range testRowsWithCK {
		err := connector.Create(context.Background(), obj, row)
		suite.NoError(err)
	}

	// read the row from C* test table for given keys
	rows, err := connector.GetAll(context.Background(), obj, keyRow)
	suite.NoError(err)
	suite.Len(rows, 2)

	for _, row := range rows {
		for _, col := range row {
			if col.Name == "ck" {
				ck := col.Value.(*int)
				suite.True(*ck == 10 || *ck == 20)
			} else if col.Name == "name" {
				testStr := "test"
				suite.Equal(col.Value, &testStr)
			} else if col.Name == "data" {
				data := col.Value.(*string)
				suite.True(*data == "testdata20" || *data == "testdata10")
			} else {
				id := col.Value.(*int)
				suite.Equal(*id, 1)
			}
		}
	}
}

// TestCreateGetAllIter tests the GetAllIter operation
func (suite *CassandraConnSuite) TestCreateGetAllIter() {
	// Definition stores schema information about an Object
	obj := &base.Definition{
		Name: testTableName2,
		Key: &base.PrimaryKey{
			PartitionKeys: []string{"id"},
			ClusteringKeys: []*base.ClusteringKey{
				{
					Name:       "ck",
					Descending: true,
				},
			},
		},
		// Column name to data type mapping of the object
		ColumnToType: map[string]reflect.Type{
			"id":   reflect.TypeOf(1),
			"ck":   reflect.TypeOf(1),
			"data": reflect.TypeOf("data"),
			"name": reflect.TypeOf("name"),
		},
	}

	// create the test rows in C*
	for _, row := range testRowsWithCK {
		err := connector.Create(context.Background(), obj, row)
		suite.NoError(err)
	}

	// read the row from C* test table for given keys using iterator
	iter, err := connector.GetAllIter(context.Background(), obj, keyRow)
	suite.NoError(err)

	numRows := 0
	for {
		row, err := iter.Next()
		suite.NoError(err)

		if row == nil {
			iter.Close()
			suite.Equal(2, numRows)
			break
		}
		numRows++
		for _, col := range row {
			if col.Name == "ck" {
				ck := col.Value.(*int)
				suite.True(*ck == 10 || *ck == 20)
			} else if col.Name == "name" {
				testStr := "test"
				suite.Equal(col.Value, &testStr)
			} else if col.Name == "data" {
				data := col.Value.(*string)
				suite.True(*data == "testdata20" || *data == "testdata10")
			} else {
				id := col.Value.(*int)
				suite.Equal(*id, 1)
			}
		}
	}
}

// TestCreateIfNotExists tests the CreateIfNotExists operation
func (suite *CassandraConnSuite) TestCreateIfNotExists() {
	// Definition stores schema information about an Object
	obj := &base.Definition{
		Name: testTableName1,
		Key: &base.PrimaryKey{
			PartitionKeys: []string{"id"},
		},
		// Column name to data type mapping of the object
		ColumnToType: map[string]reflect.Type{
			"id":   reflect.TypeOf(1),
			"data": reflect.TypeOf("data"),
			"name": reflect.TypeOf("name"),
		},
	}
	// create the test row in C* if it doesn't already exist using CAS.
	err := connector.CreateIfNotExists(context.Background(), obj, testRow)
	suite.NoError(err)

	// Create the same test row in C* with CAS. This should fail.
	err = connector.CreateIfNotExists(context.Background(), obj, testRow)
	suite.True(yarpcerrors.IsAlreadyExists(err))
}

// TestCreateDBFailures tests failures executing DB query
func (suite *CassandraConnSuite) TestDBFailures() {
	// Definition stores schema information about an Object
	obj := &base.Definition{
		Name: "table_does_not_exist",
		Key: &base.PrimaryKey{
			PartitionKeys: []string{"id"},
		},
		ColumnToType: map[string]reflect.Type{
			"id": reflect.TypeOf(1),
		},
	}

	ctx := context.Background()
	// create the test row using wrong table name
	err := connector.Create(ctx, obj, testRow)
	suite.Error(err)

	// create the test row using wrong table name for CAS write
	err = connector.CreateIfNotExists(ctx, obj, testRow)
	suite.Error(err)

	// get using wrong table name
	_, err = connector.Get(ctx, obj, keyRow)
	suite.Error(err)

	// update using wrong table name
	err = connector.Update(ctx, obj, testRow, keyRow)
	suite.Error(err)

	// delete using wrong table name
	err = connector.Delete(ctx, obj, keyRow)
	suite.Error(err)
}
