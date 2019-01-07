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

	pelotoncassandra "github.com/uber/peloton/storage/cassandra"
	"github.com/uber/peloton/storage/cassandra/impl"
	"github.com/uber/peloton/storage/objects/base"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// C* config
var config *pelotoncassandra.Config

// C* connector
var connector *cassandraConnector

// test table name to be created for this test
var testTableName string

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
	config = &pelotoncassandra.Config{
		CassandraConn: &impl.CassandraConn{
			ContactPoints: []string{"127.0.0.1"},
			Port:          9043,
			CQLVersion:    "3.4.2",
			MaxGoRoutines: 1000,
		},
		StoreName: "peloton_test",
	}

	session, err := impl.CreateStoreSession(
		config.CassandraConn, config.StoreName)
	if err != nil {
		log.Fatal(err)
	}

	testTableName = fmt.Sprintf("test_table_%d", rand.Intn(100))

	// create a test table
	table := fmt.Sprintf("CREATE TABLE peloton_test.%s"+
		" (id int, name text, data text, PRIMARY KEY (id))", testTableName)

	if err := session.Query(table).Exec(); err != nil {
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
		Name: testTableName,
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
	suite.Equal(err, gocql.ErrNotFound)

	// delete this row again from C*. It is a noop for C*
	// this should not result in error.
	err = connector.Delete(context.Background(), obj, keyRow)
	suite.NoError(err)
}

func (suite *CassandraConnSuite) TestCreateUpdateGet() {
	// Definition stores schema information about an Object
	obj := &base.Definition{
		Name: testTableName,
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
