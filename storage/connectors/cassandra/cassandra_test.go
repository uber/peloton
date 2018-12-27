package cassandra

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"time"

	pelotoncassandra "code.uber.internal/infra/peloton/storage/cassandra"
	"code.uber.internal/infra/peloton/storage/cassandra/impl"
	"code.uber.internal/infra/peloton/storage/objects/base"

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
}
