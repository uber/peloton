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
	"testing"

	"github.com/stretchr/testify/suite"
)

type CassandraConnSuite struct {
	suite.Suite
}

func (suite *CassandraConnSuite) SetupTest() {
}

func TestCassandraConnSuite(t *testing.T) {
	suite.Run(t, new(CassandraConnSuite))
}

// TestInsertStmt tests constructing insert CQL query
func (suite *CassandraConnSuite) TestInsertStmt() {
	data := []struct {
		table   string
		columns []string
		values  []interface{}
		cas     bool
		stmt    string
	}{
		{
			table:   "table1",
			columns: []string{"c1", "c2"},
			values:  []interface{}{"val1", "val2"},
			cas:     true,
			stmt: "INSERT INTO \"table1\" (\"c1\", \"c2\") VALUES (?, ?)" +
				" IF NOT EXISTS;",
		},
		{
			table:   "table2",
			columns: []string{"c1", "c2"},
			values:  []interface{}{1, 2},
			cas:     false,
			stmt:    "INSERT INTO \"table2\" (\"c1\", \"c2\") VALUES (?, ?);",
		},
	}
	for _, d := range data {
		stmt, err := InsertStmt(
			Table(d.table),
			Columns(d.columns),
			Values(d.values),
			IfNotExist(d.cas),
		)
		suite.NoError(err)
		suite.Equal(stmt, d.stmt)
	}
}

// TestSelectStmt tests constructing select CQL query
func (suite *CassandraConnSuite) TestSelectStmt() {

	data := []struct {
		table   string
		cols    []string
		keyCols []string
		stmt    string
		limit   int
	}{
		{
			table:   "table1",
			cols:    []string{"c1", "c2"},
			keyCols: []string{"c3", "c4"},
			stmt:    "SELECT \"c1\", \"c2\" FROM \"table1\" WHERE c3=? AND c4=?;",
		},
		{
			table:   "table2",
			cols:    []string{"c1", "c2", "c3"},
			keyCols: []string{"c4"},
			stmt: "SELECT \"c1\", \"c2\", \"c3\" FROM" +
				" \"table2\" WHERE c4=?;",
		},
		{
			table:   "table3",
			cols:    []string{"c1", "c2", "c3"},
			keyCols: []string{},
			stmt:    "SELECT \"c1\", \"c2\", \"c3\" FROM \"table3\";",
		},
		{
			table:   "table3",
			cols:    []string{"c1", "c2", "c3"},
			keyCols: []string{},
			limit:   10,
			stmt:    "SELECT \"c1\", \"c2\", \"c3\" FROM \"table3\" LIMIT 10;",
		},
	}
	for _, d := range data {
		stmt, err := SelectStmt(
			Table(d.table),
			Columns(d.cols),
			Conditions(d.keyCols),
			Limit(d.limit),
		)
		suite.NoError(err)
		suite.Equal(stmt, d.stmt)
	}
}

// TestDeleteStmt tests constructing delete CQL query
func (suite *CassandraConnSuite) TestDeleteStmt() {

	data := []struct {
		table   string
		cols    []string
		keyCols []string
		values  []interface{}
		stmt    string
	}{
		{
			table:   "table1",
			cols:    []string{"c1", "c2"},
			keyCols: []string{"c3", "c4"},
			values:  []interface{}{"val1", "val2"},
			stmt:    "DELETE FROM \"table1\" WHERE c3=? AND c4=?;",
		},
		{
			table:   "table2",
			cols:    []string{"c1", "c2"},
			keyCols: []string{"c4"},
			values:  []interface{}{"val1", "val2"},
			stmt:    "DELETE FROM \"table2\" WHERE c4=?;",
		},
	}
	for _, d := range data {
		stmt, err := DeleteStmt(
			Table(d.table),
			Conditions(d.keyCols),
		)
		suite.NoError(err)
		suite.Equal(stmt, d.stmt)
		// negative tests. Add unexpected clauses to the mix.
		stmt, err = DeleteStmt(
			Table(d.table),
			Columns(d.cols),
			Values(d.values),
			Conditions(d.keyCols),
		)
		suite.NoError(err)
		suite.Equal(stmt, d.stmt)
	}
}

// TestUpdateStmt tests constructing the update statement
func (suite *CassandraConnSuite) TestUpdateStmt() {
	data := []struct {
		table   string
		cols    []string
		keyCols []string
		values  []interface{}
		stmt    string
	}{
		{
			table:   "table1",
			cols:    []string{"c1", "c2"},
			keyCols: []string{"c3", "c4"},
			values:  []interface{}{1, 2},
			stmt:    "UPDATE \"table1\" SET c1=?, c2=? WHERE c3=? AND c4=?;",
		},

		{
			table:   "table2",
			cols:    []string{"c1", "c2", "c3"},
			keyCols: []string{"c4"},
			stmt: "UPDATE \"table2\" SET c1=?, c2=?, c3=? " +
				"WHERE c4=?;",
		},
	}
	for _, d := range data {
		stmt, err := UpdateStmt(
			Table(d.table),
			Updates(d.cols),
			Conditions(d.keyCols),
		)
		suite.NoError(err)
		suite.Equal(stmt, d.stmt)

		// Negative test. UpdateStmt with Values which should be ignored.
		stmt, err = UpdateStmt(
			Table(d.table),
			Updates(d.cols),
			Conditions(d.keyCols),
			Values(d.values),
		)
		suite.NoError(err)
		suite.Equal(stmt, d.stmt)
	}
}
