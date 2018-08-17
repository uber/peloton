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

func (suite *CassandraConnSuite) TestInsertStmt() {
	data := []struct {
		table   string
		columns []string
		values  []interface{}
		stmt    string
	}{
		{
			table:   "table1",
			columns: []string{"c1", "c2"},
			values:  []interface{}{"val1", "val2"},
			stmt:    "INSERT INTO \"table1\" (\"c1\", \"c2\") VALUES (?, ?);",
		},
		{
			table:   "table2",
			columns: []string{"c1", "c2"},
			values:  []interface{}{1, 2},
			stmt:    "INSERT INTO \"table2\" (\"c1\", \"c2\") VALUES (?, ?);",
		},
	}
	for _, d := range data {
		stmt, err := InsertStmt(
			Table(d.table),
			Columns(d.columns),
			Values(d.values),
		)
		suite.NoError(err)
		suite.Equal(stmt, d.stmt)
	}
}

func (suite *CassandraConnSuite) TestSelectStmt() {

	data := []struct {
		table   string
		cols    []string
		keyCols []string
		stmt    string
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
	}
	for _, d := range data {
		stmt, err := SelectStmt(
			Table(d.table),
			Columns(d.cols),
			Conditions(d.keyCols),
		)
		suite.NoError(err)
		suite.Equal(stmt, d.stmt)
	}
}
