package cassandra

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"text/template"
)

const (
	// table is used to substitute Table in template with actual table name
	table = "Table"
	// values is used to substitute Values in template with column values
	values = "Values"
	// columns is used to substitute Columns in template with column names
	columns = "Columns"
	// conditions is used to indicate <,>,= conditions in the query
	conditions = "Conditions"

	// insertTemplate is used to construct an insert query
	insertTemplate = `INSERT INTO {{.Table}} ({{ColumnFunc .Columns ", "}})` +
		` VALUES ({{QuestionMark .Values ", "}});`

	// selectTemplate is used to construct an select query
	selectTemplate = `SELECT {{ColumnFunc .Columns ", "}} FROM {{.Table}}` +
		`{{WhereFunc .Conditions}}{{ConditionsFunc .Conditions " AND "}};`
)

var (
	// function map for populating CQL templates
	funcMap = template.FuncMap{
		"ColumnFunc":     strings.Join,
		"QuestionMark":   questionMarkFunc,
		"ConditionsFunc": conditionsFunc,
		"WhereFunc":      whereFunc,
	}

	// insert CQL query template implementation
	insertTmpl = template.Must(
		template.New("insert").Funcs(funcMap).Parse(insertTemplate))
	// select CQL query template implementation
	selectTmpl = template.Must(
		template.New("select").Funcs(funcMap).Parse(selectTemplate))
)

// questionMarkFunc adds ? to the insert query in place of values to be inserted
func questionMarkFunc(qs []interface{}, sep string) string {
	questions := make([]string, len(qs))
	for i := range qs {
		questions[i] = "?"
	}
	return strings.Join(questions, sep)
}

// conditionsFunc adds a =? condition to the select query
func conditionsFunc(conds []string, sep string) string {
	cstrs := make([]string, len(conds))
	for i, cond := range conds {
		cstrs[i] = fmt.Sprintf("%s=?", cond)
	}
	return strings.Join(cstrs, sep)
}

// whereFunc adds where clause to the select query
func whereFunc(conds []string) string {
	if len(conds) > 0 {
		return " WHERE "
	}
	return ""
}

// Option to compose a cql statement
type Option map[string]interface{}

// OptFunc is the interface to set option
type OptFunc func(Option)

// Table sets the `table` to the cql statement
func Table(v string) OptFunc {
	return func(opt Option) {
		opt[table] = strconv.Quote(v)
	}
}

// Columns sets the `columns` clause to the cql statement
func Columns(v []string) OptFunc {
	return func(opt Option) {
		quoCs := make([]string, len(v))
		for i, c := range v {
			quoCs[i] = strconv.Quote(c)
		}
		opt[columns] = quoCs
	}
}

// Values sets the `values` clause to the cql statement
func Values(v interface{}) OptFunc {
	return func(opt Option) {
		opt[values] = v
	}
}

// Conditions set the `where` clause to the cql statement
func Conditions(v interface{}) OptFunc {
	return func(opt Option) {
		opt[conditions] = v
	}
}

// InsertStmt creates insert statement
func InsertStmt(opts ...OptFunc) (string, error) {
	var bb bytes.Buffer
	option := Option{}
	for _, opt := range opts {
		opt(option)
	}
	err := insertTmpl.Execute(&bb, option)
	return bb.String(), err
}

// SelectStmt creates select statement
func SelectStmt(opts ...OptFunc) (string, error) {
	var bb bytes.Buffer
	option := Option{}
	for _, opt := range opts {
		opt(option)
	}
	err := selectTmpl.Execute(&bb, option)
	return bb.String(), err
}
