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
	// updateCols is used to indicate update column names in the query
	updates = "Updates"
	// ifNotExist is used to indicate CAS write in the insert query
	ifNotExist = "IfNotExist"
	// limit is used to indicate the query limit for number of rows.
	limit = "Limit"

	// insertTemplate is used to construct an insert query
	insertTemplate = `INSERT INTO {{.Table}} ({{ColumnFunc .Columns ", "}})` +
		` VALUES ({{QuestionMark .Values ", "}}){{ExistsFunc .IfNotExist}};`

	// selectTemplate is used to construct a select query
	selectTemplate = `SELECT {{ColumnFunc .Columns ", "}} FROM {{.Table}}` +
		`{{WhereFunc .Conditions}}{{ConditionsFunc .Conditions " AND "}}` +
		`{{LimitFunc .Limit}};`

	// deleteTemplate is used to construct a delete query
	deleteTemplate = `DELETE FROM {{.Table}} WHERE ` +
		`{{ConditionsFunc .Conditions " AND "}};`

	// updateTemplate is used to construct update query
	updateTemplate = `UPDATE {{.Table}} SET {{ConditionsFunc .Updates ", "}}` +
		`{{WhereFunc .Conditions}}{{ConditionsFunc .Conditions " AND "}};`
)

var (
	// function map for populating CQL templates
	funcMap = template.FuncMap{
		"ColumnFunc":     strings.Join,
		"QuestionMark":   questionMarkFunc,
		"ConditionsFunc": conditionsFunc,
		"WhereFunc":      whereFunc,
		"ExistsFunc":     existsFunc,
		"LimitFunc":      limitFunc,
	}

	// insert CQL query template implementation
	insertTmpl = template.Must(
		template.New("insert").Funcs(funcMap).Parse(insertTemplate))
	// select CQL query template implementation
	selectTmpl = template.Must(
		template.New("select").Funcs(funcMap).Parse(selectTemplate))
	// delete CQL query template implementation
	deleteTmpl = template.Must(
		template.New("delete").Funcs(funcMap).Parse(deleteTemplate))
	// update CQL query template implementation
	updateTmpl = template.Must(
		template.New("update").Funcs(funcMap).Parse(updateTemplate))
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

// existsFunc adds an if not exists clause to the insert query
func existsFunc(exists bool) string {
	if exists {
		return " IF NOT EXISTS"
	}
	return ""
}

// limitFunc adds a LIMIT clause to the select query.
func limitFunc(num int) string {
	if num > 0 {
		return fmt.Sprintf(" LIMIT %d", num)
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

// Updates set the `SET` clause to the cql statement
func Updates(v interface{}) OptFunc {
	return func(opt Option) {
		opt[updates] = v
	}
}

// IfNotExist sets the `if not exist` clause to the cql statement
func IfNotExist(v interface{}) OptFunc {
	return func(opt Option) {
		opt[ifNotExist] = v
	}
}

// Limit sets the `limit` to the cql statement.
func Limit(v interface{}) OptFunc {
	return func(opt Option) {
		opt[limit] = v
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
	option := Option{
		limit: 0,
	}
	for _, opt := range opts {
		opt(option)
	}
	err := selectTmpl.Execute(&bb, option)
	return bb.String(), err
}

// DeleteStmt creates delete statement
func DeleteStmt(opts ...OptFunc) (string, error) {
	var bb bytes.Buffer
	option := Option{}
	for _, opt := range opts {
		opt(option)
	}
	err := deleteTmpl.Execute(&bb, option)
	return bb.String(), err
}

// UpdateStmt creates update statement
func UpdateStmt(opts ...OptFunc) (string, error) {
	var bb bytes.Buffer
	option := Option{}
	for _, opt := range opts {
		opt(option)
	}
	err := updateTmpl.Execute(&bb, option)
	return bb.String(), err
}
