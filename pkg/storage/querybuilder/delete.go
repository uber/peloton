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

package querybuilder

import (
	"bytes"
	"fmt"

	"github.com/lann/builder"
)

type deleteData struct {
	PlaceholderFormat PlaceholderFormat
	From              string
	WhereParts        []Sqlizer
	Usings            exprs
}

func (d *deleteData) ToSQL() (sqlStr string, args []interface{}, err error) {
	if len(d.From) == 0 {
		err = fmt.Errorf("delete statements must specify a From table")
		return
	}

	sql := &bytes.Buffer{}

	sql.WriteString("DELETE FROM ")
	sql.WriteString(d.From)

	if len(d.Usings) > 0 {
		sql.WriteString(" USING ")
		args, _ = d.Usings.AppendToSQL(sql, " ", args)
	}

	if len(d.WhereParts) > 0 {
		sql.WriteString(" WHERE ")
		args, err = appendToSQL(d.WhereParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sql.String())
	return
}

func (d deleteData) GetResource() string {
	return d.From
}

func (d deleteData) GetWhereParts() []Sqlizer {
	return d.WhereParts
}

func (d deleteData) GetColumns() []Sqlizer {
	return nil
}

// Builder

// DeleteBuilder builds SQL DELETE statements.
type DeleteBuilder builder.Builder

func init() {
	builder.Register(DeleteBuilder{}, deleteData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// delete.
func (b DeleteBuilder) PlaceholderFormat(f PlaceholderFormat) DeleteBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(DeleteBuilder)
}

// SQL methods

// ToSQL builds the delete into a SQL string and bound args.
func (b DeleteBuilder) ToSQL() (string, []interface{}, error) {
	data := builder.GetStruct(b).(deleteData)
	return data.ToSQL()
}

// ToUql builds the delete into a UQL string and bound args. it also returns
// simple query options
func (b DeleteBuilder) ToUql() (query string, args []interface{},
	options map[string]interface{}, err error) {
	data := builder.GetStruct(b).(deleteData)
	query, args, err = data.ToSQL()
	options = nil
	return
}

// StmtType returns type of the statement
func (b DeleteBuilder) StmtType() StmtType {
	return DeleteStmtType
}

// IsCAS always return false
func (b DeleteBuilder) IsCAS() bool {
	return false
}

// GetData returns the underlying struct as an interface
func (b DeleteBuilder) GetData() StatementAccessor {
	return builder.GetStruct(b).(deleteData)
}

// From sets the table to be deleted from.
func (b DeleteBuilder) From(from string) DeleteBuilder {
	return builder.Set(b, "From", from).(DeleteBuilder)
}

// Where adds WHERE expressions to the delete.
//
// See SelectBuilder.Where for more information.
func (b DeleteBuilder) Where(pred interface{}, args ...interface{}) DeleteBuilder {
	return builder.Append(b, "WhereParts", newWherePart(pred, args...)).(DeleteBuilder)
}

// Using adds an expression to the end of the query
func (b DeleteBuilder) Using(sql string, args ...interface{}) DeleteBuilder {
	return builder.Append(b, "Usings", expression(sql, args...)).(DeleteBuilder)
}
