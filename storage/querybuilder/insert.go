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
	"strings"

	"github.com/lann/builder"
)

type insertData struct {
	PlaceholderFormat      PlaceholderFormat
	Into                   string
	Columns                []string
	Values                 []interface{}
	Usings                 exprs
	IfNotExist             bool
	STApplyMetadata        []byte // This is ignored by the ToThrift() API.
	STApplyMetadataApplied bool
}

func (d *insertData) ToSQL() (sqlStr string, args []interface{}, err error) {
	if len(d.Into) == 0 {
		err = fmt.Errorf("insert statements must specify a table")
		return
	}
	if len(d.Values) == 0 {
		err = fmt.Errorf("insert statements must have at least one set of values")
		return
	}

	sql := &bytes.Buffer{}

	sql.WriteString("INSERT ")

	sql.WriteString("INTO ")
	sql.WriteString(d.Into)
	sql.WriteString(" ")

	if len(d.Columns) > 0 {
		sql.WriteString("(")
		sql.WriteString(strings.Join(d.Columns, ","))
		if d.STApplyMetadataApplied {
			sql.WriteString(",st_apply_metadata")
		}
		sql.WriteString(") ")
	}

	sql.WriteString("VALUES ")

	vsLen := len(d.Values)
	if d.STApplyMetadataApplied {
		vsLen = vsLen + 1
	}
	valueStrings := make([]string, vsLen)
	for v, val := range d.Values {
		e, isExpr := val.(expr)
		if isExpr {
			valueStrings[v] = e.sql
			args = append(args, e.args...)
		} else {
			valueStrings[v] = "?"
			args = append(args, val)
		}
	}
	if d.STApplyMetadataApplied {
		valueStrings[vsLen-1] = "?"
		args = append(args, d.STApplyMetadata)
	}
	sql.WriteString(fmt.Sprintf("(%s)", strings.Join(valueStrings, ",")))

	if d.IfNotExist {
		sql.WriteString(" IF NOT EXISTS")
	}

	if len(d.Usings) > 0 {
		sql.WriteString(" USING ")
		args, _ = d.Usings.AppendToSQL(sql, " ", args)
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sql.String())
	return
}

func (d insertData) GetResource() string {
	return d.Into
}

func (d insertData) GetWhereParts() []Sqlizer {
	return nil
}

func (d insertData) GetColumns() []Sqlizer {
	return nil
}

// Builder

// InsertBuilder builds SQL INSERT statements.
type InsertBuilder builder.Builder

func init() {
	builder.Register(InsertBuilder{}, insertData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b InsertBuilder) PlaceholderFormat(f PlaceholderFormat) InsertBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(InsertBuilder)
}

// SQL methods

// ToSQL builds the query into a SQL string and bound args.
func (b InsertBuilder) ToSQL() (string, []interface{}, error) {
	data := builder.GetStruct(b).(insertData)
	return data.ToSQL()
}

// ToUql builds the query into a UQL string and bound args.
// As an runtime optimization, it also returns query options
func (b InsertBuilder) ToUql() (query string, args []interface{},
	options map[string]interface{}, err error) {
	data := builder.GetStruct(b).(insertData)
	query, args, err = data.ToSQL()
	options = map[string]interface{}{
		"IsCAS": data.IfNotExist,
	}
	return
}

// StmtType returns type of the statement
func (b InsertBuilder) StmtType() StmtType {
	return InsertStmtType
}

// GetData returns the underlying struct as an interface
func (b InsertBuilder) GetData() StatementAccessor {
	return builder.GetStruct(b).(insertData)
}

// Into sets the INTO clause of the query.
func (b InsertBuilder) Into(from string) InsertBuilder {
	return builder.Set(b, "Into", from).(InsertBuilder)
}

// Columns adds insert columns to the query.
func (b InsertBuilder) Columns(columns ...string) InsertBuilder {
	return builder.Extend(b, "Columns", columns).(InsertBuilder)
}

// Values adds a single row's values to the query.
func (b InsertBuilder) Values(values ...interface{}) InsertBuilder {
	return builder.Extend(b, "Values", values).(InsertBuilder)
}

// AddSTApplyMetadata adds a value for the special st_apply_metadata column.
func (b InsertBuilder) AddSTApplyMetadata(value []byte) InsertBuilder {
	tmp := builder.Set(b, "STApplyMetadataApplied", true).(InsertBuilder)
	return builder.Set(tmp, "STApplyMetadata", value).(InsertBuilder)
}

// Using adds an expression to the end of the query
func (b InsertBuilder) Using(sql string, args ...interface{}) InsertBuilder {
	return builder.Append(b, "Usings", expression(sql, args...)).(InsertBuilder)
}

// IfNotExist performs the insert only if the value does not exist.
func (b InsertBuilder) IfNotExist() InsertBuilder {
	return builder.Set(b, "IfNotExist", true).(InsertBuilder)
}

// IsCAS returns true is the insert statement has a compare-and-set part
func (b InsertBuilder) IsCAS() bool {
	data := builder.GetStruct(b).(insertData)
	return data.IfNotExist
}

// SetMap set columns and values for insert builder from a map of column name and value
// note that it will reset all previous columns and values was set if any
func (b InsertBuilder) SetMap(clauses map[string]interface{}) InsertBuilder {
	cols := make([]string, 0, len(clauses))
	vals := make([]interface{}, 0, len(clauses))
	for col, val := range clauses {
		cols = append(cols, col)
		vals = append(vals, val)
	}

	b = builder.Set(b, "Columns", cols).(InsertBuilder)
	b = builder.Set(b, "Values", vals).(InsertBuilder)
	return b
}
