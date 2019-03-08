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
	"sort"
	"strings"

	"errors"

	"github.com/lann/builder"
)

type updateData struct {
	PlaceholderFormat      PlaceholderFormat
	Table                  string
	SetClauses             []setClause
	SetClausesAdd          []setClause // collections, +
	SetClausesRemove       []setClause // collections, -
	WhereParts             []Sqlizer
	IfOnlyParts            []Sqlizer
	Usings                 exprs
	STApplyMetadata        []byte // This is ignored by the ToThrift() API.
	STApplyMetadataApplied bool
}

type setClause struct {
	column string
	value  interface{}
}

var (
	// ErrMalformedSetClause indicates that the update is missing a set clause
	ErrMalformedSetClause = errors.New("update statements must have at least one Set clause")

	// ErrMissingTable indicates that the update is missing a target table
	ErrMissingTable = errors.New("update statements must specify a table")
)

func (d *updateData) ToSQL() (sqlStr string, args []interface{}, err error) {
	if len(d.Table) == 0 {
		err = ErrMissingTable
		return
	}
	sql := &bytes.Buffer{}

	sql.WriteString("UPDATE ")
	sql.WriteString(d.Table)

	if len(d.Usings) > 0 {
		sql.WriteString(" USING ")
		args, _ = d.Usings.AppendToSQL(sql, " ", args)
	}

	var setSqls []string
	if d.STApplyMetadataApplied || len(d.SetClauses) > 0 || len(d.SetClausesAdd) > 0 ||
		len(d.SetClausesRemove) > 0 {
		sql.WriteString(" SET ")
		cnt := len(d.SetClauses) + len(d.SetClausesAdd) + len(d.SetClausesRemove)
		if d.STApplyMetadataApplied {
			cnt++
		}
		setSqls = make([]string, cnt)
	} else {
		err = ErrMalformedSetClause
		return
	}
	setIdx := 0
	for _, setClause := range d.SetClauses {
		var valSQL string
		e, isExpr := setClause.value.(expr)
		if isExpr {
			valSQL = e.sql
			args = append(args, e.args...)
		} else {
			valSQL = "?"
			args = append(args, setClause.value)
		}
		setSqls[setIdx] = fmt.Sprintf("%s = %s", setClause.column, valSQL)
		setIdx++
	}
	if d.STApplyMetadataApplied {
		setSqls[setIdx] = "st_apply_metadata = ?"
		args = append(args, d.STApplyMetadata)
		setIdx++
	}
	for _, setClause := range d.SetClausesAdd { // SET emails = emails + ?
		args = append(args, setClause.value)
		setSqls[setIdx] = fmt.Sprintf("%s = %s + ?", setClause.column, setClause.column)
		setIdx++
	}
	for _, setClause := range d.SetClausesRemove { // SET emails = emails - ?
		args = append(args, setClause.value)
		setSqls[setIdx] = fmt.Sprintf("%s = %s - ?", setClause.column, setClause.column)
		setIdx++
	}
	sql.WriteString(strings.Join(setSqls, ", "))

	if len(d.WhereParts) > 0 {
		sql.WriteString(" WHERE ")
		args, err = appendToSQL(d.WhereParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(d.IfOnlyParts) > 0 {
		sql.WriteString(" IF ")
		args, err = appendToSQL(d.IfOnlyParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sql.String())
	return
}

func (d updateData) GetResource() string {
	return d.Table
}

func (d updateData) GetWhereParts() []Sqlizer {
	return d.WhereParts
}

func (d updateData) GetColumns() []Sqlizer {
	return nil
}

// Builder

// UpdateBuilder builds SQL UPDATE statements.
type UpdateBuilder builder.Builder

func init() {
	builder.Register(UpdateBuilder{}, updateData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// update.
func (b UpdateBuilder) PlaceholderFormat(f PlaceholderFormat) UpdateBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(UpdateBuilder)
}

// SQL methods

// ToSQL builds the update into a SQL string and bound args.
func (b UpdateBuilder) ToSQL() (string, []interface{}, error) {
	data := builder.GetStruct(b).(updateData)
	return data.ToSQL()
}

// ToUql builds the query into a UQL string and bound args.
// As an runtime optimization, it also returns query options
func (b UpdateBuilder) ToUql() (query string, args []interface{},
	options map[string]interface{}, err error) {
	data := builder.GetStruct(b).(updateData)
	query, args, err = data.ToSQL()
	options = map[string]interface{}{
		"IsCAS": len(data.IfOnlyParts) > 0,
	}
	return
}

// StmtType returns type of the statement
func (b UpdateBuilder) StmtType() StmtType {
	return UpdateStmtType
}

// GetData returns the underlying struct as an interface
func (b UpdateBuilder) GetData() StatementAccessor {
	return builder.GetStruct(b).(updateData)
}

// Table sets the table to be updated.
func (b UpdateBuilder) Table(table string) UpdateBuilder {
	return builder.Set(b, "Table", table).(UpdateBuilder)
}

// Set adds SET clauses to the update.
func (b UpdateBuilder) Set(column string, value interface{}) UpdateBuilder {
	return builder.Append(b, "SetClauses", setClause{column: column, value: value}).(UpdateBuilder)
}

// Add appends a value to a list column.
func (b UpdateBuilder) Add(column string, value interface{}) UpdateBuilder {
	return builder.Append(b, "SetClausesAdd", setClause{column: column, value: value}).(UpdateBuilder)
}

// Remove discards a value from a list column
func (b UpdateBuilder) Remove(column string, value interface{}) UpdateBuilder {
	return builder.Append(b, "SetClausesRemove", setClause{column: column, value: value}).(UpdateBuilder)
}

// SetMap is a convenience method which calls .Set for each key/value pair in clauses.
func (b UpdateBuilder) SetMap(clauses map[string]interface{}) UpdateBuilder {
	keys := make([]string, len(clauses))
	i := 0
	for key := range clauses {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	for _, key := range keys {
		val, _ := clauses[key]
		b = b.Set(key, val)
	}
	return b
}

// Where adds WHERE expressions to the update.
//
// See SelectBuilder.Where for more information.
func (b UpdateBuilder) Where(pred interface{}, args ...interface{}) UpdateBuilder {
	return builder.Append(b, "WhereParts", newWherePart(pred, args...)).(UpdateBuilder)
}

// IfOnly represents a LWT
func (b UpdateBuilder) IfOnly(pred interface{}, rest ...interface{}) UpdateBuilder {
	return builder.Append(b, "IfOnlyParts", newWherePart(pred, rest...)).(UpdateBuilder)
}

// IsCAS returns true is the update statement has a compare-and-set part
func (b UpdateBuilder) IsCAS() bool {
	data := builder.GetStruct(b).(updateData)
	return len(data.IfOnlyParts) > 0
}

// AddSTApplyMetadata adds a value for the special st_apply_metadata column.
func (b UpdateBuilder) AddSTApplyMetadata(value []byte) UpdateBuilder {
	tmp := builder.Set(b, "STApplyMetadataApplied", true).(UpdateBuilder)
	return builder.Set(tmp, "STApplyMetadata", value).(UpdateBuilder)
}

// Using adds an expression to the end of the update.
func (b UpdateBuilder) Using(sql string, args ...interface{}) UpdateBuilder {
	return builder.Append(b, "Usings", expression(sql, args...)).(UpdateBuilder)
}
