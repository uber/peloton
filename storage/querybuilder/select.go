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

type selectData struct {
	PlaceholderFormat PlaceholderFormat
	Distinct          bool
	Columns           []Sqlizer
	From              Sqlizer
	Joins             []Sqlizer
	WhereParts        []Sqlizer
	GroupBys          []string
	HavingParts       []Sqlizer
	OrderBys          []string
	Limit             string
	PageSize          int
	DisableAutoPage   bool
	PagingState       []byte
	Offset            string
}

func (d *selectData) ToSQL() (sqlStr string, args []interface{}, err error) {
	if len(d.Columns) == 0 {
		err = fmt.Errorf("select statements must have at least one result column")
		return
	}

	sql := &bytes.Buffer{}

	sql.WriteString("SELECT ")

	if d.Distinct {
		sql.WriteString("DISTINCT ")
	}

	if len(d.Columns) > 0 {
		args, err = appendToSQL(d.Columns, sql, ", ", args)
		if err != nil {
			return
		}
	}

	if d.From != nil {
		sql.WriteString(" FROM ")
		args, err = appendToSQL([]Sqlizer{d.From}, sql, "", args)
		if err != nil {
			return
		}
	}

	if len(d.Joins) > 0 {
		sql.WriteString(" ")
		args, err = appendToSQL(d.Joins, sql, " ", args)
	}

	if len(d.WhereParts) > 0 {
		sql.WriteString(" WHERE ")
		args, err = appendToSQL(d.WhereParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(d.GroupBys) > 0 {
		sql.WriteString(" GROUP BY ")
		sql.WriteString(strings.Join(d.GroupBys, ", "))
	}

	if len(d.HavingParts) > 0 {
		sql.WriteString(" HAVING ")
		args, err = appendToSQL(d.HavingParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(d.OrderBys) > 0 {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(strings.Join(d.OrderBys, ", "))
	}

	if len(d.Limit) > 0 {
		sql.WriteString(" LIMIT ")
		sql.WriteString(d.Limit)
	}

	if len(d.Offset) > 0 {
		sql.WriteString(" OFFSET ")
		sql.WriteString(d.Offset)
	}
	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sql.String())
	return
}

func (d selectData) GetResource() string {
	res, _, _ := d.From.ToSQL()
	return res
}

func (d selectData) GetWhereParts() []Sqlizer {
	return d.WhereParts
}

func (d selectData) GetColumns() []Sqlizer {
	return d.Columns
}

// Builder

// SelectBuilder builds SQL SELECT statements.
type SelectBuilder builder.Builder

func init() {
	builder.Register(SelectBuilder{}, selectData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b SelectBuilder) PlaceholderFormat(f PlaceholderFormat) SelectBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(SelectBuilder)
}

// SQL methods

// ToSQL builds the query into a SQL string and bound args.
func (b SelectBuilder) ToSQL() (string, []interface{}, error) {
	data := builder.GetStruct(b).(selectData)
	return data.ToSQL()
}

// ToUql builds the query into a UQL string and bound args.
// As an runtime optimization, it also returns query options
func (b SelectBuilder) ToUql() (query string, args []interface{},
	options map[string]interface{}, err error) {
	data := builder.GetStruct(b).(selectData)
	query, args, err = data.ToSQL()
	options = map[string]interface{}{
		"PageSize":        data.PageSize,
		"DisableAutoPage": data.DisableAutoPage,
		"PagingState":     data.PagingState,
	}
	return
}

// StmtType returns type of the statement
func (b SelectBuilder) StmtType() StmtType {
	return SelectStmtType
}

// IsCAS always return false
func (b SelectBuilder) IsCAS() bool {
	return false
}

// GetData returns the underlying struct as an interface
func (b SelectBuilder) GetData() StatementAccessor {
	return builder.GetStruct(b).(selectData)
}

// Distinct adds a DISTINCT clause to the query.
func (b SelectBuilder) Distinct() SelectBuilder {
	return builder.Set(b, "Distinct", true).(SelectBuilder)
}

// Columns adds result columns to the query.
func (b SelectBuilder) Columns(columns ...string) SelectBuilder {
	var parts []interface{}
	for _, str := range columns {
		parts = append(parts, newPart(str))
	}
	return builder.Extend(b, "Columns", parts).(SelectBuilder)
}

// Column adds a result column to the query.
// Unlike Columns, Column accepts args which will be bound to placeholders in
// the columns string, for example:
//   Column("IF(col IN ("+squirrel.Placeholders(3)+"), 1, 0) as col", 1, 2, 3)
func (b SelectBuilder) Column(column interface{}, args ...interface{}) SelectBuilder {
	return builder.Append(b, "Columns", newPart(column, args...)).(SelectBuilder)
}

// From sets the FROM clause of the query.
func (b SelectBuilder) From(from string) SelectBuilder {
	return builder.Set(b, "From", newPart(from)).(SelectBuilder)
}

// FromSelect sets a subquery into the FROM clause of the query.
func (b SelectBuilder) FromSelect(from SelectBuilder, aliasString string) SelectBuilder {
	return builder.Set(b, "From", alias(from, aliasString)).(SelectBuilder)
}

// JoinClause adds a join clause to the query.
func (b SelectBuilder) JoinClause(pred interface{}, args ...interface{}) SelectBuilder {
	return builder.Append(b, "Joins", newPart(pred, args...)).(SelectBuilder)
}

// Join adds a JOIN clause to the query.
func (b SelectBuilder) Join(join string, rest ...interface{}) SelectBuilder {
	return b.JoinClause("JOIN "+join, rest...)
}

// LeftJoin adds a LEFT JOIN clause to the query.
func (b SelectBuilder) LeftJoin(join string, rest ...interface{}) SelectBuilder {
	return b.JoinClause("LEFT JOIN "+join, rest...)
}

// RightJoin adds a RIGHT JOIN clause to the query.
func (b SelectBuilder) RightJoin(join string, rest ...interface{}) SelectBuilder {
	return b.JoinClause("RIGHT JOIN "+join, rest...)
}

// Where adds an expression to the WHERE clause of the query.
//
// Expressions are ANDed together in the generated SQL.
//
// Where accepts several types for its pred argument:
//
// nil OR "" - ignored.
//
// string - SQL expression.
// If the expression has SQL placeholders then a set of arguments must be passed
// as well, one for each placeholder.
//
// map[string]interface{} OR Eq - map of SQL expressions to values. Each key is
// transformed into an expression like "<key> = ?", with the corresponding value
// bound to the placeholder. If the value is nil, the expression will be "<key>
// IS NULL". If the value is an array or slice, the expression will be "<key> IN
// (?,?,...)", with one placeholder for each item in the value. These expressions
// are ANDed together.
//
// Where will panic if pred isn't any of the above types.
func (b SelectBuilder) Where(pred interface{}, args ...interface{}) SelectBuilder {
	return builder.Append(b, "WhereParts", newWherePart(pred, args...)).(SelectBuilder)
}

// GroupBy adds GROUP BY expressions to the query.
func (b SelectBuilder) GroupBy(groupBys ...string) SelectBuilder {
	return builder.Extend(b, "GroupBys", groupBys).(SelectBuilder)
}

// Having adds an expression to the HAVING clause of the query.
//
// See Where.
func (b SelectBuilder) Having(pred interface{}, rest ...interface{}) SelectBuilder {
	return builder.Append(b, "HavingParts", newWherePart(pred, rest...)).(SelectBuilder)
}

// OrderBy adds ORDER BY expressions to the query.
func (b SelectBuilder) OrderBy(orderBys ...string) SelectBuilder {
	return builder.Extend(b, "OrderBys", orderBys).(SelectBuilder)
}

// Limit sets a LIMIT clause on the query.
func (b SelectBuilder) Limit(limit uint64) SelectBuilder {
	return builder.Set(b, "Limit", fmt.Sprintf("%d", limit)).(SelectBuilder)
}

// Offset sets a OFFSET clause on the query.
func (b SelectBuilder) Offset(offset uint64) SelectBuilder {
	return builder.Set(b, "Offset", fmt.Sprintf("%d", offset)).(SelectBuilder)
}

// PagingState sets a continuation token on the query.
func (b SelectBuilder) disableAutoPaging() SelectBuilder {
	return builder.Set(b, "DisableAutoPage", true).(SelectBuilder)
}

// PageSize sets size of rows the query should fetch a time
func (b SelectBuilder) PageSize(pageSize int) SelectBuilder {
	return builder.Set(b, "PageSize", pageSize).(SelectBuilder)
}

// PagingState sets a continuation token on where the query will resume for the next page.
func (b SelectBuilder) PagingState(pagingState []byte) SelectBuilder {
	ret := b.disableAutoPaging()
	if len(pagingState) > 0 {
		return builder.Set(ret, "PagingState", pagingState).(SelectBuilder)
	}
	return ret
}

// IsDisableAutoPaging returns true if the select statement disable auto-paging
func (b SelectBuilder) IsDisableAutoPaging() bool {
	data := builder.GetStruct(b).(selectData)
	return data.DisableAutoPage
}

// GetPagingState returns true the continuation token
func (b SelectBuilder) GetPagingState() []byte {
	data := builder.GetStruct(b).(selectData)
	return data.PagingState
}

// GetPageSize returns true the size of the page
func (b SelectBuilder) GetPageSize() int {
	data := builder.GetStruct(b).(selectData)
	return data.PageSize
}
