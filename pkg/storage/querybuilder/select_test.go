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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelectBuilderToSql(t *testing.T) {
	subQ := Select("aa", "bb").From("dd")
	b := Select("a", "b").
		Distinct().
		Columns("c").
		Column("IF(d IN ("+Placeholders(3)+"), 1, 0) as stat_column", 1, 2, 3).
		Column(expression("a > ?", 100)).
		Column(alias(Eq{"b": []int{101, 102, 103}}, "b_alias")).
		Column(alias(subQ, "subq")).
		From("e").
		JoinClause("CROSS JOIN j1").
		Join("j2").
		LeftJoin("j3").
		RightJoin("j4").
		Where("f = ?", 4).
		Where(Eq{"g": 5}).
		Where(map[string]interface{}{"h": 6}).
		Where(Eq{"i": []int{7, 8, 9}}).
		Where(Or{expression("j = ?", 10), And{Eq{"k": 11}, expression("true")}}).
		GroupBy("l").
		Having("m = n").
		OrderBy("o ASC", "p DESC").
		PagingState([]byte("howdy")).
		PageSize(4).
		Limit(12).
		Offset(13)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL :=
		"SELECT DISTINCT a, b, c, IF(d IN (?,?,?), 1, 0) as stat_column, a > ?, " +
			"(b IN (?,?,?)) AS b_alias, " +
			"(SELECT aa, bb FROM dd) AS subq " +
			"FROM e " +
			"CROSS JOIN j1 JOIN j2 LEFT JOIN j3 RIGHT JOIN j4 " +
			"WHERE f = ? AND g = ? AND h = ? AND i IN (?,?,?) AND (j = ? OR (k = ? AND true)) " +
			"GROUP BY l HAVING m = n ORDER BY o ASC, p DESC LIMIT 12 OFFSET 13"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1, 2, 3, 100, 101, 102, 103, 4, 5, 6, 7, 8, 9, 10, 11}
	assert.Equal(t, expectedArgs, args)

	sql, args, options, err := b.ToUql()
	assert.Equal(t, options["PageSize"].(int), 4)
	assert.Equal(t, options["DisableAutoPage"].(bool), true)
	assert.Equal(t, options["PagingState"].([]byte), []byte("howdy"))
}

func TestSelectBuilderFromSelect(t *testing.T) {
	subQ := Select("c").From("d").Where(Eq{"i": 0})
	b := Select("a", "b").FromSelect(subQ, "subq")
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT a, b FROM (SELECT c FROM d WHERE i = ?) AS subq"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{0}
	assert.Equal(t, expectedArgs, args)
}

func TestSelectBuilderFromSelectUql(t *testing.T) {
	subQ := Select("c").From("d").Where(Eq{"i": 0})
	b := Select("a", "b").FromSelect(subQ, "subq")
	sql, args, options, err := b.ToUql()

	assert.NoError(t, err)
	assert.Equal(t, options["PageSize"].(int), 0)

	expectedSQL := "SELECT a, b FROM (SELECT c FROM d WHERE i = ?) AS subq"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{0}
	assert.Equal(t, expectedArgs, args)
}

func TestSelectBuilderToSqlErr(t *testing.T) {
	_, _, err := Select().From("x").ToSQL()
	assert.Error(t, err)
}

func TestSelectBuilderPlaceholders(t *testing.T) {
	b := Select("test").Where("x = ? AND y = ?")

	sql, _, _ := b.PlaceholderFormat(Question).ToSQL()
	assert.Equal(t, "SELECT test WHERE x = ? AND y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSQL()
	assert.Equal(t, "SELECT test WHERE x = $1 AND y = $2", sql)
}

func TestSelectBuilderSimpleJoin(t *testing.T) {

	expectedSQL := "SELECT * FROM bar JOIN baz ON bar.foo = baz.foo"
	expectedArgs := []interface{}(nil)

	b := Select("*").From("bar").Join("baz ON bar.foo = baz.foo")

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, args, expectedArgs)
}

func TestSelectBuilderParamJoin(t *testing.T) {

	expectedSQL := "SELECT * FROM bar JOIN baz ON bar.foo = baz.foo AND baz.foo = ?"
	expectedArgs := []interface{}{42}

	b := Select("*").From("bar").Join("baz ON bar.foo = baz.foo AND baz.foo = ?", 42)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, args, expectedArgs)
}

func TestSelectFlags(t *testing.T) {
	assert.Equal(t, Select("").IsCAS(), false)
	assert.Equal(t, Select("").StmtType(), SelectStmtType)
}

func TestSelectAccessor(t *testing.T) {
	query := Select("a", "b").From("c").Where(Eq{"i": 1})
	selectAccessor := query.GetData()

	wp := selectAccessor.GetWhereParts()
	assert.Equal(t, 1, len(wp))
	wpStr, wpArgs, err := wp[0].ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "i = ?", wpStr)
	assert.Equal(t, []interface{}{1}, wpArgs)

	assert.Equal(t, "c", selectAccessor.GetResource())

	cols := selectAccessor.GetColumns()
	assert.Equal(t, 2, len(cols))
	colStr, _, err := cols[0].(Sqlizer).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "a", colStr)
	colStr, _, err = cols[1].(Sqlizer).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "b", colStr)
}
