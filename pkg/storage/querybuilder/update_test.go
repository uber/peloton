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

func TestUpdateBuilderToSql(t *testing.T) {
	b := Update("").
		Table("a").
		Set("b", expression("? + 1", 1)).
		SetMap(Eq{"c": 2}).
		Where("d = ?", 3).
		Using("TTL ?", 550)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL :=
		"UPDATE a " +
			"USING TTL ? " +
			"SET b = ? + 1, c = ? WHERE d = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{550, 1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderToSqlErr(t *testing.T) {
	_, _, err := Update("").Set("x", 1).ToSQL()
	assert.Error(t, err)

	_, _, err = Update("x").ToSQL()
	assert.Error(t, err)
}

func TestUpdateBuilderPlaceholders(t *testing.T) {
	b := Update("test").SetMap(Eq{"x": 1, "y": 2})

	sql, _, _ := b.PlaceholderFormat(Question).ToSQL()
	assert.Equal(t, "UPDATE test SET x = ?, y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSQL()
	assert.Equal(t, "UPDATE test SET x = $1, y = $2", sql)
}

func TestUpdateBuilderAddToSql(t *testing.T) {
	b := Update("").
		Table("to").
		Set("a", 1).
		Add("b", []string{"v"}).
		Remove("c", []string{"x", "y"}).
		Where("d = ?", 3)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL :=
		"UPDATE to SET a = ?, b = b + ?, c = c - ? WHERE d = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1, []string{"v"}, []string{"x", "y"}, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderRemoveToSql(t *testing.T) {
	b := Update("").
		Table("a").
		Remove("b", []string{"v"}).
		Where("d = ?", 3)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL :=
		"UPDATE a SET b = b - ? WHERE d = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{[]string{"v"}, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderIfOnly(t *testing.T) {
	b := Update("").
		Table("T").
		Set("a", 1).
		IfOnly("b > ?", 2).
		Where("c = ?", 3)

	assert.True(t, b.IsCAS())
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedCQL := "UPDATE T SET a = ? WHERE c = ? IF b > ?"
	assert.Equal(t, expectedCQL, sql)

	expectedArgs := []interface{}{1, 3, 2}
	assert.Equal(t, expectedArgs, args)

	sql, args, options, err := b.ToUql()
	assert.NoError(t, err)
	assert.Equal(t, options["IsCAS"].(bool), true)
}

func TestUpdateBuilderAddSTApplyMetadataEmpty(t *testing.T) {
	b := Update("").
		Table("a").
		Set("b", 1).
		Add("d", []string{"v"}).
		Remove("e", []string{"v"}).
		AddSTApplyMetadata([]byte("")).
		Where("c = ?", 3)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE a SET b = ?, st_apply_metadata = ?, d = d + ?, e = e - ? WHERE c = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1, []byte(""), []string{"v"}, []string{"v"}, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderAddSTApplyMetadata(t *testing.T) {
	b := Update("").
		Table("a").
		Set("b", 1).
		Add("d", []string{"v"}).
		Remove("e", []string{"v"}).
		AddSTApplyMetadata([]byte("a,b")).
		Where("c = ?", 3)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "UPDATE a SET b = ?, st_apply_metadata = ?, d = d + ?, e = e - ? WHERE c = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1, []byte("a,b"), []string{"v"}, []string{"v"}, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateFlags(t *testing.T) {
	assert.Equal(t, Update("").IsCAS(), false)
	assert.Equal(t, Update("").StmtType(), UpdateStmtType)
}

func TestUpdateAccessor(t *testing.T) {
	query := Update("").Table("a").Set("b", 1).Where("c = ?", 3)
	updateAccessor := query.GetData()

	wp := updateAccessor.GetWhereParts()
	assert.Equal(t, 1, len(wp))
	wpStr, wpArgs, err := wp[0].ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "c = ?", wpStr)
	assert.Equal(t, []interface{}{3}, wpArgs)

	assert.Equal(t, "a", updateAccessor.GetResource())
	assert.Equal(t, 0, len(updateAccessor.GetColumns()))
}
