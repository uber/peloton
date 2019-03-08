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

func TestInsertBuilderToSql(t *testing.T) {
	b := Insert("").
		Into("a").
		Columns("b", "c").
		Values(3, expression("? + 1", 4)).
		IfNotExist().
		Using("TTL ?", 5)

	assert.True(t, b.IsCAS())

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL :=
		"INSERT INTO a (b,c) VALUES (?,? + 1) IF NOT EXISTS " +
			"USING TTL ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{3, 4, 5}
	assert.Equal(t, expectedArgs, args)

	sql, args, options, err := b.ToUql()
	assert.NoError(t, err)
	assert.Equal(t, options["IsCAS"].(bool), true)
}

func TestInsertBuilderAddSTApplyMetadataEmpty(t *testing.T) {
	b := Insert("").
		Into("a").
		Columns("b", "c").
		Values(3, 4).
		AddSTApplyMetadata([]byte("")).
		IfNotExist().
		Using("TTL ?", 5)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL :=
		"INSERT INTO a (b,c,st_apply_metadata) VALUES (?,?,?) IF NOT EXISTS " +
			"USING TTL ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{3, 4, []byte(""), 5}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderAddSTApplyMetadata(t *testing.T) {
	b := Insert("").
		Into("a").
		Columns("b", "c").
		Values(3, 4).
		AddSTApplyMetadata([]byte("a,b")).
		IfNotExist().
		Using("TTL ?", 5)

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL :=
		"INSERT INTO a (b,c,st_apply_metadata) VALUES (?,?,?) IF NOT EXISTS " +
			"USING TTL ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{3, 4, []byte("a,b"), 5}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderToSqlErr(t *testing.T) {
	_, _, err := Insert("").Values(1).ToSQL()
	assert.Error(t, err)

	_, _, err = Insert("x").ToSQL()
	assert.Error(t, err)
}

func TestInsertBuilderPlaceholders(t *testing.T) {
	b := Insert("test").Values(1, 2)

	sql, _, _ := b.PlaceholderFormat(Question).ToSQL()
	assert.Equal(t, "INSERT INTO test VALUES (?,?)", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSQL()
	assert.Equal(t, "INSERT INTO test VALUES ($1,$2)", sql)
}

func TestInsertBuilderSetMap(t *testing.T) {
	b := Insert("table").SetMap(Eq{"field1": 1})

	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO table (field1) VALUES (?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertFlags(t *testing.T) {
	assert.Equal(t, Insert("").IsCAS(), false)
	assert.Equal(t, Insert("").StmtType(), InsertStmtType)
}

func TestInsertAccessor(t *testing.T) {
	query := Insert("").Into("a").Columns("b", "c").Values(3, 4)
	insertAccessor := query.GetData()

	assert.Equal(t, 0, len(insertAccessor.GetWhereParts()))
	assert.Equal(t, "a", insertAccessor.GetResource())
	assert.Equal(t, 0, len(insertAccessor.GetColumns()))
}
