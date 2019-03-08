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
	"database/sql"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
)

func TestEqToSql(t *testing.T) {
	b := Eq{"id": 1}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestEqInToSql(t *testing.T) {
	b := Eq{"id": []int{1, 2, 3}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id IN (?,?,?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestNotEqToSql(t *testing.T) {
	b := NotEq{"id": 1}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id <> ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestEqNotInToSql(t *testing.T) {
	b := NotEq{"id": []int{1, 2, 3}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id NOT IN (?,?,?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestEqInEmptyToSql(t *testing.T) {
	b := Eq{"id": []int{}}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id IN (NULL)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{}
	assert.Equal(t, expectedArgs, args)
}

func TestLtToSql(t *testing.T) {
	b := Lt{"id": 1}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id < ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestLtOrEqToSql(t *testing.T) {
	b := LtOrEq{"id": 1}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id <= ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestGtToSql(t *testing.T) {
	b := Gt{"id": 1}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id > ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestGtOrEqToSql(t *testing.T) {
	b := GtOrEq{"id": 1}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)

	expectedSQL := "id >= ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestExprNilToSql(t *testing.T) {
	var b Sqlizer
	b = NotEq{"name": nil}
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)

	expectedSQL := "name IS NOT NULL"
	assert.Equal(t, expectedSQL, sql)

	b = Eq{"name": nil}
	sql, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)

	expectedSQL = "name IS NULL"
	assert.Equal(t, expectedSQL, sql)
}

func TestNullTypeString(t *testing.T) {
	var b Sqlizer
	var name sql.NullString

	b = Eq{"name": name}
	sql, args, err := b.ToSQL()

	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "name IS NULL", sql)

	name.Scan("Name")
	b = Eq{"name": name}
	sql, args, err = b.ToSQL()

	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"Name"}, args)
	assert.Equal(t, "name = ?", sql)
}

func TestNullTypeInt64(t *testing.T) {
	var userID sql.NullInt64
	userID.Scan(nil)
	b := Eq{"user_id": userID}
	sql, args, err := b.ToSQL()

	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "user_id IS NULL", sql)

	userID.Scan(int64(10))
	b = Eq{"user_id": userID}
	sql, args, err = b.ToSQL()

	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(10)}, args)
	assert.Equal(t, "user_id = ?", sql)
}

func TestUUID(t *testing.T) {
	id1, _ := ParseUUID("e3a94b81-ebb3-4607-8a65-10a30fab0efd")
	assert.Equal(t, true, IsUUID(id1) && IsUUID(&id1))

	id2, _ := gocql.ParseUUID("e3a94b81-ebb3-4607-8a65-10a30fab0efd")
	assert.Equal(t, true, IsUUID(id2) && IsUUID(&id2))

	assert.Equal(t, false, IsUUID("abc"))
	assert.Equal(t, false, IsUUID(nil))
}
