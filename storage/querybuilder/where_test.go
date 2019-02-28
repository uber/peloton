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

	"bytes"

	"github.com/stretchr/testify/assert"
)

func TestWherePartsAppendToSql(t *testing.T) {
	parts := []Sqlizer{
		newWherePart("x = ?", 1),
		newWherePart(nil),
		newWherePart(Eq{"y": 2}),
	}
	sql := &bytes.Buffer{}
	args, _ := appendToSQL(parts, sql, " AND ", []interface{}{})
	assert.Equal(t, "x = ? AND y = ?", sql.String())
	assert.Equal(t, []interface{}{1, 2}, args)
}

func TestWherePartsAppendToSqlErr(t *testing.T) {
	parts := []Sqlizer{newWherePart(1)}
	_, err := appendToSQL(parts, &bytes.Buffer{}, "", []interface{}{})
	assert.Error(t, err)
}

func TestWherePartNil(t *testing.T) {
	sql, _, _ := newWherePart(nil).ToSQL()
	assert.Equal(t, "", sql)
}

func TestWherePartErr(t *testing.T) {
	_, _, err := newWherePart(1).ToSQL()
	assert.Error(t, err)
}

func TestWherePartString(t *testing.T) {
	sql, args, _ := newWherePart("x = ?", 1).ToSQL()
	assert.Equal(t, "x = ?", sql)
	assert.Equal(t, []interface{}{1}, args)
}

func TestWherePartMap(t *testing.T) {
	test := func(pred interface{}) {
		sql, _, _ := newWherePart(pred).ToSQL()
		expect := []string{"x = ? AND y = ?", "y = ? AND x = ?"}
		if sql != expect[0] && sql != expect[1] {
			t.Errorf("expected one of %#v, got %#v", expect, sql)
		}
	}
	m := map[string]interface{}{"x": 1, "y": 2}
	test(m)
	test(Eq(m))
}
