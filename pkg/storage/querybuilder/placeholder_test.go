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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuestion(t *testing.T) {
	sql := "x = ? AND y = ?"
	s, _ := Question.ReplacePlaceholders(sql)
	assert.Equal(t, sql, s)
}

func TestDollar(t *testing.T) {
	sql := "x = ? AND y = ?"
	s, _ := Dollar.ReplacePlaceholders(sql)
	assert.Equal(t, "x = $1 AND y = $2", s)
}

func TestPlaceholders(t *testing.T) {
	assert.Equal(t, Placeholders(2), "?,?")
}

func TestEscape(t *testing.T) {
	sql := "SELECT uuid, \"data\" #> '{tags}' AS tags FROM nodes WHERE  \"data\" -> 'tags' ??| array['?'] AND enabled = ?"
	s, _ := Dollar.ReplacePlaceholders(sql)
	assert.Equal(t, "SELECT uuid, \"data\" #> '{tags}' AS tags FROM nodes WHERE  \"data\" -> 'tags' ?| array['$1'] AND enabled = $2", s)
}

func BenchmarkPlaceholdersArray(b *testing.B) {
	var count = b.N
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = "?"
	}
	var _ = strings.Join(placeholders, ",")
}

func BenchmarkPlaceholdersStrings(b *testing.B) {
	Placeholders(b.N)
}
