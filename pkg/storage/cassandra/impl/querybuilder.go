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

package impl

import (
	"github.com/lann/builder"

	qb "github.com/uber/peloton/pkg/storage/querybuilder"
)

// QueryBuilder is responsible for building queries
type QueryBuilder struct {
}

func (s *QueryBuilder) emptyBuilder() qb.StatementBuilderType {
	return qb.StatementBuilderType(builder.EmptyBuilder).PlaceholderFormat(qb.Question)
}

// Select returns a select query
func (s *QueryBuilder) Select(columns ...string) qb.SelectBuilder {
	return s.emptyBuilder().Select(columns...)
}

// Insert returns an insert query
func (s *QueryBuilder) Insert(into string) qb.InsertBuilder {
	return s.emptyBuilder().Insert(into)
}

// Update returns a update query
func (s *QueryBuilder) Update(table string) qb.UpdateBuilder {
	return s.emptyBuilder().Update(table)
}

// Delete returns a delete query
func (s *QueryBuilder) Delete(from string) qb.DeleteBuilder {
	return s.emptyBuilder().Delete(from)
}
