package impl

import (
	"github.com/lann/builder"

	qb "github.com/uber/peloton/storage/querybuilder"
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
