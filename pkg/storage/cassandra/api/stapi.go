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

/*
Package api provides data access management to structured storage backend

a very simple usage example is:

err := storage.Initialize(Option{})
defer storage.Close()

store, err := storage.OpenDataStore("test_schema_name")
queryBuilder := store.NewQuery()
statement := queryBuilder.Insert("user").Columns("name", "count").Values("json", 1234)
result := store.Execute(statement)
allResults, err := result.All()
*/
package api

import (
	"context"
	"errors"

	"github.com/gocql/gocql"
	qb "github.com/uber/peloton/pkg/storage/querybuilder"
)

// Statement represents a query against the backend data store.
// There are 4 kinds of statements: qb.SelectBuilder, qb.InsertBuilder, qb.UpdateBuilder, and qb.DeleteBuilder
type Statement interface {
	ToSQL() (string, []interface{}, error)
	ToUql() (string, []interface{}, map[string]interface{}, error)
	StmtType() qb.StmtType
	IsCAS() bool
	GetData() qb.StatementAccessor
}

// ResultSet contains the result of a executed query
type ResultSet interface {

	// All returns all results in a SliceMap and cleans up itself.
	All(ctx context.Context) ([]map[string]interface{}, error)

	// One is deprecated
	One(ctx context.Context, dest ...interface{}) bool

	// Close closes the underlying iterator and returns any errors
	// that happened during the query or the iteration.
	Close() error

	// Applied shows whether a set was done in a compare-and-set operation
	Applied() bool

	// Next returns the next row out of the ResultSet. Empty map indicates
	// the end of ResultSet
	Next(ctx context.Context) (map[string]interface{}, error)

	// PagingState returns the paging state for the query
	PagingState() []byte
}

// DataStore represents a connection with the backend data store
// DataStore is safe for concurrent use by multiple goroutines
type DataStore interface {

	// Execute a query and return a ResultSet
	Execute(ctx context.Context, stmt Statement) (ResultSet, error)

	// ExecuteBatch make a single RPC call for multiple statement execution.
	ExecuteBatch(ctx context.Context, stmts []Statement) error

	// NewQuery creates a QueryBuilder object
	NewQuery() QueryBuilder

	// Returns the name of this DataStore
	Name() string
}

// QueryBuilder is responsible for constructing queries
type QueryBuilder interface {

	// Select returns a select query
	Select(columns ...string) qb.SelectBuilder

	// Insert returns an insert query.
	Insert(into string) qb.InsertBuilder

	// Update returns a update query.
	Update(table string) qb.UpdateBuilder

	// Delete returns a delete query
	Delete(from string) qb.DeleteBuilder
}

// Public error values
var (
	// ErrUninitialized means storage system was not initialized
	ErrUninitialized = errors.New("Storage system not initialized")

	// ErrUnsupported indicates errors for unsupported features
	ErrUnsupported = errors.New("Feature not supported")

	// ErrConnection indicates errors for connection problems
	ErrConnection = errors.New("Fail to connect to backend nodes")

	// ErrClosed means back end data store is closed
	ErrClosed = errors.New("Data store closed")

	// ErrInvalidUsage is thrown when user makes an invalid call
	ErrInvalidUsage = errors.New("Usage is not supported")

	// ErrOverCapacity means the client is handling too many concurrent go routines.
	ErrOverCapacity = errors.New("Over capacity")

	// ErrUnauthorized indicates the operation was not allowed for the current requester
	ErrUnauthorized = errors.New("Unauthorized")
)

// Public consistency levels for use in ConsistencyOverride
// Semantics of each level are available here https://docs.datastax.com/en/cassandra/3.0/cassandra/dml/dmlConfigConsistency.html
const (
	// ConsistencyAny maps to cassandra consistency "ANY"
	ConsistencyAny = gocql.Any
	// ConsistencyOne maps to cassandra consistency "ONE"
	ConsistencyOne = gocql.One
	// ConsistencyTwo maps to cassandra consistency "TWO"
	ConsistencyTwo = gocql.Two
	// ConsistencyThree maps to cassandra consistency "THREE"
	ConsistencyThree = gocql.Three
	// ConsistencyQuorum maps to cassandra consistency "QUORUM"
	ConsistencyQuorum = gocql.Quorum
	// ConsistencyAll maps to cassandra consistency "ALL"
	ConsistencyAll = gocql.All
	// ConsistencyAll maps to cassandra consistency "LOCAL_QUORUM"
	ConsistencyLocalQuorum = gocql.LocalQuorum
	// ConsistencyEachQuorum maps to cassandra consistency "EACH_QUORUM"
	ConsistencyEachQuorum = gocql.EachQuorum
	// ConsistencyLocalOne maps to cassandra consistency "LOCAL_ONE"
	ConsistencyLocalOne = gocql.LocalOne
)

type contextKey string

// TagKey is used to reference tags in the context
const TagKey = contextKey("stapi.tags")

// QueryOverridesKey is used to reference query level config overrides in the context
const QueryOverridesKey = contextKey("stapi.query.overrides")

// ContextWithTags returns a context with tags
func ContextWithTags(ctx context.Context, t map[string]string) context.Context {
	return context.WithValue(ctx, TagKey, t)
}

// ContextWithQueryOverrides returns a context with query overrides
func ContextWithQueryOverrides(ctx context.Context, queryOverrides *QueryOverrides) context.Context {
	return context.WithValue(ctx, QueryOverridesKey, queryOverrides)
}

// FuncType is a function being decorated
type FuncType func() error

// Decorator is a function that decorates a function
type Decorator func(ef FuncType) FuncType

// ConsistencyOverride represents a query level override to the default consistency
type ConsistencyOverride struct {
	Value gocql.Consistency
}

// QueryOverrides represent query level overrides to the default configuration
type QueryOverrides struct {
	Consistency *ConsistencyOverride
}
