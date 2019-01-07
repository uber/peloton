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

package api

import "context"

// RangeFilter represents a ranged entity key.
type RangeFilter struct {
	start  interface{}
	end    interface{}
	values []interface{}
}

// EntityKey is used by a Table object to run retrieve or delete
// operations.
type EntityKey interface {

	// TableName returns the name of the table
	TableName() string

	// Filter returns a filtering condition that is applied to key columns.
	// This is typically what is in the "Where" clause of a query
	Filter() (cql string, args []interface{}, err error)
}

// Table allows CRUD operations against a single table.
type Table interface {

	// NewKey creates a EntityKey object with the given values
	NewKey(obj interface{}, value ...interface{}) EntityKey

	// NewRangeKey creates a EntityKey object representing a range query on key
	// columns
	NewRangeKey(obj interface{}, filter RangeFilter) EntityKey

	// Get retrieves the object by its key. fields parameter allows users to
	// specify a subset of columns that they want to retrieve. all fields
	// are retrieved if fields is nil
	Get(ctx context.Context, key EntityKey, obj *interface{}, fields []string) error

	// Delete removes the object represented by the key
	Delete(ctx context.Context, key EntityKey) error

	// UpSert adds the object to the store regardless of its prior existence.
	UpSert(ctx context.Context, obj *interface{}) error

	// CompareAndSet persists the object only when the record dose not already
	// exist.
	CompareAndSet(ctx context.Context, obj *interface{}) error
}

// MultiTable allows CRUD operations against a collection of tables.
type MultiTable interface {

	// NewKey creates a EntityKey object with the given values
	NewKey(obj interface{}, value ...[]interface{}) []EntityKey

	// NewRangeKey creates a EntityKey object representing a range query on key
	// columns
	NewRangeKey(obj interface{}, filters []RangeFilter) []EntityKey

	Get(ctx context.Context, keys []EntityKey, objs *[]interface{}, fields []string) error

	// Delete removes all the objects represented by the keys
	Delete(ctx context.Context, keys []EntityKey) error

	// UpSert adds the object to the store regardless of its prior existence.
	UpSert(ctx context.Context, objs *[]interface{}) error

	// CompareAndSet persists the object only when the record dose not already
	// exist.
	CompareAndSet(ctx context.Context, objs *[]interface{}) error
}
