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

package orm

import (
	"context"

	"github.com/uber/peloton/pkg/storage/objects/base"
)

// Connector is the interface that must be implemented for a backend service
type Connector interface {
	// CreateIfNotExists creates a row in the DB for the base object if it
	// doesn't already exist
	CreateIfNotExists(
		ctx context.Context,
		e *base.Definition,
		values []base.Column,
	) error

	// Create creates a row in the DB for the base object
	Create(ctx context.Context, e *base.Definition, values []base.Column) error

	// Get fetches a row by primary key of base object
	Get(
		ctx context.Context,
		e *base.Definition,
		keys []base.Column,
		colNamesToRead ...string,
	) (map[string]interface{}, error)

	// GetAll fetches a list of base objects for the partition key
	// and the given clustering keys
	GetAll(
		ctx context.Context,
		e *base.Definition,
		keys []base.Column,
	) ([]map[string]interface{}, error)

	GetAllIter(
		ctx context.Context,
		e *base.Definition,
		keys []base.Column,
	) (Iterator, error)

	// Update updates a row in the DB for the base object
	Update(
		ctx context.Context,
		e *base.Definition,
		values []base.Column,
		keys []base.Column,
	) error

	// Delete deletes a row from the DB for the base object
	Delete(ctx context.Context, e *base.Definition, keys []base.Column) error
}

// Iterator allows the caller to iterate over the results of a query.
type Iterator interface {
	// Next fetches the next row of the result. On reaching the end of
	// results, it returns nil. Error is returned if there is a failure
	// during iteration. Next should not be called once error is returned
	// or Close is called.
	Next() ([]base.Column, error)

	// Close indicates that iterator is no longer required and so any
	// clean-up actions may be performed.
	Close()
}
