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
	"reflect"

	"github.com/uber/peloton/pkg/storage/objects/base"

	"go.uber.org/yarpc/yarpcerrors"
)

// Client defines the methods to operate with storage objects
type Client interface {
	// CreateIfNotExists creates the storage object in the database if it
	// doesn't already exist
	CreateIfNotExists(ctx context.Context, e base.Object) error
	// Create creates the storage object in the database
	Create(ctx context.Context, e base.Object) error
	// Get gets the storage object from the database
	Get(ctx context.Context, e base.Object, fieldsToRead ...string) (
		map[string]interface{}, error)
	// GetAll gets all the storage objects for the partition key from the
	// database
	GetAll(ctx context.Context, e base.Object) ([]map[string]interface{}, error)
	// GetAllIter provides an iterative way to fetch all storage objects
	// for the partition key
	GetAllIter(ctx context.Context, e base.Object) (Iterator, error)
	// Update updates the storage object in the database
	// The fields to be updated can be specified as fieldsToUpdate which is
	// a variable list of field names and is to be optionally specified by
	// the caller. If not specified, all fields in the object will be updated
	// to the DB
	Update(ctx context.Context, e base.Object, fieldsToUpdate ...string) error
	// Delete deletes the storage object from the database
	Delete(ctx context.Context, e base.Object) error
}

type client struct {
	objectIndex map[reflect.Type]*Table
	connector   Connector
}

// NewClient returns a new ORM client for the base instance and
// connector provided.
func NewClient(conn Connector, objects ...base.Object) (Client, error) {
	oi, err := BuildObjectIndex(objects)
	if err != nil {
		return nil, err
	}
	return &client{
		objectIndex: oi,
		connector:   conn,
	}, nil
}

// getTable gets the base Table structure that matches the base instance
// provided. Return an error when not found.
func (c *client) getTable(e base.Object) (*Table, error) {
	t := reflect.TypeOf(e).Elem()
	table, ok := c.objectIndex[t]
	if !ok {
		return nil, yarpcerrors.NotFoundErrorf(
			"Table not found for base: %q", t.Name())
	}
	return table, nil
}

// CreateIfNotExists creates the storage object in the database if it doesn't
// already exist
func (c *client) CreateIfNotExists(ctx context.Context, e base.Object) error {
	// lookup if a table exists for this object, return error if not found
	table, err := c.getTable(e)
	if err != nil {
		return err
	}

	// Tell the connector to create a row in the DB using this row if it
	// doesn't already exist
	return c.connector.CreateIfNotExists(
		ctx,
		&table.Definition,
		table.GetRowFromObject(e),
	)
}

// Create creates the storage object in the database
func (c *client) Create(ctx context.Context, e base.Object) error {
	// lookup if a table exists for this object, return error if not found
	table, err := c.getTable(e)
	if err != nil {
		return err
	}

	// Tell the connector to create a row in the DB using this row
	return c.connector.Create(ctx, &table.Definition, table.GetRowFromObject(e))
}

// Get fetches an base by primary key, The base provided must contain
// values for all components of its primary key for the operation to succeed.
func (c *client) Get(
	ctx context.Context,
	e base.Object,
	fieldsToRead ...string) (map[string]interface{}, error) {

	// lookup if a table exists for this object, return error if not found
	table, err := c.getTable(e)
	if err != nil {
		return nil, err
	}

	// build a primary key row from storage object
	keyRow := table.GetKeyRowFromObject(e)

	row, err := c.connector.Get(ctx, &table.Definition, keyRow, fieldsToRead...)
	if err != nil {
		return nil, err
	}

	return row, nil
}

// GetAll fetches a list of base objects for the given partition key and
// clustering keys
// The base object provided must contain the value of its partition key and
// clustering keys
func (c *client) GetAll(
	ctx context.Context,
	e base.Object,
) ([]map[string]interface{}, error) {

	// lookup if a table exists for this object, return error if not found
	table, err := c.getTable(e)
	if err != nil {
		return nil, err
	}

	// build a partition and clustering key row from storage object
	keyRow := table.GetKeyRowFromObject(e)

	rows, err := c.connector.GetAll(ctx, &table.Definition, keyRow)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

// GetAllIter fetches a list of base objects for the given partition key
// using an iterator. The base object provided must contain the value of
// its partition key
func (c *client) GetAllIter(
	ctx context.Context,
	e base.Object,
) (Iterator, error) {

	// lookup if a table exists for this object, return error if not found
	table, err := c.getTable(e)
	if err != nil {
		return nil, err
	}

	// build a partition and clustering key row from storage object
	keyRow := table.GetKeyRowFromObject(e)

	return c.connector.GetAllIter(ctx, &table.Definition, keyRow)
}

// Update updates the storage object in the database
func (c *client) Update(
	ctx context.Context,
	e base.Object,
	fieldsToUpdate ...string,
) error {
	// lookup if a table exists for this object, return error if not found
	table, err := c.getTable(e)
	if err != nil {
		return err
	}

	// translate the storage object into a row (list of column)
	row := table.GetRowFromObject(e, fieldsToUpdate...)

	// build a primary key row from storage object
	keyRow := table.GetKeyRowFromObject(e)

	// Tell the connector to update a row in the DB using this row
	return c.connector.Update(ctx, &table.Definition, row, keyRow)
}

// Delete deletes the storage object in the database
func (c *client) Delete(ctx context.Context, e base.Object) error {
	// lookup if a table exists for this object, return error if not found
	table, err := c.getTable(e)
	if err != nil {
		return err
	}

	// build a primary key row from storage object
	keyRow := table.GetKeyRowFromObject(e)

	// Tell the connector to delete the row in the DB using this keyRow
	return c.connector.Delete(ctx, &table.Definition, keyRow)
}
