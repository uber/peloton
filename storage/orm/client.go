package orm

import (
	"context"
	"reflect"

	"code.uber.internal/infra/peloton/storage/objects/base"

	"go.uber.org/yarpc/yarpcerrors"
)

// Client defines the methods to operate with storage objects
type Client interface {
	// Create creates the storage object in the database
	Create(ctx context.Context, e base.Object) error
	// Create gets the storage object from the database
	Get(ctx context.Context, e base.Object) error
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

// Create creates the storage object in the database
func (c *client) Create(ctx context.Context, e base.Object) error {
	// lookup if a table exists for this object, return error if not found
	table, err := c.getTable(e)
	if err != nil {
		return err
	}

	// translate the storage object into a row (list of column)
	row := table.GetRowFromObject(e)

	// Tell the connector to create a row in the DB using this row
	return c.connector.Create(ctx, &table.Definition, row)
}

// Get fetches an base by primary key, The base provided must contain
// values for all components of its primary key for the operation to succeed.
func (c *client) Get(ctx context.Context, e base.Object) error {

	// lookup if a table exists for this object, return error if not found
	table, err := c.getTable(e)
	if err != nil {
		return err
	}

	// build a primary key row from storage object
	keyRow := table.GetKeyRowFromObject(e)

	row, err := c.connector.Get(ctx, &table.Definition, keyRow)
	if err != nil {
		return err
	}

	// build a storage object from the row
	table.SetObjectFromRow(e, row)

	return nil
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
