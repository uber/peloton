package orm

import (
	"context"

	"code.uber.internal/infra/peloton/storage/objects/base"
)

// Connector is the interface that must be implemented for a backend service
type Connector interface {
	// Create creates a row in the DB for the base object
	Create(ctx context.Context, e *base.Definition, values []base.Column) error

	// Read fetches a row by primary key of base object
	Get(ctx context.Context, e *base.Definition,
		keys []base.Column) ([]base.Column, error)

	// Update updates a row in the DB for the base object
	Update(ctx context.Context, e *base.Definition,
		values []base.Column, keys []base.Column) error

	// Delete deletes a row from the DB for the base object
	Delete(ctx context.Context, e *base.Definition, keys []base.Column) error
}
