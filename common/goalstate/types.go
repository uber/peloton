package goalstate

import (
	"context"
)

// Entity defines the interface of an item which can queued into the goal state engine.
type Entity interface {
	// GetID fetches the identifier of the entity.
	GetID() string
	// GetState fetches the current state of the entity.
	GetState() interface{}
	// GetGoalState fetches the current goal state of the entity.
	GetGoalState() interface{}
	// GetActionList fetches the set of actions which need to be executed
	// for the entity's current state and goal state.
	// The action list is an array of actions which are executed in order.
	// Each action needs to be idempotent.
	GetActionList(state interface{}, goalstate interface{}) (context.Context, context.CancelFunc, []Action)
}

// Action defines the interface for the function to be used by goal state actions.
// If the action call returns an error, the entity is rescheduled in the goal state
// engine with an exponential backoff.
type Action func(ctx context.Context, entity Entity) error
