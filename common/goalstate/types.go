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
	GetActionList(state interface{}, goalState interface{}) (
		context.Context, context.CancelFunc, []Action)
}

// ActionExecute defines the interface for the function to be used by the
// goal state engine clients to implement the execution of an action.
type ActionExecute func(ctx context.Context, entity Entity) error

// Action defines the interface for the function to be used by goal
// state actions. If the Execute call returns an error, the entity is
// rescheduled in the goal state engine with an exponential backoff.
type Action struct {
	// Name of the action which will be used as the tag in the emitted metrics.
	Name string
	// Execute is the function called by the goal state
	// engine to execute the action.
	Execute ActionExecute
}
