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

package statemachine

// State defines the state
type State string

// Transition defines the transition for the callback
// which will be passed to the call back function
// during the transition
type Transition struct {
	// StateMachine object
	StateMachine StateMachine

	// From which state transition is happening
	From State

	// TO which state transition is happening
	To State

	// Arguments passed during the transition
	// which will be passed to callback function
	Params []interface{}
}
