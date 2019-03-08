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
Package goalstate implements a engine to drive a goal state machine.
Any item enqueued into the goal state engine needs to implement the
Entity interface. The enqueued entity should be able to fetch a unique
string identifier, its state and goal state, and should be able to fetch
an action list for a given state and goal state. Each action should
implement the action function interface.
The engine provides an Enqueue function which can be used to enqueue
an entity with a deadline of when it is should be dequeued for evaluation.
When its deadline expires, the action list corresponding to its state and
goal state are executed in order. If any of the actions return an error on
execution, the entity is requeued for evaluation with an exponential backoff.
*/
package goalstate
