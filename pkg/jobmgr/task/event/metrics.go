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

package event

import (
	"github.com/uber-go/tally"
	"github.com/uber/peloton/.gen/mesos/v1"
)

// Metrics is the struct containing all the counters that track
// internal state of the task updater.
type Metrics struct {
	SkipOrphanTasksTotal tally.Counter

	TasksFailedTotal    tally.Counter
	TasksLostTotal      tally.Counter
	TasksKilledTotal    tally.Counter
	TasksSucceededTotal tally.Counter
	TasksRunningTotal   tally.Counter
	TasksLaunchedTotal  tally.Counter
	TasksStartingTotal  tally.Counter

	TasksHealthyTotal   tally.Counter
	TasksUnHealthyTotal tally.Counter

	TasksReconciledTotal tally.Counter

	// metrics for in-place update/restart success rate
	TasksInPlacePlacementTotal   tally.Counter
	TasksInPlacePlacementSuccess tally.Counter

	TasksFailedReason map[int32]tally.Counter
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		SkipOrphanTasksTotal: scope.Counter("skip_orphan_task_total"),

		TasksFailedTotal:    scope.Counter("tasks_failed_total"),
		TasksLostTotal:      scope.Counter("tasks_lost_total"),
		TasksSucceededTotal: scope.Counter("tasks_succeeded_total"),
		TasksKilledTotal:    scope.Counter("tasks_killed_total"),
		TasksRunningTotal:   scope.Counter("tasks_running_total"),
		TasksLaunchedTotal:  scope.Counter("tasks_launched_total"),
		TasksStartingTotal:  scope.Counter("tasks_starting_total"),

		TasksHealthyTotal:   scope.Counter("tasks_healthy_total"),
		TasksUnHealthyTotal: scope.Counter("tasks_unhealthy_total"),

		TasksInPlacePlacementTotal:   scope.Counter("tasks_in_place_placement_total"),
		TasksInPlacePlacementSuccess: scope.Counter("tasks_in_place_placement_success"),

		TasksReconciledTotal: scope.Counter("tasks_reconciled_total"),
		TasksFailedReason:    newTasksFailedReasonScope(scope),
	}
}

// newTasksFailedReasonScope creates a map of task failed reason to counter
func newTasksFailedReasonScope(scope tally.Scope) map[int32]tally.Counter {
	var taggedScopes = make(map[int32]tally.Counter)
	for reasonID, reasonName := range mesos_v1.TaskStatus_Reason_name {
		taggedScopes[reasonID] = scope.Tagged(map[string]string{"reason": reasonName}).Counter("task_failed")
	}
	return taggedScopes
}
