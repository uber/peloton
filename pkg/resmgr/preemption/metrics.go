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

package preemption

import (
	"github.com/uber/peloton/pkg/common/scalar"

	"github.com/uber-go/tally"
)

// Metrics is a placeholder for all metrics in preemption
type Metrics struct {
	RevocableRunningTasksToPreempt    tally.Counter
	RevocableNonRunningTasksToPreempt tally.Counter

	NonRevocableRunningTasksToPreempt    tally.Counter
	NonRevocableNonRunningTasksToPreempt tally.Counter

	PreemptionQueueSize tally.Gauge
	TasksToEvict        tally.Gauge

	TasksFailedPreemption tally.Counter

	NonSlackTotalResourcesToFree          scalar.CounterMaps
	NonSlackNonRunningTasksResourcesFreed scalar.CounterMaps
	NonSlackRunningTasksResourcesToFreed  scalar.CounterMaps

	SlackTotalResourcesToFree          scalar.CounterMaps
	SlackNonRunningTasksResourcesFreed scalar.CounterMaps
	SlackRunningTasksResourcesToFreed  scalar.CounterMaps

	OverAllocationCount tally.Gauge
}

// NewMetrics returns a new instance of preemption.Metrics
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		RevocableRunningTasksToPreempt:    scope.Counter("revocable_running_tasks"),
		RevocableNonRunningTasksToPreempt: scope.Counter("revocable_non_running_tasks"),

		NonRevocableRunningTasksToPreempt:    scope.Counter("non_revocable_running_tasks"),
		NonRevocableNonRunningTasksToPreempt: scope.Counter("non_revocable_non_running_tasks"),

		TasksFailedPreemption: scope.Counter("num_tasks_failed"),

		PreemptionQueueSize: scope.Gauge("preemption_queue_size"),
		TasksToEvict:        scope.Gauge("tasks_to_evict"),

		NonSlackTotalResourcesToFree:          scalar.NewCounterMaps(scope.SubScope("non_slack_total_resources_to_free")),
		NonSlackNonRunningTasksResourcesFreed: scalar.NewCounterMaps(scope.SubScope("non_slack_non_running_tasks_resources_freed")),
		NonSlackRunningTasksResourcesToFreed:  scalar.NewCounterMaps(scope.SubScope("non_slack_running_tasks_resources_freed")),

		SlackTotalResourcesToFree:          scalar.NewCounterMaps(scope.SubScope("slack_total_resources_to_free")),
		SlackNonRunningTasksResourcesFreed: scalar.NewCounterMaps(scope.SubScope("slack_non_running_tasks_resources_freed")),
		SlackRunningTasksResourcesToFreed:  scalar.NewCounterMaps(scope.SubScope("slack_running_tasks_resources_freed")),

		OverAllocationCount: scope.Gauge("over_allocation_count"),
	}
}
