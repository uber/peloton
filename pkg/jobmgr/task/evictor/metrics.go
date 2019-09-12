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

package evictor

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track internal state
// of task evictor.
type Metrics struct {
	TaskEvictPreemptionSuccess tally.Counter
	TaskEvictPreemptionFail    tally.Counter

	TaskEvictHostMaintenanceSuccess tally.Counter
	TaskEvictHostMaintenanceFail    tally.Counter

	GetPreemptibleTasksCallDuration tally.Timer

	GetTasksOnDrainingHostsCallDuration tally.Timer
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	taskSuccessScope := scope.Tagged(map[string]string{"result": "success"})
	taskFailScope := scope.Tagged(map[string]string{"result": "fail"})
	getTasksToPreemptScope := scope.SubScope("get_preemptible_tasks")
	getTasksOnDrainingHostsScope := scope.SubScope("get_tasks_on_draining_hosts")

	return &Metrics{
		TaskEvictPreemptionSuccess: taskSuccessScope.Counter("preempt"),
		TaskEvictPreemptionFail:    taskFailScope.Counter("preempt"),

		TaskEvictHostMaintenanceSuccess: taskSuccessScope.Counter("host_maintenance"),
		TaskEvictHostMaintenanceFail:    taskFailScope.Counter("host_maintenance"),

		GetPreemptibleTasksCallDuration: getTasksToPreemptScope.Timer("call_duration"),

		GetTasksOnDrainingHostsCallDuration: getTasksOnDrainingHostsScope.Timer("call_duration"),
	}
}
