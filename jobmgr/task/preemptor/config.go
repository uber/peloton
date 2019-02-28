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

package preemptor

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track internal state
// of task preemptor.
type Metrics struct {
	TaskPreemptSuccess tally.Counter
	TaskPreemptFail    tally.Counter

	GetPreemptibleTasks             tally.Counter
	GetPreemptibleTasksFail         tally.Counter
	GetPreemptibleTasksCallDuration tally.Timer
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	taskSuccessScope := scope.Tagged(map[string]string{"result": "success"})
	taskFailScope := scope.Tagged(map[string]string{"result": "fail"})
	taskAPIScope := scope.SubScope("task_api")
	getTasksToPreemptScope := scope.SubScope("get_preemptible_tasks")

	return &Metrics{
		TaskPreemptSuccess: taskSuccessScope.Counter("preempt"),
		TaskPreemptFail:    taskFailScope.Counter("preempt"),

		GetPreemptibleTasks:             taskAPIScope.Counter("get_preemptible_tasks"),
		GetPreemptibleTasksFail:         taskFailScope.Counter("get_preemptible_tasks"),
		GetPreemptibleTasksCallDuration: getTasksToPreemptScope.Timer("call_duration"),
	}
}
