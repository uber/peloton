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

package activermtask

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters and timers that track
type Metrics struct {
	ActiveTaskQuerySuccess tally.Counter
	ActiveTaskQueryFail    tally.Counter
	UpdaterRMTasksDuraion  tally.Timer
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	activeTaskQuerySuccess := scope.Tagged(map[string]string{"result": "success"})
	activeTaskQueryFailed := scope.Tagged(map[string]string{"result": "fail"})

	return &Metrics{
		ActiveTaskQuerySuccess: activeTaskQuerySuccess.Counter("active_task_query"),
		ActiveTaskQueryFail:    activeTaskQueryFailed.Counter("active_task_query"),
		UpdaterRMTasksDuraion:  scope.Timer("update_rm_tasks_duration"),
	}

}
