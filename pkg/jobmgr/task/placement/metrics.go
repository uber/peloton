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

package placement

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track internal state
// of placement processor.
type Metrics struct {
	// Increment this counter when we see failure to
	// populate the task's volume secret from DB
	TaskPopulateSecretFail   tally.Counter
	TaskRequeuedOnLaunchFail tally.Counter

	GetPlacement              tally.Counter
	GetPlacementFail          tally.Counter
	GetPlacementsCallDuration tally.Timer
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	taskFailScope := scope.Tagged(map[string]string{"result": "fail"})
	taskAPIScope := scope.SubScope("task_api")
	getPlacementScope := scope.SubScope("get_placement")

	return &Metrics{
		GetPlacement:              taskAPIScope.Counter("get_placement"),
		GetPlacementFail:          taskFailScope.Counter("get_placement"),
		GetPlacementsCallDuration: getPlacementScope.Timer("call_duration"),
		TaskPopulateSecretFail:    taskFailScope.Counter("populate_secret"),
		TaskRequeuedOnLaunchFail:  taskFailScope.Counter("launch_fail_requeued_total"),
	}
}
