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

package launcher

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track internal state
// of task launcher.
type Metrics struct {
	TaskLaunch     tally.Counter
	TaskLaunchFail tally.Counter
	// Increment this counter when we see failure to
	// populate the task's volume secret from DB
	TaskPopulateSecretFail tally.Counter
	TaskLaunchRetry        tally.Counter

	TaskRequeuedOnLaunchFail tally.Counter

	GetDBTaskInfo           tally.Timer
	LauncherGoRoutines      tally.Counter
	LaunchTasksCallDuration tally.Timer
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	taskSuccessScope := scope.Tagged(map[string]string{"result": "success"})
	taskFailScope := scope.Tagged(map[string]string{"result": "fail"})
	functionCallScope := scope.SubScope("functioncall")
	getPlacementScope := scope.SubScope("get_placement")
	launchTaskScope := scope.SubScope("launch_tasks")

	return &Metrics{
		TaskLaunch:             taskSuccessScope.Counter("launch"),
		TaskLaunchFail:         taskFailScope.Counter("launch"),
		TaskPopulateSecretFail: taskFailScope.Counter("populate_secret"),
		TaskLaunchRetry:        launchTaskScope.Counter("retry"),

		TaskRequeuedOnLaunchFail: taskFailScope.Counter("launch_fail_requeued_total"),
		GetDBTaskInfo:            functionCallScope.Timer("get_taskinfo"),
		LauncherGoRoutines:       getPlacementScope.Counter("go_routines"),
		LaunchTasksCallDuration:  launchTaskScope.Timer("call_duration"),
	}
}
