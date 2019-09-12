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

package lifecyclemgr

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track internal state
// of lifecyclemgr.
type Metrics struct {
	Launch          tally.Counter
	LaunchFail      tally.Counter
	LaunchRetry     tally.Counter
	LaunchDuration  tally.Timer
	LaunchRateLimit tally.Counter

	Kill          tally.Counter
	KillFail      tally.Counter
	Shutdown      tally.Counter
	ShutdownFail  tally.Counter
	KillRateLimit tally.Counter

	TerminateLease     tally.Counter
	TerminateLeaseFail tally.Counter

	GetTasksOnDrainingHosts     tally.Counter
	GetTasksOnDrainingHostsFail tally.Counter
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	successScope := scope.Tagged(map[string]string{"result": "success"})
	failScope := scope.Tagged(map[string]string{"result": "fail"})

	launchScope := scope.SubScope("launch")
	killScope := scope.SubScope("kill")

	return &Metrics{
		Launch:          successScope.Counter("launch"),
		LaunchFail:      failScope.Counter("launch"),
		LaunchRetry:     launchScope.Counter("retry"),
		LaunchDuration:  launchScope.Timer("call_duration"),
		LaunchRateLimit: launchScope.Counter("rate_limit"),

		Kill:          successScope.Counter("kill"),
		KillFail:      failScope.Counter("kill"),
		Shutdown:      successScope.Counter("kill"),
		ShutdownFail:  failScope.Counter("kill"),
		KillRateLimit: killScope.Counter("rate_limit"),

		TerminateLease:     successScope.Counter("terminate_lease"),
		TerminateLeaseFail: failScope.Counter("terminate_lease"),

		GetTasksOnDrainingHosts:     successScope.Counter("tasks_on_draining_hosts"),
		GetTasksOnDrainingHostsFail: failScope.Counter("tasks_on_draining_hosts"),
	}
}
