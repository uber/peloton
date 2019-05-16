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

package cached

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track internal state of the cache.
type Metrics struct {
	scope tally.Scope
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		scope: scope,
	}
}

// TaskMetrics contains counters for task that are managed by cache.
type TaskMetrics struct {
	TimeToAssignNonRevocable tally.Timer
	TimeToAssignRevocable    tally.Timer

	TimeToRunNonRevocable tally.Timer
	TimeToRunRevocable    tally.Timer

	MeanSpreadQuotient tally.Gauge
}

// NewTaskMetrics returns a new TaskMetrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewTaskMetrics(scope tally.Scope) *TaskMetrics {
	return &TaskMetrics{
		TimeToAssignNonRevocable: scope.Timer("time_to_assign_non_revocable"),
		TimeToAssignRevocable:    scope.Timer("time_to_assign_revocable"),

		TimeToRunNonRevocable: scope.Timer("time_to_run_non_revocable"),
		TimeToRunRevocable:    scope.Timer("time_to_run_revocable"),

		MeanSpreadQuotient: scope.Gauge("mean_spread_quotient"),
	}
}
