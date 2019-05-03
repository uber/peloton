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

package entitlement

import "github.com/uber-go/tally"

// metrics tracks entitlement calculation metrics.
type metrics struct {
	// Tracks if multiple entitle calculation goroutines were attempted to be
	// started.
	calculationDuplicate tally.Counter
	// Tracks the failure count of the calculation cycle.
	calculationFailed tally.Counter
	// Tracks the duration of the calculation cycle.
	calculationDuration tally.Timer
}

// newMetrics returns a new instance of task.metrics.
func newMetrics(scope tally.Scope) *metrics {
	cScope := scope.SubScope("entitlement")

	return &metrics{
		calculationDuplicate: cScope.Counter(
			"calculation_duplicate"),
		calculationFailed: cScope.Counter(
			"calculation_failed"),
		calculationDuration: cScope.Timer(
			"calculation_duration"),
	}
}
