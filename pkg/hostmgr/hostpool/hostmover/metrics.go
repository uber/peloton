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

package hostmover

import (
	"github.com/uber-go/tally"
)

// Metrics tracks various metrics at task state level.
type Metrics struct {
	startMaintenanceSuccess    tally.Counter
	startMaintenanceFail       tally.Counter
	completeMaintenanceSuccess tally.Counter
	completeMaintenanceFail    tally.Counter
	pendingMoveRequests        tally.Gauge
	dequeueErrors              tally.Counter
	enqueueErrors              tally.Counter
	reconcileHostRequests      tally.Gauge
	reconcileHostSuccess       tally.Counter
	reconcileHostFailed        tally.Counter

	scope tally.Scope
}

// NewMetrics returns a new Metrics struct, with all metrics initialized
// and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		startMaintenanceSuccess:    scope.Counter("start_maintenance_success"),
		startMaintenanceFail:       scope.Counter("start_maintenance_fail"),
		completeMaintenanceSuccess: scope.Counter("complete_maintenance_success"),
		completeMaintenanceFail:    scope.Counter("complete_maintenance_fail"),
		dequeueErrors:              scope.Counter("dequeue_errors"),
		enqueueErrors:              scope.Counter("enqueue_errors"),
		pendingMoveRequests:        scope.Gauge("pending_move_requests"),
		reconcileHostRequests:      scope.Gauge("reconcile_host_requests"),
		reconcileHostSuccess:       scope.Counter("reconcile_host_success"),
		reconcileHostFailed:        scope.Counter("reconcile_host_failed"),

		scope: scope,
	}
}
