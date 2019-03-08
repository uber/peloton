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

package reconcile

import (
	"github.com/uber-go/tally"
)

// Metrics is a placeholder for all metrics in hostmgr
// reconciliation package.
type Metrics struct {
	ReconcileImplicitly      tally.Counter
	ReconcileImplicitlyFail  tally.Counter
	ReconcileExplicitly      tally.Counter
	ReconcileExplicitlyAbort tally.Counter
	ReconcileExplicitlyFail  tally.Counter
	ReconcileGetTasksFail    tally.Counter

	ExplicitTasksPerRun tally.Gauge
}

// NewMetrics returns a new instance of Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	successScope := scope.Tagged(map[string]string{"result": "success"})
	failScope := scope.Tagged(map[string]string{"result": "fail"})
	return &Metrics{
		ReconcileImplicitly:      successScope.Counter("implicitly_total"),
		ReconcileImplicitlyFail:  failScope.Counter("implicitly_total"),
		ReconcileExplicitly:      successScope.Counter("explicitly_total"),
		ReconcileExplicitlyAbort: failScope.Counter("explicitly_abort_total"),
		ReconcileExplicitlyFail:  failScope.Counter("explicitly_total"),
		ReconcileGetTasksFail:    failScope.Counter("explicitly_gettasks_total"),

		ExplicitTasksPerRun: scope.Gauge("explicit_tasks_per_run"),
	}
}
