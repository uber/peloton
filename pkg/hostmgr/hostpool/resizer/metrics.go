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

package resizer

import (
	"github.com/uber-go/tally"
)

type Metrics struct {

	// MoveScope is used to emit host move metrics per pool.
	RootScope tally.Scope
	MoveScope tally.Scope

	// metrics.
	GetPool        tally.Counter
	GetPoolFail    tally.Counter
	MoveHosts      tally.Counter
	MoveHostsFail  tally.Counter
	GetMetrics     tally.Counter
	GetMetricsFail tally.Counter

	// Resizer run duration for one run.
	ResizerRunDuration tally.Timer
}

func NewMetrics(scope tally.Scope) *Metrics {
	resizerScope := scope.SubScope("resizer")

	return &Metrics{
		RootScope: resizerScope,
		MoveScope: resizerScope.SubScope("move"),

		GetMetrics:     resizerScope.Counter("get_metrics"),
		GetMetricsFail: resizerScope.Counter("get_metrics_fail"),
		GetPool:        resizerScope.Counter("get_pool"),
		GetPoolFail:    resizerScope.Counter("get_pool_fail"),
		MoveHosts:      resizerScope.Counter("move_hosts"),
		MoveHostsFail:  resizerScope.Counter("move_hosts_fail"),

		ResizerRunDuration: resizerScope.Timer("run_duration"),
	}
}
