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

package health

import (
	"github.com/uber-go/tally"
)

// Metrics is a placeholder for all metrics in health package.
type Metrics struct {
	Init      tally.Counter
	Heartbeat tally.Gauge
	Leader    tally.Gauge
}

// NewMetrics returns a new instance of Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		Init:      scope.Counter("init"),
		Heartbeat: scope.Gauge("heartbeat"),
		Leader:    scope.Gauge("leader"),
	}
}
