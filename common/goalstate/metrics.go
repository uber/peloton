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

package goalstate

import (
	"github.com/uber-go/tally"
)

// Metrics contains counters to track goal state engine metrics
type Metrics struct {
	// the metrics scope for goal state engine
	scope tally.Scope
	// counter to track items not found in goal state engine after dequeue
	missingItems tally.Counter
	// counter to track total items in the goal state engine
	totalItems tally.Gauge
}

// NewMetrics returns a new Metrics struct.
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		scope:        scope,
		missingItems: scope.Counter("missing_items"),
		totalItems:   scope.Gauge("total_items"),
	}
}
