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

package offer

import (
	"github.com/uber-go/tally"
)

// Metrics tracks various metrics at task state level.
type Metrics struct {
	taskUpdateCounter   tally.Counter
	taskUpdateAck       tally.Counter
	taskAckChannelSize  tally.Gauge
	taskAckMapSize      tally.Gauge
	taskUpdateAckDeDupe tally.Counter

	scope tally.Scope
}

// NewMetrics returns a new Metrics struct, with all metrics initialized
// and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	return &Metrics{
		taskUpdateCounter:   scope.Counter("task_updates"),
		taskUpdateAck:       scope.Counter("task_update_ack"),
		taskAckChannelSize:  scope.Gauge("task_ack_channel_size"),
		taskAckMapSize:      scope.Gauge("task_ack_map_size"),
		taskUpdateAckDeDupe: scope.Counter("task_update_ack_dedupe"),

		scope: scope,
	}
}
