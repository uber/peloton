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

package hostpool

import (
	"github.com/uber/peloton/pkg/common/scalar"

	"github.com/uber-go/tally"
)

// Metrics contains all per-pool metrics.
type Metrics struct {
	TotalHosts       tally.Gauge
	PhysicalCapacity scalar.GaugeMaps
	SlackCapacity    scalar.GaugeMaps
}

// NewMetrics creates Metrics for a given pool-ID.
func NewMetrics(scope tally.Scope, poolID string) *Metrics {
	hostPoolScope := scope.SubScope("host_pool").Tagged(map[string]string{"pool": poolID})
	phyCapScope := hostPoolScope.SubScope("physical_capacity")
	slackCapScope := hostPoolScope.SubScope("slack_capacity")

	return &Metrics{
		TotalHosts:       hostPoolScope.Gauge("total_hosts"),
		PhysicalCapacity: scalar.NewGaugeMaps(phyCapScope),
		SlackCapacity:    scalar.NewGaugeMaps(slackCapScope),
	}
}
