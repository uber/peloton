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

package hostcache

import (
	"github.com/uber/peloton/pkg/common/scalar"

	"github.com/uber-go/tally"
)

// Metrics tracks various metrics at offer hostCache level.
type Metrics struct {
	// Available and Allocated resources in host cache.
	Available scalar.GaugeMaps
	Allocated scalar.GaugeMaps

	// Metrics for number of hosts on each status.
	ReadyHosts     tally.Gauge
	PlacingHosts   tally.Gauge
	HeldHosts      tally.Gauge
	AvailableHosts tally.Gauge
}

// NewMetrics returns a new Metrics struct, with all metrics initialized
// and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	hostCacheScope := scope.SubScope("hostcache")

	// resources in ready & placing host status
	resourceScope := hostCacheScope.SubScope("resource")
	hostsScope := hostCacheScope.SubScope("hosts")

	return &Metrics{
		Available:      scalar.NewGaugeMaps(resourceScope),
		Allocated:      scalar.NewGaugeMaps(resourceScope),
		ReadyHosts:     hostsScope.Gauge("ready"),
		PlacingHosts:   hostsScope.Gauge("placing"),
		HeldHosts:      hostsScope.Gauge("held"),
		AvailableHosts: hostsScope.Gauge("available"),
	}
}
