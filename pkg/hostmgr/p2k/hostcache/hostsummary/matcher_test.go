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

package hostsummary

import (
	"math"
	"strings"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	hostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"

	"github.com/uber/peloton/pkg/hostmgr/models"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
)

// TestEffectiveHostLimit tests calculating the max hosts limit
func (suite *HostSummaryTestSuite) TestEffectiveHostLimit() {
	// this deprecated semantic is equivalent to a single constraints
	// with same limit.
	c := &hostmgr.HostFilter{}
	suite.Equal(uint32(math.MaxUint32), effectiveHostLimit(c))

	c.MaxHosts = 10
	suite.Equal(uint32(10), effectiveHostLimit(c))
}

// TestTryMatch tests matchers TryMatch functionality where it tries to match
// existing hosts in host cache with the given filter constraints
func (suite *HostSummaryTestSuite) TestTryMatch() {
	testTable := map[string]struct {
		filter           *hostmgr.HostFilter
		allocatedPerHost scalar.Resources
		matched          int
		filterCounts     map[string]uint32
	}{
		// match all 10 hosts because
		"filter-match-all": {
			filter: &hostmgr.HostFilter{
				ResourceConstraint: &hostmgr.ResourceConstraint{
					Minimum: &pod.ResourceSpec{
						CpuLimit:   2.0,
						MemLimitMb: 2.0,
					},
				},
			},
			allocatedPerHost: scalar.Resources{},
			matched:          10,
			filterCounts: map[string]uint32{
				strings.ToLower("HOST_FILTER_MATCH"): 10,
			},
		},
		// num of actual matching hosts > max limit in the filter, should
		// match MaxHosts only
		"filter-match-maxlimit": {
			filter: &hostmgr.HostFilter{
				ResourceConstraint: &hostmgr.ResourceConstraint{
					Minimum: &pod.ResourceSpec{
						CpuLimit:   2.0,
						MemLimitMb: 2.0,
					},
				},
				MaxHosts: 5,
			},
			allocatedPerHost: scalar.Resources{},
			matched:          5,
			filterCounts: map[string]uint32{
				strings.ToLower("HOST_FILTER_MATCH"):                   5,
				strings.ToLower("HOST_FILTER_MISMATCH_MAX_HOST_LIMIT"): 5,
			},
		},
		// there is 0 allocation on each host but the resource constraint needs
		// a lot more resources
		"filter-match-none-high-demand": {
			filter: &hostmgr.HostFilter{
				ResourceConstraint: &hostmgr.ResourceConstraint{
					Minimum: &pod.ResourceSpec{
						CpuLimit:   100.0,
						MemLimitMb: 100.0,
					},
				},
			},
			allocatedPerHost: scalar.Resources{},
			matched:          0,
			filterCounts: map[string]uint32{
				strings.ToLower("HOST_FILTER_INSUFFICIENT_RESOURCES"): 10,
			},
		},
		// hosts are heavily allocated so none of them matches the resource
		// constraint
		"filter-match-none-high-allocation": {
			filter: &hostmgr.HostFilter{
				ResourceConstraint: &hostmgr.ResourceConstraint{
					Minimum: &pod.ResourceSpec{
						CpuLimit:   2.0,
						MemLimitMb: 20.0,
					},
				},
			},
			// Each host is allocated 9 CPU and 90Mem
			// only available resource is 1 CPU and 10Mem per host
			// demand is 2 CPU and 20Mem resulting in no match
			allocatedPerHost: CreateResource(9.0, 90.0),
			matched:          0,
			filterCounts: map[string]uint32{
				strings.ToLower("HOST_FILTER_INSUFFICIENT_RESOURCES"): 10,
			},
		},
	}
	for ttName, tt := range testTable {
		// Generate 10 host summary with 10 CPU and 100 Mem per host which
		// are in ReadyHost state
		hosts := GenerateFakeHostSummaries(10)

		matcher := NewMatcher(tt.filter)

		// Run matcher on all hosts
		for _, hs := range hosts {
			hs.SetAllocated(tt.allocatedPerHost)
			hs.SetAvailable(models.HostResources{
				NonSlack: hs.GetCapacity().NonSlack.Subtract(tt.allocatedPerHost),
			})

			matcher.TryMatch(hs.GetHostname(), hs)
		}
		suite.Equal(tt.matched, len(matcher.hostNames), "test case %s", ttName)
		suite.Equal(
			tt.filterCounts, matcher.filterCounts, "test case %s", ttName)
	}
}
