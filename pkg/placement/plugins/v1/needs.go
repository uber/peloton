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

package plugins_v1

import (
	peloton_api_v0_task "github.com/uber/peloton/.gen/peloton/api/v0/task"
	v1alpha "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	hostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"

	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/placement/plugins"
)

// PlacementNeedsToHostFilter returns the v0 hostfilter corresponding to the
// PlacementNeeds passed in.
func PlacementNeedsToHostFilter(needs plugins.PlacementNeeds) *hostmgr.HostFilter {
	filter := &hostmgr.HostFilter{
		ResourceConstraint: &hostmgr.ResourceConstraint{
			NumPorts: uint32(needs.Ports),
			Minimum: &pod.ResourceSpec{
				CpuLimit:    needs.Resources.CPU,
				MemLimitMb:  needs.Resources.Mem,
				DiskLimitMb: needs.Resources.Disk,
				GpuLimit:    needs.Resources.GPU,
			},
		},
		MaxHosts: needs.MaxHosts,
		Hint:     &hostmgr.FilterHint{},
	}

	for podID, hostname := range needs.HostHints {
		hint := &hostmgr.FilterHint_Host{
			Hostname: hostname,
			PodId:    &v1alpha.PodID{Value: podID},
		}
		filter.Hint.HostHint = append(filter.Hint.HostHint, hint)
	}

	if cast, ok := needs.Constraint.(*peloton_api_v0_task.Constraint); ok && cast != nil {
		filter.SchedulingConstraint = api.ConvertTaskConstraintsToPodConstraints(
			[]*peloton_api_v0_task.Constraint{cast},
		)[0]
	}

	return filter
}
