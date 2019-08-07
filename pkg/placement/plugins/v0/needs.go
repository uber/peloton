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

package plugins_v0

import (
	peloton_api_v0_peloton "github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	peloton_api_v0_task "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"github.com/uber/peloton/pkg/placement/plugins"
)

// PlacementNeedsToHostFilter returns the v0 hostfilter corresponding to the
// PlacementNeeds passed in.
func PlacementNeedsToHostFilter(needs plugins.PlacementNeeds) *hostsvc.HostFilter {
	resConstraint := &hostsvc.ResourceConstraint{
		Minimum: &peloton_api_v0_task.ResourceConfig{
			CpuLimit:    needs.Resources.CPU,
			MemLimitMb:  needs.Resources.Mem,
			DiskLimitMb: needs.Resources.Disk,
			GpuLimit:    needs.Resources.GPU,
			FdLimit:     needs.FDs,
		},
		NumPorts:  uint32(needs.Ports),
		Revocable: needs.Revocable,
	}
	quantity := &hostsvc.QuantityControl{
		MaxHosts: needs.MaxHosts,
	}
	hint := &hostsvc.FilterHint{}
	for taskID, hostname := range needs.HostHints {
		hint.HostHint = append(hint.HostHint, &hostsvc.FilterHint_Host{
			TaskID:   &peloton_api_v0_peloton.TaskID{Value: taskID},
			Hostname: hostname,
		})
	}

	filter := &hostsvc.HostFilter{
		ResourceConstraint: resConstraint,
		Hint:               hint,
		Quantity:           quantity,
	}

	if needs.Constraint != nil {
		// TODO: It assumes PlacementNeeds.Constraint is
		//  *peloton_api_v0_task.Constraint, though it is actually defined as interface{}
		filter.SchedulingConstraint = needs.Constraint.(*peloton_api_v0_task.Constraint)
	}
	if needs.RankHint != nil {
		filter.Hint.RankHint = needs.RankHint.(hostsvc.FilterHint_Ranking)
	}

	return filter
}
