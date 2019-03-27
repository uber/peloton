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

package util

import "github.com/uber/peloton/pkg/placement/models"

func AssignPorts(offer *models.HostOffers, tasks []*models.Task) []uint32 {
	availablePortRanges := map[*models.PortRange]struct{}{}
	for _, resource := range offer.GetOffer().GetResources() {
		if resource.GetName() != "ports" {
			continue
		}
		for _, portRange := range resource.GetRanges().GetRange() {
			availablePortRanges[models.NewPortRange(portRange)] = struct{}{}
		}
	}
	var selectedPorts []uint32
	for _, taskEntity := range tasks {
		assignedPorts := uint32(0)
		neededPorts := taskEntity.GetTask().NumPorts
		depletedRanges := []*models.PortRange{}
		for portRange := range availablePortRanges {
			ports := portRange.TakePorts(neededPorts - assignedPorts)
			assignedPorts += uint32(len(ports))
			selectedPorts = append(selectedPorts, ports...)
			if portRange.NumPorts() == 0 {
				depletedRanges = append(depletedRanges, portRange)
			}
			if assignedPorts >= neededPorts {
				break
			}
		}
		for _, portRange := range depletedRanges {
			delete(availablePortRanges, portRange)
		}
	}
	return selectedPorts
}
