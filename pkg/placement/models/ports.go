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

package models

// PortRange represents a modifiable closed-open port range [Begin:End[
// used when assigning ports to tasks.
type PortRange struct {
	Begin uint64
	End   uint64
}

// NewPortRange creates a new modifiable port range from a Mesos value range.
// Both the begin and end ports are part of the port range.
func NewPortRange(begin, end uint64) *PortRange {
	return &PortRange{
		Begin: begin,
		End:   end + 1,
	}
}

// NumPorts returns the number of available ports in the range.
func (portRange *PortRange) NumPorts() uint64 {
	return portRange.End - portRange.Begin
}

// TakePorts will take the number of ports from the range or as many as
// available if more ports are requested than are in the range.
func (portRange *PortRange) TakePorts(numPorts uint64) []uint64 {
	// Try to select ports in a random fashion to avoid ports conflict.
	ports := make([]uint64, 0, numPorts)
	stop := portRange.Begin + numPorts
	if numPorts >= portRange.NumPorts() {
		stop = portRange.End
	}
	for i := portRange.Begin; i < stop; i++ {
		ports = append(ports, i)
	}
	portRange.Begin = stop
	return ports
}

// AssignPorts selects available ports from the offer and returns them.
func AssignPorts(
	offer Offer,
	tasks []Task,
) []uint64 {
	availablePortRanges := offer.AvailablePortRanges()

	var selectedPorts []uint64
	for _, taskEntity := range tasks {
		assignedPorts := uint64(0)
		neededPorts := taskEntity.GetPlacementNeeds().Ports
		depletedRanges := []*PortRange{}
		for portRange := range availablePortRanges {
			ports := portRange.TakePorts(neededPorts - assignedPorts)
			assignedPorts += uint64(len(ports))
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
