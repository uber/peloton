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

import "github.com/uber/peloton/.gen/mesos/v1"

// PortRange represents a modifiable closed-open port range [begin:end[ used when assigning ports to tasks.
type PortRange struct {
	begin uint32
	end   uint32
}

// NewPortRange creates a new modifiable port range from a Mesos value range.
func NewPortRange(portRange *mesos_v1.Value_Range) *PortRange {
	return &PortRange{
		begin: uint32(*portRange.Begin),
		end:   uint32(*portRange.End + 1),
	}
}

// NumPorts returns the number of available ports in the range.
func (portRange *PortRange) NumPorts() uint32 {
	return portRange.end - portRange.begin
}

// TakePorts will take the number of ports from the range or as many as
// available if more ports are requested than are in the range.
func (portRange *PortRange) TakePorts(numPorts uint32) []uint32 {
	// Try to select ports in a random fashion to avoid ports conflict.
	ports := make([]uint32, 0, numPorts)
	stop := portRange.begin + numPorts
	if numPorts >= portRange.NumPorts() {
		stop = portRange.end
	}
	for i := portRange.begin; i < stop; i++ {
		ports = append(ports, i)
	}
	portRange.begin = stop
	return ports
}
