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

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/peloton/.gen/mesos/v1"
)

func setupPortRangeVariables() *PortRange {
	begin := uint64(31000)
	end := uint64(31006)
	portRange := NewPortRange(&mesos_v1.Value_Range{
		Begin: &begin,
		End:   &end,
	})
	return portRange
}

func TestPortRange_NumPorts(t *testing.T) {
	portRange := setupPortRangeVariables()
	assert.Equal(t, uint32(7), portRange.NumPorts())
}

func TestPortRange_TakePorts_take_no_ports_from_range(t *testing.T) {
	portRange := setupPortRangeVariables()
	begin, end := portRange.begin, portRange.end
	ports := portRange.TakePorts(0)
	assert.Equal(t, 0, len(ports))
	assert.Equal(t, begin, portRange.begin)
	assert.Equal(t, end, portRange.end)
}

func TestPortRange_TakePorts(t *testing.T) {
	portRange := setupPortRangeVariables()
	assert.Equal(t, uint32(31000), portRange.begin)
	assert.Equal(t, uint32(31007), portRange.end)
	assert.Equal(t, uint32(7), portRange.NumPorts())

	ports1 := portRange.TakePorts(3)
	assert.Equal(t, uint32(4), portRange.NumPorts())
	assert.Equal(t, 3, len(ports1))
	assert.Equal(t, uint32(31000), ports1[0])
	assert.Equal(t, uint32(31001), ports1[1])
	assert.Equal(t, uint32(31002), ports1[2])

	ports2 := portRange.TakePorts(3)
	assert.Equal(t, uint32(1), portRange.NumPorts())
	assert.Equal(t, 3, len(ports2))
	assert.Equal(t, uint32(31003), ports2[0])
	assert.Equal(t, uint32(31004), ports2[1])
	assert.Equal(t, uint32(31005), ports2[2])

	ports3 := portRange.TakePorts(3)
	assert.Equal(t, uint32(0), portRange.NumPorts())
	assert.Equal(t, 1, len(ports3))
	assert.Equal(t, uint32(31006), ports3[0])

	ports4 := portRange.TakePorts(3)
	assert.Equal(t, uint32(0), portRange.NumPorts())
	assert.Equal(t, 0, len(ports4))
}
