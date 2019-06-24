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

package models_test

import (
	"sort"
	"testing"
	"time"

	mesos_v1 "github.com/uber/peloton/.gen/mesos/v1"

	"github.com/stretchr/testify/assert"
	"github.com/uber/peloton/pkg/placement/models"
	"github.com/uber/peloton/pkg/placement/testutil"
)

const (
	_portRange1Begin = 31000
	_portRange1End   = 31001
	_portRange2Begin = 31002
	_portRange2End   = 31009
)

func TestPortRange_NumPorts(t *testing.T) {
	portRange := models.NewPortRange(31000, 31006)
	assert.Equal(t, uint64(7), portRange.NumPorts())
}

func TestPortRange_TakePorts_take_no_ports_from_range(t *testing.T) {
	portRange := models.NewPortRange(31000, 31006)
	begin, end := portRange.Begin, portRange.End
	ports := portRange.TakePorts(0)
	assert.Equal(t, 0, len(ports))
	assert.Equal(t, begin, portRange.Begin)
	assert.Equal(t, end, portRange.End)
}

func TestPortRange_TakePorts(t *testing.T) {
	portRange := models.NewPortRange(31000, 31006)
	assert.Equal(t, uint64(31000), portRange.Begin)
	assert.Equal(t, uint64(31007), portRange.End)
	assert.Equal(t, uint64(7), portRange.NumPorts())

	ports1 := portRange.TakePorts(3)
	assert.Equal(t, uint64(4), portRange.NumPorts())
	assert.Equal(t, 3, len(ports1))
	assert.Equal(t, uint64(31000), ports1[0])
	assert.Equal(t, uint64(31001), ports1[1])
	assert.Equal(t, uint64(31002), ports1[2])

	ports2 := portRange.TakePorts(3)
	assert.Equal(t, uint64(1), portRange.NumPorts())
	assert.Equal(t, 3, len(ports2))
	assert.Equal(t, uint64(31003), ports2[0])
	assert.Equal(t, uint64(31004), ports2[1])
	assert.Equal(t, uint64(31005), ports2[2])

	ports3 := portRange.TakePorts(3)
	assert.Equal(t, uint64(0), portRange.NumPorts())
	assert.Equal(t, 1, len(ports3))
	assert.Equal(t, uint64(31006), ports3[0])

	ports4 := portRange.TakePorts(3)
	assert.Equal(t, uint64(0), portRange.NumPorts())
	assert.Equal(t, 0, len(ports4))
}

// TestAssignPortsAllFromASingleRange tests that the port assignment
// works if we have exactly enough ports in a single range.
func TestAssignPortsAllFromASingleRange(t *testing.T) {
	now := time.Now()
	deadline := now.Add(30 * time.Second)
	assignment1 := testutil.SetupAssignment(deadline, 1)
	assignment2 := testutil.SetupAssignment(deadline, 1)
	tasks := []models.Task{
		assignment1,
		assignment2,
	}
	offer := testutil.SetupHostOffers()

	ports := models.AssignPorts(offer, tasks)
	assert.Equal(t, 6, len(ports))
	assert.Equal(t, uint64(31000), ports[0])
	assert.Equal(t, uint64(31001), ports[1])
	assert.Equal(t, uint64(31002), ports[2])
	assert.Equal(t, uint64(31003), ports[3])
	assert.Equal(t, uint64(31004), ports[4])
	assert.Equal(t, uint64(31005), ports[5])
}

// TestAssignPortsFromMultipleRanges tests that the port assignment
// works if we have exactly enough ports in multiple separate ranges.
func TestAssignPortsFromMultipleRanges(t *testing.T) {
	now := time.Now()
	deadline := now.Add(30 * time.Second)
	assignment1 := testutil.SetupAssignment(deadline, 1)
	assignment2 := testutil.SetupAssignment(deadline, 1)
	tasks := []models.Task{
		assignment1,
		assignment2,
	}
	host := testutil.SetupHostOffers()
	*host.GetOffer().Resources[4].Ranges.Range[0].End = uint64(_portRange1End)
	begin, end := uint64(_portRange2Begin), uint64(_portRange2End)
	host.GetOffer().Resources[4].Ranges.Range = append(
		host.GetOffer().Resources[4].Ranges.Range, &mesos_v1.Value_Range{
			Begin: &begin,
			End:   &end,
		})

	ports := models.AssignPorts(host, tasks)
	intPorts := make([]int, len(ports))
	for i := range ports {
		intPorts[i] = int(ports[i])
	}
	sort.Ints(intPorts)
	assert.Equal(t, 6, len(ports))

	// range 1 (31000-31001) and range 2 (31002-31009)
	// ports selected from both ranges
	if intPorts[0] == _portRange1Begin {
		for i := 0; i < len(intPorts); i++ {
			assert.Equal(t, intPorts[i], 31000+i)
		}
	} else {
		// ports selected from range2 only
		for i := 0; i < len(intPorts); i++ {
			assert.Equal(t, intPorts[i], _portRange2Begin+i)
		}
	}
}
