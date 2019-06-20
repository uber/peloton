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

import (
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
	"time"

	mesos_v1 "github.com/uber/peloton/.gen/mesos/v1"

	"github.com/uber/peloton/pkg/placement/models"
	"github.com/uber/peloton/pkg/placement/testutil"
)

const (
	_portRange1Begin = 31000
	_portRange1End   = 31001
	_portRange2Begin = 31002
	_portRange2End   = 31009
)

func TestAssignPortsAllFromASingleRange(t *testing.T) {
	now := time.Now()
	deadline := now.Add(30 * time.Second)
	assignment1 := testutil.SetupAssignment(deadline, 1)
	assignment2 := testutil.SetupAssignment(deadline, 1)
	tasks := []*models.TaskV0{
		assignment1.GetTask(),
		assignment2.GetTask(),
	}
	offer := testutil.SetupHostOffers()

	ports := AssignPorts(offer, tasks)
	assert.Equal(t, 6, len(ports))
	assert.Equal(t, uint32(31000), ports[0])
	assert.Equal(t, uint32(31001), ports[1])
	assert.Equal(t, uint32(31002), ports[2])
	assert.Equal(t, uint32(31003), ports[3])
	assert.Equal(t, uint32(31004), ports[4])
	assert.Equal(t, uint32(31005), ports[5])
}

func TestAssignPortsFromMultipleRanges(t *testing.T) {

	now := time.Now()
	deadline := now.Add(30 * time.Second)
	assignment1 := testutil.SetupAssignment(deadline, 1)
	assignment2 := testutil.SetupAssignment(deadline, 1)
	tasks := []*models.TaskV0{
		assignment1.GetTask(),
		assignment2.GetTask(),
	}
	host := testutil.SetupHostOffers()
	*host.GetOffer().Resources[4].Ranges.Range[0].End = uint64(_portRange1End)
	begin, end := uint64(_portRange2Begin), uint64(_portRange2End)
	host.GetOffer().Resources[4].Ranges.Range = append(
		host.GetOffer().Resources[4].Ranges.Range, &mesos_v1.Value_Range{
			Begin: &begin,
			End:   &end,
		})

	ports := AssignPorts(host, tasks)
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
