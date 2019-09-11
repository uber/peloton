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
	"github.com/uber/peloton/pkg/hostmgr/models"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
)

// TestMesosHostSummarySetCapacity tests for mesos change capacity
// only affects capacity
func (suite *HostSummaryTestSuite) TestMesosHostSummarySetCapacity() {
	ms := NewMesosHostSummary(_hostname).(*mesosHostSummary)
	available := scalar.Resources{
		CPU: 1.0,
		Mem: 100.0,
	}
	allocated := scalar.Resources{
		CPU: 2.0,
		Mem: 200.0,
	}
	capacity := scalar.Resources{
		CPU: 3.0,
		Mem: 300.0,
	}

	ms.available.NonSlack = available
	ms.allocated.NonSlack = allocated
	ms.capacity.NonSlack = capacity

	newCapacity := scalar.Resources{
		CPU: 4.0,
		Mem: 400.0,
	}
	ms.SetCapacity(models.HostResources{
		NonSlack: newCapacity,
	})

	suite.Equal(ms.GetAvailable().NonSlack, available)
	suite.Equal(ms.GetAllocated().NonSlack, allocated)
	suite.Equal(ms.GetCapacity().NonSlack, newCapacity)
}

// TestMesosHostSummarySetAvailable
// test set available would update allocated resources
func (suite *HostSummaryTestSuite) TestMesosHostSummarySetAvailable() {
	testTable := map[string]struct {
		capacity          scalar.Resources
		available         scalar.Resources
		expectedAllocated scalar.Resources
	}{
		"normal-test-case-1": {
			capacity: scalar.Resources{
				CPU: 4.0,
				Mem: 100.0,
			},
			available: scalar.Resources{
				CPU: 1.0,
				Mem: 20.0,
			},
			expectedAllocated: scalar.Resources{
				CPU: 3.0,
				Mem: 80.0,
			},
		},
		"normal-test-case-2": {
			capacity: scalar.Resources{
				CPU: 4.0,
				Mem: 100.0,
			},
			available: scalar.Resources{
				CPU: 3.0,
				Mem: 10.0,
			},
			expectedAllocated: scalar.Resources{
				CPU: 1.0,
				Mem: 90.0,
			},
		},
		"all-capacity-allocated": {
			capacity: scalar.Resources{
				CPU: 4.0,
				Mem: 100.0,
			},
			available: scalar.Resources{
				CPU: 4.0,
				Mem: 100.0,
			},
			expectedAllocated: scalar.Resources{
				CPU: 0.0,
				Mem: 0.0,
			},
		},
		"more-available-than-capacity": {
			capacity: scalar.Resources{
				CPU: 4.0,
				Mem: 100.0,
			},
			available: scalar.Resources{
				CPU: 5.0,
				Mem: 200.0,
			},
			expectedAllocated: scalar.Resources{
				CPU: 0.0,
				Mem: 0.0,
			},
		},
	}

	for msg, test := range testTable {
		ms := NewMesosHostSummary(_hostname).(*mesosHostSummary)
		ms.capacity.NonSlack = test.capacity
		ms.SetAvailable(models.HostResources{
			NonSlack: test.available,
		})
		suite.Equal(ms.GetCapacity().NonSlack, test.capacity, msg)
		suite.Equal(ms.GetAvailable().NonSlack, test.available, msg)
		suite.Equal(ms.GetAllocated().NonSlack, test.expectedAllocated, msg)
	}
}
