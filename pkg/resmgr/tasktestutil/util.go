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

package tasktestutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/resmgr/scalar"
	rm_task "github.com/uber/peloton/pkg/resmgr/task"
)

/**
This is the test util package , which will be used across all the test
files under resmgr package
*/

// ValidateStateTransitions takes the rmtask and list of transition states.
// It transitions to those states in order received and after transition and
// verify there is no error.
func ValidateStateTransitions(rmtask *rm_task.RMTask,
	states []task.TaskState) {
	for _, state := range states {
		err := rmtask.TransitTo(state.String())
		assert.NoError(&testing.T{}, err)
	}
}

// CreateTaskConfig crates a test task config with all the default values
// for the tests. It is a TEST task config.
func CreateTaskConfig() *rm_task.Config {
	return &rm_task.Config{
		LaunchingTimeout:          1 * time.Minute,
		PlacingTimeout:            1 * time.Minute,
		PolicyName:                rm_task.ExponentialBackOffPolicy,
		PlacementRetryBackoff:     30 * time.Second,
		PlacementRetryCycle:       3,
		PlacementAttemptsPerCycle: 3,
		EnablePlacementBackoff:    true,
	}
}

// ValidateResources validates the resources and the values passed
// if anything differs it returns false else true
func ValidateResources(resources *scalar.Resources, values map[string]int64) bool {
	return int64(resources.CPU) == values["CPU"] &&
		int64(resources.GPU) == values["GPU"] &&
		int64(resources.MEMORY) == values["MEMORY"] &&
		int64(resources.DISK) == values["DISK"]
}

// GetReservationFromResourceConfig gets the reservation from the respools resource config
func GetReservationFromResourceConfig(
	resourcesMap map[string]*respool.ResourceConfig) *scalar.Resources {
	return &scalar.Resources{
		CPU:    resourcesMap[common.CPU].GetReservation(),
		GPU:    resourcesMap[common.GPU].GetReservation(),
		MEMORY: resourcesMap[common.MEMORY].GetReservation(),
		DISK:   resourcesMap[common.DISK].GetReservation(),
	}
}

// GetLimitFromResourceConfig gets the limit from the respools resource config
func GetLimitFromResourceConfig(
	resourcesMap map[string]*respool.ResourceConfig) *scalar.Resources {
	return &scalar.Resources{
		CPU:    resourcesMap[common.CPU].GetLimit(),
		GPU:    resourcesMap[common.GPU].GetLimit(),
		MEMORY: resourcesMap[common.MEMORY].GetLimit(),
		DISK:   resourcesMap[common.DISK].GetLimit(),
	}
}
