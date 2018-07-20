package tasktestutil

import (
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/resmgr/scalar"
	rm_task "code.uber.internal/infra/peloton/resmgr/task"

	"github.com/stretchr/testify/assert"
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
		LaunchingTimeout:       1 * time.Minute,
		PlacingTimeout:         1 * time.Minute,
		PolicyName:             rm_task.ExponentialBackOffPolicy,
		PlacementRetryBackoff:  30 * time.Second,
		PlacementRetryCycle:    1,
		EnablePlacementBackoff: true,
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
