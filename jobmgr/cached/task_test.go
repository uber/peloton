package cached

import (
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
)

func TestTask(t *testing.T) {
	jobID := peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(1)

	tt := &task{
		id:    instanceID,
		jobID: &jobID,
	}

	assert.Equal(t, instanceID, tt.ID())
	assert.Equal(t, jobID, *tt.JobID())

	// Test fetching state and goal state of task
	runtime := pbtask.RuntimeInfo{
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	tt.UpdateRuntime(&runtime)

	curState := tt.CurrentState()
	curGoalState := tt.GoalState()
	assert.Equal(t, runtime.State, curState.State)
	assert.Equal(t, runtime.GoalState, curGoalState.State)
}

func TaskTestUpdateRuntime(t *testing.T) {
	jobID := peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(1)
	runtime := pbtask.RuntimeInfo{
		Revision: &peloton.ChangeLog{
			Version: 3,
		},
		State:     pbtask.TaskState_RUNNING,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}

	tt := &task{
		id:      instanceID,
		jobID:   &jobID,
		runtime: &runtime,
	}

	// Test if updating with same runtime again
	tt.UpdateRuntime(&runtime)
	assert.Equal(t, runtime, tt.runtime)

	// Now update with older version and check not updated
	oldRuntime := pbtask.RuntimeInfo{
		Revision: &peloton.ChangeLog{
			Version: 1,
		},
		State:     pbtask.TaskState_PENDING,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	tt.UpdateRuntime(&oldRuntime)
	assert.Equal(t, runtime, tt.runtime)

	// Now update with same version
	sameRuntime := pbtask.RuntimeInfo{
		Revision: &peloton.ChangeLog{
			Version: 3,
		},
		State:     pbtask.TaskState_PENDING,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	tt.UpdateRuntime(&sameRuntime)
	assert.Nil(t, tt.runtime)

	tt.runtime = &runtime
	// Finally update to new version
	newRuntime := pbtask.RuntimeInfo{
		Revision: &peloton.ChangeLog{
			Version: 4,
		},
		State:     pbtask.TaskState_SUCCEEDED,
		GoalState: pbtask.TaskState_SUCCEEDED,
	}
	tt.UpdateRuntime(&newRuntime)
	assert.Equal(t, newRuntime, tt.runtime)
}
