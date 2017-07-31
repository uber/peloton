package goalstate

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
)

func TestKeeperOnEvents(t *testing.T) {
	k := &keeper{
		tracker: NewTracker(),
	}

	jmti, err := k.tracker.AddTask(&task.TaskInfo{
		JobId: &peloton.JobID{
			Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c",
		},
		InstanceId: 1,
	})
	assert.NotNil(t, jmti)
	assert.NoError(t, err)

	before := time.Now()

	k.OnEvents([]*pb_eventstream.Event{{
		MesosTaskStatus: &mesos_v1.TaskStatus{
			TaskId: &mesos_v1.TaskID{
				Value: &[]string{"3c8a3c3e-71e3-49c5-9aed-2929823f595c-1-3c8a3c3e-71e3-49c5-9aed-2929823f5957"}[0],
			},
		},
		Offset: 5,
	}})

	assert.Equal(t, uint64(5), k.progress.Load())
	assert.True(t, jmti.(*jmTask).lastActionTime.After(before))
}

func TestKeeperUpdateTaskGoalState(t *testing.T) {
	k := &keeper{
		tracker: NewTracker(),
	}

	jmti, err := k.tracker.AddTask(&task.TaskInfo{
		JobId: &peloton.JobID{
			Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c",
		},
		InstanceId: 1,
	})
	assert.NotNil(t, jmti)
	assert.NoError(t, err)

	before := time.Now()

	assert.NoError(t, k.UpdateTaskGoalState(context.Background(), &task.TaskInfo{
		JobId: &peloton.JobID{
			Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c",
		},
		InstanceId: 1,
		Runtime: &task.RuntimeInfo{
			GoalState:            task.TaskState_PREEMPTING,
			DesiredConfigVersion: 42,
			ConfigVersion:        42,
		},
	}))

	assert.True(t, jmti.(*jmTask).goalStateTime.After(before))
	assert.Equal(t, State{task.TaskState_PREEMPTING, 42}, jmti.(*jmTask).goalState)
}
