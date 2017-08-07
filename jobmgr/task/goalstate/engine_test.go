package goalstate

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
)

func TestEngineOnEvents(t *testing.T) {
	e := &engine{
		tracker: newTracker(),
	}

	jmt, err := e.tracker.addTask(&task.TaskInfo{
		JobId: &peloton.JobID{
			Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c",
		},
		InstanceId: 1,
	})
	assert.NotNil(t, jmt)
	assert.NoError(t, err)

	before := time.Now()

	e.OnEvents([]*pb_eventstream.Event{{
		MesosTaskStatus: &mesos_v1.TaskStatus{
			TaskId: &mesos_v1.TaskID{
				Value: &[]string{"3c8a3c3e-71e3-49c5-9aed-2929823f595c-1-3c8a3c3e-71e3-49c5-9aed-2929823f5957"}[0],
			},
		},
		Offset: 5,
	}})

	assert.Equal(t, uint64(5), e.progress.Load())
	assert.True(t, jmt.lastActionTime.After(before))
}

func TestEngineUpdateTaskGoalState(t *testing.T) {
	e := &engine{
		tracker: newTracker(),
	}

	jmt, err := e.tracker.addTask(&task.TaskInfo{
		JobId: &peloton.JobID{
			Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c",
		},
		InstanceId: 1,
	})
	assert.NotNil(t, jmt)
	assert.NoError(t, err)

	before := time.Now()

	assert.NoError(t, e.UpdateTaskGoalState(context.Background(), &task.TaskInfo{
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

	assert.True(t, jmt.goalStateTime.After(before))
	assert.Equal(t, State{task.TaskState_PREEMPTING, 42}, jmt.goalState)
}

func TestEngineSyncFromDB(t *testing.T) {
	ctrl := gomock.NewController(t)

	jobstoreMock := store_mocks.NewMockJobStore(ctrl)
	taskstoreMock := store_mocks.NewMockTaskStore(ctrl)

	e := &engine{
		tracker:   newTracker(),
		jobStore:  jobstoreMock,
		taskStore: taskstoreMock,
	}

	jobstoreMock.EXPECT().GetAllJobs(gomock.Any()).Return(map[string]*job.RuntimeInfo{
		"3c8a3c3e-71e3-49c5-9aed-2929823f595c": nil,
	}, nil)

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	taskstoreMock.EXPECT().GetTasksForJob(gomock.Any(), jobID).
		Return(map[uint32]*task.TaskInfo{
			0: &task.TaskInfo{
				JobId:      jobID,
				InstanceId: 0,
			},
			1: &task.TaskInfo{
				JobId:      jobID,
				InstanceId: 1,
				Runtime: &task.RuntimeInfo{
					GoalState:            task.TaskState_RUNNING,
					DesiredConfigVersion: 42,
					ConfigVersion:        42,
				},
			},
		}, nil)

	e.Start()

	jm := e.tracker.getTask(&peloton.TaskID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c-1"})

	assert.Equal(t, State{task.TaskState_RUNNING, 42}, jm.goalState)
}
