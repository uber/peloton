package goalstate

import (
	"context"
	"testing"

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

	jobID := &peloton.JobID{
		Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c",
	}
	err := e.UpdateTaskGoalState(context.Background(), &task.TaskInfo{
		JobId:      jobID,
		Runtime:    &task.RuntimeInfo{},
		InstanceId: 1,
	})
	assert.NoError(t, err)

	assert.NotNil(t, e.tracker.getJob(jobID).queue.popIfReady())
	assert.Nil(t, e.tracker.getJob(jobID).queue.popIfReady())

	e.OnEvents([]*pb_eventstream.Event{{
		MesosTaskStatus: &mesos_v1.TaskStatus{
			TaskId: &mesos_v1.TaskID{
				Value: &[]string{"3c8a3c3e-71e3-49c5-9aed-2929823f595c-1-3c8a3c3e-71e3-49c5-9aed-2929823f5957"}[0],
			},
		},
		Offset: 5,
	}})

	assert.Equal(t, uint64(5), e.progress.Load())
	assert.NotNil(t, e.tracker.getJob(jobID).queue.popIfReady())
}

func TestEngineUpdateTaskGoalState(t *testing.T) {
	e := &engine{
		tracker: newTracker(),
	}

	jobID := &peloton.JobID{
		Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c",
	}

	assert.Nil(t, e.tracker.getJob(jobID))

	assert.NoError(t, e.UpdateTaskGoalState(context.Background(), &task.TaskInfo{
		JobId:      jobID,
		InstanceId: 1,
		Runtime: &task.RuntimeInfo{
			GoalState:            task.TaskState_PREEMPTING,
			DesiredConfigVersion: 42,
			ConfigVersion:        42,
		},
	}))

	assert.NotNil(t, e.tracker.getJob(jobID).queue.popIfReady())
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
			1: {
				JobId:      jobID,
				InstanceId: 1,
				Runtime: &task.RuntimeInfo{
					GoalState:            task.TaskState_RUNNING,
					DesiredConfigVersion: 42,
					ConfigVersion:        42,
				},
			},
		}, nil)

	e.syncFromDB(context.Background())

	j := e.tracker.getJob(jobID)
	assert.NotNil(t, j)
	assert.NotNil(t, j.queue.popIfReady())
}
