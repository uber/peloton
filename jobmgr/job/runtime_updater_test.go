package job

import (
	"fmt"
	"testing"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"

	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestUpdateJobRuntime_Events(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var mockJobStore = store_mocks.NewMockJobStore(ctrl)
	var mockTaskStore = store_mocks.NewMockTaskStore(ctrl)

	updater := NewJobRuntimeUpdater(mockJobStore, mockTaskStore, nil, tally.NoopScope)

	var events []*pb_eventstream.Event
	var times []float64

	for i := 0; i < 10; i++ {
		t := float64(i)
		taskID := fmt.Sprintf("job%d-%d-%s", i, i, uuid.NewUUID().String())
		events = append(events, &pb_eventstream.Event{
			MesosTaskStatus: &mesos.TaskStatus{
				TaskId: &mesos.TaskID{
					Value: &taskID,
				},
				Timestamp: &t,
			},
		})
		times = append(times, t)
	}
	updater.OnEvents(events)

	for i := 0; i < 10; i++ {
		assert.True(t, updater.taskUpdatedFlags[fmt.Sprintf("job%d", i)])
		assert.Equal(t, updater.lastTaskUpdateTime[fmt.Sprintf("job%d", i)], times[i])
	}

	updater.Start()
	assert.True(t, updater.started.Load())
	updater.Stop()
	assert.False(t, updater.started.Load())
	assert.Equal(t, 0, len(updater.lastTaskUpdateTime))
	assert.Equal(t, 0, len(updater.taskUpdatedFlags))
}

func TestUpdateJobRuntime_UpdateJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	mockTaskStore2 := store_mocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{
		Value: "job0",
	}
	jobConfig := job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"money", "team6", "qa"},
		InstanceCount: uint32(3),
	}
	var jobRuntime = job.RuntimeInfo{
		State: job.JobState_PENDING,
	}

	mockJobStore.EXPECT().
		GetJobConfig(jobID).
		Return(&jobConfig, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		GetJobsByState(job.JobState_PENDING).
		Return([]peloton.JobID{*jobID}, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		GetJobsByState(gomock.Any()).
		Return([]peloton.JobID{}, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		GetJobRuntime(jobID).
		Return(&jobRuntime, nil).
		AnyTimes()
	mockTaskStore.EXPECT().
		GetTasksForJobAndState(gomock.Any(), task.TaskState_SUCCEEDED.String()).
		Return(map[uint32]*task.TaskInfo{uint32(0): nil, uint32(1): nil}, nil).
		AnyTimes()
	mockTaskStore.EXPECT().
		GetTasksForJobAndState(gomock.Any(), task.TaskState_RUNNING.String()).
		Return(map[uint32]*task.TaskInfo{uint32(2): nil}, nil).
		AnyTimes()
	mockTaskStore.EXPECT().
		GetTasksForJobAndState(gomock.Any(), gomock.Any()).
		Return(map[uint32]*task.TaskInfo{}, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		UpdateJobRuntime(jobID, gomock.Any()).
		Return(nil).
		AnyTimes()

	taskID := fmt.Sprintf("job%d-%d-%s", 0, 1, uuid.NewUUID().String())
	updater := NewJobRuntimeUpdater(mockJobStore, mockTaskStore, nil, tally.NoopScope)
	timeChange := float64(100000)
	var events = []*pb_eventstream.Event{
		{
			MesosTaskStatus: &mesos.TaskStatus{
				TaskId: &mesos.TaskID{
					Value: &taskID,
				},
				Timestamp: &timeChange,
			},
		},
	}
	updater.Start()
	updater.checkAllJobs()

	assert.Equal(t, job.JobState_RUNNING, jobRuntime.State)
	assert.Equal(t, uint32(2), jobRuntime.TaskStats[task.TaskState_SUCCEEDED.String()])
	assert.Equal(t, uint32(1), jobRuntime.TaskStats[task.TaskState_RUNNING.String()])

	mockTaskStore2.EXPECT().
		GetTasksForJobAndState(gomock.Any(), task.TaskState_SUCCEEDED.String()).
		Return(map[uint32]*task.TaskInfo{uint32(0): nil, uint32(1): nil, uint32(2): nil}, nil).
		AnyTimes()
	mockTaskStore2.EXPECT().
		GetTasksForJobAndState(gomock.Any(), task.TaskState_RUNNING.String()).
		Return(map[uint32]*task.TaskInfo{}, nil).
		AnyTimes()
	mockTaskStore2.EXPECT().
		GetTasksForJobAndState(gomock.Any(), gomock.Any()).
		Return(map[uint32]*task.TaskInfo{}, nil).
		AnyTimes()

	updater.taskStore = mockTaskStore2
	timeChange = float64(200000)
	events = []*pb_eventstream.Event{
		{
			MesosTaskStatus: &mesos.TaskStatus{
				TaskId: &mesos.TaskID{
					Value: &taskID,
				},
				Timestamp: &timeChange,
			},
		},
	}
	updater.OnEvents(events)
	assert.Equal(t, timeChange, updater.lastTaskUpdateTime["job0"])
	updater.updateJobsRuntime()

	assert.Equal(t, job.JobState_SUCCEEDED, jobRuntime.State)
	assert.Equal(t, uint32(3), jobRuntime.TaskStats[task.TaskState_SUCCEEDED.String()])
	assert.Equal(t, uint32(0), jobRuntime.TaskStats[task.TaskState_RUNNING.String()])
	updater.Stop()
}
