package job

import (
	"context"
	"fmt"
	"testing"
	"time"

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

const (
	numJobs  = 10
	numTasks = 20

	jobCreationTime   = "2017-01-02T11:00:00.123456789Z"
	jobStartTime      = "2017-01-02T15:04:05.456789016Z"
	jobCompletionTime = "2017-01-03T18:04:05.987654447Z"
)

func TestUpdateJobRuntime_Events(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var mockJobStore = store_mocks.NewMockJobStore(ctrl)
	var mockTaskStore = store_mocks.NewMockTaskStore(ctrl)

	updater := NewJobRuntimeUpdater(nil, mockJobStore, mockTaskStore, Config{}, tally.NoopScope)

	var events []*pb_eventstream.Event

	times := [numJobs][numTasks]float64{}

	for i := 0; i < numJobs; i++ {
		for j := 0; j < numTasks; j++ {
			times[i][j] = float64(i*numTasks + j)
			taskID := fmt.Sprintf("job%d-%d-%s", i, j, uuid.NewUUID().String())
			events = append(events, &pb_eventstream.Event{
				MesosTaskStatus: &mesos.TaskStatus{
					TaskId: &mesos.TaskID{
						Value: &taskID,
					},
					Timestamp: &times[i][j],
				},
			})
		}
	}
	updater.OnEvents(events)

	for i := 0; i < 10; i++ {
		assert.True(t, updater.taskUpdatedFlags[peloton.JobID{Value: fmt.Sprintf("job%d", i)}])
		assert.Equal(t, updater.firstTaskUpdateTime[peloton.JobID{Value: fmt.Sprintf("job%d", i)}],
			times[i][0])
		assert.Equal(t, updater.lastTaskUpdateTime[peloton.JobID{Value: fmt.Sprintf("job%d", i)}],
			times[i][numTasks-1])
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
		State:        job.JobState_PENDING,
		CreationTime: jobCreationTime,
	}
	nonTerminatedStates := []job.JobState{
		job.JobState_PENDING,
		job.JobState_RUNNING,
		job.JobState_UNKNOWN,
	}

	mockJobStore.EXPECT().
		GetJobConfig(context.Background(), jobID).
		Return(&jobConfig, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		GetJobsByStates(context.Background(), nonTerminatedStates).
		Return([]peloton.JobID{*jobID}, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		GetJobsByStates(context.Background(), gomock.Any()).
		Return([]peloton.JobID{}, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		GetJobRuntime(context.Background(), jobID).
		Return(&jobRuntime, nil).
		AnyTimes()
	mockTaskStore.EXPECT().
		GetTaskStateSummaryForJob(context.Background(), gomock.Any()).
		Return(map[string]uint32{task.TaskState_SUCCEEDED.String(): 2, task.TaskState_RUNNING.String(): 1}, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		UpdateJobRuntime(context.Background(), jobID, gomock.Any()).
		Return(nil).
		AnyTimes()

	taskID := fmt.Sprintf("job%d-%d-%s", 0, 1, uuid.NewUUID().String())
	eventTime, _ := time.Parse(time.RFC3339Nano, jobStartTime)
	eventTimeUnix := float64(eventTime.UnixNano()) / float64(time.Second/time.Nanosecond)
	var events = []*pb_eventstream.Event{
		{
			MesosTaskStatus: &mesos.TaskStatus{
				TaskId: &mesos.TaskID{
					Value: &taskID,
				},
				Timestamp: &eventTimeUnix,
			},
		},
	}
	// Initialize a new job runtime updater and expect to load three
	// tasks (two succeded and one running) from DB. Also process one
	// task event and verify the job start time is as expected.
	updater := NewJobRuntimeUpdater(nil, mockJobStore, mockTaskStore, Config{}, tally.NoopScope)
	updater.Start()
	updater.OnEvents(events)
	updater.checkAllJobs(context.Background())

	assert.Equal(t, job.JobState_RUNNING, jobRuntime.State)
	assert.Equal(t, uint32(2), jobRuntime.TaskStats[task.TaskState_SUCCEEDED.String()])
	assert.Equal(t, uint32(1), jobRuntime.TaskStats[task.TaskState_RUNNING.String()])
	assert.Equal(t, jobCreationTime, jobRuntime.CreationTime)
	assert.Equal(t, jobStartTime, jobRuntime.StartTime)

	// Update the DB to have all three tasks in successed
	// state. Process another task event and verify the job completion
	// time is as expected.
	mockTaskStore2.EXPECT().
		GetTaskStateSummaryForJob(context.Background(), gomock.Any()).
		Return(map[string]uint32{task.TaskState_SUCCEEDED.String(): 3}, nil)

	updater.taskStore = mockTaskStore2
	eventTime, _ = time.Parse(time.RFC3339Nano, jobCompletionTime)
	eventTimeUnix = float64(eventTime.UnixNano()) / float64(time.Second/time.Nanosecond)
	events = []*pb_eventstream.Event{
		{
			MesosTaskStatus: &mesos.TaskStatus{
				TaskId: &mesos.TaskID{
					Value: &taskID,
				},
				Timestamp: &eventTimeUnix,
			},
		},
	}
	updater.OnEvents(events)
	assert.Equal(t, eventTimeUnix, updater.lastTaskUpdateTime[peloton.JobID{Value: "job0"}])
	updater.updateJobsRuntime(context.Background())

	assert.Equal(t, job.JobState_SUCCEEDED, jobRuntime.State)
	assert.Equal(t, uint32(3), jobRuntime.TaskStats[task.TaskState_SUCCEEDED.String()])
	assert.Equal(t, uint32(0), jobRuntime.TaskStats[task.TaskState_RUNNING.String()])
	assert.Equal(t, jobCreationTime, jobRuntime.CreationTime)
	assert.Equal(t, jobStartTime, jobRuntime.StartTime)
	assert.Equal(t, jobCompletionTime, jobRuntime.CompletionTime)

	updater.Stop()
}

func TestFormatTime(t *testing.T) {
	str := formatTime(1495230211.12345, time.RFC3339Nano)
	assert.Equal(t, str, "2017-05-19T21:43:31.12345004Z")

}

func TestUpdateJobRuntime_SynchronousJobUpdate(t *testing.T) {
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
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: uint32(3),
	}
	var jobRuntime = job.RuntimeInfo{
		State: job.JobState_PENDING,
	}

	mockJobStore.EXPECT().
		GetJobConfig(context.Background(), jobID).
		Return(&jobConfig, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		GetJobsByStates(context.Background(), []job.JobState{job.JobState_PENDING}).
		Return([]peloton.JobID{*jobID}, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		GetJobsByStates(context.Background(), gomock.Any()).
		Return([]peloton.JobID{}, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		GetJobRuntime(context.Background(), jobID).
		Return(&jobRuntime, nil).
		AnyTimes()
	mockTaskStore.EXPECT().
		GetTaskStateSummaryForJob(context.Background(), gomock.Any()).
		Return(map[string]uint32{task.TaskState_SUCCEEDED.String(): 2, task.TaskState_RUNNING.String(): 1}, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		UpdateJobRuntime(context.Background(), jobID, gomock.Any()).
		Return(nil).
		AnyTimes()

	updater := NewJobRuntimeUpdater(nil, mockJobStore, mockTaskStore, Config{}, tally.NoopScope)
	updater.Start()
	updater.UpdateJob(context.Background(), jobID)

	assert.Equal(t, job.JobState_RUNNING, jobRuntime.State)
	assert.Equal(t, uint32(2), jobRuntime.TaskStats[task.TaskState_SUCCEEDED.String()])
	assert.Equal(t, uint32(1), jobRuntime.TaskStats[task.TaskState_RUNNING.String()])

	mockTaskStore2.EXPECT().
		GetTaskStateSummaryForJob(context.Background(), gomock.Any()).
		Return(map[string]uint32{task.TaskState_SUCCEEDED.String(): 3}, nil).
		AnyTimes()
	updater.taskStore = mockTaskStore2
	updater.UpdateJob(context.Background(), jobID)

	assert.Equal(t, job.JobState_SUCCEEDED, jobRuntime.State)
	assert.Equal(t, uint32(3), jobRuntime.TaskStats[task.TaskState_SUCCEEDED.String()])
	assert.Equal(t, uint32(0), jobRuntime.TaskStats[task.TaskState_RUNNING.String()])
	updater.Stop()
}
