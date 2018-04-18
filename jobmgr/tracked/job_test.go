package tracked

import (
	"context"
	"testing"

	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestJob(t *testing.T) {
	j := &job{
		id: &peloton.JobID{Value: uuid.NewRandom().String()},
		m: &manager{
			mtx: NewMetrics(tally.NoopScope),
		},
	}

	reschedule, err := j.RunAction(context.Background(), JobNoAction)
	assert.False(t, reschedule)
	assert.NoError(t, err)

	jobRuntime := &pb_job.RuntimeInfo{
		State:     pb_job.JobState_RUNNING,
		GoalState: pb_job.JobState_SUCCEEDED,
	}
	jobConfig := &pb_job.JobConfig{
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		InstanceCount: 1,
	}
	jobInfo := &pb_job.JobInfo{
		Runtime: jobRuntime,
		Config:  jobConfig,
	}
	j.updateRuntime(jobInfo)

	runtime, err := j.GetJobRuntime(context.Background())
	assert.Equal(t, jobRuntime, runtime)
	assert.NoError(t, err)

	config, err := j.GetConfig()
	assert.Equal(t, jobConfig.RespoolID, config.respoolID)
	assert.Equal(t, jobConfig.InstanceCount, config.instanceCount)
	assert.NoError(t, err)

	j.ClearJobRuntime()
	assert.Nil(t, j.runtime)
}

func TestJobKill(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := store_mocks.NewMockJobStore(ctrl)
	taskStore := store_mocks.NewMockTaskStore(ctrl)

	j := &job{
		id: &peloton.JobID{Value: uuid.NewRandom().String()},
		m: &manager{
			mtx:           NewMetrics(tally.NoopScope),
			jobStore:      jobStore,
			taskStore:     taskStore,
			jobs:          map[string]*job{},
			running:       true,
			taskScheduler: newScheduler(NewQueueMetrics(tally.NoopScope)),
			jobScheduler:  newScheduler(NewQueueMetrics(tally.NoopScope)),
			stopChan:      make(chan struct{}),
		},
		tasks: map[uint32]*task{},
	}
	j.m.jobs[j.id.GetValue()] = j

	instanceCount := uint32(4)
	jobConfig := pb_job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: instanceCount,
		Type:          pb_job.JobType_BATCH,
	}

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), j.id).
		Return(&jobConfig, nil)

	runtimes := make(map[uint32]*pb_task.RuntimeInfo)
	runtimes[0] = &pb_task.RuntimeInfo{
		State:     pb_task.TaskState_RUNNING,
		GoalState: pb_task.TaskState_SUCCEEDED,
	}
	runtimes[1] = &pb_task.RuntimeInfo{
		State:     pb_task.TaskState_RUNNING,
		GoalState: pb_task.TaskState_SUCCEEDED,
	}
	runtimes[2] = &pb_task.RuntimeInfo{
		State:     pb_task.TaskState_INITIALIZED,
		GoalState: pb_task.TaskState_SUCCEEDED,
	}
	runtimes[3] = &pb_task.RuntimeInfo{
		State:     pb_task.TaskState_INITIALIZED,
		GoalState: pb_task.TaskState_SUCCEEDED,
	}

	newRuntimes := make(map[uint32]*pb_task.RuntimeInfo)
	newRuntimes[0] = &pb_task.RuntimeInfo{
		State:     pb_task.TaskState_RUNNING,
		GoalState: pb_task.TaskState_KILLED,
		Message:   "Task stop API request",
		Reason:    "",
	}
	newRuntimes[1] = &pb_task.RuntimeInfo{
		State:     pb_task.TaskState_RUNNING,
		GoalState: pb_task.TaskState_KILLED,
		Message:   "Task stop API request",
		Reason:    "",
	}
	newRuntimes[2] = &pb_task.RuntimeInfo{
		State:     pb_task.TaskState_INITIALIZED,
		GoalState: pb_task.TaskState_KILLED,
		Message:   "Task stop API request",
		Reason:    "",
	}
	newRuntimes[3] = &pb_task.RuntimeInfo{
		State:     pb_task.TaskState_INITIALIZED,
		GoalState: pb_task.TaskState_KILLED,
		Message:   "Task stop API request",
		Reason:    "",
	}

	jobRuntime := &pb_job.RuntimeInfo{
		State:     pb_job.JobState_RUNNING,
		GoalState: pb_job.JobState_SUCCEEDED,
	}
	newJobRuntime := &pb_job.RuntimeInfo{
		State:     pb_job.JobState_KILLING,
		GoalState: pb_job.JobState_SUCCEEDED,
	}

	taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), j.id, gomock.Any()).
		Return(runtimes, nil)
	taskStore.EXPECT().
		UpdateTaskRuntimes(gomock.Any(), j.id, newRuntimes).
		Return(nil)
	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), j.id).
		Return(jobRuntime, nil)
	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), j.id, newJobRuntime).
		Return(nil)

	reschedule, err := j.RunAction(context.Background(), JobKill)
	assert.False(t, reschedule)
	assert.NoError(t, err)
	assert.Equal(t, instanceCount, uint32(len(j.tasks)))

}

func TestJobKillPartiallyCreatedJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := store_mocks.NewMockJobStore(ctrl)
	taskStore := store_mocks.NewMockTaskStore(ctrl)

	j := &job{
		id: &peloton.JobID{Value: uuid.NewRandom().String()},
		m: &manager{
			mtx:           NewMetrics(tally.NoopScope),
			jobStore:      jobStore,
			taskStore:     taskStore,
			jobs:          map[string]*job{},
			running:       true,
			taskScheduler: newScheduler(NewQueueMetrics(tally.NoopScope)),
			jobScheduler:  newScheduler(NewQueueMetrics(tally.NoopScope)),
			stopChan:      make(chan struct{}),
		},
		tasks: map[uint32]*task{},
	}
	j.m.jobs[j.id.GetValue()] = j

	instanceCount := uint32(4)
	jobConfig := pb_job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: instanceCount,
		Type:          pb_job.JobType_BATCH,
	}

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), j.id).
		Return(&jobConfig, nil)

	runtimes := make(map[uint32]*pb_task.RuntimeInfo)
	runtimes[2] = &pb_task.RuntimeInfo{
		State:     pb_task.TaskState_SUCCEEDED,
		GoalState: pb_task.TaskState_KILLED,
	}
	runtimes[3] = &pb_task.RuntimeInfo{
		State:     pb_task.TaskState_KILLED,
		GoalState: pb_task.TaskState_KILLED,
	}
	jobRuntime := &pb_job.RuntimeInfo{
		State:     pb_job.JobState_INITIALIZED,
		GoalState: pb_job.JobState_KILLED,
	}
	newJobRuntime := &pb_job.RuntimeInfo{
		State:     pb_job.JobState_KILLED,
		GoalState: pb_job.JobState_KILLED,
	}

	taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), j.id, gomock.Any()).
		Return(runtimes, nil)
	taskStore.EXPECT().
		UpdateTaskRuntimes(gomock.Any(), j.id, gomock.Any()).
		Return(nil)
	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), j.id).
		Return(jobRuntime, nil)
	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), j.id, newJobRuntime).
		Return(nil)

	reschedule, err := j.RunAction(context.Background(), JobKill)
	assert.False(t, reschedule)
	assert.NoError(t, err)

	runtimes[2] = &pb_task.RuntimeInfo{
		State:     pb_task.TaskState_RUNNING,
		GoalState: pb_task.TaskState_KILLED,
	}
	newJobRuntime.State = pb_job.JobState_KILLING

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), j.id).
		Return(&jobConfig, nil)
	taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), j.id, gomock.Any()).
		Return(runtimes, nil)
	taskStore.EXPECT().
		UpdateTaskRuntimes(gomock.Any(), j.id, gomock.Any()).
		Return(nil)
	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), j.id).
		Return(jobRuntime, nil)
	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), j.id, newJobRuntime).
		Return(nil)

	reschedule, err = j.RunAction(context.Background(), JobKill)
	assert.False(t, reschedule)
	assert.NoError(t, err)
}
