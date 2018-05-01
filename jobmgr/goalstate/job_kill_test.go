package goalstate

import (
	"context"
	"testing"

	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestJobKill(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := store_mocks.NewMockJobStore(ctrl)
	taskStore := store_mocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)

	goalStateDriver := &driver{
		jobEngine:  jobGoalStateEngine,
		taskEngine: taskGoalStateEngine,
		jobStore:   jobStore,
		taskStore:  taskStore,
		jobFactory: jobFactory,
		mtx:        NewMetrics(tally.NoopScope),
		cfg:        &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	jobEnt := &jobEntity{
		id:     jobID,
		driver: goalStateDriver,
	}

	instanceCount := uint32(4)
	jobConfig := pb_job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: instanceCount,
		Type:          pb_job.JobType_BATCH,
	}

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), jobID).
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
		GetTaskRuntimesForJobByRange(gomock.Any(), jobID, gomock.Any()).
		Return(runtimes, nil)
	jobFactory.EXPECT().
		GetJob(jobID).
		Return(cachedJob).AnyTimes()
	cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), newRuntimes, cached.UpdateCacheAndDB).
		Return(nil)
	cachedJob.EXPECT().
		GetJobType().Return(pb_job.JobType_BATCH).Times(int(instanceCount))
	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return().Times(int(instanceCount))
	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return().Times(int(instanceCount))
	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), jobID).
		Return(jobRuntime, nil)
	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), jobID, newJobRuntime).
		Return(nil)

	err := JobKill(context.Background(), jobEnt)
	assert.NoError(t, err)
}

func TestJobKillPartiallyCreatedJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := store_mocks.NewMockJobStore(ctrl)
	taskStore := store_mocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)

	goalStateDriver := &driver{
		jobEngine:  jobGoalStateEngine,
		taskEngine: taskGoalStateEngine,
		jobStore:   jobStore,
		taskStore:  taskStore,
		jobFactory: jobFactory,
		mtx:        NewMetrics(tally.NoopScope),
		cfg:        &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	jobEnt := &jobEntity{
		id:     jobID,
		driver: goalStateDriver,
	}

	instanceCount := uint32(4)
	jobConfig := pb_job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: instanceCount,
		Type:          pb_job.JobType_BATCH,
	}

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), jobID).
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
		GetTaskRuntimesForJobByRange(gomock.Any(), jobID, gomock.Any()).
		Return(runtimes, nil)
	jobFactory.EXPECT().
		GetJob(jobID).
		Return(cachedJob)
	cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Return(nil)
	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), jobID).
		Return(jobRuntime, nil)
	cachedJob.EXPECT().
		IsPartiallyCreated().Return(true)
	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), jobID, newJobRuntime).
		Return(nil)

	err := JobKill(context.Background(), jobEnt)
	assert.NoError(t, err)

	runtimes[2] = &pb_task.RuntimeInfo{
		State:     pb_task.TaskState_RUNNING,
		GoalState: pb_task.TaskState_KILLED,
	}
	jobRuntime.State = pb_job.JobState_INITIALIZED
	newJobRuntime.State = pb_job.JobState_KILLING

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), jobID).
		Return(&jobConfig, nil)
	taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), jobID, gomock.Any()).
		Return(runtimes, nil)
	jobFactory.EXPECT().
		GetJob(jobID).
		Return(cachedJob)
	cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Return(nil)
	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), jobID).
		Return(jobRuntime, nil)
	cachedJob.EXPECT().
		IsPartiallyCreated().Return(true)
	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), jobID, newJobRuntime).
		Return(nil)

	err = JobKill(context.Background(), jobEnt)
	assert.NoError(t, err)
}
