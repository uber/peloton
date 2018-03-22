package goalstate

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"

	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

const (
	jobStartTime      = "2017-01-02T15:04:05.456789016Z"
	jobCompletionTime = "2017-01-03T18:04:05.987654447Z"
)

func TestJobUpdateJobRuntime(t *testing.T) {
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

	instanceCount := uint32(100)

	jobRuntime := pb_job.RuntimeInfo{
		State:     pb_job.JobState_PENDING,
		GoalState: pb_job.JobState_SUCCEEDED,
	}

	// Simulate RUNNING job
	stateCounts := make(map[string]uint32)
	stateCounts[pb_task.TaskState_PENDING.String()] = instanceCount / 4
	stateCounts[pb_task.TaskState_RUNNING.String()] = instanceCount / 4
	stateCounts[pb_task.TaskState_LAUNCHED.String()] = instanceCount / 4
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount / 4

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), jobID).
		Return(&jobRuntime, nil)

	cachedJob.EXPECT().
		GetInstanceCount().
		Return(instanceCount)

	taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), jobID).
		Return(stateCounts, nil)

	cachedJob.EXPECT().
		GetFirstTaskUpdateTime().
		Return(float64(0))

	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), jobID, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			instanceCount := uint32(100)
			assert.Equal(t, runtime.State, pb_job.JobState_RUNNING)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_PENDING.String()], instanceCount/4)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_RUNNING.String()], instanceCount/4)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_LAUNCHED.String()], instanceCount/4)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_SUCCEEDED.String()], instanceCount/4)
		}).
		Return(nil)

	err := JobRuntimeUpdater(context.Background(), jobEnt)
	assert.NoError(t, err)

	// Simulate SUCCEEDED job
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount

	startTime, _ := time.Parse(time.RFC3339Nano, jobStartTime)
	startTimeUnix := float64(startTime.UnixNano()) / float64(time.Second/time.Nanosecond)
	endTime, _ := time.Parse(time.RFC3339Nano, jobCompletionTime)
	endTimeUnix := float64(endTime.UnixNano()) / float64(time.Second/time.Nanosecond)

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), jobID).
		Return(&jobRuntime, nil)

	cachedJob.EXPECT().
		GetInstanceCount().
		Return(instanceCount)

	taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), jobID).
		Return(stateCounts, nil)

	cachedJob.EXPECT().
		GetFirstTaskUpdateTime().
		Return(startTimeUnix)

	cachedJob.EXPECT().
		GetLastTaskUpdateTime().
		Return(endTimeUnix)

	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), jobID, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			instanceCount := uint32(100)
			assert.Equal(t, runtime.State, pb_job.JobState_SUCCEEDED)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_SUCCEEDED.String()], instanceCount)
			assert.Equal(t, runtime.StartTime, jobStartTime)
			assert.Equal(t, runtime.CompletionTime, jobCompletionTime)
		}).
		Return(nil)

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err = JobRuntimeUpdater(context.Background(), jobEnt)
	assert.NoError(t, err)

	// Simulate PENDING job
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_PENDING.String()] = instanceCount / 2
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount / 2

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), jobID).
		Return(&jobRuntime, nil)

	cachedJob.EXPECT().
		GetInstanceCount().
		Return(instanceCount)

	taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), jobID).
		Return(stateCounts, nil)

	cachedJob.EXPECT().
		GetFirstTaskUpdateTime().
		Return(startTimeUnix)

	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), jobID, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			instanceCount := uint32(100)
			assert.Equal(t, runtime.State, pb_job.JobState_PENDING)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_SUCCEEDED.String()], instanceCount/2)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_PENDING.String()], instanceCount/2)
		}).
		Return(nil)

	err = JobRuntimeUpdater(context.Background(), jobEnt)
	assert.NoError(t, err)

	// Simulate FAILED job
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_FAILED.String()] = instanceCount / 2
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount / 2

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), jobID).
		Return(&jobRuntime, nil)

	cachedJob.EXPECT().
		GetInstanceCount().
		Return(instanceCount)

	taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), jobID).
		Return(stateCounts, nil)

	cachedJob.EXPECT().
		GetFirstTaskUpdateTime().
		Return(startTimeUnix)

	cachedJob.EXPECT().
		GetLastTaskUpdateTime().
		Return(endTimeUnix)

	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), jobID, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			instanceCount := uint32(100)
			assert.Equal(t, runtime.State, pb_job.JobState_FAILED)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_SUCCEEDED.String()], instanceCount/2)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_FAILED.String()], instanceCount/2)
		}).
		Return(nil)

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err = JobRuntimeUpdater(context.Background(), jobEnt)
	assert.NoError(t, err)

	// Simulate KILLED job
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_FAILED.String()] = instanceCount / 4
	stateCounts[pb_task.TaskState_KILLED.String()] = instanceCount / 2
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount / 4

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), jobID).
		Return(&jobRuntime, nil)

	cachedJob.EXPECT().
		GetInstanceCount().
		Return(instanceCount)

	taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), jobID).
		Return(stateCounts, nil)

	cachedJob.EXPECT().
		GetFirstTaskUpdateTime().
		Return(startTimeUnix)

	cachedJob.EXPECT().
		GetLastTaskUpdateTime().
		Return(endTimeUnix)

	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), jobID, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			instanceCount := uint32(100)
			assert.Equal(t, runtime.State, pb_job.JobState_KILLED)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_SUCCEEDED.String()], instanceCount/4)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_FAILED.String()], instanceCount/4)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_KILLED.String()], instanceCount/2)
		}).
		Return(nil)

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err = JobRuntimeUpdater(context.Background(), jobEnt)
	assert.NoError(t, err)

	// Simulate INITIALIZED job
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount / 2
	jobRuntime = pb_job.RuntimeInfo{
		State:     pb_job.JobState_INITIALIZED,
		GoalState: pb_job.JobState_SUCCEEDED,
	}

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), jobID).
		Return(&jobRuntime, nil)

	cachedJob.EXPECT().
		GetInstanceCount().
		Return(instanceCount)

	taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), jobID).
		Return(stateCounts, nil)

	cachedJob.EXPECT().
		IsPartiallyCreated().
		Return(true).AnyTimes()

	cachedJob.EXPECT().
		GetFirstTaskUpdateTime().
		Return(startTimeUnix)

	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), jobID, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			instanceCount := uint32(100)
			assert.Equal(t, runtime.State, pb_job.JobState_INITIALIZED)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_SUCCEEDED.String()], instanceCount/2)
		}).
		Return(nil)

	err = JobRuntimeUpdater(context.Background(), jobEnt)
	assert.Error(t, err)

	// Simulate fake DB error
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount
	jobRuntime = pb_job.RuntimeInfo{
		State:     pb_job.JobState_RUNNING,
		GoalState: pb_job.JobState_SUCCEEDED,
	}

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), jobID).
		Return(&jobRuntime, nil)

	cachedJob.EXPECT().
		GetInstanceCount().
		Return(instanceCount)

	taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), jobID).
		Return(stateCounts, nil)

	cachedJob.EXPECT().
		GetFirstTaskUpdateTime().
		Return(startTimeUnix)

	cachedJob.EXPECT().
		GetLastTaskUpdateTime().
		Return(endTimeUnix)

	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), jobID, gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err = JobRuntimeUpdater(context.Background(), jobEnt)
	assert.Error(t, err)

	// Simulate SUCCEEDED job with correct task stats in runtime but incorrect state
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount
	jobRuntime = pb_job.RuntimeInfo{
		State:     pb_job.JobState_RUNNING,
		GoalState: pb_job.JobState_SUCCEEDED,
		TaskStats: stateCounts,
	}

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), jobID).
		Return(&jobRuntime, nil)

	cachedJob.EXPECT().
		GetInstanceCount().
		Return(instanceCount)

	taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), jobID).
		Return(stateCounts, nil)

	cachedJob.EXPECT().
		GetFirstTaskUpdateTime().
		Return(startTimeUnix)

	cachedJob.EXPECT().
		GetLastTaskUpdateTime().
		Return(endTimeUnix)

	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), jobID, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			assert.Equal(t, runtime.State, pb_job.JobState_SUCCEEDED)
		}).
		Return(nil)

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err = JobRuntimeUpdater(context.Background(), jobEnt)
	assert.NoError(t, err)

	// Simulate killed job with no tasks created
	stateCounts = make(map[string]uint32)
	jobRuntime = pb_job.RuntimeInfo{
		State:     pb_job.JobState_KILLED,
		GoalState: pb_job.JobState_KILLED,
		TaskStats: stateCounts,
	}

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), jobID).
		Return(&jobRuntime, nil)

	cachedJob.EXPECT().
		GetInstanceCount().
		Return(instanceCount)

	taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), jobID).
		Return(stateCounts, nil)

	err = JobRuntimeUpdater(context.Background(), jobEnt)
	assert.NoError(t, err)
}

func TestJobEvaluateMaxRunningInstances(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := store_mocks.NewMockJobStore(ctrl)
	taskStore := store_mocks.NewMockTaskStore(ctrl)
	resmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)

	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)

	goalStateDriver := &driver{
		jobEngine:    jobGoalStateEngine,
		taskEngine:   taskGoalStateEngine,
		jobStore:     jobStore,
		taskStore:    taskStore,
		jobFactory:   jobFactory,
		resmgrClient: resmgrClient,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	jobEnt := &jobEntity{
		id:     jobID,
		driver: goalStateDriver,
	}

	instanceCount := uint32(100)
	maxRunningInstances := uint32(10)

	jobConfig := pb_job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: instanceCount,
		Type:          pb_job.JobType_BATCH,
		Sla: &pb_job.SlaConfig{
			MaximumRunningInstances: maxRunningInstances,
		},
	}

	jobRuntime := pb_job.RuntimeInfo{
		State:     pb_job.JobState_RUNNING,
		GoalState: pb_job.JobState_SUCCEEDED,
	}

	// Simulate RUNNING job
	stateCounts := make(map[string]uint32)
	stateCounts[pb_task.TaskState_INITIALIZED.String()] = instanceCount / 2
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount / 2
	jobRuntime.TaskStats = stateCounts

	var initializedTasks []uint32
	for i := uint32(0); i < instanceCount/2; i++ {
		initializedTasks = append(initializedTasks, i)
	}

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), jobID).
		Return(&jobConfig, nil)

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), jobID).
		Return(&jobRuntime, nil)

	taskStore.EXPECT().
		GetTaskIDsForJobAndState(gomock.Any(), jobID, pb_task.TaskState_INITIALIZED.String()).
		Return(initializedTasks, nil)

	for i := uint32(0); i < jobConfig.Sla.MaximumRunningInstances; i++ {
		taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), jobID, gomock.Any()).
			Return(&pb_task.RuntimeInfo{
				State: pb_task.TaskState_INITIALIZED,
			}, nil)
		cachedJob.EXPECT().
			GetTask(gomock.Any()).Return(cachedTask)
		taskGoalStateEngine.EXPECT().
			IsScheduled(gomock.Any()).
			Return(false)
	}

	resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	jobFactory.EXPECT().
		GetJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(ctx context.Context, runtimes map[uint32]*pb_task.RuntimeInfo, req cached.UpdateRequest) {
			assert.Equal(t, uint32(len(runtimes)), jobConfig.Sla.MaximumRunningInstances)
			for _, runtime := range runtimes {
				assert.Equal(t, runtime.GetState(), pb_task.TaskState_PENDING)
			}
		}).
		Return(nil)

	err := JobEvaluateMaxRunningInstancesSLA(context.Background(), jobEnt)
	assert.NoError(t, err)

	// Simulate when max running instances are already running
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_INITIALIZED.String()] = instanceCount - maxRunningInstances
	stateCounts[pb_task.TaskState_RUNNING.String()] = maxRunningInstances
	jobRuntime.TaskStats = stateCounts

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), jobID).
		Return(&jobConfig, nil)

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), jobID).
		Return(&jobRuntime, nil)

	err = JobEvaluateMaxRunningInstancesSLA(context.Background(), jobEnt)
	assert.NoError(t, err)

	// Simulate error when scheduled instances is greater than maximum running instances
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_INITIALIZED.String()] = instanceCount / 2
	stateCounts[pb_task.TaskState_RUNNING.String()] = instanceCount / 2
	jobRuntime.TaskStats = stateCounts

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), jobID).
		Return(&jobConfig, nil)

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), jobID).
		Return(&jobRuntime, nil)

	err = JobEvaluateMaxRunningInstancesSLA(context.Background(), jobEnt)
	assert.NoError(t, err)
}
