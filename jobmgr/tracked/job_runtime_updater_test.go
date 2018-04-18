package tracked

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

func addTasks(j *job, start uint32, end uint32, state pb_task.TaskState) {
	for i := start; i < end; i++ {
		task := &task{
			job: j,
			id:  i,
			runtime: &pb_task.RuntimeInfo{
				State: state,
			},
		}
		j.tasks[i] = task
	}
}

func TestJobUpdateJobRuntime(t *testing.T) {
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
			taskScheduler: newScheduler(NewQueueMetrics(tally.NoopScope)),
			jobScheduler:  newScheduler(NewQueueMetrics(tally.NoopScope)),
			running:       true,
		},
		tasks:            map[uint32]*task{},
		initializedTasks: map[uint32]*task{},
	}

	instanceCount := uint32(100)

	jobConfig := pb_job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: instanceCount,
		Type:          pb_job.JobType_BATCH,
	}

	jobRuntime := pb_job.RuntimeInfo{
		State:     pb_job.JobState_PENDING,
		GoalState: pb_job.JobState_SUCCEEDED,
	}

	// Simulate RUNNING job
	addTasks(j, 0, instanceCount/4, pb_task.TaskState_PENDING)
	addTasks(j, instanceCount/4, instanceCount/2, pb_task.TaskState_RUNNING)
	addTasks(j, instanceCount/2, 3*instanceCount/4, pb_task.TaskState_LAUNCHED)
	addTasks(j, 3*instanceCount/4, instanceCount, pb_task.TaskState_SUCCEEDED)
	stateCounts := make(map[string]uint32)
	stateCounts[pb_task.TaskState_PENDING.String()] = instanceCount / 4
	stateCounts[pb_task.TaskState_RUNNING.String()] = instanceCount / 4
	stateCounts[pb_task.TaskState_LAUNCHED.String()] = instanceCount / 4
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount / 4

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), j.id).
		Return(&jobRuntime, nil)

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), j.id).
		Return(&jobConfig, nil)

	taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), j.id).
		Return(stateCounts, nil)

	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), j.id, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			instanceCount := uint32(100)
			assert.Equal(t, runtime.State, pb_job.JobState_RUNNING)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_PENDING.String()], instanceCount/4)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_RUNNING.String()], instanceCount/4)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_LAUNCHED.String()], instanceCount/4)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_SUCCEEDED.String()], instanceCount/4)
		}).
		Return(nil)

	reschedule, err := j.JobRuntimeUpdater(context.Background())
	assert.False(t, reschedule)
	assert.NoError(t, err)

	// Simulate SUCCEEDED job
	j.tasks = map[uint32]*task{}
	addTasks(j, 0, instanceCount, pb_task.TaskState_SUCCEEDED)
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount

	startTime, _ := time.Parse(time.RFC3339Nano, jobStartTime)
	startTimeUnix := float64(startTime.UnixNano()) / float64(time.Second/time.Nanosecond)
	j.firstTaskUpdateTime = startTimeUnix
	endTime, _ := time.Parse(time.RFC3339Nano, jobCompletionTime)
	endTimeUnix := float64(endTime.UnixNano()) / float64(time.Second/time.Nanosecond)
	j.lastTaskUpdateTime = endTimeUnix

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), j.id).
		Return(&jobRuntime, nil)

	taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), j.id).
		Return(stateCounts, nil)

	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), j.id, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			instanceCount := uint32(100)
			assert.Equal(t, runtime.State, pb_job.JobState_SUCCEEDED)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_SUCCEEDED.String()], instanceCount)
			assert.Equal(t, runtime.StartTime, jobStartTime)
			assert.Equal(t, runtime.CompletionTime, jobCompletionTime)
		}).
		Return(nil)

	reschedule, err = j.JobRuntimeUpdater(context.Background())
	assert.False(t, reschedule)
	assert.NoError(t, err)

	// Simulate PENDING job
	j.tasks = map[uint32]*task{}
	addTasks(j, 0, instanceCount/2, pb_task.TaskState_SUCCEEDED)
	addTasks(j, instanceCount/2, instanceCount, pb_task.TaskState_PENDING)
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_PENDING.String()] = instanceCount / 2
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount / 2

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), j.id).
		Return(&jobRuntime, nil)

	taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), j.id).
		Return(stateCounts, nil)

	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), j.id, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			instanceCount := uint32(100)
			assert.Equal(t, runtime.State, pb_job.JobState_PENDING)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_SUCCEEDED.String()], instanceCount/2)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_PENDING.String()], instanceCount/2)
		}).
		Return(nil)

	reschedule, err = j.JobRuntimeUpdater(context.Background())
	assert.False(t, reschedule)
	assert.NoError(t, err)

	// Simulate FAILED job
	j.tasks = map[uint32]*task{}
	addTasks(j, 0, instanceCount/2, pb_task.TaskState_SUCCEEDED)
	addTasks(j, instanceCount/2, instanceCount, pb_task.TaskState_FAILED)
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_FAILED.String()] = instanceCount / 2
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount / 2

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), j.id).
		Return(&jobRuntime, nil)

	taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), j.id).
		Return(stateCounts, nil)

	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), j.id, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			instanceCount := uint32(100)
			assert.Equal(t, runtime.State, pb_job.JobState_FAILED)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_SUCCEEDED.String()], instanceCount/2)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_FAILED.String()], instanceCount/2)
		}).
		Return(nil)

	reschedule, err = j.JobRuntimeUpdater(context.Background())
	assert.False(t, reschedule)
	assert.NoError(t, err)

	// Simulate KILLED job
	j.tasks = map[uint32]*task{}
	addTasks(j, 0, instanceCount/4, pb_task.TaskState_SUCCEEDED)
	addTasks(j, instanceCount/4, instanceCount, pb_task.TaskState_FAILED)
	addTasks(j, instanceCount/2, instanceCount, pb_task.TaskState_KILLED)
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_FAILED.String()] = instanceCount / 4
	stateCounts[pb_task.TaskState_KILLED.String()] = instanceCount / 2
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount / 4

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), j.id).
		Return(&jobRuntime, nil)

	taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), j.id).
		Return(stateCounts, nil)

	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), j.id, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			instanceCount := uint32(100)
			assert.Equal(t, runtime.State, pb_job.JobState_KILLED)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_SUCCEEDED.String()], instanceCount/4)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_FAILED.String()], instanceCount/4)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_KILLED.String()], instanceCount/2)
		}).
		Return(nil)

	reschedule, err = j.JobRuntimeUpdater(context.Background())
	assert.False(t, reschedule)
	assert.NoError(t, err)

	// Simulate INITIALIZED job
	j.tasks = map[uint32]*task{}
	addTasks(j, 0, instanceCount/2, pb_task.TaskState_SUCCEEDED)
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount / 2
	jobRuntime = pb_job.RuntimeInfo{
		State:     pb_job.JobState_INITIALIZED,
		GoalState: pb_job.JobState_SUCCEEDED,
	}

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), j.id).
		Return(&jobRuntime, nil)

	taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), j.id).
		Return(stateCounts, nil)

	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), j.id, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			instanceCount := uint32(100)
			assert.Equal(t, runtime.State, pb_job.JobState_INITIALIZED)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_SUCCEEDED.String()], instanceCount/2)
		}).
		Return(nil)

	reschedule, err = j.JobRuntimeUpdater(context.Background())
	assert.True(t, reschedule)
	assert.Error(t, err)

	// Simulate fake DB error
	j.tasks = map[uint32]*task{}
	addTasks(j, 0, instanceCount, pb_task.TaskState_SUCCEEDED)
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount
	jobRuntime = pb_job.RuntimeInfo{
		State:     pb_job.JobState_RUNNING,
		GoalState: pb_job.JobState_SUCCEEDED,
	}

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), j.id).
		Return(&jobRuntime, nil)

	taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), j.id).
		Return(stateCounts, nil)

	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), j.id, gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	reschedule, err = j.JobRuntimeUpdater(context.Background())
	assert.True(t, reschedule)
	assert.Error(t, err)

	// Simulate SUCCEEDED job with correct task stats in runtime but incorrect state
	j.tasks = map[uint32]*task{}
	addTasks(j, 0, instanceCount, pb_task.TaskState_SUCCEEDED)
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount
	jobRuntime = pb_job.RuntimeInfo{
		State:     pb_job.JobState_RUNNING,
		GoalState: pb_job.JobState_SUCCEEDED,
		TaskStats: stateCounts,
	}

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), j.id).
		Return(&jobRuntime, nil)

	taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), j.id).
		Return(stateCounts, nil)

	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), j.id, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			assert.Equal(t, runtime.State, pb_job.JobState_SUCCEEDED)
		}).
		Return(nil)

	reschedule, err = j.JobRuntimeUpdater(context.Background())
	assert.False(t, reschedule)
	assert.NoError(t, err)

	// Simulate killed job with no tasks created
	j.tasks = map[uint32]*task{}
	stateCounts = make(map[string]uint32)
	jobRuntime = pb_job.RuntimeInfo{
		State:     pb_job.JobState_KILLED,
		GoalState: pb_job.JobState_KILLED,
		TaskStats: stateCounts,
	}
	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), j.id).
		Return(&jobRuntime, nil)

	taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), j.id).
		Return(stateCounts, nil)

	reschedule, err = j.JobRuntimeUpdater(context.Background())
	assert.False(t, reschedule)
	assert.NoError(t, err)
}

func TestJobUpdateJobWithMaxRunningInstances(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := store_mocks.NewMockJobStore(ctrl)
	taskStore := store_mocks.NewMockTaskStore(ctrl)
	resmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)

	j := &job{
		id: &peloton.JobID{Value: uuid.NewRandom().String()},
		m: &manager{
			mtx:           NewMetrics(tally.NoopScope),
			jobStore:      jobStore,
			taskStore:     taskStore,
			jobs:          map[string]*job{},
			taskScheduler: newScheduler(NewQueueMetrics(tally.NoopScope)),
			jobScheduler:  newScheduler(NewQueueMetrics(tally.NoopScope)),
			resmgrClient:  resmgrClient,
			running:       true,
		},
		tasks:            map[uint32]*task{},
		initializedTasks: map[uint32]*task{},
	}
	j.m.jobs[j.id.GetValue()] = j

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
	addTasks(j, 0, instanceCount/2, pb_task.TaskState_INITIALIZED)
	addTasks(j, instanceCount/2, instanceCount, pb_task.TaskState_SUCCEEDED)
	stateCounts := make(map[string]uint32)
	stateCounts[pb_task.TaskState_INITIALIZED.String()] = instanceCount / 2
	stateCounts[pb_task.TaskState_SUCCEEDED.String()] = instanceCount / 2
	var initializedTasks []uint32
	for i := uint32(0); i < instanceCount/2; i++ {
		initializedTasks = append(initializedTasks, i)
	}

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), j.id).
		Return(&jobRuntime, nil)

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), j.id).
		Return(&jobConfig, nil)

	taskStore.EXPECT().
		GetTaskStateSummaryForJob(gomock.Any(), j.id).
		Return(stateCounts, nil)

	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), j.id, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtime *pb_job.RuntimeInfo) {
			instanceCount := uint32(100)
			assert.Equal(t, runtime.State, pb_job.JobState_PENDING)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_INITIALIZED.String()], instanceCount/2)
			assert.Equal(t, runtime.TaskStats[pb_task.TaskState_SUCCEEDED.String()], instanceCount/2)
		}).
		Return(nil)

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), j.id).
		Return(&jobConfig, nil)

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), j.id).
		Return(&jobRuntime, nil)

	taskStore.EXPECT().
		GetTaskIDsForJobAndState(gomock.Any(), j.id, pb_task.TaskState_INITIALIZED.String()).
		Return(initializedTasks, nil)

	for i := uint32(0); i < jobConfig.Sla.MaximumRunningInstances; i++ {
		taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), j.ID(), gomock.Any()).
			Return(&pb_task.RuntimeInfo{
				State: pb_task.TaskState_INITIALIZED,
			}, nil)
	}

	resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	taskStore.EXPECT().
		UpdateTaskRuntimes(gomock.Any(), j.id, gomock.Any()).
		Do(func(ctx context.Context, id *peloton.JobID, runtimes map[uint32]*pb_task.RuntimeInfo) {
			assert.Equal(t, uint32(len(runtimes)), jobConfig.Sla.MaximumRunningInstances)
			for _, runtime := range runtimes {
				assert.Equal(t, runtime.GetState(), pb_task.TaskState_PENDING)
			}
		}).
		Return(nil)

	reschedule, err := j.JobRuntimeUpdater(context.Background())
	assert.False(t, reschedule)
	assert.NoError(t, err)
	reschedule, err = j.EvaluateMaxRunningInstancesSLA(context.Background())
	assert.False(t, reschedule)
	assert.NoError(t, err)

	// Simulate when max running instances are already running
	addTasks(j, 0, maxRunningInstances, pb_task.TaskState_RUNNING)
	addTasks(j, maxRunningInstances, instanceCount, pb_task.TaskState_INITIALIZED)
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_INITIALIZED.String()] = instanceCount - maxRunningInstances
	stateCounts[pb_task.TaskState_RUNNING.String()] = maxRunningInstances
	jobRuntime.TaskStats = stateCounts

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), j.id).
		Return(&jobConfig, nil)

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), j.id).
		Return(&jobRuntime, nil)

	reschedule, err = j.EvaluateMaxRunningInstancesSLA(context.Background())
	assert.False(t, reschedule)
	assert.NoError(t, err)

	// Simulate error when scheduled instances is greater than maximum running instances
	addTasks(j, 0, instanceCount/2, pb_task.TaskState_INITIALIZED)
	addTasks(j, instanceCount/2, instanceCount, pb_task.TaskState_RUNNING)
	stateCounts = make(map[string]uint32)
	stateCounts[pb_task.TaskState_INITIALIZED.String()] = instanceCount / 2
	stateCounts[pb_task.TaskState_RUNNING.String()] = instanceCount / 2
	jobRuntime.TaskStats = stateCounts

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), j.id).
		Return(&jobConfig, nil)

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), j.id).
		Return(&jobRuntime, nil)

	reschedule, err = j.EvaluateMaxRunningInstancesSLA(context.Background())
	assert.False(t, reschedule)
	assert.NoError(t, err)
}
