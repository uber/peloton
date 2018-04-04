package goalstate

import (
	"context"
	"fmt"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/common/goalstate"
	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

// TestEnqueueJob tests enqueuing job into goal state engine.
func TestEnqueueJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	goalStateDriver := &driver{
		jobEngine: jobGoalStateEngine,
		mtx:       NewMetrics(tally.NoopScope),
		cfg:       &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(jobEntity goalstate.Entity, deadline time.Time) {
			assert.Equal(t, jobID.GetValue(), jobEntity.GetID())
		})

	goalStateDriver.EnqueueJob(jobID, time.Now())
}

// TestEnqueueTask tests enqueuing task into goal state engine.
func TestEnqueueTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)
	taskID := fmt.Sprintf("%s-%d", jobID.GetValue(), instanceID)

	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	goalStateDriver := &driver{
		taskEngine: taskGoalStateEngine,
		jobEngine:  jobGoalStateEngine,
		jobFactory: jobFactory,
		mtx:        NewMetrics(tally.NoopScope),
		cfg:        &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobFactory.EXPECT().
		GetJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetJobType().Return(job.JobType_BATCH)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(taskEntity goalstate.Entity, deadline time.Time) {
			assert.Equal(t, taskID, taskEntity.GetID())
		})

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).Return()

	goalStateDriver.EnqueueTask(jobID, instanceID, time.Now())
}

// TestEnqueueJob tests deleting job from goal state engine.
func TestDeleteJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	goalStateDriver := &driver{
		jobEngine: jobGoalStateEngine,
		mtx:       NewMetrics(tally.NoopScope),
		cfg:       &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Do(func(jobEntity goalstate.Entity) {
			assert.Equal(t, jobID.GetValue(), jobEntity.GetID())
		})

	goalStateDriver.DeleteJob(jobID)
}

// TestEnqueueTask tests deleting task from goal state engine.
func TestDeleteTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)
	taskID := fmt.Sprintf("%s-%d", jobID.GetValue(), instanceID)

	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	goalStateDriver := &driver{
		taskEngine: taskGoalStateEngine,
		mtx:        NewMetrics(tally.NoopScope),
		cfg:        &Config{},
	}
	goalStateDriver.cfg.normalize()

	taskGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Do(func(taskEntity goalstate.Entity) {
			assert.Equal(t, taskID, taskEntity.GetID())
		})

	goalStateDriver.DeleteTask(jobID, instanceID)
}

// TestIsScheduledTask tests determination oif whether a task
// is scheduled in goal state engine.
func TestIsScheduledTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)
	taskID := fmt.Sprintf("%s-%d", jobID.GetValue(), instanceID)

	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	goalStateDriver := &driver{
		taskEngine: taskGoalStateEngine,
		mtx:        NewMetrics(tally.NoopScope),
		cfg:        &Config{},
	}
	goalStateDriver.cfg.normalize()

	taskGoalStateEngine.EXPECT().
		IsScheduled(gomock.Any()).
		Do(func(taskEntity goalstate.Entity) {
			assert.Equal(t, taskID, taskEntity.GetID())
		}).Return(true)

	assert.True(t, goalStateDriver.IsScheduledTask(jobID, instanceID))
}

// TestRecoverJobStates will fail if a new job state is added without putting
// an explicit check here that the new state does not need to be recovered.
func TestRecoverJobStates(t *testing.T) {
	// Jobs which should not be recovered.
	var jobStatesNotRecover = []job.JobState{
		job.JobState_SUCCEEDED,
		job.JobState_FAILED,
		job.JobState_KILLED,
	}

	jobKnownStates := append(jobStatesNotRecover, jobStatesToRecover...)
	for _, state := range job.JobState_name {
		found := false
		for _, notRecover := range jobKnownStates {
			if notRecover.String() == state {
				found = true
			}
		}
		assert.True(t, found)
	}
}

// TestSyncFromDB tests syncing job manager with jobs and tasks in DB.
func TestSyncFromDB(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	id := "3c8a3c3e-71e3-49c5-9aed-2929823f595c"

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

	jobID := &peloton.JobID{Value: id}
	instanceID := uint32(0)
	var jobIDList []peloton.JobID
	jobIDList = append(jobIDList, peloton.JobID{Value: id})

	jobConfig := &job.JobConfig{
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		InstanceCount: 1,
	}

	jobStore.EXPECT().GetJobsByStates(gomock.Any(), gomock.Any()).Return(jobIDList, nil)

	jobStore.EXPECT().GetJobRuntime(gomock.Any(), jobID).Return(&job.RuntimeInfo{
		State:     job.JobState_RUNNING,
		GoalState: job.JobState_SUCCEEDED,
	}, nil)

	jobStore.EXPECT().GetJobConfig(gomock.Any(), jobID).Return(jobConfig, nil)

	jobFactory.EXPECT().
		GetJob(jobID).
		Return(nil)

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheOnly).Return(nil)

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	taskStore.EXPECT().GetTaskRuntimesForJobByRange(gomock.Any(), jobID, gomock.Any()).
		Return(map[uint32]*task.RuntimeInfo{
			instanceID: {
				GoalState:            task.TaskState_RUNNING,
				DesiredConfigVersion: 42,
				ConfigVersion:        42,
			},
		}, nil)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(nil)

	cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheOnly).Return(nil)

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetJobType().Return(job.JobType_BATCH)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	goalStateDriver.syncFromDB(context.Background())
}

// TestInitializedJobSyncFromDB tests syncing job manager with
// jobs in INITIALIZED job state.
func TestInitializedJobSyncFromDB(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	id := "3c8a3c3e-71e3-49c5-9aed-2929823f595c"

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

	jobID := &peloton.JobID{Value: id}
	instanceID := uint32(0)
	var jobIDList []peloton.JobID
	jobIDList = append(jobIDList, peloton.JobID{Value: id})

	jobConfig := &job.JobConfig{
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		InstanceCount: 1,
	}

	jobStore.EXPECT().GetJobsByStates(gomock.Any(), gomock.Any()).Return(jobIDList, nil)

	jobStore.EXPECT().GetJobRuntime(gomock.Any(), jobID).Return(&job.RuntimeInfo{
		State:     job.JobState_INITIALIZED,
		GoalState: job.JobState_SUCCEEDED,
	}, nil)

	jobStore.EXPECT().GetJobConfig(gomock.Any(), jobID).Return(jobConfig, nil)

	jobFactory.EXPECT().
		GetJob(jobID).
		Return(nil)

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheOnly).Return(nil)

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	taskStore.EXPECT().GetTaskRuntimesForJobByRange(gomock.Any(), jobID, gomock.Any()).
		Return(map[uint32]*task.RuntimeInfo{
			instanceID: {
				State:                task.TaskState_INITIALIZED,
				GoalState:            task.TaskState_SUCCEEDED,
				DesiredConfigVersion: 42,
				ConfigVersion:        42,
			},
		}, nil)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(nil)

	cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheOnly).Return(nil)

	goalStateDriver.syncFromDB(context.Background())
}

// TestEngineStartStop tests start and stop of goal state driver.
func TestEngineStartStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := store_mocks.NewMockJobStore(ctrl)
	taskStore := store_mocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)

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

	id := "3c8a3c3e-71e3-49c5-9aed-2929823f595c"

	// Test start
	var jobIDList []peloton.JobID
	jobGoalStateEngine.EXPECT().Start()
	taskGoalStateEngine.EXPECT().Start()
	jobStore.EXPECT().GetJobsByStates(gomock.Any(), gomock.Any()).Return(jobIDList, nil)

	goalStateDriver.Start()

	// Test stop
	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = cachedTask

	jobMap := make(map[string]cached.Job)
	jobMap[id] = cachedJob

	jobGoalStateEngine.EXPECT().Stop()
	taskGoalStateEngine.EXPECT().Stop()
	jobFactory.EXPECT().GetAllJobs().Return(jobMap)
	cachedJob.EXPECT().GetAllTasks().Return(taskMap)
	taskGoalStateEngine.EXPECT().Delete(gomock.Any())
	jobGoalStateEngine.EXPECT().Delete(gomock.Any())

	goalStateDriver.Stop()
}
