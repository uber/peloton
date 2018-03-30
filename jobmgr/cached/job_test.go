package cached

import (
	"context"
	"fmt"
	"testing"
	"time"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func initializeJob(jobStore *storemocks.MockJobStore, taskStore *storemocks.MockTaskStore, jobID *peloton.JobID) *job {
	j := &job{
		id: jobID,
		jobFactory: &jobFactory{
			mtx:       NewMetrics(tally.NoopScope),
			jobStore:  jobStore,
			taskStore: taskStore,
			running:   true,
			jobs:      map[string]*job{},
		},
		tasks: map[uint32]*task{},
	}
	j.jobFactory.jobs[j.id.GetValue()] = j
	return j
}

func initializeRuntimes(instanceCount uint32, state pbtask.TaskState) map[uint32]*pbtask.RuntimeInfo {
	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State: state,
		}
		runtimes[i] = runtime
	}
	return runtimes
}

func initializeCurrentRuntime(state pbtask.TaskState) *pbtask.RuntimeInfo {
	runtime := &pbtask.RuntimeInfo{
		State: state,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}
	return runtime
}

// TestJobFetchID tests fetching job ID.
func TestJobFetchID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := storemocks.NewMockJobStore(ctrl)
	jobID := peloton.JobID{Value: uuid.NewRandom().String()}

	j := initializeJob(jobStore, nil, &jobID)

	// Test fetching ID
	assert.Equal(t, jobID, *j.ID())
}

// TestJobSetAndFetchConfigAndRuntime tests setting and fetching
// job configuration and runtime.
func TestJobSetAndFetchConfigAndRuntime(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := storemocks.NewMockJobStore(ctrl)
	jobID := peloton.JobID{Value: uuid.NewRandom().String()}

	j := initializeJob(jobStore, nil, &jobID)

	// Test setting and fetching job config and runtime
	instanceCount := uint32(10)
	maxRunningInstances := uint32(2)
	maxRunningTime := uint32(5)
	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_SUCCEEDED,
	}
	jobConfig := &pbjob.JobConfig{
		Sla: &pbjob.SlaConfig{
			MaximumRunningInstances: maxRunningInstances,
			MaxRunningTime:          maxRunningTime,
		},
		InstanceCount: instanceCount,
		Type:          pbjob.JobType_BATCH,
	}
	jobInfo := &pbjob.JobInfo{
		Runtime: jobRuntime,
		Config:  jobConfig,
	}

	j.Update(context.Background(), jobInfo, UpdateCacheOnly)
	actJobRuntime, _ := j.GetRuntime(context.Background())
	assert.Equal(t, jobRuntime, actJobRuntime)
	assert.Equal(t, instanceCount, j.GetInstanceCount())
	assert.Equal(t, pbjob.JobType_BATCH, j.GetJobType())
	assert.Equal(t, maxRunningInstances, j.GetSLAConfig().GetMaximumRunningInstances())
	assert.Equal(t, maxRunningTime, j.GetSLAConfig().GetMaxRunningTime())
}

// TestJobClearRuntime tests clearing the runtime of a job.
func TestJobClearRuntime(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := storemocks.NewMockJobStore(ctrl)
	jobID := peloton.JobID{Value: uuid.NewRandom().String()}

	j := initializeJob(jobStore, nil, &jobID)

	instanceCount := uint32(10)
	maxRunningInstances := uint32(2)
	maxRunningTime := uint32(5)
	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_SUCCEEDED,
	}
	jobConfig := &pbjob.JobConfig{
		Sla: &pbjob.SlaConfig{
			MaximumRunningInstances: maxRunningInstances,
			MaxRunningTime:          maxRunningTime,
		},
		InstanceCount: instanceCount,
		Type:          pbjob.JobType_BATCH,
	}
	jobInfo := &pbjob.JobInfo{
		Runtime: jobRuntime,
		Config:  jobConfig,
	}

	j.Update(context.Background(), jobInfo, UpdateCacheOnly)

	// Test clearing job runtime and the fetching the job runtime
	j.ClearRuntime()
	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), &jobID).Return(nil, nil)
	actJobRuntime, _ := j.GetRuntime(context.Background())
	assert.Nil(t, actJobRuntime)
}

// TestJobDBError tests DB errors during job operations.
func TestJobDBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := storemocks.NewMockJobStore(ctrl)
	jobID := peloton.JobID{Value: uuid.NewRandom().String()}

	j := initializeJob(jobStore, nil, &jobID)

	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_SUCCEEDED,
	}

	// Test db error in fetching job runtime
	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), &jobID).Return(nil, fmt.Errorf("fake db error"))
	actJobRuntime, err := j.GetRuntime(context.Background())
	assert.Error(t, err)

	// Test updating job runtime in DB and cache
	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), &jobID, jobRuntime).Return(nil)
	err = j.Update(context.Background(), &pbjob.JobInfo{Runtime: jobRuntime}, UpdateCacheAndDB)
	assert.NoError(t, err)
	actJobRuntime, err = j.GetRuntime(context.Background())
	assert.Equal(t, jobRuntime, actJobRuntime)
	assert.NoError(t, err)

	// Test error in DB while update job runtime
	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), &jobID, jobRuntime).Return(fmt.Errorf("fake db error"))
	err = j.Update(context.Background(), &pbjob.JobInfo{Runtime: jobRuntime}, UpdateCacheAndDB)
	assert.Error(t, err)
}

// TestJobSetJobUpdateTime tests update the task update time coming from mesos.
func TestJobSetJobUpdateTime(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := storemocks.NewMockJobStore(ctrl)
	jobID := peloton.JobID{Value: uuid.NewRandom().String()}

	j := initializeJob(jobStore, nil, &jobID)

	// Test setting and fetching job update time
	updateTime := float64(time.Now().UnixNano())
	j.SetTaskUpdateTime(&updateTime)
	assert.Equal(t, updateTime, j.GetFirstTaskUpdateTime())
	assert.Equal(t, updateTime, j.GetLastTaskUpdateTime())
}

// TestJobCreateTasks tests creating task runtimes in cache and DB
func TestJobCreateTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceCount := uint32(10)
	j := initializeJob(nil, taskstore, jobID)
	runtimes := initializeRuntimes(instanceCount, pbtask.TaskState_INITIALIZED)

	for i := uint32(0); i < instanceCount; i++ {
		taskstore.EXPECT().
			CreateTaskRuntime(gomock.Any(), jobID, i, gomock.Any(), "peloton").
			Do(func(ctx context.Context, jobID *peloton.JobID, instanceID uint32, runtime *pbtask.RuntimeInfo, owner string) {
				assert.Equal(t, pbtask.TaskState_INITIALIZED, runtime.GetState())
				assert.Equal(t, uint64(1), runtime.GetRevision().GetVersion())
			}).Return(nil)
	}

	err := j.CreateTasks(context.Background(), runtimes, "peloton")
	assert.NoError(t, err)

	// Validate the state of the tasks in cache is correct
	for i := uint32(0); i < instanceCount; i++ {
		tt := j.GetTask(i)
		assert.NotNil(t, tt)
		actRuntime, _ := tt.GetRunTime(context.Background())
		assert.NotNil(t, actRuntime)
		assert.Equal(t, pbtask.TaskState_INITIALIZED, actRuntime.GetState())
		assert.Equal(t, uint64(1), actRuntime.GetRevision().GetVersion())
	}
}

// TestJobCreateTasksWithDBError tests getting DB error while creating task runtimes
func TestJobCreateTasksWithDBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceCount := uint32(10)
	j := initializeJob(nil, taskstore, jobID)

	runtimes := initializeRuntimes(instanceCount, pbtask.TaskState_INITIALIZED)
	for i := uint32(0); i < instanceCount; i++ {
		taskstore.EXPECT().
			CreateTaskRuntime(gomock.Any(), jobID, i, gomock.Any(), "peloton").
			Return(fmt.Errorf("fake db error"))
	}

	err := j.CreateTasks(context.Background(), runtimes, "peloton")
	assert.Error(t, err)
}

// TestSetGetTasksInJobInCacheSingle tests setting and getting single task in job in cache.
func TestSetGetTasksInJobInCacheSingle(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := storemocks.NewMockJobStore(ctrl)
	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := peloton.JobID{Value: uuid.NewRandom().String()}
	instanceCount := uint32(10)
	j := initializeJob(jobStore, taskstore, &jobID)

	runtime := pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}

	// Test updating tasks one at a time in cache
	for i := uint32(0); i < instanceCount; i++ {
		j.UpdateTasks(context.Background(), map[uint32]*pbtask.RuntimeInfo{i: &runtime}, UpdateCacheOnly)
	}
	assert.Equal(t, instanceCount, uint32(len(j.tasks)))

	// Validate the state of the tasks in cache is correct
	for i := uint32(0); i < instanceCount; i++ {
		tt := j.GetTask(i)
		actRuntime, _ := tt.GetRunTime(context.Background())
		assert.Equal(t, runtime, *actRuntime)
	}
}

// TestSetGetTasksInJobInCacheBlock tests setting and getting tasks as a chunk in a job in cache.
func TestSetGetTasksInJobInCacheBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := storemocks.NewMockJobStore(ctrl)
	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := peloton.JobID{Value: uuid.NewRandom().String()}
	instanceCount := uint32(10)
	j := initializeJob(jobStore, taskstore, &jobID)

	// Test updating tasks in one call in cache
	runtimes := initializeRuntimes(instanceCount, pbtask.TaskState_SUCCEEDED)
	j.UpdateTasks(context.Background(), runtimes, UpdateCacheOnly)

	// Validate the state of the tasks in cache is correct
	for instID, runtime := range runtimes {
		tt := j.GetTask(instID)
		actRuntime, _ := tt.GetRunTime(context.Background())
		assert.Equal(t, runtime, actRuntime)
	}
}

// TestTasksUpdateInDB tests updating task runtimes in DB.
func TestTasksUpdateInDB(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := storemocks.NewMockJobStore(ctrl)
	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceCount := uint32(10)
	j := initializeJob(jobStore, taskstore, jobID)

	runtimes := initializeRuntimes(instanceCount, pbtask.TaskState_RUNNING)

	for i := uint32(0); i < instanceCount; i++ {
		oldRuntime := initializeCurrentRuntime(pbtask.TaskState_LAUNCHED)
		taskstore.EXPECT().
			GetTaskRuntime(gomock.Any(), jobID, i).Return(oldRuntime, nil)
		taskstore.EXPECT().
			UpdateTaskRuntime(gomock.Any(), jobID, i, gomock.Any()).Return(nil)
	}

	// Update task runtimes in DB and cache
	err := j.UpdateTasks(context.Background(), runtimes, UpdateCacheAndDB)
	assert.NoError(t, err)

	// Validate the state of the tasks in cache is correct
	for instID := range runtimes {
		tt := j.GetTask(instID)
		assert.NotNil(t, tt)
		actRuntime, _ := tt.GetRunTime(context.Background())
		assert.NotNil(t, actRuntime)
		assert.Equal(t, pbtask.TaskState_RUNNING, actRuntime.GetState())
		assert.Equal(t, uint64(2), actRuntime.GetRevision().GetVersion())
	}
}

// TestTasksUpdateDBError tests getting DB error during update task runtimes.
func TestTasksUpdateDBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := storemocks.NewMockJobStore(ctrl)
	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceCount := uint32(10)
	j := initializeJob(jobStore, taskstore, jobID)

	runtime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}
	oldRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_LAUNCHED,
	}
	runtimes := make(map[uint32]*pbtask.RuntimeInfo)

	for i := uint32(0); i < instanceCount; i++ {
		tt := j.addTask(i)
		tt.runtime = oldRuntime
		runtimes[i] = runtime
		// Simulate fake DB error
		taskstore.EXPECT().
			UpdateTaskRuntime(gomock.Any(), jobID, i, gomock.Any()).Return(fmt.Errorf("fake db error"))
	}
	err := j.UpdateTasks(context.Background(), runtimes, UpdateCacheAndDB)
	assert.Error(t, err)
}

// TestTasksUpdateRuntimeSingleTask tests updating task runtime of a single task in DB.
func TestTasksUpdateRuntimeSingleTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := storemocks.NewMockJobStore(ctrl)
	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := peloton.JobID{Value: uuid.NewRandom().String()}
	j := initializeJob(jobStore, taskstore, &jobID)

	runtime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}
	oldRuntime := initializeCurrentRuntime(pbtask.TaskState_LAUNCHED)
	tt := j.addTask(0)
	tt.runtime = oldRuntime

	// Update task runtime of only one task
	taskstore.EXPECT().
		UpdateTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	err := j.UpdateTasks(context.Background(), map[uint32]*pbtask.RuntimeInfo{0: runtime}, UpdateCacheAndDB)
	assert.NoError(t, err)

	// Validate the state of the task in cache is correct
	att := j.GetTask(0)
	actRuntime, _ := att.GetRunTime(context.Background())
	assert.Equal(t, pbtask.TaskState_RUNNING, actRuntime.GetState())
	assert.Equal(t, uint64(2), actRuntime.GetRevision().GetVersion())
}

// TestTasksGetAllTasks tests getting all tasks.
func TestTasksGetAllTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := storemocks.NewMockJobStore(ctrl)
	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := peloton.JobID{Value: uuid.NewRandom().String()}
	instanceCount := uint32(10)
	j := initializeJob(jobStore, taskstore, &jobID)

	runtimes := initializeRuntimes(instanceCount, pbtask.TaskState_RUNNING)
	j.UpdateTasks(context.Background(), runtimes, UpdateCacheOnly)

	// Test get all tasks
	ttMap := j.GetAllTasks()
	assert.Equal(t, instanceCount, uint32(len(ttMap)))
}

// TestPartialJobCheck checks the partial job check.
func TestPartialJobCheck(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := storemocks.NewMockJobStore(ctrl)
	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := peloton.JobID{Value: uuid.NewRandom().String()}
	instanceCount := uint32(10)
	j := initializeJob(jobStore, taskstore, &jobID)

	runtimes := initializeRuntimes(instanceCount, pbtask.TaskState_RUNNING)
	j.UpdateTasks(context.Background(), runtimes, UpdateCacheOnly)

	// Test partial job check
	j.instanceCount = 20
	assert.True(t, j.IsPartiallyCreated())
}

// TestClearTasks tests cleaning up all tasks in the job in cache.
func TestClearTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := storemocks.NewMockJobStore(ctrl)
	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := peloton.JobID{Value: uuid.NewRandom().String()}
	instanceCount := uint32(10)

	j := initializeJob(jobStore, taskstore, &jobID)

	runtimes := initializeRuntimes(instanceCount, pbtask.TaskState_RUNNING)
	j.UpdateTasks(context.Background(), runtimes, UpdateCacheOnly)

	// Test clearing all tasks of job
	j.ClearAllTasks()
	assert.Equal(t, uint32(0), uint32(len(j.tasks)))
}
