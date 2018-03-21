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

	for i := uint32(0); i < instanceCount; i++ {
		tt := j.GetTask(i)
		actRuntime := tt.GetRunTime()
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
	runtime := pbtask.RuntimeInfo{
		State: pbtask.TaskState_SUCCEEDED,
	}
	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtimes[i] = &runtime
	}
	j.UpdateTasks(context.Background(), runtimes, UpdateCacheOnly)
	for instID := range runtimes {
		tt := j.GetTask(instID)
		actRuntime := tt.GetRunTime()
		assert.Equal(t, runtime, *actRuntime)
	}
}

// TestTasksUpdateInDB tests updating task runtimes in DB.
func TestTasksUpdateInDB(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := storemocks.NewMockJobStore(ctrl)
	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := peloton.JobID{Value: uuid.NewRandom().String()}
	instanceCount := uint32(10)
	j := initializeJob(jobStore, taskstore, &jobID)

	// Update task runtimes in DB and cache
	runtime := pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}
	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtimes[i] = &runtime
	}
	taskstore.EXPECT().
		UpdateTaskRuntimes(gomock.Any(), gomock.Any(), runtimes).Return(nil)
	err := j.UpdateTasks(context.Background(), runtimes, UpdateCacheAndDB)
	assert.NoError(t, err)
	for instID := range runtimes {
		tt := j.GetTask(instID)
		actRuntime := tt.GetRunTime()
		assert.Equal(t, runtime, *actRuntime)
	}
}

// TestTasksUpdateDBError tests getting DB error during update task runtimes.
func TestTasksUpdateDBError(t *testing.T) {
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
	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtimes[i] = &runtime
	}

	// Simulate fake DB error
	taskstore.EXPECT().
		UpdateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("fake db error"))
	err := j.UpdateTasks(context.Background(), runtimes, UpdateCacheAndDB)
	assert.Error(t, err)
	for instID := range runtimes {
		tt := j.GetTask(instID)
		actRuntime := tt.GetRunTime()
		assert.Nil(t, actRuntime)
	}
}

// TestTasksUpdateRuntimeSingleTask tests updating task runtime of a single task in DB.
func TestTasksUpdateRuntimeSingleTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := storemocks.NewMockJobStore(ctrl)
	taskstore := storemocks.NewMockTaskStore(ctrl)
	jobID := peloton.JobID{Value: uuid.NewRandom().String()}
	j := initializeJob(jobStore, taskstore, &jobID)

	runtime := pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}

	// Update task runtime of only one task
	taskstore.EXPECT().
		UpdateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	err := j.UpdateTasks(context.Background(), map[uint32]*pbtask.RuntimeInfo{0: &runtime}, UpdateCacheAndDB)
	assert.NoError(t, err)
	tt := j.GetTask(0)
	actRuntime := tt.GetRunTime()
	assert.Equal(t, runtime, *actRuntime)
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

	runtime := pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}
	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtimes[i] = &runtime
	}
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

	runtime := pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}
	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtimes[i] = &runtime
	}
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

	runtime := pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}
	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtimes[i] = &runtime
	}
	j.UpdateTasks(context.Background(), runtimes, UpdateCacheOnly)

	// Test clearing all tasks of job
	j.ClearAllTasks()
	assert.Equal(t, uint32(0), uint32(len(j.tasks)))
}
