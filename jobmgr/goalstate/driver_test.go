package goalstate

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/goalstate"
	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	launchermocks "code.uber.internal/infra/peloton/jobmgr/task/launcher/mocks"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/http"
)

type DriverTestSuite struct {
	suite.Suite
	ctrl                  *gomock.Controller
	jobGoalStateEngine    *goalstatemocks.MockEngine
	updateGoalStateEngine *goalstatemocks.MockEngine
	taskGoalStateEngine   *goalstatemocks.MockEngine
	jobStore              *storemocks.MockJobStore
	taskStore             *storemocks.MockTaskStore
	jobFactory            *cachedmocks.MockJobFactory
	updateFactory         *cachedmocks.MockUpdateFactory
	goalStateDriver       *driver
	cachedJob             *cachedmocks.MockJob
	jobID                 *peloton.JobID
	updateID              *peloton.UpdateID
	instanceID            uint32
}

func TestDriver(t *testing.T) {
	suite.Run(t, new(DriverTestSuite))
}

func (suite *DriverTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.updateGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.updateFactory = cachedmocks.NewMockUpdateFactory(suite.ctrl)
	suite.goalStateDriver = &driver{
		jobEngine:     suite.jobGoalStateEngine,
		taskEngine:    suite.taskGoalStateEngine,
		updateEngine:  suite.updateGoalStateEngine,
		jobStore:      suite.jobStore,
		taskStore:     suite.taskStore,
		jobFactory:    suite.jobFactory,
		updateFactory: suite.updateFactory,
		mtx:           NewMetrics(tally.NoopScope),
		cfg:           &Config{},
		jobType:       job.JobType_BATCH,
	}
	suite.goalStateDriver.cfg.normalize()
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.updateID = &peloton.UpdateID{Value: uuid.NewRandom().String()}
	suite.instanceID = uint32(0)
}

// Test constructor
func (suite *DriverTestSuite) TestNewDriver() {
	volumeStore := storemocks.NewMockPersistentVolumeStore(suite.ctrl)
	updateStore := storemocks.NewMockUpdateStore(suite.ctrl)
	taskLauncher := launchermocks.NewMockLauncher(suite.ctrl)
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: common.PelotonJobManager,
		Outbounds: yarpc.Outbounds{
			common.PelotonResourceManager: transport.Outbounds{
				Unary: http.NewTransport().NewSingleOutbound(""),
			},
			common.PelotonHostManager: transport.Outbounds{
				Unary: http.NewTransport().NewSingleOutbound(""),
			},
		},
	})
	config := Config{
		NumWorkerJobThreads:    4,
		NumWorkerTaskThreads:   5,
		NumWorkerUpdateThreads: 6,
	}
	driver := NewDriver(
		dispatcher,
		suite.jobStore,
		suite.taskStore,
		volumeStore,
		updateStore,
		suite.jobFactory,
		suite.updateFactory,
		taskLauncher,
		job.JobType_BATCH,
		tally.NoopScope,
		config,
	)
	suite.NotNil(driver)
}

func (suite *DriverTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestEnqueueJob tests enqueuing job into goal state engine.
func (suite *DriverTestSuite) TestEnqueueJob() {
	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(jobEntity goalstate.Entity, deadline time.Time) {
			suite.Equal(suite.jobID.GetValue(), jobEntity.GetID())
		})

	suite.goalStateDriver.EnqueueJob(suite.jobID, time.Now())
}

// TestEnqueueTask tests enqueuing task into goal state engine.
func (suite *DriverTestSuite) TestEnqueueTask() {
	taskID := fmt.Sprintf("%s-%d", suite.jobID.GetValue(), suite.instanceID)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(taskEntity goalstate.Entity, deadline time.Time) {
			suite.Equal(taskID, taskEntity.GetID())
		})

	suite.goalStateDriver.EnqueueTask(suite.jobID, suite.instanceID, time.Now())
}

// TestEnqueueUpdate tests enqueuing job update into goal state engine.
func (suite *DriverTestSuite) TestEnqueueUpdate() {
	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(entity goalstate.Entity, deadline time.Time) {
			suite.Equal(suite.jobID.GetValue(), entity.GetID())
			updateEnt := entity.(*updateEntity)
			suite.Equal(suite.updateID.GetValue(), updateEnt.id.GetValue())
		})

	suite.goalStateDriver.EnqueueUpdate(suite.jobID, suite.updateID, time.Now())
}

// TestDeleteJob tests deleting job from goal state engine.
func (suite *DriverTestSuite) TestDeleteJob() {
	suite.jobGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Do(func(jobEntity goalstate.Entity) {
			suite.Equal(suite.jobID.GetValue(), jobEntity.GetID())
		})

	suite.goalStateDriver.DeleteJob(suite.jobID)
}

// TestDeleteTask tests deleting task from goal state engine.
func (suite *DriverTestSuite) TestDeleteTask() {
	taskID := fmt.Sprintf("%s-%d", suite.jobID.GetValue(), suite.instanceID)

	suite.taskGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Do(func(taskEntity goalstate.Entity) {
			suite.Equal(taskID, taskEntity.GetID())
		})

	suite.goalStateDriver.DeleteTask(suite.jobID, suite.instanceID)
}

// TestDeleteUpdate tests deleting a job update from goal state engine.
func (suite *DriverTestSuite) TestDeleteUpdate() {
	suite.updateGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Do(func(updateEntity goalstate.Entity) {
			suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
		})

	suite.goalStateDriver.DeleteUpdate(suite.jobID, suite.updateID)
}

// TestIsScheduledTask tests determination oif whether a task
// is scheduled in goal state engine.
func (suite *DriverTestSuite) TestIsScheduledTask() {
	taskID := fmt.Sprintf("%s-%d", suite.jobID.GetValue(), suite.instanceID)

	suite.taskGoalStateEngine.EXPECT().
		IsScheduled(gomock.Any()).
		Do(func(taskEntity goalstate.Entity) {
			suite.Equal(taskID, taskEntity.GetID())
		}).Return(true)

	suite.True(suite.goalStateDriver.IsScheduledTask(
		suite.jobID,
		suite.instanceID,
	))
}

// TestRecoverJobStates will fail if a new job state is added without putting
// an explicit check here that the new state does not need to be recovered.
func (suite *DriverTestSuite) TestRecoverJobStates() {
	// Jobs which should not be recovered.
	var jobStatesNotRecover = []job.JobState{
		job.JobState_SUCCEEDED,
		job.JobState_FAILED,
		job.JobState_KILLED,
	}

	jobStatesToRecover := append(serviceJobStatesToRecover, batchJobStatesToRecover...)
	jobKnownStates := append(jobStatesNotRecover, jobStatesToRecover...)
	for _, state := range job.JobState_name {
		found := false
		for _, notRecover := range jobKnownStates {
			if notRecover.String() == state {
				found = true
			}
		}
		suite.True(found)
	}
}

func (suite *DriverTestSuite) prepareTestSyncDB(jobType job.JobType) {
	var jobIDList []peloton.JobID
	jobIDList = append(jobIDList, *suite.jobID)

	jobConfig := &job.JobConfig{
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		InstanceCount: 1,
	}

	if jobType == job.JobType_BATCH {
		suite.jobStore.EXPECT().
			GetJobsByStates(gomock.Any(), batchJobStatesToRecover).
			Return(jobIDList, nil)
	} else if jobType == job.JobType_SERVICE {
		suite.jobStore.EXPECT().
			GetJobsByStates(gomock.Any(), serviceJobStatesToRecover).
			Return(jobIDList, nil)
	}

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&job.RuntimeInfo{
			State:     job.JobState_RUNNING,
			GoalState: job.JobState_SUCCEEDED,
		}, nil)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(jobConfig, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheOnly).
		Return(nil)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()
}

// TestSyncFromDBFailed tests SyncFromDB when GetTaskRuntimesForJobByRange failed
func (suite *DriverTestSuite) TestSyncFromDBFailed() {
	suite.prepareTestSyncDB(job.JobType_BATCH)
	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, gomock.Any()).
		Return(nil, errors.New(""))
	suite.Error(suite.goalStateDriver.syncFromDB(context.Background()))
}

// TestSyncFromDBForBatchCluster tests syncing job manager for batch type
// with jobs and tasks in DB.
func (suite *DriverTestSuite) TestSyncFromDBForBatchCluster() {
	suite.prepareTestSyncDB(job.JobType_BATCH)
	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, gomock.Any()).
		Return(map[uint32]*task.RuntimeInfo{
			suite.instanceID: {
				GoalState:            task.TaskState_RUNNING,
				DesiredConfigVersion: 42,
				ConfigVersion:        42,
			},
		}, nil)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(nil)

	suite.cachedJob.EXPECT().
		ReplaceTasks(gomock.Any(), false).Return(nil)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.cachedJob.EXPECT().
		RecalculateResourceUsage(gomock.Any())

	suite.NoError(suite.goalStateDriver.syncFromDB(context.Background()))
}

// TestSyncFromDBForBatchCluster tests syncing job manager for service type
// with jobs and tasks in DB.
func (suite *DriverTestSuite) TestSyncFromDBForServiceCluster() {
	suite.goalStateDriver.jobType = job.JobType_SERVICE
	suite.prepareTestSyncDB(job.JobType_SERVICE)
	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, gomock.Any()).
		Return(map[uint32]*task.RuntimeInfo{
			suite.instanceID: {
				GoalState:            task.TaskState_RUNNING,
				DesiredConfigVersion: 42,
				ConfigVersion:        42,
			},
		}, nil)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(nil)

	suite.cachedJob.EXPECT().
		ReplaceTasks(gomock.Any(), false).Return(nil)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.cachedJob.EXPECT().
		RecalculateResourceUsage(gomock.Any())

	suite.NoError(suite.goalStateDriver.syncFromDB(context.Background()))
}

// TestSyncFromDB tests syncing job manager with jobs and tasks in DB.
func (suite *DriverTestSuite) TestSyncFromDBWithMaxRunningInstancesSLA() {
	instanceID1 := uint32(0)
	instanceID2 := uint32(1)
	var jobIDList []peloton.JobID
	jobIDList = append(jobIDList, *suite.jobID)

	jobConfig := &job.JobConfig{
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		InstanceCount: 2,
		SLA: &job.SlaConfig{
			MaximumRunningInstances: 1,
		},
	}

	suite.jobStore.EXPECT().
		GetJobsByStates(gomock.Any(), gomock.Any()).
		Return(jobIDList, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&job.RuntimeInfo{
			State:     job.JobState_RUNNING,
			GoalState: job.JobState_SUCCEEDED,
		}, nil)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(jobConfig, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheOnly).
		Return(nil)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, gomock.Any()).
		Return(map[uint32]*task.RuntimeInfo{
			instanceID1: {
				GoalState:            task.TaskState_RUNNING,
				DesiredConfigVersion: 42,
				ConfigVersion:        42,
			},
			instanceID2: {
				GoalState:            task.TaskState_INITIALIZED,
				DesiredConfigVersion: 42,
				ConfigVersion:        42,
			},
		}, nil)

	suite.cachedJob.EXPECT().
		GetTask(instanceID1).Return(nil)

	suite.cachedJob.EXPECT().
		GetTask(instanceID2).Return(nil)

	suite.cachedJob.EXPECT().
		ReplaceTasks(gomock.Any(), false).Return(nil).Times(2)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return().Times(2)

	suite.cachedJob.EXPECT().
		RecalculateResourceUsage(gomock.Any())

	suite.goalStateDriver.syncFromDB(context.Background())
}

// TestInitializedJobSyncFromDB tests syncing job manager with
// jobs in INITIALIZED job state.
func (suite *DriverTestSuite) TestInitializedJobSyncFromDB() {
	var jobIDList []peloton.JobID
	jobIDList = append(jobIDList, *suite.jobID)

	jobConfig := &job.JobConfig{
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		InstanceCount: 1,
	}

	suite.jobStore.EXPECT().
		GetJobsByStates(gomock.Any(), gomock.Any()).
		Return(jobIDList, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&job.RuntimeInfo{
			State:     job.JobState_INITIALIZED,
			GoalState: job.JobState_SUCCEEDED,
		}, nil)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(jobConfig, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheOnly).
		Return(nil)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, gomock.Any()).
		Return(map[uint32]*task.RuntimeInfo{
			suite.instanceID: {
				State:                task.TaskState_INITIALIZED,
				GoalState:            task.TaskState_SUCCEEDED,
				DesiredConfigVersion: 42,
				ConfigVersion:        42,
			},
		}, nil)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).Return(nil)

	suite.cachedJob.EXPECT().
		ReplaceTasks(gomock.Any(), false).Return(nil)

	suite.cachedJob.EXPECT().
		RecalculateResourceUsage(gomock.Any())

	suite.goalStateDriver.syncFromDB(context.Background())
}

// TestSyncFromDBRecoverUpdate tests syncing job manager with jobs and and updates
func (suite *DriverTestSuite) TestSyncFromDBRecoverUpdate() {
	updateID := &peloton.UpdateID{Value: uuid.New()}
	instanceID1 := uint32(0)
	instanceID2 := uint32(1)
	var jobIDList []peloton.JobID
	jobIDList = append(jobIDList, *suite.jobID)

	jobConfig := &job.JobConfig{
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		InstanceCount: 2,
		SLA: &job.SlaConfig{
			MaximumRunningInstances: 1,
		},
	}

	suite.jobStore.EXPECT().
		GetJobsByStates(gomock.Any(), gomock.Any()).
		Return(jobIDList, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&job.RuntimeInfo{
			State:     job.JobState_RUNNING,
			GoalState: job.JobState_SUCCEEDED,
			UpdateID:  updateID,
		}, nil)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(jobConfig, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheOnly).
		Return(nil)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, gomock.Any()).
		Return(map[uint32]*task.RuntimeInfo{
			instanceID1: {
				GoalState:            task.TaskState_RUNNING,
				DesiredConfigVersion: 42,
				ConfigVersion:        42,
			},
			instanceID2: {
				GoalState:            task.TaskState_INITIALIZED,
				DesiredConfigVersion: 42,
				ConfigVersion:        42,
			},
		}, nil)

	suite.cachedJob.EXPECT().
		GetTask(instanceID1).Return(nil)

	suite.cachedJob.EXPECT().
		GetTask(instanceID2).Return(nil)

	suite.cachedJob.EXPECT().
		ReplaceTasks(gomock.Any(), false).Return(nil).Times(2)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return().Times(2)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).Return()

	suite.cachedJob.EXPECT().
		RecalculateResourceUsage(gomock.Any())

	suite.goalStateDriver.syncFromDB(context.Background())
}

// TestEngineStartStop tests start and stop of goal state driver.
func (suite *DriverTestSuite) TestEngineStartStop() {
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	cachedUpdate := cachedmocks.NewMockUpdate(suite.ctrl)

	// Test start
	var jobIDList []peloton.JobID

	suite.jobGoalStateEngine.EXPECT().Start()
	suite.taskGoalStateEngine.EXPECT().Start()
	suite.updateGoalStateEngine.EXPECT().Start()
	suite.jobStore.EXPECT().
		GetJobsByStates(gomock.Any(), gomock.Any()).
		Return(jobIDList, nil)

	suite.goalStateDriver.Start()

	// Test stop
	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = cachedTask

	jobMap := make(map[string]cached.Job)
	jobMap[suite.jobID.GetValue()] = suite.cachedJob

	updateMap := make(map[string]cached.Update)
	updateMap[suite.updateID.GetValue()] = cachedUpdate

	suite.jobGoalStateEngine.EXPECT().Stop()
	suite.taskGoalStateEngine.EXPECT().Stop()
	suite.updateGoalStateEngine.EXPECT().Stop()
	suite.updateFactory.EXPECT().GetAllUpdates().Return(updateMap)
	cachedUpdate.EXPECT().JobID().Return(suite.jobID)
	suite.updateGoalStateEngine.EXPECT().Delete(gomock.Any())
	suite.jobFactory.EXPECT().GetAllJobs().Return(jobMap)
	suite.cachedJob.EXPECT().GetAllTasks().Return(taskMap)
	suite.taskGoalStateEngine.EXPECT().Delete(gomock.Any())
	suite.jobGoalStateEngine.EXPECT().Delete(gomock.Any())

	suite.goalStateDriver.Stop()
}
