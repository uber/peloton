// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package goalstate

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/goalstate"
	goalstatemocks "github.com/uber/peloton/pkg/common/goalstate/mocks"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	lmmocks "github.com/uber/peloton/pkg/jobmgr/task/lifecyclemgr/mocks"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"
	ormStore "github.com/uber/peloton/pkg/storage/objects"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/peloton/.gen/peloton/private/models"
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
	activeJobsOps         *objectmocks.MockActiveJobsOps
	jobConfigOps          *objectmocks.MockJobConfigOps
	jobRuntimeOps         *objectmocks.MockJobRuntimeOps
	mockedPodEventsOps    *objectmocks.MockPodEventsOps
	jobFactory            *cachedmocks.MockJobFactory
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
	suite.activeJobsOps = objectmocks.NewMockActiveJobsOps(suite.ctrl)
	suite.jobConfigOps = objectmocks.NewMockJobConfigOps(suite.ctrl)
	suite.jobRuntimeOps = objectmocks.NewMockJobRuntimeOps(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.mockedPodEventsOps = objectmocks.NewMockPodEventsOps(suite.ctrl)
	lmMock := lmmocks.NewMockManager(suite.ctrl)

	suite.goalStateDriver = &driver{
		jobEngine:     suite.jobGoalStateEngine,
		taskEngine:    suite.taskGoalStateEngine,
		updateEngine:  suite.updateGoalStateEngine,
		jobStore:      suite.jobStore,
		taskStore:     suite.taskStore,
		activeJobsOps: suite.activeJobsOps,
		jobConfigOps:  suite.jobConfigOps,
		jobRuntimeOps: suite.jobRuntimeOps,
		podEventsOps:  suite.mockedPodEventsOps,
		jobFactory:    suite.jobFactory,
		mtx:           NewMetrics(tally.NoopScope),
		jobScope:      tally.NoopScope,
		cfg:           &Config{},
		jobType:       job.JobType_BATCH,
		lm:            lmMock,
	}
	suite.goalStateDriver.cfg.normalize()
	suite.goalStateDriver.setState(stopped)
	suite.goalStateDriver.setCacheState(cleaned)
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.updateID = &peloton.UpdateID{Value: uuid.NewRandom().String()}
	suite.instanceID = uint32(0)
}

// Test constructor
func (suite *DriverTestSuite) TestNewDriver() {
	volumeStore := storemocks.NewMockPersistentVolumeStore(suite.ctrl)
	updateStore := storemocks.NewMockUpdateStore(suite.ctrl)
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
	dr := NewDriver(
		dispatcher,
		suite.jobStore,
		suite.taskStore,
		volumeStore,
		updateStore,
		&ormStore.Store{},
		suite.jobFactory,
		job.JobType_SERVICE,
		tally.NoopScope,
		config,
		api.V0,
	)
	suite.NotNil(dr)
	suite.Equal(dr.(*driver).jobType, job.JobType_SERVICE)

	dr = NewDriver(
		dispatcher,
		suite.jobStore,
		suite.taskStore,
		volumeStore,
		updateStore,
		&ormStore.Store{},
		suite.jobFactory,
		job.JobType_SERVICE,
		tally.NoopScope,
		config,
		api.V1Alpha,
	)
	suite.NotNil(dr)
	suite.Equal(dr.(*driver).jobType, job.JobType_SERVICE)
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

func (suite *DriverTestSuite) prepareTestSyncDB(jobType job.JobType) {
	jobConfig := &job.JobConfig{
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		InstanceCount: 1,
	}

	suite.activeJobsOps.EXPECT().
		GetAll(gomock.Any()).
		Return([]*peloton.JobID{suite.jobID}, nil)

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), suite.jobID).
		Return(&job.RuntimeInfo{
			State:     job.JobState_RUNNING,
			GoalState: job.JobState_SUCCEEDED,
		}, nil)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), suite.jobID, gomock.Any()).
		Return(jobConfig, &models.ConfigAddOn{}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		Update(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			nil,
			cached.UpdateCacheOnly).
		Return(nil)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()
}

// TestSyncFromDBFailed tests SyncFromDB when GetTasksForJobByRange failed
func (suite *DriverTestSuite) TestSyncFromDBFailed() {
	suite.prepareTestSyncDB(job.JobType_BATCH)
	suite.taskStore.EXPECT().
		GetTasksForJobByRange(gomock.Any(), suite.jobID, gomock.Any()).
		Return(nil, errors.New(""))
	suite.Error(suite.goalStateDriver.syncFromDB(context.Background()))
}

// TestSyncFromDBForBatchCluster tests syncing job manager for batch type
// with jobs and tasks in DB.
func (suite *DriverTestSuite) TestSyncFromDBForBatchCluster() {
	suite.prepareTestSyncDB(job.JobType_BATCH)
	suite.taskStore.EXPECT().
		GetTasksForJobByRange(gomock.Any(), suite.jobID, gomock.Any()).
		Return(map[uint32]*task.TaskInfo{
			suite.instanceID: {
				Runtime: &task.RuntimeInfo{
					GoalState:            task.TaskState_RUNNING,
					DesiredConfigVersion: 42,
					ConfigVersion:        42,
				},
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
		GetTasksForJobByRange(gomock.Any(), suite.jobID, gomock.Any()).
		Return(map[uint32]*task.TaskInfo{
			suite.instanceID: {
				Runtime: &task.RuntimeInfo{
					GoalState:            task.TaskState_RUNNING,
					DesiredConfigVersion: 42,
					ConfigVersion:        42,
				},
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

	jobConfig := &job.JobConfig{
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		InstanceCount: 2,
		SLA: &job.SlaConfig{
			MaximumRunningInstances: 1,
		},
	}

	suite.activeJobsOps.EXPECT().
		GetAll(gomock.Any()).
		Return([]*peloton.JobID{suite.jobID}, nil)

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), suite.jobID).
		Return(&job.RuntimeInfo{
			State:     job.JobState_RUNNING,
			GoalState: job.JobState_SUCCEEDED,
		}, nil)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), suite.jobID, gomock.Any()).
		Return(jobConfig, &models.ConfigAddOn{}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		Update(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			nil,
			cached.UpdateCacheOnly).
		Return(nil)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.taskStore.EXPECT().
		GetTasksForJobByRange(gomock.Any(), suite.jobID, gomock.Any()).
		Return(map[uint32]*task.TaskInfo{
			instanceID1: {
				Runtime: &task.RuntimeInfo{
					GoalState:            task.TaskState_RUNNING,
					DesiredConfigVersion: 42,
					ConfigVersion:        42,
				},
			},
			instanceID2: {
				Runtime: &task.RuntimeInfo{
					GoalState:            task.TaskState_INITIALIZED,
					DesiredConfigVersion: 42,
					ConfigVersion:        42,
				},
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
	jobConfig := &job.JobConfig{
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		InstanceCount: 1,
	}

	suite.activeJobsOps.EXPECT().
		GetAll(gomock.Any()).
		Return([]*peloton.JobID{suite.jobID}, nil)

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), suite.jobID).
		Return(&job.RuntimeInfo{
			State:     job.JobState_INITIALIZED,
			GoalState: job.JobState_SUCCEEDED,
		}, nil)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), suite.jobID, gomock.Any()).
		Return(jobConfig, &models.ConfigAddOn{}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		Update(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			nil,
			cached.UpdateCacheOnly).
		Return(nil)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.taskStore.EXPECT().
		GetTasksForJobByRange(gomock.Any(), suite.jobID, gomock.Any()).
		Return(map[uint32]*task.TaskInfo{
			suite.instanceID: {
				Runtime: &task.RuntimeInfo{
					State:                task.TaskState_INITIALIZED,
					GoalState:            task.TaskState_SUCCEEDED,
					DesiredConfigVersion: 42,
					ConfigVersion:        42,
				},
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

	jobConfig := &job.JobConfig{
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		InstanceCount: 2,
		SLA: &job.SlaConfig{
			MaximumRunningInstances: 1,
		},
	}

	suite.activeJobsOps.EXPECT().
		GetAll(gomock.Any()).
		Return([]*peloton.JobID{suite.jobID}, nil)

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), suite.jobID).
		Return(&job.RuntimeInfo{
			State:     job.JobState_RUNNING,
			GoalState: job.JobState_SUCCEEDED,
			UpdateID:  updateID,
		}, nil)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), suite.jobID, gomock.Any()).
		Return(jobConfig, &models.ConfigAddOn{}, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		Update(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			nil,
			cached.UpdateCacheOnly).
		Return(nil)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.taskStore.EXPECT().
		GetTasksForJobByRange(gomock.Any(), suite.jobID, gomock.Any()).
		Return(map[uint32]*task.TaskInfo{
			instanceID1: {
				Runtime: &task.RuntimeInfo{
					GoalState:            task.TaskState_RUNNING,
					DesiredConfigVersion: 42,
					ConfigVersion:        42,
				},
			},
			instanceID2: {
				Runtime: &task.RuntimeInfo{
					GoalState:            task.TaskState_INITIALIZED,
					DesiredConfigVersion: 42,
					ConfigVersion:        42,
				},
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

	suite.jobGoalStateEngine.EXPECT().Start()
	suite.taskGoalStateEngine.EXPECT().Start()
	suite.updateGoalStateEngine.EXPECT().Start()

	suite.activeJobsOps.EXPECT().
		GetAll(gomock.Any()).
		Return([]*peloton.JobID{}, nil)

	suite.False(suite.goalStateDriver.Started())
	suite.goalStateDriver.Start()
	suite.True(suite.goalStateDriver.Started())

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
	suite.jobFactory.EXPECT().GetAllJobs().Return(jobMap)
	suite.cachedJob.EXPECT().GetAllTasks().Return(taskMap)
	suite.cachedJob.EXPECT().GetAllWorkflows().Return(updateMap)
	suite.taskGoalStateEngine.EXPECT().Delete(gomock.Any())
	suite.jobGoalStateEngine.EXPECT().Delete(gomock.Any())
	suite.updateGoalStateEngine.EXPECT().Delete(gomock.Any())

	suite.goalStateDriver.Stop(true)
	suite.False(suite.goalStateDriver.Started())
}

// Test the case of starting a driver that has already been started
func (suite *DriverTestSuite) TestEngineStartAlreadyStartedDriver() {
	// noop for starting a starting driver with clean cache
	suite.goalStateDriver.setState(starting)
	suite.goalStateDriver.setCacheState(cleaned)
	suite.goalStateDriver.Start()
	suite.Equal(suite.goalStateDriver.getState(), starting)

	// noop for starting a started driver with clean cache
	suite.goalStateDriver.setState(started)
	suite.goalStateDriver.setCacheState(cleaned)
	suite.goalStateDriver.Start()
	suite.Equal(suite.goalStateDriver.getState(), started)

	// noop for starting a starting driver with populated cache
	suite.goalStateDriver.setState(starting)
	suite.goalStateDriver.setCacheState(populated)
	suite.goalStateDriver.Start()
	suite.Equal(suite.goalStateDriver.getState(), starting)

	// noop for starting a started driver with populated cache
	suite.goalStateDriver.setState(started)
	suite.goalStateDriver.setCacheState(populated)
	suite.goalStateDriver.Start()
	suite.Equal(suite.goalStateDriver.getState(), started)
}

// Test the case of starting a driver that is stopped but with cache populated
func (suite *DriverTestSuite) TestEngineStartStoppedDriverWithCachePopulated() {
	// start the stopping engines without loading cache
	suite.jobGoalStateEngine.EXPECT().Start()
	suite.taskGoalStateEngine.EXPECT().Start()
	suite.updateGoalStateEngine.EXPECT().Start()

	suite.goalStateDriver.setState(stopping)
	suite.goalStateDriver.setCacheState(populated)
	suite.goalStateDriver.Start()
	suite.Equal(suite.goalStateDriver.getState(), started)

	// start the stopped engines without loading cache
	suite.jobGoalStateEngine.EXPECT().Start()
	suite.taskGoalStateEngine.EXPECT().Start()
	suite.updateGoalStateEngine.EXPECT().Start()

	suite.goalStateDriver.setState(stopped)
	suite.goalStateDriver.setCacheState(populated)
	suite.goalStateDriver.Start()
	suite.Equal(suite.goalStateDriver.getState(), started)
}

// Test the case of starting a driver that is stopped and with cache populated
func (suite *DriverTestSuite) TestEngineStartStoppedDriverWithoutCachePopulated() {
	// start the stopped engines and load cache
	suite.activeJobsOps.EXPECT().GetAll(gomock.Any()).Return(nil, nil)
	suite.jobGoalStateEngine.EXPECT().Start()
	suite.taskGoalStateEngine.EXPECT().Start()
	suite.updateGoalStateEngine.EXPECT().Start()

	suite.goalStateDriver.setState(stopping)
	suite.goalStateDriver.setCacheState(cleaned)
	suite.goalStateDriver.Start()
	suite.Equal(suite.goalStateDriver.getState(), started)

	// start the stopped engines and load cache
	suite.activeJobsOps.EXPECT().GetAll(gomock.Any()).Return(nil, nil)
	suite.jobGoalStateEngine.EXPECT().Start()
	suite.taskGoalStateEngine.EXPECT().Start()
	suite.updateGoalStateEngine.EXPECT().Start()

	suite.goalStateDriver.setState(stopped)
	suite.goalStateDriver.setCacheState(cleaned)
	suite.goalStateDriver.Start()
	suite.Equal(suite.goalStateDriver.getState(), started)
}

// Test the case of stopping a driver that has already been stopped without cleaning cache
func (suite *DriverTestSuite) TestEngineStopStoppedDriverWithoutCleanUpCache() {
	// noop for starting a stopping driver with clean cache
	suite.goalStateDriver.setState(stopping)
	suite.goalStateDriver.setCacheState(cleaned)
	suite.goalStateDriver.Stop(false)
	suite.Equal(suite.goalStateDriver.getState(), stopping)

	// noop for starting a stopped driver with clean cache
	suite.goalStateDriver.setState(stopped)
	suite.goalStateDriver.setCacheState(cleaned)
	suite.goalStateDriver.Stop(false)
	suite.Equal(suite.goalStateDriver.getState(), stopped)

	// noop for stopping a stopping driver with populated cache
	suite.goalStateDriver.setState(stopping)
	suite.goalStateDriver.setCacheState(populated)
	suite.goalStateDriver.Stop(false)
	suite.Equal(suite.goalStateDriver.getState(), stopping)

	// noop for stopping a stopped driver with populated cache
	suite.goalStateDriver.setState(stopped)
	suite.goalStateDriver.setCacheState(populated)
	suite.goalStateDriver.Stop(false)
	suite.Equal(suite.goalStateDriver.getState(), stopped)
}

// Test the case of stopping a driver that has already been stopped with cleaning cache
func (suite *DriverTestSuite) TestEngineStopStoppedDriverWithCleanUpCache() {
	// noop for starting a stopping driver with clean cache
	suite.goalStateDriver.setState(stopping)
	suite.goalStateDriver.setCacheState(cleaned)
	suite.goalStateDriver.Stop(true)
	suite.Equal(suite.goalStateDriver.getState(), stopping)

	// noop for starting a stopped driver with clean cache
	suite.goalStateDriver.setState(stopped)
	suite.goalStateDriver.setCacheState(cleaned)
	suite.goalStateDriver.Stop(true)
	suite.Equal(suite.goalStateDriver.getState(), stopped)

	// cache needs to be cleaned up
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	cachedUpdate := cachedmocks.NewMockUpdate(suite.ctrl)
	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = cachedTask

	jobMap := make(map[string]cached.Job)
	jobMap[suite.jobID.GetValue()] = suite.cachedJob

	updateMap := make(map[string]cached.Update)
	updateMap[suite.updateID.GetValue()] = cachedUpdate

	suite.jobGoalStateEngine.EXPECT().Stop()
	suite.taskGoalStateEngine.EXPECT().Stop()
	suite.updateGoalStateEngine.EXPECT().Stop()
	suite.jobFactory.EXPECT().GetAllJobs().Return(jobMap)
	suite.cachedJob.EXPECT().GetAllTasks().Return(taskMap)
	suite.cachedJob.EXPECT().GetAllWorkflows().Return(updateMap)
	suite.taskGoalStateEngine.EXPECT().Delete(gomock.Any())
	suite.jobGoalStateEngine.EXPECT().Delete(gomock.Any())
	suite.updateGoalStateEngine.EXPECT().Delete(gomock.Any())

	suite.goalStateDriver.setState(stopping)
	suite.goalStateDriver.setCacheState(populated)
	suite.goalStateDriver.Stop(true)
	suite.Equal(suite.goalStateDriver.getState(), stopped)
	suite.Equal(suite.goalStateDriver.getCacheState(), cleaned)

	// cache needs to be cleaned up
	suite.jobGoalStateEngine.EXPECT().Stop()
	suite.taskGoalStateEngine.EXPECT().Stop()
	suite.updateGoalStateEngine.EXPECT().Stop()
	suite.jobFactory.EXPECT().GetAllJobs().Return(jobMap)
	suite.cachedJob.EXPECT().GetAllTasks().Return(taskMap)
	suite.cachedJob.EXPECT().GetAllWorkflows().Return(updateMap)
	suite.taskGoalStateEngine.EXPECT().Delete(gomock.Any())
	suite.jobGoalStateEngine.EXPECT().Delete(gomock.Any())
	suite.updateGoalStateEngine.EXPECT().Delete(gomock.Any())

	suite.goalStateDriver.setState(stopped)
	suite.goalStateDriver.setCacheState(populated)
	suite.goalStateDriver.Stop(true)
	suite.Equal(suite.goalStateDriver.getState(), stopped)
	suite.Equal(suite.goalStateDriver.getCacheState(), cleaned)
}

// TestDriverGetLockable tests GetLockable returns a non-nil interface
func (suite *DriverTestSuite) TestDriverGetLockable() {
	suite.NotNil(suite.goalStateDriver.GetLockable())
}
