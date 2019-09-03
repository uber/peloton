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
	"fmt"
	"testing"
	"time"

	mesos_v1 "github.com/uber/peloton/.gen/mesos/v1"
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbupdate "github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/private/models"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	resmocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"

	"github.com/uber/peloton/pkg/common/goalstate"
	goalstatemocks "github.com/uber/peloton/pkg/common/goalstate/mocks"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

type UpdateRunTestSuite struct {
	suite.Suite
	ctrl                  *gomock.Controller
	jobFactory            *cachedmocks.MockJobFactory
	updateGoalStateEngine *goalstatemocks.MockEngine
	jobGoalStateEngine    *goalstatemocks.MockEngine
	taskGoalStateEngine   *goalstatemocks.MockEngine
	goalStateDriver       *driver
	jobID                 *peloton.JobID
	updateID              *peloton.UpdateID
	updateEnt             *updateEntity
	cachedJob             *cachedmocks.MockJob
	cachedUpdate          *cachedmocks.MockUpdate
	cachedTask            *cachedmocks.MockTask
	jobStore              *storemocks.MockJobStore
	taskStore             *storemocks.MockTaskStore
	mockedPodEventsOps    *objectmocks.MockPodEventsOps
	jobConfigOps          *objectmocks.MockJobConfigOps
	resmgrClient          *resmocks.MockResourceManagerServiceYARPCClient
}

func TestUpdateRun(t *testing.T) {
	suite.Run(t, new(UpdateRunTestSuite))
}

func (suite *UpdateRunTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.updateGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.jobConfigOps = objectmocks.NewMockJobConfigOps(suite.ctrl)
	suite.resmgrClient = resmocks.NewMockResourceManagerServiceYARPCClient(suite.ctrl)

	suite.mockedPodEventsOps = objectmocks.NewMockPodEventsOps(suite.ctrl)
	suite.goalStateDriver = &driver{
		jobFactory:   suite.jobFactory,
		updateEngine: suite.updateGoalStateEngine,
		taskEngine:   suite.taskGoalStateEngine,
		jobEngine:    suite.jobGoalStateEngine,
		jobStore:     suite.jobStore,
		taskStore:    suite.taskStore,
		jobConfigOps: suite.jobConfigOps,
		podEventsOps: suite.mockedPodEventsOps,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
		resmgrClient: suite.resmgrClient,
	}
	suite.goalStateDriver.cfg.normalize()

	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.updateID = &peloton.UpdateID{Value: uuid.NewRandom().String()}
	suite.updateEnt = &updateEntity{
		id:     suite.updateID,
		jobID:  suite.jobID,
		driver: suite.goalStateDriver,
	}

	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedUpdate = cachedmocks.NewMockUpdate(suite.ctrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.ctrl)
}

func (suite *UpdateRunTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *UpdateRunTestSuite) TestRunningUpdate() {
	instancesTotal := []uint32{2, 3, 4, 5, 6}
	oldJobConfigVer := uint64(3)
	newJobConfigVer := uint64(4)

	updateConfig := &pbupdate.UpdateConfig{
		BatchSize: 0,
	}

	runtimeDone := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_RUNNING,
		GoalState:            pbtask.TaskState_RUNNING,
		Healthy:              pbtask.HealthState_HEALTHY,
		ConfigVersion:        newJobConfigVer,
		DesiredConfigVersion: newJobConfigVer,
	}

	runtimeRunning := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_RUNNING,
		GoalState:            pbtask.TaskState_RUNNING,
		Healthy:              pbtask.HealthState_HEALTHY,
		ConfigVersion:        oldJobConfigVer,
		DesiredConfigVersion: newJobConfigVer,
	}

	runtimeTerminated := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_KILLED,
		GoalState:            pbtask.TaskState_KILLED,
		Healthy:              pbtask.HealthState_INVALID,
		ConfigVersion:        oldJobConfigVer,
		DesiredConfigVersion: newJobConfigVer,
	}

	runtimeInitialized := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_INITIALIZED,
		GoalState:            pbtask.TaskState_RUNNING,
		Healthy:              pbtask.HealthState_HEALTH_UNKNOWN,
		ConfigVersion:        newJobConfigVer,
		DesiredConfigVersion: newJobConfigVer,
	}

	runtimeNotReady := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_RUNNING,
		GoalState:            pbtask.TaskState_RUNNING,
		Healthy:              pbtask.HealthState_HEALTH_UNKNOWN,
		ConfigVersion:        newJobConfigVer,
		DesiredConfigVersion: newJobConfigVer,
	}

	cachedTasks := make(map[uint32]*cachedmocks.MockTask)
	for _, instID := range instancesTotal {
		cachedTasks[instID] = cachedmocks.NewMockTask(suite.ctrl)
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: uint64(4),
		}).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(instancesTotal).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(nil).Times(3)

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return([]uint32{}).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return([]uint32{}).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(instancesTotal).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(updateConfig).
		Times(3)

	for _, instID := range instancesTotal {
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), instID).
			Return(cachedTasks[instID], nil).
			AnyTimes()
	}

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, instancesTotal[0]).
		Return(runtimeRunning, nil)
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, instancesTotal[1]).
		Return(runtimeTerminated, nil)
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, instancesTotal[2]).
		Return(runtimeInitialized, nil)
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, instancesTotal[3]).
		Return(runtimeNotReady, nil)
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, instancesTotal[4]).
		Return(runtimeDone, nil)

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobConfigVer, runtimeRunning).
		Return(false)
	suite.cachedUpdate.EXPECT().
		IsInstanceFailed(runtimeRunning, updateConfig.GetMaxInstanceAttempts()).
		Return(false)
	suite.cachedUpdate.EXPECT().
		IsInstanceInProgress(newJobConfigVer, runtimeRunning).
		Return(true)

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobConfigVer, runtimeTerminated).
		Return(true)

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobConfigVer, runtimeInitialized).
		Return(false)
	suite.cachedUpdate.EXPECT().
		IsInstanceFailed(runtimeInitialized, updateConfig.GetMaxInstanceAttempts()).
		Return(false)
	suite.cachedUpdate.EXPECT().
		IsInstanceInProgress(newJobConfigVer, runtimeInitialized).
		Return(true)

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobConfigVer, runtimeNotReady).
		Return(false)
	suite.cachedUpdate.EXPECT().
		IsInstanceFailed(runtimeNotReady, updateConfig.GetMaxInstanceAttempts()).
		Return(false)
	suite.cachedUpdate.EXPECT().
		IsInstanceInProgress(newJobConfigVer, runtimeNotReady).
		Return(true)

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobConfigVer, runtimeDone).
		Return(true)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{3, 6},
			[]uint32{},
			[]uint32{2, 4, 5},
		).Return(nil)

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

func (suite *UpdateRunTestSuite) TestRunningUpdateWithStartTasksOn() {
	instancesTotal := []uint32{0, 1}
	oldJobConfigVer := uint64(3)
	newJobConfigVer := uint64(4)

	updateConfig := &pbupdate.UpdateConfig{
		BatchSize:  0,
		StartTasks: true,
	}

	runtimeRunning := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_RUNNING,
		GoalState:            pbtask.TaskState_RUNNING,
		Healthy:              pbtask.HealthState_HEALTHY,
		ConfigVersion:        oldJobConfigVer,
		DesiredConfigVersion: oldJobConfigVer,
	}

	runtimeKilled := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_KILLED,
		GoalState:            pbtask.TaskState_KILLED,
		Healthy:              pbtask.HealthState_INVALID,
		ConfigVersion:        oldJobConfigVer,
		DesiredConfigVersion: oldJobConfigVer,
	}

	instanceAvailabilityMap := map[uint32]jobmgrcommon.InstanceAvailability_Type{}
	for _, i := range instancesTotal {
		instanceAvailabilityMap[i] = jobmgrcommon.InstanceAvailability_AVAILABLE
	}

	cachedTasks := make(map[uint32]*cachedmocks.MockTask)
	for _, instID := range instancesTotal {
		cachedTasks[instID] = cachedmocks.NewMockTask(suite.ctrl)
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: uint64(4),
		}).
		AnyTimes()

	suite.cachedJob.EXPECT().
		GetInstanceAvailabilityType(gomock.Any(), instancesTotal).
		Return(instanceAvailabilityMap)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(instancesTotal).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(nil).Times(3)

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return([]uint32{}).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return([]uint32{}).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(instancesTotal).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(updateConfig).
		AnyTimes()

	for _, instID := range instancesTotal {
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), instID).
			Return(cachedTasks[instID], nil).
			AnyTimes()

		if instID == instancesTotal[0] {
			cachedTasks[instID].EXPECT().
				GetRuntime(gomock.Any()).
				Return(runtimeRunning, nil).
				AnyTimes()
		}

		if instID == instancesTotal[1] {
			cachedTasks[instID].EXPECT().
				GetRuntime(gomock.Any()).
				Return(runtimeKilled, nil).
				AnyTimes()
		}
	}

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, instancesTotal[0]).
		Return(runtimeRunning, nil)
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, instancesTotal[1]).
		Return(runtimeKilled, nil)

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobConfigVer, runtimeRunning).
		Return(false)
	suite.cachedUpdate.EXPECT().
		IsInstanceFailed(runtimeRunning, updateConfig.GetMaxInstanceAttempts()).
		Return(false)
	suite.cachedUpdate.EXPECT().
		IsInstanceInProgress(newJobConfigVer, runtimeRunning).
		Return(false)

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobConfigVer, runtimeKilled).
		Return(false)
	suite.cachedUpdate.EXPECT().
		IsInstanceFailed(runtimeKilled, updateConfig.GetMaxInstanceAttempts()).
		Return(false)
	suite.cachedUpdate.EXPECT().
		IsInstanceInProgress(newJobConfigVer, runtimeKilled).
		Return(false)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), gomock.Any(), newJobConfigVer).
		Return(
			&pbjob.JobConfig{
				ChangeLog: &peloton.ChangeLog{Version: newJobConfigVer},
			},
			&models.ConfigAddOn{},
			nil,
		)

	// using loops instead of .Times function, because the latter
	// would return a reference to the map. Any modification to the
	// map would be reflected in other places.
	for i := 0; i < len(instancesTotal); i++ {
		suite.cachedUpdate.EXPECT().
			GetRuntimeDiff(gomock.Any()).
			Return(jobmgrcommon.RuntimeDiff{
				jobmgrcommon.DesiredConfigVersionField: newJobConfigVer,
				jobmgrcommon.FailureCountField:         uint32(0),
				jobmgrcommon.ReasonField:               "",
			})
	}

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Do(func(_ context.Context,
			diffs map[uint32]jobmgrcommon.RuntimeDiff,
			_ bool) {
			suite.Equal(len(diffs), len(instancesTotal))
			suite.Equal(diffs[0][jobmgrcommon.GoalStateField], pbtask.TaskState_RUNNING)
			suite.Equal(diffs[1][jobmgrcommon.GoalStateField], pbtask.TaskState_RUNNING)
		}).Return(nil, nil, nil)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Times(2)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{},
			[]uint32{},
			[]uint32{0, 1},
		).Return(nil)

	for _, instID := range instancesTotal {
		suite.cachedJob.EXPECT().
			GetTask(instID).
			Return(cachedTasks[instID]).
			AnyTimes()

		if instID == instancesTotal[0] {
			cachedTasks[instID].EXPECT().
				GetRuntime(gomock.Any()).
				Return(runtimeRunning, nil).
				AnyTimes()
		}

		if instID == instancesTotal[1] {
			// goal state should be changed to RUNNING by
			// job.PatchTasks above
			runtimeKilled.GoalState = pbtask.TaskState_RUNNING
			cachedTasks[instID].EXPECT().
				GetRuntime(gomock.Any()).
				Return(runtimeKilled, nil).
				AnyTimes()
		}
	}

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

func (suite *UpdateRunTestSuite) TestRunningInPlaceUpdate() {
	instancesTotal := []uint32{0, 1}
	oldJobConfigVer := uint64(3)
	newJobConfigVer := uint64(4)

	updateConfig := &pbupdate.UpdateConfig{
		BatchSize: 0,
		InPlace:   true,
	}

	runtimeRunning := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_RUNNING,
		GoalState:            pbtask.TaskState_RUNNING,
		Healthy:              pbtask.HealthState_HEALTHY,
		ConfigVersion:        oldJobConfigVer,
		DesiredConfigVersion: oldJobConfigVer,
		Host:                 "host1",
	}

	runtimeRestarting := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_KILLED,
		GoalState:            pbtask.TaskState_RUNNING,
		Healthy:              pbtask.HealthState_INVALID,
		ConfigVersion:        oldJobConfigVer,
		DesiredConfigVersion: oldJobConfigVer,
		DesiredHost:          "host2",
	}

	instanceAvailabilityMap := map[uint32]jobmgrcommon.InstanceAvailability_Type{}
	for _, i := range instancesTotal {
		instanceAvailabilityMap[i] = jobmgrcommon.InstanceAvailability_AVAILABLE
	}

	cachedTasks := make(map[uint32]*cachedmocks.MockTask)
	for _, instID := range instancesTotal {
		cachedTasks[instID] = cachedmocks.NewMockTask(suite.ctrl)
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: uint64(4),
		}).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.cachedJob.EXPECT().
		GetInstanceAvailabilityType(gomock.Any(), instancesTotal).
		Return(instanceAvailabilityMap)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(instancesTotal).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(nil).Times(3)

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return([]uint32{}).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return([]uint32{}).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(instancesTotal).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(updateConfig).
		AnyTimes()

	for _, instID := range instancesTotal {
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), instID).
			Return(cachedTasks[instID], nil).
			AnyTimes()

		if instID == instancesTotal[0] {
			cachedTasks[instID].EXPECT().
				GetRuntime(gomock.Any()).
				Return(runtimeRunning, nil).
				AnyTimes()
		}

		if instID == instancesTotal[1] {
			cachedTasks[instID].EXPECT().
				GetRuntime(gomock.Any()).
				Return(runtimeRestarting, nil).
				AnyTimes()
		}
	}

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, instancesTotal[0]).
		Return(runtimeRunning, nil)
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, instancesTotal[1]).
		Return(runtimeRestarting, nil)

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobConfigVer, runtimeRunning).
		Return(false)
	suite.cachedUpdate.EXPECT().
		IsInstanceFailed(runtimeRunning, updateConfig.GetMaxInstanceAttempts()).
		Return(false)
	suite.cachedUpdate.EXPECT().
		IsInstanceInProgress(newJobConfigVer, runtimeRunning).
		Return(false)

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobConfigVer, runtimeRestarting).
		Return(false)
	suite.cachedUpdate.EXPECT().
		IsInstanceFailed(runtimeRestarting, updateConfig.GetMaxInstanceAttempts()).
		Return(false)
	suite.cachedUpdate.EXPECT().
		IsInstanceInProgress(newJobConfigVer, runtimeRestarting).
		Return(false)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), gomock.Any(), newJobConfigVer).
		Return(
			&pbjob.JobConfig{
				ChangeLog: &peloton.ChangeLog{Version: newJobConfigVer},
			},
			&models.ConfigAddOn{},
			nil,
		)

	// using loops instead of .Times function, because the latter
	// would return a reference to the map. Any modification to the
	// map would be reflected in other places.
	for i := 0; i < len(instancesTotal); i++ {
		suite.cachedUpdate.EXPECT().
			GetRuntimeDiff(gomock.Any()).
			Return(jobmgrcommon.RuntimeDiff{
				jobmgrcommon.DesiredConfigVersionField: newJobConfigVer,
				jobmgrcommon.FailureCountField:         uint32(0),
				jobmgrcommon.ReasonField:               "",
			})
	}

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Do(func(_ context.Context,
			diffs map[uint32]jobmgrcommon.RuntimeDiff,
			_ bool) {
			suite.Equal(len(diffs), len(instancesTotal))
			suite.Equal(diffs[0][jobmgrcommon.DesiredHostField], "host1")
			suite.Equal(diffs[1][jobmgrcommon.DesiredHostField], "host2")
		}).Return(nil, nil, nil)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Times(2)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{},
			[]uint32{},
			[]uint32{0, 1},
		).Return(nil)

	for _, instID := range instancesTotal {
		suite.cachedJob.EXPECT().
			GetTask(instID).
			Return(cachedTasks[instID]).
			AnyTimes()

		if instID == instancesTotal[0] {
			cachedTasks[instID].EXPECT().
				GetRuntime(gomock.Any()).
				Return(runtimeRunning, nil).
				AnyTimes()
		}

		if instID == instancesTotal[1] {
			cachedTasks[instID].EXPECT().
				GetRuntime(gomock.Any()).
				Return(runtimeRestarting, nil).
				AnyTimes()
		}
	}

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

func (suite *UpdateRunTestSuite) TestCompletedUpdate() {
	desiredConfigVersion := uint64(3)
	var instancesRemaining []uint32
	instancesTotal := []uint32{2, 3, 4, 5}
	runtime := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_RUNNING,
		Healthy:              pbtask.HealthState_HEALTHY,
		GoalState:            pbtask.TaskState_RUNNING,
		ConfigVersion:        desiredConfigVersion,
		DesiredConfigVersion: desiredConfigVersion,
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(instancesTotal).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(instancesTotal).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: uint64(3),
		}).
		Times(2)

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return([]uint32{}).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return(instancesRemaining).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: 0,
		}).
		Times(3)

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	for _, instID := range instancesTotal {
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), instID).
			Return(suite.cachedTask, nil).
			AnyTimes()
		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, instID).
			Return(runtime, nil)
	}

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(desiredConfigVersion, runtime).
		Return(true).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{2, 3, 4, 5},
			instancesRemaining,
			instancesRemaining,
		).Return(nil)

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(entity goalstate.Entity, deadline time.Time) {
			suite.Equal(suite.jobID.GetValue(), entity.GetID())
			updateEnt := entity.(*updateEntity)
			suite.Equal(suite.updateID.GetValue(), updateEnt.id.GetValue())
		})

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

func (suite *UpdateRunTestSuite) TestUpdateFailGetJob() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(nil)

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		Cancel(gomock.Any(), gomock.Any()).
		Return(nil)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

func (suite *UpdateRunTestSuite) TestUpdateTaskRuntimeGetFail() {
	instancesTotal := []uint32{2, 3, 4, 5}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: uint64(1),
		})

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(instancesTotal)

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return([]uint32{})

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Return(nil, fmt.Errorf("fake db error"))

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{})

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

func (suite *UpdateRunTestSuite) TestUpdateProgressDBError() {
	newJobVer := uint64(3)
	var instancesRemaining []uint32
	instancesTotal := []uint32{2, 3, 4, 5}
	runtime := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_RUNNING,
		GoalState:            pbtask.TaskState_RUNNING,
		Healthy:              pbtask.HealthState_HEALTHY,
		ConfigVersion:        newJobVer,
		DesiredConfigVersion: newJobVer,
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: newJobVer,
		})

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(instancesTotal)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(instancesTotal)

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(nil).Times(2)

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: 0,
		}).
		Times(3)

	for _, instID := range instancesTotal {
		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, instID).
			Return(runtime, nil)
	}

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobVer, runtime).
		Return(true).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLING_FORWARD,
			[]uint32{2, 3, 4, 5},
			instancesRemaining,
			gomock.Any(),
		).Return(fmt.Errorf("fake db error"))

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

// TestUpdateRunFullyRunningAddInstances test add instances for a fully running
// job
func (suite *UpdateRunTestSuite) TestUpdateRunFullyRunningAddInstances() {
	oldInstanceNumber := uint32(10)
	newInstanceNumber := uint32(20)
	batchSize := uint32(5)
	newJobVersion := uint64(4)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  newSlice(oldInstanceNumber, newInstanceNumber),
			JobVersion: newJobVersion,
		}).AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(newSlice(oldInstanceNumber, newInstanceNumber)).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(nil).Times(3)

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: batchSize,
		}).AnyTimes()

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), gomock.Any(), uint64(4)).
		Return(
			&pbjob.JobConfig{
				ChangeLog: &peloton.ChangeLog{Version: uint64(4)},
			},
			&models.ConfigAddOn{},
			nil,
		)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{State: pbjob.JobState_RUNNING}, nil)

	for _, instID := range newSlice(oldInstanceNumber, oldInstanceNumber+batchSize) {
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), instID).
			Return(nil, yarpcerrors.NotFoundErrorf("not found"))
	}

	for _, instID := range newSlice(oldInstanceNumber, oldInstanceNumber+batchSize) {
		suite.cachedJob.EXPECT().
			GetTask(instID).
			Return(suite.cachedTask)
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).
			Return(nil, yarpcerrors.NotFoundErrorf("not found"))
		suite.mockedPodEventsOps.EXPECT().
			GetAll(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, nil)
	}

	suite.cachedJob.EXPECT().
		CreateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, runtimes map[uint32]*pbtask.RuntimeInfo, _ string) {
			suite.Len(runtimes, int(batchSize))
		}).Return(nil)

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, nil)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLING_FORWARD,
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context, _ *peloton.UpdateID, _ pbupdate.State,
			instancesDone []uint32, instancesFailed []uint32, instancesCurrent []uint32) {
			suite.EqualValues(instancesCurrent,
				newSlice(oldInstanceNumber, oldInstanceNumber+batchSize))
			suite.Empty(instancesFailed)
			suite.Empty(instancesDone)
		}).Return(nil)

	for _, instID := range newSlice(oldInstanceNumber, oldInstanceNumber+batchSize) {
		suite.cachedJob.EXPECT().
			GetTask(instID).
			Return(suite.cachedTask)
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbtask.RuntimeInfo{
				State:     pbtask.TaskState_PENDING,
				GoalState: pbtask.TaskState_RUNNING,
			}, nil)
	}

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateRun_FullyRunning_AddShrinkInstances test adding shrink instances
// for a fully running job
func (suite *UpdateRunTestSuite) TestUpdateRunFullyRunningAddShrinkInstances() {
	oldInstanceNumber := uint32(10)
	newInstanceNumber := uint32(20)
	batchSize := uint32(5)
	newJobVersion := uint64(4)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  newSlice(oldInstanceNumber, newInstanceNumber),
			JobVersion: newJobVersion,
		}).AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(newSlice(oldInstanceNumber, newInstanceNumber)).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(nil).Times(3)

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: batchSize,
		}).AnyTimes()

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), gomock.Any(), uint64(4)).
		Return(
			&pbjob.JobConfig{
				ChangeLog: &peloton.ChangeLog{Version: uint64(4)},
			},
			&models.ConfigAddOn{},
			nil,
		)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{State: pbjob.JobState_RUNNING}, nil)

	for _, instID := range newSlice(oldInstanceNumber, oldInstanceNumber+batchSize) {
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), instID).
			Return(nil, yarpcerrors.NotFoundErrorf("not found"))

		suite.cachedJob.EXPECT().
			GetTask(instID).
			Return(nil)

		mesosTaskID := fmt.Sprintf("%s-%d-%d", suite.jobID.GetValue(), instID, 50)
		podID := &mesos_v1.TaskID{
			Value: &mesosTaskID,
		}

		prevMesosTaskID := fmt.Sprintf("%s-%d-%d", suite.jobID.GetValue(), instID, 49)
		prevPodID := &mesos_v1.TaskID{
			Value: &prevMesosTaskID,
		}

		var podEvents []*pbtask.PodEvent
		podEvent := &pbtask.PodEvent{
			TaskId:     podID,
			PrevTaskId: prevPodID,
		}
		podEvents = append(podEvents, podEvent)

		suite.mockedPodEventsOps.EXPECT().
			GetAll(gomock.Any(), suite.jobID.GetValue(), uint32(instID)).
			Return(podEvents, nil)
	}

	suite.cachedJob.EXPECT().
		CreateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, runtimes map[uint32]*pbtask.RuntimeInfo, _ string) {
			suite.Len(runtimes, int(batchSize))
		}).Return(nil)

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, nil)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLING_FORWARD,
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context, _ *peloton.UpdateID, _ pbupdate.State,
			instancesDone []uint32, instancesFailed []uint32, instancesCurrent []uint32) {
			suite.EqualValues(instancesCurrent,
				newSlice(oldInstanceNumber, oldInstanceNumber+batchSize))
			suite.Empty(instancesDone)
		}).Return(nil)

	for _, instID := range newSlice(oldInstanceNumber, oldInstanceNumber+batchSize) {
		suite.cachedJob.EXPECT().
			GetTask(instID).
			Return(suite.cachedTask)
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbtask.RuntimeInfo{
				State:     pbtask.TaskState_PENDING,
				GoalState: pbtask.TaskState_RUNNING,
			}, nil)
	}

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateRunFullyRunningUpdateInstances test update instances for a fully running
// job
func (suite *UpdateRunTestSuite) TestUpdateRunFullyRunningUpdateInstances() {
	instanceNumber := uint32(10)
	batchSize := uint32(5)
	jobVersion := uint64(3)
	newJobVersion := uint64(4)
	updateConfig := &pbupdate.UpdateConfig{
		BatchSize: batchSize,
	}

	instancesUpdated := newSlice(0, instanceNumber)
	instanceAvailabilityMap := map[uint32]jobmgrcommon.InstanceAvailability_Type{}
	for _, i := range instancesUpdated {
		instanceAvailabilityMap[i] = jobmgrcommon.InstanceAvailability_AVAILABLE
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	for _, instID := range newSlice(0, batchSize) {
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), instID).
			Return(suite.cachedTask, nil).
			AnyTimes()
		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, instID).
			Return(&pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				ConfigVersion:        jobVersion,
				DesiredConfigVersion: jobVersion,
			}, nil)
	}
	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbtask.RuntimeInfo{
			State:                pbtask.TaskState_RUNNING,
			ConfigVersion:        jobVersion,
			DesiredConfigVersion: jobVersion,
		}, nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  newSlice(0, instanceNumber),
			JobVersion: newJobVersion,
		}).AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(newSlice(0, batchSize)).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(instancesUpdated).
		AnyTimes()

	suite.cachedJob.EXPECT().
		GetInstanceAvailabilityType(gomock.Any(), instancesUpdated).
		Return(instanceAvailabilityMap)

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(nil).Times(3)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(updateConfig).AnyTimes()

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), gomock.Any(), newJobVersion).
		Return(
			&pbjob.JobConfig{
				ChangeLog: &peloton.ChangeLog{Version: newJobVersion},
			},
			&models.ConfigAddOn{},
			nil,
		)

	for range newSlice(0, batchSize) {
		runtime := &pbtask.RuntimeInfo{
			State:                pbtask.TaskState_RUNNING,
			ConfigVersion:        jobVersion,
			DesiredConfigVersion: jobVersion,
		}

		suite.cachedUpdate.EXPECT().
			IsInstanceComplete(newJobVersion, runtime).
			Return(false)

		suite.cachedUpdate.EXPECT().
			IsInstanceInProgress(newJobVersion, runtime).
			Return(false)

		suite.cachedUpdate.EXPECT().
			IsInstanceFailed(runtime, updateConfig.GetMaxInstanceAttempts()).
			Return(false)

		suite.cachedUpdate.EXPECT().
			GetRuntimeDiff(gomock.Any()).
			Return(jobmgrcommon.RuntimeDiff{
				jobmgrcommon.DesiredConfigVersionField: newJobVersion,
			})
	}

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return().
		Times(int(batchSize))

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, nil)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLING_FORWARD,
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context, _ *peloton.UpdateID, _ pbupdate.State,
			instancesDone []uint32, instancesFailed []uint32, instancesCurrent []uint32) {
			suite.EqualValues(instancesCurrent,
				newSlice(0, batchSize))
			suite.Empty(instancesFailed)
			suite.Empty(instancesDone)
		}).Return(nil)

	for _, instID := range newSlice(0, batchSize) {
		suite.cachedJob.EXPECT().
			GetTask(instID).
			Return(suite.cachedTask)
	}

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateRunContainsKilledTaskUpdateInstances tests the case to update
// a job with killed tasks
func (suite *UpdateRunTestSuite) TestUpdateRunContainsKilledTaskUpdateInstances() {
	instanceNumber := uint32(10)
	batchSize := uint32(5)
	jobVersion := uint64(3)
	newJobVersion := uint64(4)

	instancesUpdated := newSlice(0, instanceNumber)
	instanceAvailabilityMap := map[uint32]jobmgrcommon.InstanceAvailability_Type{}
	for _, i := range instancesUpdated {
		instanceAvailabilityMap[i] = jobmgrcommon.InstanceAvailability_AVAILABLE
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  newSlice(0, instanceNumber),
			JobVersion: newJobVersion,
		}).AnyTimes()

	suite.cachedJob.EXPECT().
		GetInstanceAvailabilityType(gomock.Any(), instancesUpdated).
		Return(instanceAvailabilityMap)

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(instancesUpdated).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(nil).Times(3)

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return(nil).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddTask(gomock.Any(), gomock.Any()).
		Return(suite.cachedTask, nil).
		AnyTimes()

	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbtask.RuntimeInfo{
			State:                pbtask.TaskState_KILLED,
			GoalState:            pbtask.TaskState_KILLED,
			ConfigVersion:        jobVersion,
			DesiredConfigVersion: jobVersion,
		}, nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: batchSize,
		}).AnyTimes()

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), gomock.Any(), newJobVersion).
		Return(
			&pbjob.JobConfig{
				ChangeLog: &peloton.ChangeLog{Version: newJobVersion},
			},
			&models.ConfigAddOn{},
			nil,
		)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return().
		Times(int(batchSize))

	suite.cachedUpdate.EXPECT().
		GetRuntimeDiff(gomock.Any()).
		Return(jobmgrcommon.RuntimeDiff{
			jobmgrcommon.DesiredConfigVersionField: newJobVersion,
		}).Times(int(batchSize))

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, nil)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLING_FORWARD,
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context, _ *peloton.UpdateID, _ pbupdate.State,
			instancesDone []uint32, instancesFailed []uint32, instancesCurrent []uint32) {
			suite.EqualValues(instancesCurrent,
				newSlice(0, batchSize))
			suite.Empty(instancesDone)
			suite.Empty(instancesFailed)
		}).Return(nil)

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	task0 := cachedmocks.NewMockTask(suite.ctrl)
	suite.cachedJob.EXPECT().
		GetTask(uint32(0)).
		Return(task0)
	task0.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbtask.RuntimeInfo{
			State:                pbtask.TaskState_KILLED,
			GoalState:            pbtask.TaskState_KILLED,
			ConfigVersion:        jobVersion,
			DesiredConfigVersion: jobVersion,
		}, nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateRunContainsTerminatedTaskInstances tests the case to update
// a job with failed tasks with killed goal state
func (suite *UpdateRunTestSuite) TestUpdateRunContainsTerminatedTaskInstances() {
	instanceNumber := uint32(10)
	batchSize := uint32(5)
	jobVersion := uint64(3)
	newJobVersion := uint64(4)

	instancesUpdated := newSlice(0, instanceNumber)
	instanceAvailabilityMap := map[uint32]jobmgrcommon.InstanceAvailability_Type{}
	for _, i := range instancesUpdated {
		instanceAvailabilityMap[i] = jobmgrcommon.InstanceAvailability_AVAILABLE
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.cachedJob.EXPECT().
		GetInstanceAvailabilityType(gomock.Any(), instancesUpdated).
		Return(instanceAvailabilityMap)

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  newSlice(0, instanceNumber),
			JobVersion: newJobVersion,
		}).AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(instancesUpdated).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(nil).Times(3)

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return(nil).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddTask(gomock.Any(), gomock.Any()).
		Return(suite.cachedTask, nil).
		AnyTimes()

	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbtask.RuntimeInfo{
			State:                pbtask.TaskState_KILLED,
			GoalState:            pbtask.TaskState_KILLED,
			ConfigVersion:        jobVersion,
			DesiredConfigVersion: jobVersion,
		}, nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: batchSize,
		}).AnyTimes()

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), gomock.Any(), newJobVersion).
		Return(
			&pbjob.JobConfig{
				ChangeLog: &peloton.ChangeLog{Version: newJobVersion},
			},
			&models.ConfigAddOn{},
			nil,
		)

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return().
		Times(int(batchSize))

	suite.cachedUpdate.EXPECT().
		GetRuntimeDiff(gomock.Any()).
		Return(jobmgrcommon.RuntimeDiff{
			jobmgrcommon.DesiredConfigVersionField: newJobVersion,
		}).Times(int(batchSize))

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, nil)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLING_FORWARD,
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context, _ *peloton.UpdateID, _ pbupdate.State,
			instancesDone []uint32, instancesFailed []uint32, instancesCurrent []uint32) {
			suite.EqualValues(instancesCurrent,
				newSlice(0, batchSize))
			suite.Empty(instancesDone)
			suite.Empty(instancesFailed)
		}).Return(nil)

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	task0 := cachedmocks.NewMockTask(suite.ctrl)
	suite.cachedJob.EXPECT().
		GetTask(uint32(0)).
		Return(task0)
	task0.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbtask.RuntimeInfo{
			State:                pbtask.TaskState_FAILED,
			GoalState:            pbtask.TaskState_KILLED,
			ConfigVersion:        jobVersion,
			DesiredConfigVersion: jobVersion,
		}, nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateRunKilledJobAddInstances tests add instances to killed job
func (suite *UpdateRunTestSuite) TestUpdateRunKilledJobAddInstances() {
	oldInstanceNumber := uint32(10)
	newInstanceNumber := uint32(20)
	batchSize := uint32(5)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  newSlice(oldInstanceNumber, newInstanceNumber),
			JobVersion: uint64(4),
		}).AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(newSlice(oldInstanceNumber, newInstanceNumber)).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(newSlice(oldInstanceNumber, newInstanceNumber)).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(nil).Times(3)

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: batchSize,
		}).AnyTimes()

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), gomock.Any(), uint64(4)).
		Return(
			&pbjob.JobConfig{
				ChangeLog: &peloton.ChangeLog{Version: uint64(4)},
			},
			&models.ConfigAddOn{},
			nil,
		)

	for _, instID := range newSlice(oldInstanceNumber, newInstanceNumber) {
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), instID).
			Return(nil,
				yarpcerrors.NotFoundErrorf("new instance has no runtime yet")).
			AnyTimes()
		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, instID).
			Return(nil,
				yarpcerrors.NotFoundErrorf("new instance has no runtime yet")).
			AnyTimes()
	}

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{GoalState: pbjob.JobState_KILLED}, nil)

	for _, instID := range newSlice(oldInstanceNumber, oldInstanceNumber+batchSize) {
		suite.cachedJob.EXPECT().
			GetTask(instID).
			Return(nil)
		suite.mockedPodEventsOps.EXPECT().
			GetAll(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, nil)
	}

	suite.cachedJob.EXPECT().
		CreateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, runtimes map[uint32]*pbtask.RuntimeInfo, _ string) {
			suite.Len(runtimes, int(batchSize))
		}).Return(nil)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	for _, instID := range newSlice(oldInstanceNumber, oldInstanceNumber+batchSize) {
		suite.cachedJob.EXPECT().
			GetTask(instID).
			Return(suite.cachedTask)
		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbtask.RuntimeInfo{
				State:     pbtask.TaskState_PENDING,
				GoalState: pbtask.TaskState_RUNNING,
			}, nil)
	}

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLING_FORWARD,
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context, _ *peloton.UpdateID, _ pbupdate.State,
			instancesDone []uint32, instancesFailed []uint32, instancesCurrent []uint32) {
			suite.EqualValues(instancesCurrent,
				newSlice(oldInstanceNumber, oldInstanceNumber+batchSize))
			suite.Empty(instancesDone)
			suite.Empty(instancesFailed)
		}).Return(nil)

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateRunDBErrorAddInstances test add instances and db has error
// when add the tasks
func (suite *UpdateRunTestSuite) TestUpdateRunDBErrorAddInstances() {
	oldInstanceNumber := uint32(10)
	newInstanceNumber := uint32(20)
	batchSize := uint32(5)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  newSlice(oldInstanceNumber, newInstanceNumber),
			JobVersion: uint64(4),
		}).AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(newSlice(oldInstanceNumber, newInstanceNumber))

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(nil).Times(2)

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddTask(gomock.Any(), gomock.Any()).
		Return(nil, yarpcerrors.NotFoundErrorf("not-found")).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: batchSize,
		}).AnyTimes()

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), gomock.Any(), uint64(4)).
		Return(
			&pbjob.JobConfig{
				ChangeLog: &peloton.ChangeLog{Version: uint64(4)},
			},
			&models.ConfigAddOn{},
			nil,
		)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{State: pbjob.JobState_RUNNING}, nil)

	for _, instID := range newSlice(oldInstanceNumber, oldInstanceNumber+batchSize) {
		suite.cachedJob.EXPECT().
			GetTask(instID).
			Return(nil)
		suite.mockedPodEventsOps.EXPECT().
			GetAll(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, nil)
	}

	suite.cachedJob.EXPECT().
		CreateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, runtimes map[uint32]*pbtask.RuntimeInfo, _ string) {
			suite.Len(runtimes, int(batchSize))
		}).Return(nil)

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, yarpcerrors.UnavailableErrorf("test error"))

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.Error(err)
}

// TestUpdateRunDBErrorUpdateInstances test add instances and db has error
// when update the tasks
func (suite *UpdateRunTestSuite) TestUpdateRunDBErrorUpdateInstances() {
	instanceNumber := uint32(10)
	batchSize := uint32(5)
	newJobVersion := uint64(4)

	instancesUpdated := newSlice(0, instanceNumber)
	instanceAvailabilityMap := map[uint32]jobmgrcommon.InstanceAvailability_Type{}
	for _, i := range instancesUpdated {
		instanceAvailabilityMap[i] = jobmgrcommon.InstanceAvailability_AVAILABLE
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  newSlice(0, instanceNumber),
			JobVersion: newJobVersion,
		}).AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(instancesUpdated)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(nil)

	suite.cachedJob.EXPECT().
		GetInstanceAvailabilityType(gomock.Any(), instancesUpdated).
		Return(instanceAvailabilityMap)

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(nil).Times(2)

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return(nil)

	suite.cachedJob.EXPECT().
		AddTask(gomock.Any(), gomock.Any()).
		Return(suite.cachedTask, nil).
		AnyTimes()

	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbtask.RuntimeInfo{
			State: pbtask.TaskState_RUNNING,
		}, nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: batchSize,
		}).AnyTimes()

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), gomock.Any(), newJobVersion).
		Return(
			&pbjob.JobConfig{
				ChangeLog: &peloton.ChangeLog{Version: newJobVersion},
			},
			&models.ConfigAddOn{},
			nil,
		)

	suite.cachedUpdate.EXPECT().
		GetRuntimeDiff(gomock.Any()).
		Return(jobmgrcommon.RuntimeDiff{
			jobmgrcommon.DesiredConfigVersionField: newJobVersion,
		}).Times(int(batchSize))

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, yarpcerrors.UnavailableErrorf("test error"))

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.Error(err)
}

// TestRunningUpdateRemoveInstances tests removing instances
func (suite *UpdateRunTestSuite) TestRunningUpdateRemoveInstances() {
	instancesTotal := []uint32{4, 5, 6}
	oldJobConfigVer := uint64(3)
	newJobConfigVer := uint64(4)

	runtimeTerminated := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_KILLED,
		GoalState:            pbtask.TaskState_KILLED,
		Healthy:              pbtask.HealthState_INVALID,
		ConfigVersion:        oldJobConfigVer,
		DesiredConfigVersion: newJobConfigVer,
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: newJobConfigVer,
		}).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(instancesTotal).Times(3)

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return(nil).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddTask(gomock.Any(), gomock.Any()).
		Return(suite.cachedTask, nil).
		AnyTimes()

	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).
		Return(runtimeTerminated, nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: 2,
		}).AnyTimes()

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), gomock.Any(), uint64(4)).
		Return(
			&pbjob.JobConfig{
				ChangeLog: &peloton.ChangeLog{Version: uint64(4)},
			},
			&models.ConfigAddOn{},
			nil,
		)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Do(func(_ context.Context, runtimes map[uint32]jobmgrcommon.RuntimeDiff, _ bool) {
			suite.Equal(2, len(runtimes))
			for _, runtime := range runtimes {
				suite.Equal(pbtask.TaskState_DELETED, runtime[jobmgrcommon.GoalStateField])
				suite.Equal(newJobConfigVer, runtime[jobmgrcommon.DesiredConfigVersionField])
				suite.Equal(&pbtask.TerminationStatus{
					Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_UPDATE,
				}, runtime[jobmgrcommon.TerminationStatusField])
			}
		}).Return(nil, nil, nil)

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).AnyTimes()

	suite.taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return().
		Times(2)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLING_FORWARD,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).Return(nil)

	suite.cachedJob.EXPECT().
		GetTask(gomock.Any()).
		Return(suite.cachedTask)

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestRunningUpdateRemoveInstancesDBError tests removing instances with DB error
// during patch tasks
func (suite *UpdateRunTestSuite) TestRunningUpdateRemoveInstancesDBError() {
	instancesTotal := []uint32{4, 5, 6}
	newJobConfigVer := uint64(4)
	runtimeTerminated := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_KILLED,
		GoalState:            pbtask.TaskState_KILLED,
		Healthy:              pbtask.HealthState_INVALID,
		DesiredConfigVersion: newJobConfigVer,
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: newJobConfigVer,
		}).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(instancesTotal).Times(2)

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return(nil)

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return(nil)

	suite.cachedJob.EXPECT().
		AddTask(gomock.Any(), gomock.Any()).
		Return(suite.cachedTask, nil).
		AnyTimes()

	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).
		Return(runtimeTerminated, nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(&pbupdate.UpdateConfig{
			BatchSize: 2,
		}).AnyTimes()

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), gomock.Any(), uint64(4)).
		Return(
			&pbjob.JobConfig{
				ChangeLog: &peloton.ChangeLog{Version: uint64(4)},
			},
			&models.ConfigAddOn{},
			nil,
		)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, fmt.Errorf("fake db error"))

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

// TestRunningUpdateFailed tests the case that update fails due to
// too many instances failed
func (suite *UpdateRunTestSuite) TestRunningUpdateFailed() {
	instancesTotal := []uint32{0, 1, 2, 3, 4, 5, 6}
	newJobConfigVer := uint64(4)
	failureCount := uint32(5)
	failedInstances := uint32(3)

	updateConfig := &pbupdate.UpdateConfig{
		BatchSize:           0,
		MaxFailureInstances: failedInstances,
	}

	runtimeFailed := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_FAILED,
		GoalState:            pbtask.TaskState_RUNNING,
		FailureCount:         failureCount,
		Healthy:              pbtask.HealthState_UNHEALTHY,
		ConfigVersion:        newJobConfigVer,
		DesiredConfigVersion: newJobConfigVer,
	}

	runtimeDone := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_RUNNING,
		GoalState:            pbtask.TaskState_RUNNING,
		Healthy:              pbtask.HealthState_HEALTHY,
		ConfigVersion:        newJobConfigVer,
		DesiredConfigVersion: newJobConfigVer,
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: uint64(4),
		}).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(instancesTotal)

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(updateConfig).
		Times(4)

	for i, instID := range instancesTotal {
		if uint32(i) < failedInstances {
			suite.taskStore.EXPECT().
				GetTaskRuntime(gomock.Any(), suite.jobID, instID).
				Return(runtimeFailed, nil)
		} else {
			suite.taskStore.EXPECT().
				GetTaskRuntime(gomock.Any(), suite.jobID, instID).
				Return(runtimeDone, nil)
		}
	}
	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobConfigVer, runtimeFailed).
		Return(false).
		AnyTimes()
	suite.cachedUpdate.EXPECT().
		IsInstanceFailed(runtimeFailed, updateConfig.GetMaxInstanceAttempts()).
		Return(true).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobConfigVer, runtimeDone).
		Return(true).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return([]uint32{})

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_FAILED,
			newSlice(failedInstances, uint32(len(instancesTotal))),
			newSlice(0, failedInstances),
			nil,
		).Return(nil)

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.updateGoalStateEngine.
		EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestRunningUpdateRolledBack tests the case that update fails due to
// too many instances failed and rollback is triggered
func (suite *UpdateRunTestSuite) TestRunningUpdateRolledBack() {
	totalInstances := uint32(10)
	totalInstancesToUpdate := []uint32{0, 1, 2, 3, 4, 5, 6}
	newJobConfigVer := uint64(4)
	failureCount := uint32(5)
	failedInstances := uint32(3)

	updateConfig := &pbupdate.UpdateConfig{
		BatchSize:           0,
		MaxFailureInstances: failedInstances,
		RollbackOnFailure:   true,
	}

	runtimeFailed := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_FAILED,
		GoalState:            pbtask.TaskState_RUNNING,
		FailureCount:         failureCount,
		Healthy:              pbtask.HealthState_UNHEALTHY,
		ConfigVersion:        newJobConfigVer,
		DesiredConfigVersion: newJobConfigVer,
	}

	runtimeDone := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_RUNNING,
		GoalState:            pbtask.TaskState_RUNNING,
		Healthy:              pbtask.HealthState_HEALTHY,
		ConfigVersion:        newJobConfigVer,
		DesiredConfigVersion: newJobConfigVer,
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  totalInstancesToUpdate,
			JobVersion: uint64(4),
		}).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(totalInstancesToUpdate)

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(updateConfig).
		Times(4)

	for i, instID := range totalInstancesToUpdate {
		if uint32(i) < failedInstances {
			suite.taskStore.EXPECT().
				GetTaskRuntime(gomock.Any(), suite.jobID, instID).
				Return(runtimeFailed, nil)
		} else {
			suite.taskStore.EXPECT().
				GetTaskRuntime(gomock.Any(), suite.jobID, instID).
				Return(runtimeDone, nil)
		}
	}

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobConfigVer, runtimeFailed).
		Return(false).
		AnyTimes()
	suite.cachedUpdate.EXPECT().
		IsInstanceFailed(runtimeFailed, updateConfig.GetMaxInstanceAttempts()).
		Return(true).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobConfigVer, runtimeDone).
		Return(true).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		}).
		Times(2)

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLING_FORWARD,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).Return(nil)

	suite.cachedJob.EXPECT().
		RollbackWorkflow(gomock.Any()).
		Return(nil)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&pbjob.JobConfig{
			InstanceCount: totalInstances,
		}, nil)

	suite.cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Do(func(_ context.Context,
			runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff,
			_ bool) {
			suite.Len(runtimeDiffs, int(totalInstances)-len(totalInstancesToUpdate))
			for i := uint32(len(totalInstancesToUpdate)); i < totalInstances; i++ {
				suite.NotEmpty(runtimeDiffs[i])
			}
		}).Return(nil, nil, nil)

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID).
		Times(2)

	suite.updateGoalStateEngine.
		EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestRunningUpdateRolledBack tests the case that update fails due to
// too many instances failed and rollback is triggered but fails
func (suite *UpdateRunTestSuite) TestRunningUpdateRolledBackFail() {
	totalInstancesToUpdate := []uint32{0, 1, 2, 3, 4, 5, 6}
	newJobConfigVer := uint64(4)
	failureCount := uint32(5)
	failedInstances := uint32(3)

	updateConfig := &pbupdate.UpdateConfig{
		BatchSize:           0,
		MaxFailureInstances: failedInstances,
		RollbackOnFailure:   true,
	}

	runtimeFailed := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_FAILED,
		GoalState:            pbtask.TaskState_RUNNING,
		FailureCount:         failureCount,
		Healthy:              pbtask.HealthState_UNHEALTHY,
		ConfigVersion:        newJobConfigVer,
		DesiredConfigVersion: newJobConfigVer,
	}

	runtimeDone := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_RUNNING,
		GoalState:            pbtask.TaskState_RUNNING,
		Healthy:              pbtask.HealthState_HEALTHY,
		ConfigVersion:        newJobConfigVer,
		DesiredConfigVersion: newJobConfigVer,
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  totalInstancesToUpdate,
			JobVersion: uint64(4),
		}).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(totalInstancesToUpdate)

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(updateConfig).
		Times(4)

	for i, instID := range totalInstancesToUpdate {
		if uint32(i) < failedInstances {
			suite.taskStore.EXPECT().
				GetTaskRuntime(gomock.Any(), suite.jobID, instID).
				Return(runtimeFailed, nil)
		} else {
			suite.taskStore.EXPECT().
				GetTaskRuntime(gomock.Any(), suite.jobID, instID).
				Return(runtimeDone, nil)
		}
	}

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobConfigVer, runtimeFailed).
		Return(false).
		AnyTimes()
	suite.cachedUpdate.EXPECT().
		IsInstanceFailed(runtimeFailed, updateConfig.GetMaxInstanceAttempts()).
		Return(true).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobConfigVer, runtimeDone).
		Return(true).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		}).
		Times(2)

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLING_FORWARD,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).Return(nil)

	suite.cachedJob.EXPECT().
		RollbackWorkflow(gomock.Any()).
		Return(yarpcerrors.InternalErrorf("test error"))

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.Error(err)
}

// TestUpdateRollingBackFailed tests the case that update rollback
// failed due to too many failure
func (suite *UpdateRunTestSuite) TestUpdateRollingBackFailed() {
	instancesTotal := []uint32{0, 1, 2, 3, 4, 5, 6}
	newJobConfigVer := uint64(4)
	failureCount := uint32(5)
	failedInstances := uint32(3)

	updateConfig := &pbupdate.UpdateConfig{
		BatchSize:           0,
		MaxFailureInstances: failedInstances,
		RollbackOnFailure:   true,
	}

	runtimeFailed := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_FAILED,
		GoalState:            pbtask.TaskState_RUNNING,
		FailureCount:         failureCount,
		Healthy:              pbtask.HealthState_UNHEALTHY,
		ConfigVersion:        newJobConfigVer,
		DesiredConfigVersion: newJobConfigVer,
	}

	runtimeDone := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_RUNNING,
		GoalState:            pbtask.TaskState_RUNNING,
		Healthy:              pbtask.HealthState_HEALTHY,
		ConfigVersion:        newJobConfigVer,
		DesiredConfigVersion: newJobConfigVer,
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_BACKWARD,
		})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: uint64(4),
		}).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(instancesTotal)

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(updateConfig).
		Times(4)

	for i, instID := range instancesTotal {
		if uint32(i) < failedInstances {
			suite.taskStore.EXPECT().
				GetTaskRuntime(gomock.Any(), suite.jobID, instID).
				Return(runtimeFailed, nil)
		} else {
			suite.taskStore.EXPECT().
				GetTaskRuntime(gomock.Any(), suite.jobID, instID).
				Return(runtimeDone, nil)
		}
	}

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobConfigVer, runtimeFailed).
		Return(false).
		AnyTimes()
	suite.cachedUpdate.EXPECT().
		IsInstanceFailed(runtimeFailed, updateConfig.GetMaxInstanceAttempts()).
		Return(true).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobConfigVer, runtimeDone).
		Return(true).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_BACKWARD,
		})

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_FAILED,
			newSlice(failedInstances, uint32(len(instancesTotal))),
			newSlice(0, failedInstances),
			nil,
		).Return(nil)

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.updateGoalStateEngine.
		EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestRunningUpdatePartialFailure tests the case that some instances
// in the job failed but has not reached updateConfig.MaxFailureInstances
func (suite *UpdateRunTestSuite) TestRunningUpdatePartialFailure() {
	instancesTotal := []uint32{0, 1, 2, 3, 4, 5, 6}
	newJobConfigVer := uint64(4)
	failureCount := uint32(5)
	failedInstances := uint32(3)

	updateConfig := &pbupdate.UpdateConfig{
		BatchSize:           0,
		MaxFailureInstances: failedInstances + 1,
	}

	runtimeFailed := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_FAILED,
		GoalState:            pbtask.TaskState_RUNNING,
		FailureCount:         failureCount,
		Healthy:              pbtask.HealthState_UNHEALTHY,
		ConfigVersion:        newJobConfigVer,
		DesiredConfigVersion: newJobConfigVer,
	}

	runtimeDone := &pbtask.RuntimeInfo{
		State:                pbtask.TaskState_RUNNING,
		GoalState:            pbtask.TaskState_RUNNING,
		Healthy:              pbtask.HealthState_HEALTHY,
		ConfigVersion:        newJobConfigVer,
		DesiredConfigVersion: newJobConfigVer,
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob).
		AnyTimes()

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID).
		AnyTimes()

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances:  instancesTotal,
			JobVersion: uint64(4),
		}).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return([]uint32{}).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return([]uint32{}).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(instancesTotal).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(nil).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(instancesTotal).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(nil).Times(3)

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(updateConfig).
		Times(4)

	for i, instID := range instancesTotal {
		if uint32(i) < failedInstances {
			suite.taskStore.EXPECT().
				GetTaskRuntime(gomock.Any(), suite.jobID, instID).
				Return(runtimeFailed, nil)
		} else {
			suite.taskStore.EXPECT().
				GetTaskRuntime(gomock.Any(), suite.jobID, instID).
				Return(runtimeDone, nil)
		}
	}

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobConfigVer, runtimeFailed).
		Return(false).
		AnyTimes()
	suite.cachedUpdate.EXPECT().
		IsInstanceFailed(runtimeFailed, updateConfig.GetMaxInstanceAttempts()).
		Return(true).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		IsInstanceComplete(newJobConfigVer, runtimeDone).
		Return(true).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLING_FORWARD,
			newSlice(failedInstances, uint32(len(instancesTotal))),
			newSlice(0, failedInstances),
			nil,
		).Return(nil)

	suite.cachedUpdate.EXPECT().
		ID().
		Return(suite.updateID)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	err := UpdateRun(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

func (suite *UpdateRunTestSuite) TestConfirmInstancesStatus() {
	instanceCount := uint32(6)
	instancesToAdd := []uint32{2, 3, 4}
	instancesToUpdate := []uint32{0, 1}
	instancesToRemove := []uint32{5}
	cachedTasks := make(map[uint32]*cachedmocks.MockTask)

	for i := uint32(1); i < instanceCount-1; i++ {
		cachedTasks[i] = cachedmocks.NewMockTask(suite.ctrl)
		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), i).
			Return(cachedTasks[i], nil)
	}

	suite.cachedJob.EXPECT().
		AddTask(gomock.Any(), uint32(0)).
		Return(nil, yarpcerrors.NotFoundErrorf("not-found"))

	suite.cachedJob.EXPECT().
		AddTask(gomock.Any(), uint32(5)).
		Return(nil, yarpcerrors.NotFoundErrorf("not-found"))

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			JobVersion: 3,
		}).AnyTimes()

	cachedTasks[2].EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, yarpcerrors.NotFoundErrorf("not-found"))

	cachedTasks[3].EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbtask.RuntimeInfo{
			ConfigVersion: 3,
		}, nil)

	cachedTasks[4].EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbtask.RuntimeInfo{
			ConfigVersion: 2,
		}, nil)

	cachedTasks[1].EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, yarpcerrors.NotFoundErrorf("not-found"))

	newInstancesToAdd, newInstancesToUpdate, newInstancesToRemove,
		instancesDone, err := confirmInstancesStatus(
		context.Background(),
		suite.cachedJob,
		suite.cachedUpdate,
		instancesToAdd,
		instancesToUpdate,
		instancesToRemove,
	)

	suite.NoError(err)
	suite.Len(newInstancesToAdd, 4)
	suite.Len(newInstancesToUpdate, 1)
	suite.Empty(newInstancesToRemove)
	suite.Len(instancesDone, 1)
}

func newSlice(start uint32, end uint32) []uint32 {
	result := make([]uint32, 0, end-start)
	for i := start; i < end; i++ {
		result = append(result, i)
	}
	return result
}
