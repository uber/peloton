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

	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbupdate "github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/common/goalstate"
	goalstatemocks "github.com/uber/peloton/pkg/common/goalstate/mocks"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

type UpdateActionsTestSuite struct {
	suite.Suite
	ctrl                  *gomock.Controller
	updateStore           *storemocks.MockUpdateStore
	taskStore             *storemocks.MockTaskStore
	jobFactory            *cachedmocks.MockJobFactory
	updateGoalStateEngine *goalstatemocks.MockEngine
	jobGoalStateEngine    *goalstatemocks.MockEngine
	goalStateDriver       *driver
	jobID                 *peloton.JobID
	updateID              *peloton.UpdateID
	updateEnt             *updateEntity
	cachedJob             *cachedmocks.MockJob
	cachedUpdate          *cachedmocks.MockUpdate
	cachedTask            *cachedmocks.MockTask
}

func TestUpdateActions(t *testing.T) {
	suite.Run(t, new(UpdateActionsTestSuite))
}

func (suite *UpdateActionsTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.updateStore = storemocks.NewMockUpdateStore(suite.ctrl)
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.updateGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.goalStateDriver = &driver{
		updateStore:  suite.updateStore,
		taskStore:    suite.taskStore,
		jobFactory:   suite.jobFactory,
		updateEngine: suite.updateGoalStateEngine,
		jobEngine:    suite.jobGoalStateEngine,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
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

func (suite *UpdateActionsTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestUpdateCheckIfAbortedRuntimeGetFail tests if fetching the job runtime
// fails while trying to check if current job update in the goal state
// engine needs to be aborted
func (suite *UpdateActionsTestSuite) TestUpdateCheckIfAbortedRuntimeGetFail() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateEnt.id).
		Return(suite.cachedUpdate)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, fmt.Errorf("fake db error"))

	err := UpdateAbortIfNeeded(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

// TestUpdateCheckIfAbortedRuntimeGetFail tests that the current update
// in the goal state engine and job runtime are the same
func (suite *UpdateActionsTestSuite) TestUpdateCheckIfAbortedRunUpdate() {
	jobRuntime := &pbjob.RuntimeInfo{
		UpdateID: suite.updateID,
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateEnt.id).
		Return(suite.cachedUpdate)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.NoError(UpdateAbortIfNeeded(context.Background(), suite.updateEnt))
}

// TestUpdateAbortIfNeededUntrackTerminatedJob tests that UpdateAbortIfNeeded
// would untrack a terminated job
func (suite *UpdateActionsTestSuite) TestUpdateAbortIfNeededTerminatedJob() {
	jobRuntime := &pbjob.RuntimeInfo{
		UpdateID:  suite.updateID,
		State:     pbjob.JobState_KILLED,
		GoalState: pbjob.JobState_KILLED,
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateEnt.id).
		Return(suite.cachedUpdate)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	suite.NoError(UpdateAbortIfNeeded(context.Background(), suite.updateEnt))
}

// TestUpdateAbortIfNeededTerminatedJobToBeRestarted tests that UpdateAbortIfNeeded
// would not untrack a terminated job that would restart
func (suite *UpdateActionsTestSuite) TestUpdateAbortIfNeededTerminatedJobToBeRestarted() {
	jobRuntime := &pbjob.RuntimeInfo{
		UpdateID:  suite.updateID,
		State:     pbjob.JobState_KILLED,
		GoalState: pbjob.JobState_RUNNING,
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateEnt.id).
		Return(suite.cachedUpdate)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.NoError(UpdateAbortIfNeeded(context.Background(), suite.updateEnt))
}

// TestUpdateCheckIfAbortedUpdateAbort tests that the current update in the
// goal state engine and the job runtime are not the same
func (suite *UpdateActionsTestSuite) TestUpdateCheckIfAbortedUpdateAbort() {
	jobRuntime := &pbjob.RuntimeInfo{
		UpdateID: &peloton.UpdateID{Value: uuid.NewRandom().String()},
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateEnt.id).
		Return(suite.cachedUpdate)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.cachedUpdate.EXPECT().
		Cancel(gomock.Any(), gomock.Any()).
		Return(nil)

	err := UpdateAbortIfNeeded(context.Background(), suite.updateEnt)
	suite.True(yarpcerrors.IsAborted(err))
	suite.EqualError(err, "code:aborted message:update aborted")
}

// TestUpdateCheckIfAbortedUpdateAbortFail tests that the current update in the
// goal state engine and the job runtime are not the same and aborting the
// update fails
func (suite *UpdateActionsTestSuite) TestUpdateCheckIfAbortedUpdateAbortFail() {
	jobRuntime := &pbjob.RuntimeInfo{
		UpdateID: &peloton.UpdateID{Value: uuid.NewRandom().String()},
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateEnt.id).
		Return(suite.cachedUpdate)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.cachedUpdate.EXPECT().
		Cancel(gomock.Any(), gomock.Any()).
		Return(yarpcerrors.InternalErrorf("test error"))

	suite.Error(UpdateAbortIfNeeded(context.Background(), suite.updateEnt))
}

// TestUpdateReload tests reloading the update from the DB
func (suite *UpdateActionsTestSuite) TestUpdateReload() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateEnt.id).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		Recover(gomock.Any()).
		Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(updateEntity goalstate.Entity, deadline time.Time) {
			suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
		})

	err := UpdateReload(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateReloadNotExists tests reloading an update which
// does not exist in the DB
func (suite *UpdateActionsTestSuite) TestUpdateReloadNotExists() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateEnt.id).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		Recover(gomock.Any()).
		Return(yarpcerrors.NotFoundErrorf("update not found"))

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{}, nil)

	suite.cachedJob.EXPECT().
		ClearWorkflow(suite.updateEnt.id)

	suite.updateGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Do(func(updateEntity goalstate.Entity) {
			suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
		})

	suite.updateStore.EXPECT().
		GetUpdatesForJob(gomock.Any(), suite.jobID.GetValue()).
		Return([]*peloton.UpdateID{}, nil)

	err := UpdateReload(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateRollingForwardComplete tests completing an update
// which is rolling forward
func (suite *UpdateActionsTestSuite) TestUpdateRollingForwardComplete() {
	instancesDone := []uint32{4, 5}
	instancesFailed := []uint32{2, 3}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: uuid.New()})

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		}).
		Times(2)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE)

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return(instancesFailed).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(make([]uint32, 1)).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(make([]uint32, 1)).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(make([]uint32, 1)).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return(instancesDone).
		AnyTimes()

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_SUCCEEDED,
			instancesDone,
			instancesFailed,
			[]uint32{}).
		Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(updateEntity goalstate.Entity, deadline time.Time) {
			suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
		})

	err := UpdateComplete(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateRollingBackwardComplete tests completing an update
// which is rolling backward
func (suite *UpdateActionsTestSuite) TestUpdateRollingBackwardComplete() {
	instancesDone := []uint32{4, 5}
	instancesFailed := []uint32{2, 3}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: uuid.New()})

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_BACKWARD,
		}).
		Times(2)

	suite.cachedUpdate.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE)

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return(instancesFailed).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesAdded().
		Return(instancesFailed).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesUpdated().
		Return(instancesFailed).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return(instancesFailed).
		AnyTimes()

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return(instancesDone).
		AnyTimes()

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_ROLLED_BACK,
			instancesDone,
			instancesFailed,
			[]uint32{}).
		Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(updateEntity goalstate.Entity, deadline time.Time) {
			suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
		})

	err := UpdateComplete(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateCompleteMissingInCache tests completing an update which
// is not present in the cache
func (suite *UpdateActionsTestSuite) TestUpdateCompleteMissingInCache() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_INVALID,
		})

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		Recover(gomock.Any()).
		Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := UpdateComplete(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateCompleteDBError tests failing to mark an update complete
func (suite *UpdateActionsTestSuite) TestUpdateCompleteDBError() {
	instancesTotal := []uint32{2, 3, 4, 5}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		}).
		Times(2)

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return(instancesTotal)

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_SUCCEEDED,
			instancesTotal,
			[]uint32{},
			[]uint32{}).
		Return(fmt.Errorf("fake db error"))

	err := UpdateComplete(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

// TestUpdateUntrackRuntimeGetFail tests failing to fetch job runtime
// while untracking an update
func (suite *UpdateActionsTestSuite) TestUpdateUntrackRuntimeGetFail() {
	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, fmt.Errorf("fake db error"))

	err := UpdateUntrack(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

// TestUpdateUntrack tests untracking an update successfully
func (suite *UpdateActionsTestSuite) TestUpdateUntrack() {
	jobRuntime := &pbjob.RuntimeInfo{
		UpdateID: suite.updateID,
	}
	updateInfo := &models.UpdateModel{
		State: pbupdate.State_INITIALIZED,
	}

	prevUpdateIDs := []*peloton.UpdateID{
		{Value: uuid.NewRandom().String()},
		{Value: uuid.NewRandom().String()},
	}

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.updateGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Do(func(updateEntity goalstate.Entity) {
			suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
		})

	suite.cachedJob.EXPECT().
		ClearWorkflow(suite.updateID).
		Return()

	suite.updateStore.EXPECT().
		GetUpdatesForJob(gomock.Any(), suite.jobID.GetValue()).
		Return(prevUpdateIDs, nil)

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), gomock.Any()).
		Return(updateInfo, nil)
	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	err := UpdateUntrack(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateUntrackTerminatedJobToBeRestarted tests untracking an update would untrack
// a terminated job that would be started again
func (suite *UpdateActionsTestSuite) TestUpdateUntrackTerminatedJobToBeRestarted() {
	jobRuntime := &pbjob.RuntimeInfo{
		UpdateID:  suite.updateID,
		State:     pbjob.JobState_FAILED,
		GoalState: pbjob.JobState_RUNNING,
	}
	updateInfo := &models.UpdateModel{
		State: pbupdate.State_INITIALIZED,
	}

	prevUpdateIDs := []*peloton.UpdateID{
		{Value: uuid.NewRandom().String()},
		{Value: uuid.NewRandom().String()},
	}

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.updateGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Do(func(updateEntity goalstate.Entity) {
			suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
		})

	suite.cachedJob.EXPECT().
		ClearWorkflow(suite.updateID).
		Return()

	suite.updateStore.EXPECT().
		GetUpdatesForJob(gomock.Any(), suite.jobID.GetValue()).
		Return(prevUpdateIDs, nil)

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), gomock.Any()).
		Return(updateInfo, nil)
	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	err := UpdateUntrack(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateUntrackTerminatedJob tests untracking an update would untrack
// a terminated job that would not be started again
func (suite *UpdateActionsTestSuite) TestUpdateUntrackTerminatedJob() {
	jobRuntime := &pbjob.RuntimeInfo{
		UpdateID:  suite.updateID,
		State:     pbjob.JobState_FAILED,
		GoalState: pbjob.JobState_KILLED,
	}
	updateInfo := &models.UpdateModel{
		State: pbupdate.State_INITIALIZED,
	}

	prevUpdateIDs := []*peloton.UpdateID{
		{Value: uuid.NewRandom().String()},
		{Value: uuid.NewRandom().String()},
	}

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.updateGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Do(func(updateEntity goalstate.Entity) {
			suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
		})

	suite.cachedJob.EXPECT().
		ClearWorkflow(suite.updateID).
		Return()

	suite.updateStore.EXPECT().
		GetUpdatesForJob(gomock.Any(), suite.jobID.GetValue()).
		Return(prevUpdateIDs, nil)

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), gomock.Any()).
		Return(updateInfo, nil)
	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	err := UpdateUntrack(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateUntrackNewUpdate tests untracking an update and job runtime
// has a new update to run
func (suite *UpdateActionsTestSuite) TestUpdateUntrackNewUpdate() {
	newID := &peloton.UpdateID{Value: uuid.NewRandom().String()}
	jobRuntime := &pbjob.RuntimeInfo{
		UpdateID: newID,
	}

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.updateGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Do(func(updateEntity goalstate.Entity) {
			suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
		})

	suite.cachedJob.EXPECT().
		ClearWorkflow(suite.updateID).
		Return()

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(entity goalstate.Entity, deadline time.Time) {
			suite.Equal(suite.jobID.GetValue(), entity.GetID())
			updateEnt := entity.(*updateEntity)
			suite.Equal(newID.GetValue(), updateEnt.id.GetValue())
		})

	err := UpdateUntrack(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateWriteProgressSuccess test the success case of UpdateWriteProgress
func (suite *UpdateActionsTestSuite) TestUpdateWriteProgressSuccess() {
	instancesCurrent := []uint32{0, 1, 2}
	instancesDone := []uint32{3, 4, 5}
	instancesFailed := []uint32{1}
	oldJobVersion := uint64(1)
	newJobVersion := uint64(2)
	updateConfig := &pbupdate.UpdateConfig{}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateEnt.id).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(instancesCurrent)

	suite.cachedJob.EXPECT().
		ID().
		Return(suite.jobID)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			JobVersion: newJobVersion,
		})

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return(instancesCurrent)

	for i, instanceID := range instancesCurrent {
		// the first instance has finished update,
		// the second is failed,
		// rest is still updating
		if i == 0 {
			runtime := &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				Healthy:              pbtask.HealthState_HEALTHY,
				ConfigVersion:        newJobVersion,
				DesiredConfigVersion: newJobVersion,
			}
			suite.taskStore.EXPECT().
				GetTaskRuntime(gomock.Any(), suite.jobID, instanceID).
				Return(runtime, nil)

			suite.cachedUpdate.EXPECT().
				IsInstanceComplete(newJobVersion, runtime).
				Return(true)
		} else if i == 1 {
			runtime := &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_FAILED,
				Healthy:              pbtask.HealthState_UNHEALTHY,
				ConfigVersion:        newJobVersion,
				DesiredConfigVersion: newJobVersion,
			}
			suite.taskStore.EXPECT().
				GetTaskRuntime(gomock.Any(), suite.jobID, instanceID).
				Return(runtime, nil)

			suite.cachedUpdate.EXPECT().
				IsInstanceComplete(newJobVersion, runtime).
				Return(false)

			suite.cachedUpdate.EXPECT().
				IsInstanceFailed(runtime, updateConfig.GetMaxInstanceAttempts()).
				Return(true)
		} else {
			runtime := &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				ConfigVersion:        oldJobVersion,
				DesiredConfigVersion: newJobVersion,
			}

			suite.taskStore.EXPECT().
				GetTaskRuntime(gomock.Any(), suite.jobID, instanceID).
				Return(runtime, nil)

			suite.cachedUpdate.EXPECT().
				IsInstanceComplete(newJobVersion, runtime).
				Return(false)

			suite.cachedUpdate.EXPECT().
				IsInstanceFailed(runtime, updateConfig.GetMaxInstanceAttempts()).
				Return(false)

			suite.cachedUpdate.EXPECT().
				IsInstanceInProgress(newJobVersion, runtime).
				Return(true)
		}
	}

	suite.cachedUpdate.EXPECT().
		GetUpdateConfig().
		Return(updateConfig)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State:     pbupdate.State_PAUSED,
			Instances: append(instancesDone, instancesFailed...),
		})

	suite.cachedUpdate.EXPECT().
		GetInstancesFailed().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetInstancesDone().
		Return(instancesDone)

	suite.cachedUpdate.EXPECT().
		GetInstancesRemoved().
		Return([]uint32{})

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State:     pbupdate.State_PAUSED,
			Instances: append(instancesDone, instancesFailed...),
		})

	suite.cachedJob.EXPECT().
		WriteWorkflowProgress(
			gomock.Any(),
			suite.updateID,
			pbupdate.State_PAUSED,
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(func(_ context.Context, _ *peloton.UpdateID, _ pbupdate.State,
			instancesDone []uint32,
			instancesFailed []uint32,
			instancesCurrent []uint32) {
			suite.Equal(instancesDone, []uint32{3, 4, 5, 0})
			suite.Equal(instancesCurrent, []uint32{2})
			suite.Equal(instancesFailed, []uint32{1})
		}).Return(nil)

	err := UpdateWriteProgress(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateWriteProgressMissingCache test the failure case of UpdateWriteProgress
// because of missing cache
func (suite *UpdateActionsTestSuite) TestUpdateWriteProgressMissingCache() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_INVALID,
		})

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		Recover(gomock.Any()).
		Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := UpdateWriteProgress(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateWriteProgressNoUpdatingInstance test the case that there is no instance
// in update
func (suite *UpdateActionsTestSuite) TestUpdateWriteProgressNoUpdatingInstance() {
	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateEnt.id).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedUpdate.EXPECT().
		GetInstancesCurrent().
		Return([]uint32{})

	err := UpdateWriteProgress(context.Background(), suite.updateEnt)
	suite.NoError(err)
}
