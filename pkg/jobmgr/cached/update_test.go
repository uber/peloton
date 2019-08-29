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

package cached

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	mesosv1 "github.com/uber/peloton/.gen/mesos/v1"
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbupdate "github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/private/models"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

type UpdateTestSuite struct {
	suite.Suite

	ctrl               *gomock.Controller
	jobStore           *storemocks.MockJobStore
	taskStore          *storemocks.MockTaskStore
	updateStore        *storemocks.MockUpdateStore
	jobUpdateEventsOps *objectmocks.MockJobUpdateEventsOps
	taskConfigV2Ops    *objectmocks.MockTaskConfigV2Ops
	jobID              *peloton.JobID
	updateID           *peloton.UpdateID
	update             *update
}

func TestUpdate(t *testing.T) {
	suite.Run(t, new(UpdateTestSuite))
}

func (suite *UpdateTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.updateStore = storemocks.NewMockUpdateStore(suite.ctrl)
	suite.jobUpdateEventsOps = objectmocks.NewMockJobUpdateEventsOps(suite.ctrl)
	suite.taskConfigV2Ops = objectmocks.NewMockTaskConfigV2Ops(suite.ctrl)
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.updateID = &peloton.UpdateID{Value: uuid.NewRandom().String()}
	suite.update = initializeUpdate(
		suite.jobStore,
		suite.taskStore,
		suite.updateStore,
		suite.jobUpdateEventsOps,
		suite.taskConfigV2Ops,
		suite.updateID,
	)
}

func (suite *UpdateTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// initializeUpdate initializes a job update in the suite
func initializeUpdate(
	jobStore *storemocks.MockJobStore,
	taskStore *storemocks.MockTaskStore,
	updateStore *storemocks.MockUpdateStore,
	jobUpdateEventsOps *objectmocks.MockJobUpdateEventsOps,
	taskConfigV2Ops *objectmocks.MockTaskConfigV2Ops,
	updateID *peloton.UpdateID,
) *update {
	jobFactory := &jobFactory{
		mtx:                NewMetrics(tally.NoopScope),
		jobStore:           jobStore,
		taskStore:          taskStore,
		updateStore:        updateStore,
		jobUpdateEventsOps: jobUpdateEventsOps,
		taskConfigV2Ops:    taskConfigV2Ops,
		jobs:               map[string]*job{},
		running:            true,
	}

	u := newUpdate(updateID, jobFactory)
	return u
}

// TestUpdateFetchID tests fetching update and job ID.
func (suite *UpdateTestSuite) TestUpdateFetchID() {
	suite.Equal(suite.updateID, suite.update.ID())
}

// TestModifyValid tests a valid modification to the update.
func (suite *UpdateTestSuite) TestModifyValid() {
	instancesAdded := []uint32{3, 4}
	instancesUpdated := []uint32{0, 1, 2}
	instancesRemoved := []uint32{5}

	suite.update.workflowType = models.WorkflowType_UPDATE
	suite.update.state = pbupdate.State_ROLLING_FORWARD

	suite.updateStore.EXPECT().
		ModifyUpdate(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateModel *models.UpdateModel) {
			suite.Equal(suite.updateID, updateModel.UpdateID)
			suite.Equal(instancesAdded, updateModel.InstancesAdded)
			suite.Equal(instancesUpdated, updateModel.InstancesUpdated)
			suite.Equal(instancesRemoved, updateModel.InstancesRemoved)
		}).Return(nil)

	suite.updateStore.EXPECT().
		AddWorkflowEvent(
			gomock.Any(),
			suite.updateID,
			gomock.Any(),
			suite.update.workflowType,
			suite.update.state).
		Return(nil).
		Times(6)

	err := suite.update.Modify(
		context.Background(),
		instancesAdded,
		instancesUpdated,
		instancesRemoved,
	)
	suite.NoError(err)
	suite.Equal(instancesAdded, suite.update.instancesAdded)
	suite.Equal(instancesUpdated, suite.update.instancesUpdated)
	suite.Equal(instancesRemoved, suite.update.instancesRemoved)
	suite.Equal([]uint32{0, 1, 2, 3, 4, 5}, suite.update.instancesTotal)
}

// TestModifyDBError tests getting a DB error while modifying an update.
func (suite *UpdateTestSuite) TestModifyDBError() {
	instancesAdded := []uint32{3, 4}
	instancesUpdated := []uint32{0, 1, 2}
	instancesRemoved := []uint32{5}

	suite.update.state = pbupdate.State_ROLLING_FORWARD

	suite.updateStore.EXPECT().
		AddWorkflowEvent(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD).
		Return(nil).Times(6)

	suite.updateStore.EXPECT().
		ModifyUpdate(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := suite.update.Modify(
		context.Background(),
		instancesAdded,
		instancesUpdated,
		instancesRemoved,
	)
	suite.Error(err)
}

// TestModifyRecoverFail tests the failure case of
// modify an update due to recover failure
func (suite *UpdateTestSuite) TestModifyRecoverFail() {
	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.update.Modify(context.Background(), nil, nil, nil))
}

// TestValidWriteProgress tests successfully writing the status
// of an update into the DB.
func (suite *UpdateTestSuite) TestValidWriteProgress() {
	state := pbupdate.State_ROLLING_FORWARD
	instancesDone := []uint32{0, 1, 2, 3}
	instancesCurrent := []uint32{4, 5}
	instanceFailed := []uint32{6, 7}

	suite.update.instancesCurrent = []uint32{3, 6}

	suite.update.workflowType = models.WorkflowType_UPDATE
	suite.update.state = pbupdate.State_ROLLING_FORWARD

	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateModel *models.UpdateModel) {
			suite.Equal(suite.updateID, updateModel.UpdateID)
			suite.Equal(state, updateModel.State)
			suite.Equal(uint32(len(instancesDone)), updateModel.InstancesDone)
			suite.Equal(instancesCurrent, updateModel.InstancesCurrent)
			suite.Equal(uint32(len(instanceFailed)), updateModel.InstancesFailed)
		}).
		Return(nil)
	suite.updateStore.EXPECT().
		AddWorkflowEvent(
			gomock.Any(),
			suite.updateID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).Return(nil).Times(4)

	err := suite.update.WriteProgress(
		context.Background(),
		state,
		instancesDone,
		instanceFailed,
		instancesCurrent,
	)

	suite.NoError(err)
	suite.Equal(state, suite.update.state)
	suite.Equal(instancesCurrent, suite.update.instancesCurrent)
	suite.Equal(instancesDone, suite.update.instancesDone)
	suite.Equal(instanceFailed, suite.update.instancesFailed)
}

// TestWriteDuplicateProgress tests writing the same progress in
// DB would not trigger a DB write
func (suite *UpdateTestSuite) TestWriteDuplicateProgress() {
	state := pbupdate.State_ROLLING_FORWARD
	instancesDone := []uint32{0, 1, 2, 3}
	instancesCurrent := []uint32{4, 5}
	instanceFailed := []uint32{6, 7}

	suite.update.instancesCurrent = []uint32{3, 6}

	suite.update.workflowType = models.WorkflowType_UPDATE
	suite.update.state = pbupdate.State_ROLLING_FORWARD
	suite.update.instancesCurrent = instancesCurrent
	suite.update.instancesDone = instancesDone
	suite.update.instancesFailed = instanceFailed

	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Do(func(
			ctx context.Context,
			updateInfo *models.UpdateModel) {
			suite.NotEmpty(updateInfo.GetUpdateTime())
			suite.Equal(updateInfo.GetState(), pbupdate.State_INVALID)
		}).Return(nil)

	err := suite.update.WriteProgress(
		context.Background(),
		state,
		instancesDone,
		instanceFailed,
		instancesCurrent,
	)

	suite.NoError(err)
	suite.Equal(state, suite.update.state)
	suite.Equal(instancesCurrent, suite.update.instancesCurrent)
	suite.Equal(instancesDone, suite.update.instancesDone)
	suite.Equal(instanceFailed, suite.update.instancesFailed)
}

// TestWriteProgressAbortedUpdate tests WriteProgress invalidates
// progress update after it reaches terminated state
func (suite *UpdateTestSuite) TestWriteProgressAbortedUpdate() {
	suite.update.state = pbupdate.State_ABORTED
	state := pbupdate.State_ROLLING_FORWARD
	instancesDone := []uint32{0, 1, 2, 3}
	instancesCurrent := []uint32{4, 5}
	instanceFailed := []uint32{}

	err := suite.update.WriteProgress(
		context.Background(),
		state,
		instancesDone,
		instancesCurrent,
		instanceFailed,
	)

	suite.NoError(err)
	suite.Equal(suite.update.state, pbupdate.State_ABORTED)
}

// TestWriteJobUpdateEventError tests failure case to write job
// update event
func (suite *UpdateTestSuite) TestWriteJobUpdateEventError() {
	state := pbupdate.State_ROLLING_FORWARD
	instancesDone := []uint32{0, 1, 2, 3}
	instancesCurrent := []uint32{4, 5}
	instanceFailed := []uint32{}

	suite.update.state = pbupdate.State_INITIALIZED

	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD).
		Return(fmt.Errorf("fake db error"))

	err := suite.update.WriteProgress(
		context.Background(),
		state,
		instancesDone,
		instancesCurrent,
		instanceFailed,
	)
	suite.EqualError(err, "fake db error")
}

// TestValidWriteProgress tests failing to persist the status
// of an update into the DB.
func (suite *UpdateTestSuite) TestWriteProgressDBError() {
	state := pbupdate.State_ROLLING_FORWARD
	instancesDone := []uint32{0, 1, 2, 3}
	instancesCurrent := []uint32{4, 5}
	instanceFailed := []uint32{}

	suite.update.state = pbupdate.State_ROLLING_FORWARD

	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := suite.update.WriteProgress(
		context.Background(),
		state,
		instancesDone,
		instancesCurrent,
		instanceFailed,
	)
	suite.EqualError(err, "fake db error")
}

// TestWriteProgressRecoverFail tests the failure case of
// write progress due to recover failure
func (suite *UpdateTestSuite) TestWriteProgressRecoverFail() {
	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.update.WriteProgress(context.Background(), pbupdate.State_ROLLING_FORWARD, nil, nil, nil))
}

// TestConsecutiveWriteProgressPrevState tests after consecutive call to
// WriteProgress, prevState is correcct
func (suite *UpdateTestSuite) TestConsecutiveWriteProgressPrevState() {
	suite.update.instancesAdded = []uint32{0, 1, 2, 3, 4, 5}
	suite.update.workflowType = models.WorkflowType_UPDATE

	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD).
		Return(nil)

	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	suite.update.state = pbupdate.State_INITIALIZED
	suite.NoError(suite.update.WriteProgress(
		context.Background(),
		pbupdate.State_ROLLING_FORWARD,
		[]uint32{},
		[]uint32{},
		[]uint32{},
	))
	suite.Equal(suite.update.prevState, pbupdate.State_INITIALIZED)
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_FORWARD)

	instancesDone := []uint32{}
	instancesCurrent := []uint32{0, 1}
	instanceFailed := []uint32{}

	suite.updateStore.EXPECT().
		AddWorkflowEvent(
			gomock.Any(),
			suite.updateID,
			uint32(0),
			models.WorkflowType_UPDATE,
			pbupdate.State_ROLLING_FORWARD).
		Return(nil)

	suite.updateStore.EXPECT().
		AddWorkflowEvent(
			gomock.Any(),
			suite.updateID,
			uint32(1),
			models.WorkflowType_UPDATE,
			pbupdate.State_ROLLING_FORWARD).
		Return(nil)

	// prev state should still be INITIALIZED after we write
	// State_ROLLING_FORWARD twice
	// state is progressing
	// Job Update Event is only written once, as ROLLING_FORWARD
	suite.NoError(suite.update.WriteProgress(
		context.Background(),
		pbupdate.State_ROLLING_FORWARD,
		instancesDone,
		instanceFailed,
		instancesCurrent,
	))
	suite.Equal(suite.update.prevState, pbupdate.State_INITIALIZED)
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_FORWARD)

	instancesDone = []uint32{0, 1}
	instancesCurrent = []uint32{2, 3}
	instanceFailed = []uint32{}

	suite.updateStore.EXPECT().
		AddWorkflowEvent(
			gomock.Any(),
			suite.updateID,
			uint32(0),
			models.WorkflowType_UPDATE,
			pbupdate.State_SUCCEEDED).
		Return(nil)

	suite.updateStore.EXPECT().
		AddWorkflowEvent(
			gomock.Any(),
			suite.updateID,
			uint32(1),
			models.WorkflowType_UPDATE,
			pbupdate.State_SUCCEEDED).
		Return(nil)

	suite.updateStore.EXPECT().
		AddWorkflowEvent(
			gomock.Any(),
			suite.updateID,
			uint32(2),
			models.WorkflowType_UPDATE,
			pbupdate.State_ROLLING_FORWARD).
		Return(nil)

	suite.updateStore.EXPECT().
		AddWorkflowEvent(
			gomock.Any(),
			suite.updateID,
			uint32(3),
			models.WorkflowType_UPDATE,
			pbupdate.State_ROLLING_FORWARD).
		Return(nil)

	suite.NoError(suite.update.WriteProgress(
		context.Background(),
		pbupdate.State_ROLLING_FORWARD,
		instancesDone,
		instanceFailed,
		instancesCurrent,
	))
	suite.Equal(suite.update.prevState, pbupdate.State_INITIALIZED)
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_FORWARD)

	// Write rolling backward for instances that are not in terminal state
	instancesDone = []uint32{0, 1}
	instancesCurrent = []uint32{2, 3}
	instanceFailed = []uint32{}

	suite.updateStore.EXPECT().
		AddWorkflowEvent(
			gomock.Any(),
			suite.updateID,
			uint32(2),
			models.WorkflowType_UPDATE,
			pbupdate.State_ROLLING_BACKWARD).
		Return(nil)

	suite.updateStore.EXPECT().
		AddWorkflowEvent(
			gomock.Any(),
			suite.updateID,
			uint32(3),
			models.WorkflowType_UPDATE,
			pbupdate.State_ROLLING_BACKWARD).
		Return(nil)

	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_ROLLING_BACKWARD).
		Return(nil)

	suite.NoError(suite.update.WriteProgress(
		context.Background(),
		pbupdate.State_ROLLING_BACKWARD,
		instancesDone,
		instanceFailed,
		instancesCurrent,
	))
	suite.Equal(suite.update.prevState, pbupdate.State_ROLLING_FORWARD)
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_BACKWARD)
}

// TestPauseSuccess tests successfully pause an update
func (suite *UpdateTestSuite) TestPauseSuccess() {
	suite.update.workflowType = models.WorkflowType_UPDATE
	suite.update.state = pbupdate.State_ROLLING_FORWARD
	opaque := "test"

	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			suite.updateID,
			gomock.Any(),
			pbupdate.State_PAUSED).
		Return(nil)

	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateModel *models.UpdateModel) {
			suite.Equal(suite.updateID, updateModel.UpdateID)
			suite.Equal(pbupdate.State_PAUSED, updateModel.State)
			suite.Equal(opaque, updateModel.GetOpaqueData().GetData())
		}).
		Return(nil)

	suite.NoError(suite.update.Pause(
		context.Background(),
		&peloton.OpaqueData{Data: opaque}),
	)
}

// TestPauseRecoverFail tests the failure case of
// pause an update due to recover failure
func (suite *UpdateTestSuite) TestPauseRecoverFail() {
	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.update.Pause(context.Background(), nil))
}

// TestPausePausedUpdate tests pause an update already paused
func (suite *UpdateTestSuite) TestPausePausedUpdate() {
	suite.update.state = pbupdate.State_PAUSED
	suite.NoError(suite.update.Pause(context.Background(), nil))
}

// TestPauseResumeRollingForwardUpdate tests pause and resume a rolling
// forward update
func (suite *UpdateTestSuite) TestPauseResumeRollingForwardUpdate() {
	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_PAUSED).
		Return(nil)
	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	suite.update.state = pbupdate.State_ROLLING_FORWARD
	suite.NoError(suite.update.Pause(context.Background(), nil))
	suite.Equal(pbupdate.State_PAUSED, suite.update.state)

	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_ROLLING_FORWARD).
		Return(nil)
	suite.NoError(suite.update.Resume(context.Background(), nil))
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_FORWARD)
}

// TestPauseResumeRollingBackwardUpdate tests pause and resume a rolling
// backward update
func (suite *UpdateTestSuite) TestPauseResumeRollingBackwardUpdate() {
	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_PAUSED).
		Return(nil)
	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	suite.update.state = pbupdate.State_ROLLING_BACKWARD
	suite.NoError(suite.update.Pause(context.Background(), nil))
	suite.Equal(pbupdate.State_PAUSED, suite.update.state)

	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_ROLLING_BACKWARD).
		Return(nil)
	suite.NoError(suite.update.Resume(context.Background(), nil))
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_BACKWARD)
}

// TestPauseResumeInitializedUpdate tests pause and resume an
// initialized update
func (suite *UpdateTestSuite) TestPauseResumeInitializedUpdate() {
	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_PAUSED).
		Return(nil)

	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	suite.update.state = pbupdate.State_INITIALIZED
	suite.NoError(suite.update.Pause(context.Background(), nil))
	suite.Equal(pbupdate.State_PAUSED, suite.update.state)

	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_INITIALIZED).
		Return(nil)

	suite.NoError(suite.update.Resume(context.Background(), nil))
	suite.Equal(suite.update.state, pbupdate.State_INITIALIZED)
}

// TestResumeRecoverFail tests the failure case of
// resume an update due to recover failure
func (suite *UpdateTestSuite) TestResumeRecoverFail() {
	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.update.Resume(context.Background(), nil))
}

// TestCancelValid tests successfully canceling a job update
func (suite *UpdateTestSuite) TestCancelValid() {
	instancesDone := []uint32{1, 2, 3, 4, 5}
	instancesCurrent := []uint32{6, 7}
	suite.update.instancesAdded = []uint32{1, 2, 3, 4, 5, 6, 7}
	suite.update.workflowType = models.WorkflowType_UPDATE
	suite.update.state = pbupdate.State_INITIALIZED
	suite.update.instancesDone = instancesDone
	suite.update.instancesCurrent = instancesCurrent
	opaque := "test"
	suite.update.instancesFailed = []uint32{}
	suite.update.instancesUpdated = []uint32{}
	suite.update.instancesRemoved = []uint32{}

	for _, i := range instancesCurrent {
		suite.updateStore.EXPECT().
			AddWorkflowEvent(
				gomock.Any(),
				suite.updateID,
				i,
				suite.update.workflowType,
				pbupdate.State_ABORTED)
	}

	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_ABORTED).
		Return(nil)

	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateModel *models.UpdateModel) {
			suite.Equal(suite.updateID, updateModel.UpdateID)
			suite.Equal(pbupdate.State_ABORTED, updateModel.State)
			suite.NotEmpty(updateModel.GetCompletionTime())
			suite.Equal(uint32(len(instancesDone)), updateModel.InstancesDone)
			suite.Equal(instancesCurrent, updateModel.InstancesCurrent)
			suite.Equal(opaque, updateModel.GetOpaqueData().GetData())
		}).
		Return(nil)

	err := suite.update.Cancel(
		context.Background(),
		&peloton.OpaqueData{Data: opaque},
	)
	suite.NoError(err)
	suite.Equal(pbupdate.State_ABORTED, suite.update.state)
}

// TestCancelDBError tests receiving a DB eror when canceling a job update
func (suite *UpdateTestSuite) TestCancelDBError() {
	instancesDone := []uint32{1, 2, 3, 4, 5}
	instancesCurrent := []uint32{6, 7}
	suite.update.state = pbupdate.State_INITIALIZED
	suite.update.instancesDone = instancesDone
	suite.update.instancesCurrent = instancesCurrent

	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_ABORTED).
		Return(nil)
	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := suite.update.Cancel(context.Background(), nil)
	suite.EqualError(err, "fake db error")
	suite.Equal(pbupdate.State_INVALID, suite.update.state)
}

// TestCancelRecoverFail tests the failure case of
// cancel an update due to recover failure
func (suite *UpdateTestSuite) TestCancelRecoverFail() {
	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.update.Cancel(context.Background(), nil))
}

// TestCancelTerminatedUpdate tests canceling a terminated update
func (suite *UpdateTestSuite) TestCancelTerminatedUpdate() {
	suite.update.state = pbupdate.State_SUCCEEDED

	err := suite.update.Cancel(context.Background(), nil)
	suite.NoError(err)
}

// TestUpdateGetState tests getting state of a job update
func (suite *UpdateTestSuite) TestUpdateGetState() {
	suite.update.instancesDone = []uint32{1, 2, 3, 4, 5}
	suite.update.instancesFailed = []uint32{6, 7}
	suite.update.state = pbupdate.State_ROLLING_FORWARD

	state := suite.update.GetState()
	suite.Equal(suite.update.state, state.State)
	suite.Equal(state.Instances,
		append(suite.update.instancesDone, suite.update.instancesFailed...))
}

// TestUpdateGetState tests getting update config
func (suite *UpdateTestSuite) TestUpdateGetConfig() {
	batchSize := uint32(5)
	suite.update.updateConfig = &pbupdate.UpdateConfig{
		BatchSize: batchSize,
	}

	suite.Equal(suite.update.GetUpdateConfig().GetBatchSize(), batchSize)
}

// TestUpdateGetGoalState tests getting goal state of a job update
func (suite *UpdateTestSuite) TestUpdateGetGoalState() {
	suite.update.instancesTotal = []uint32{1, 2, 3, 4, 5}

	state := suite.update.GetGoalState()
	suite.True(reflect.DeepEqual(state.Instances, suite.update.instancesTotal))
}

// TestUpdateGetInstancesAdded tests getting instances added in a job update
func (suite *UpdateTestSuite) TestUpdateGetInstancesAdded() {
	suite.update.instancesAdded = []uint32{1, 2, 3, 4, 5}

	instances := suite.update.GetInstancesAdded()
	suite.True(reflect.DeepEqual(instances, suite.update.instancesAdded))
}

// TestUpdateGetInstancesDone tests getting instances done in a job update
func (suite *UpdateTestSuite) TestUpdateGetInstancesDone() {
	suite.update.instancesDone = []uint32{1, 2, 3, 4, 5}

	instances := suite.update.GetInstancesDone()
	suite.True(reflect.DeepEqual(instances, suite.update.instancesDone))
}

// TestUpdateGetInstancesFailed tests getting instances failed in a job update
func (suite *UpdateTestSuite) TestUpdateGetInstancesFailed() {
	suite.update.instancesFailed = []uint32{1, 2, 3, 4, 5}

	instances := suite.update.GetInstancesFailed()
	suite.True(reflect.DeepEqual(instances, suite.update.instancesFailed))
}

// TestTestUpdateGetInstancesUpdated tests getting
// udpated instances in a job update
func (suite *UpdateTestSuite) TestUpdateGetInstancesUpdated() {
	suite.update.instancesUpdated = []uint32{1, 2, 3, 4, 5}

	instances := suite.update.GetInstancesUpdated()
	suite.True(reflect.DeepEqual(instances, suite.update.instancesUpdated))
}

// TestUpdateGetInstancesRemoved tests getting instances removed in a job update
func (suite *UpdateTestSuite) TestUpdateGetInstancesRemoved() {
	suite.update.instancesRemoved = []uint32{1, 2, 3, 4, 5}

	instances := suite.update.GetInstancesRemoved()
	suite.True(reflect.DeepEqual(instances, suite.update.instancesRemoved))
}

// TestUpdateGetInstancesCurrent tests getting current
// instances in a job update.
func (suite *UpdateTestSuite) TestUpdateGetInstancesCurrent() {
	suite.update.instancesCurrent = []uint32{1, 2, 3, 4, 5}

	instances := suite.update.GetInstancesCurrent()
	suite.True(reflect.DeepEqual(instances, suite.update.instancesCurrent))
}

func (suite *UpdateTestSuite) TestUpdateRecover_RollingForward() {
	instancesTotal := []uint32{0, 1, 2, 3, 4}
	instancesDone := []uint32{0, 1}
	instancesCurrent := []uint32{2, 3, 4}
	instanceCount := uint32(len(instancesTotal))
	preJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 0,
		},
		InstanceCount: instanceCount,
	}
	newJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 1,
		},
		InstanceCount: instanceCount,
		Labels:        []*peloton.Label{{Key: "test-key", Value: "test-value"}},
	}

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(&models.UpdateModel{
			JobID:                suite.jobID,
			InstancesTotal:       uint32(len(instancesTotal)),
			InstancesUpdated:     instancesTotal,
			InstancesDone:        uint32(len(instancesDone)),
			InstancesCurrent:     instancesCurrent,
			PrevJobConfigVersion: preJobConfig.GetChangeLog().GetVersion(),
			JobConfigVersion:     newJobConfig.GetChangeLog().GetVersion(),
			State:                pbupdate.State_ROLLING_FORWARD,
			Type:                 models.WorkflowType_UPDATE,
		}, nil)

	for i := uint32(0); i < instanceCount; i++ {
		taskRuntime := &pbtask.RuntimeInfo{
			State:   pbtask.TaskState_RUNNING,
			Healthy: pbtask.HealthState_DISABLED,
		}
		if contains(i, instancesCurrent) {
			taskRuntime.DesiredConfigVersion = newJobConfig.GetChangeLog().GetVersion()
			taskRuntime.ConfigVersion = preJobConfig.GetChangeLog().GetVersion()
		} else if contains(i, instancesDone) {
			taskRuntime.DesiredConfigVersion = newJobConfig.GetChangeLog().GetVersion()
			taskRuntime.ConfigVersion = newJobConfig.GetChangeLog().GetVersion()
		} else {
			taskRuntime.DesiredConfigVersion = preJobConfig.GetChangeLog().GetVersion()
			taskRuntime.ConfigVersion = preJobConfig.GetChangeLog().GetVersion()
		}

		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).
			Return(taskRuntime, nil)
	}

	err := suite.update.Recover(context.Background())
	suite.NoError(err)

	suite.Equal(suite.update.instancesDone, instancesDone)
	suite.Equal(suite.update.instancesCurrent, instancesCurrent)
	suite.Equal(suite.update.instancesTotal, instancesTotal)
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_FORWARD)
	suite.Equal(suite.update.GetWorkflowType(), models.WorkflowType_UPDATE)
}

func (suite *UpdateTestSuite) TestUpdateRecover_Succeeded() {
	var instancesCurrent []uint32
	instancesTotal := []uint32{0, 1, 2, 3, 4}
	instancesDone := instancesTotal

	instanceCount := uint32(len(instancesTotal))
	preJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 0,
		},
		InstanceCount: instanceCount,
	}
	newJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 1,
		},
		InstanceCount: instanceCount,
		Labels:        []*peloton.Label{{Key: "test-key", Value: "test-value"}},
	}

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(&models.UpdateModel{
			JobID:                suite.jobID,
			InstancesTotal:       uint32(len(instancesTotal)),
			InstancesDone:        uint32(len(instancesDone)),
			InstancesCurrent:     instancesCurrent,
			PrevJobConfigVersion: preJobConfig.GetChangeLog().GetVersion(),
			JobConfigVersion:     newJobConfig.GetChangeLog().GetVersion(),
			State:                pbupdate.State_SUCCEEDED,
			Type:                 models.WorkflowType_UPDATE,
		}, nil)

	err := suite.update.Recover(context.Background())
	suite.NoError(err)

	suite.Empty(suite.update.instancesDone)
	suite.Equal(suite.update.instancesCurrent, instancesCurrent)
	suite.Empty(suite.update.instancesTotal)
	suite.Equal(suite.update.state, pbupdate.State_SUCCEEDED)
	suite.Equal(suite.update.GetWorkflowType(), models.WorkflowType_UPDATE)
}

func (suite *UpdateTestSuite) TestUpdateRecover_Initialized() {
	instancesTotal := []uint32{0, 1, 2, 3, 4}
	instancesCurrent := []uint32{0, 1, 2, 3, 4}
	var instancesDone []uint32
	instanceCount := uint32(len(instancesTotal))
	preJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 0,
		},
		InstanceCount: instanceCount,
	}
	newJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 1,
		},
		InstanceCount: instanceCount,
		Labels:        []*peloton.Label{{Key: "test-key", Value: "test-value"}},
	}

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(&models.UpdateModel{
			JobID:                suite.jobID,
			InstancesTotal:       uint32(len(instancesTotal)),
			InstancesUpdated:     instancesTotal,
			InstancesDone:        uint32(len(instancesDone)),
			InstancesCurrent:     instancesCurrent,
			PrevJobConfigVersion: preJobConfig.GetChangeLog().GetVersion(),
			JobConfigVersion:     newJobConfig.GetChangeLog().GetVersion(),
			State:                pbupdate.State_INITIALIZED,
			Type:                 models.WorkflowType_UPDATE,
		}, nil)

	for i := uint32(0); i < instanceCount; i++ {
		taskRuntime := &pbtask.RuntimeInfo{
			State: pbtask.TaskState_RUNNING,
		}
		if contains(i, instancesCurrent) {
			taskRuntime.DesiredConfigVersion = newJobConfig.GetChangeLog().GetVersion()
			taskRuntime.ConfigVersion = preJobConfig.GetChangeLog().GetVersion()
		} else if contains(i, instancesDone) {
			taskRuntime.DesiredConfigVersion = newJobConfig.GetChangeLog().GetVersion()
			taskRuntime.ConfigVersion = newJobConfig.GetChangeLog().GetVersion()
		} else {
			taskRuntime.DesiredConfigVersion = preJobConfig.GetChangeLog().GetVersion()
			taskRuntime.ConfigVersion = preJobConfig.GetChangeLog().GetVersion()
		}

		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).
			Return(taskRuntime, nil)
	}

	err := suite.update.Recover(context.Background())
	suite.NoError(err)

	suite.Equal(suite.update.instancesDone, instancesDone)
	suite.Equal(suite.update.instancesCurrent, instancesCurrent)
	suite.Equal(suite.update.instancesTotal, instancesTotal)
	suite.Equal(suite.update.state, pbupdate.State_INITIALIZED)
	suite.Equal(suite.update.GetWorkflowType(), models.WorkflowType_UPDATE)
}

func (suite *UpdateTestSuite) TestUpdateRecover_UpdateStoreErr() {
	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(nil, yarpcerrors.UnavailableErrorf("test error"))

	err := suite.update.Recover(context.Background())
	suite.Error(err)

	suite.Empty(suite.update.instancesDone)
	suite.Empty(suite.update.instancesCurrent)
	suite.Empty(suite.update.instancesTotal)
	suite.Equal(suite.update.state, pbupdate.State_INVALID)
}

func (suite *UpdateTestSuite) TestUpdateRecoverGetRuntimeFailure() {
	instancesTotal := []uint32{0, 1, 2, 3, 4}
	instancesCurrent := []uint32{0, 1, 2, 3, 4}
	var instancesDone []uint32
	instanceCount := uint32(len(instancesTotal))
	preJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 0,
		},
		InstanceCount: instanceCount,
	}
	newJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 1,
		},
		InstanceCount: instanceCount,
		Labels:        []*peloton.Label{{Key: "test-key", Value: "test-value"}},
	}

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(&models.UpdateModel{
			JobID:                suite.jobID,
			InstancesTotal:       uint32(len(instancesTotal)),
			InstancesUpdated:     instancesTotal,
			InstancesDone:        uint32(len(instancesDone)),
			InstancesCurrent:     instancesCurrent,
			PrevJobConfigVersion: preJobConfig.GetChangeLog().GetVersion(),
			JobConfigVersion:     newJobConfig.GetChangeLog().GetVersion(),
			State:                pbupdate.State_INITIALIZED,
			Type:                 models.WorkflowType_UPDATE,
		}, nil)

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, uint32(0)).
		Return(nil, fmt.Errorf("test db error"))

	err := suite.update.Recover(context.Background())
	suite.Error(err)

	suite.Empty(suite.update.instancesDone)
	suite.Empty(suite.update.instancesCurrent)
	suite.Empty(suite.update.instancesTotal)
	suite.Equal(suite.update.state, pbupdate.State_INVALID)
}

func (suite *UpdateTestSuite) TestUpdateRecoverRemoveInstances() {
	instancesTotal := []uint32{0, 1, 2, 3, 4}
	instancesCurrent := []uint32{0, 1, 2, 3, 4}
	var instancesDone []uint32
	instanceCount := uint32(len(instancesTotal))
	preJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 0,
		},
		InstanceCount: instanceCount,
	}
	newJobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 1,
		},
		InstanceCount: instanceCount,
		Labels:        []*peloton.Label{{Key: "test-key", Value: "test-value"}},
	}

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(&models.UpdateModel{
			JobID:                suite.jobID,
			InstancesTotal:       uint32(len(instancesTotal)),
			InstancesUpdated:     []uint32{},
			InstancesDone:        uint32(len(instancesDone)),
			InstancesCurrent:     instancesCurrent,
			InstancesRemoved:     instancesCurrent,
			PrevJobConfigVersion: preJobConfig.GetChangeLog().GetVersion(),
			JobConfigVersion:     newJobConfig.GetChangeLog().GetVersion(),
			State:                pbupdate.State_INITIALIZED,
			Type:                 models.WorkflowType_UPDATE,
		}, nil)

	for i := uint32(0); i < instanceCount; i++ {
		taskRuntime := &pbtask.RuntimeInfo{
			State:   pbtask.TaskState_RUNNING,
			Healthy: pbtask.HealthState_DISABLED,
		}

		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).
			Return(taskRuntime, yarpcerrors.NotFoundErrorf("not-found"))
	}

	err := suite.update.Recover(context.Background())
	suite.NoError(err)

	suite.Equal(suite.update.instancesDone, instancesCurrent)
}

// Test function IsTaskInUpdateProgress
func (suite *UpdateTestSuite) TestIsTaskInUpdateProgress() {
	suite.update.instancesCurrent = []uint32{1, 2}
	suite.True(suite.update.IsTaskInUpdateProgress(uint32(1)))
	suite.False(suite.update.IsTaskInUpdateProgress(uint32(0)))
}

// Test function IsTaskInFailed
func (suite *UpdateTestSuite) TestIsTaskInFailed() {
	suite.update.instancesFailed = []uint32{1, 2}
	suite.True(suite.update.IsTaskInFailed(uint32(1)))
	suite.False(suite.update.IsTaskInFailed(uint32(0)))
}

// TestUpdateRollbackSuccess tests the success case of rolling back update
func (suite *UpdateTestSuite) TestUpdateRollbackSuccess() {
	jobVersion := uint64(1)

	suite.update.state = pbupdate.State_ROLLING_FORWARD
	suite.update.jobVersion = jobVersion
	suite.update.jobID = suite.jobID

	currentConfig := &pbjob.JobConfig{}
	targetConfig := &pbjob.JobConfig{}

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(nil, nil)

	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_ROLLING_BACKWARD).
		Return(nil)

	suite.updateStore.EXPECT().
		ModifyUpdate(gomock.Any(), gomock.Any()).
		Return(nil)

	suite.NoError(suite.update.Rollback(context.Background(), currentConfig, targetConfig))
}

// TestUpdateRollbackModifyUpdateFailure tests the failure case of
// rolling back update due to fail to modify update in db
func (suite *UpdateTestSuite) TestUpdateRollbackModifyUpdateFailure() {
	jobVersion := uint64(1)

	suite.update.state = pbupdate.State_ROLLING_FORWARD
	suite.update.jobVersion = jobVersion
	suite.update.jobID = suite.jobID

	currentConfig := &pbjob.JobConfig{}
	targetConfig := &pbjob.JobConfig{}

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(nil, nil)

	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_ROLLING_BACKWARD).
		Return(nil)

	suite.updateStore.EXPECT().
		ModifyUpdate(gomock.Any(), gomock.Any()).
		Return(yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.update.Rollback(context.Background(), currentConfig, targetConfig))
}

// TestUpdateRollbackRollingBackwardUpdate tests the case of rolling back
// an update that is already rolling back
func (suite *UpdateTestSuite) TestUpdateRollbackRollingBackwardUpdate() {
	jobVersion := uint64(1)

	suite.update.state = pbupdate.State_ROLLING_BACKWARD
	suite.update.jobVersion = jobVersion
	suite.update.jobID = suite.jobID

	currentConfig := &pbjob.JobConfig{}
	targetConfig := &pbjob.JobConfig{}

	suite.NoError(suite.update.Rollback(context.Background(), currentConfig, targetConfig))
}

// TestUpdateRollbackRecoverFail tests the failure case of
// rollback an update due to recover failure
func (suite *UpdateTestSuite) TestUpdateRollbackRecoverFail() {
	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.update.Rollback(context.Background(), nil, nil))
}

// TestGetInstancesToProcessForUpdateWithLabelAddAndUpdate tests
// GetInstancesToProcessForUpdate, when config have labels added and updated
func (suite *UpdateTestSuite) TestGetInstancesToProcessForUpdateWithLabelAddAndUpdate() {
	instanceCount := uint32(10)
	prevJobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		Labels: []*peloton.Label{
			{Key: "key1", Value: "val1"},
			{Key: "key2", Value: "val2"},
		},
	}
	jobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		Labels: []*peloton.Label{
			{Key: "key1", Value: "val1"},
			{Key: "key2", Value: "val2-1"},
			{Key: "key3", Value: "val3"},
		},
	}

	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State:                pbtask.TaskState_RUNNING,
			ConfigVersion:        3,
			DesiredConfigVersion: 3,
		}
		taskRuntimes[i] = runtime
	}

	testJob := &job{
		id: suite.jobID,
	}

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(taskRuntimes, nil)

	for i := uint32(0); i < instanceCount; i++ {
		suite.taskConfigV2Ops.EXPECT().
			GetTaskConfig(gomock.Any(), suite.jobID, i, uint64(3)).
			Return(&pbtask.TaskConfig{}, &models.ConfigAddOn{}, nil)
	}

	instancesAdded, instancesUpdated, instancesRemoved, instancesUnchanged, err :=
		GetInstancesToProcessForUpdate(
			context.Background(),
			testJob.ID(),
			prevJobConfig,
			jobConfig,
			suite.taskStore,
			suite.taskConfigV2Ops,
		)

	suite.NoError(err)
	suite.Len(instancesUpdated, int(instanceCount))
	suite.Empty(instancesRemoved)
	suite.Empty(instancesAdded)
	suite.Empty(instancesUnchanged)
}

// TestGetInstancesToProcessForUpdateWithLabelUpdated tests
// GetInstancesToProcessForUpdate, when config have labels updated
func (suite *UpdateTestSuite) TestGetInstancesToProcessForUpdateWithLabelUpdated() {
	instanceCount := uint32(10)
	prevJobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		Labels: []*peloton.Label{
			{Key: "key1", Value: "val1"},
			{Key: "key2", Value: "val2"},
		},
	}
	jobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		Labels: []*peloton.Label{
			{Key: "key1", Value: "val1"},
			{Key: "key2", Value: "val2-1"},
		},
	}

	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State:                pbtask.TaskState_RUNNING,
			ConfigVersion:        3,
			DesiredConfigVersion: 3,
		}
		taskRuntimes[i] = runtime
	}

	testJob := &job{
		id: suite.jobID,
	}

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(taskRuntimes, nil)

	for i := uint32(0); i < instanceCount; i++ {
		suite.taskConfigV2Ops.EXPECT().
			GetTaskConfig(gomock.Any(), suite.jobID, i, uint64(3)).
			Return(&pbtask.TaskConfig{}, &models.ConfigAddOn{}, nil)
	}

	instancesAdded, instancesUpdated, instancesRemoved, instancesUnchanged, err :=
		GetInstancesToProcessForUpdate(
			context.Background(),
			testJob.ID(),
			prevJobConfig,
			jobConfig,
			suite.taskStore,
			suite.taskConfigV2Ops,
		)

	suite.NoError(err)
	suite.Len(instancesUpdated, int(instanceCount))
	suite.Empty(instancesRemoved)
	suite.Empty(instancesAdded)
	suite.Empty(instancesUnchanged)
}

// TestGetInstancesToProcessForUpdateWithAddedInstances tests adding and updating instances
func (suite *UpdateTestSuite) TestGetInstancesToProcessForUpdateWithAddedInstances() {
	commandValue := "entrypoint.sh"
	taskConfig := &pbtask.TaskConfig{
		Command: &mesosv1.CommandInfo{
			Value: &commandValue,
		},
	}

	commandValueNew := "new.sh"
	taskConfigNew := &pbtask.TaskConfig{
		Command: &mesosv1.CommandInfo{
			Value: &commandValueNew,
		},
	}

	testJob := &job{
		runtime: &pbjob.RuntimeInfo{
			Revision: &peloton.ChangeLog{
				Version: 1,
			},
		},
		jobFactory: &jobFactory{
			jobStore:  suite.jobStore,
			taskStore: suite.taskStore,
		},
		tasks: make(map[uint32]*task),
		id:    suite.jobID,
	}

	prevInstanceCount := uint32(10)
	prevJobConfig := &pbjob.JobConfig{
		InstanceCount: prevInstanceCount,
		DefaultConfig: taskConfig,
	}

	instanceCount := uint32(12)
	jobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		DefaultConfig: taskConfigNew,
	}

	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < prevInstanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State:                pbtask.TaskState_RUNNING,
			ConfigVersion:        3,
			DesiredConfigVersion: 3,
		}
		taskRuntimes[i] = runtime
	}

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(taskRuntimes, nil)

	suite.taskConfigV2Ops.EXPECT().
		GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(taskConfig, nil, nil).
		Times(int(prevInstanceCount))

	instancesAdded, instancesUpdated, instancesRemoved, instancesUnchanged, err :=
		GetInstancesToProcessForUpdate(
			context.Background(),
			testJob.ID(),
			prevJobConfig,
			jobConfig,
			suite.taskStore,
			suite.taskConfigV2Ops,
		)

	suite.NoError(err)
	suite.Len(instancesAdded, int(instanceCount-prevInstanceCount))
	suite.Empty(instancesRemoved)
	suite.Len(instancesUpdated, int(prevInstanceCount))
	suite.Empty(instancesUnchanged)
}

// TestGetInstancesToProcessForUpdateWithRemovedInstances tests removing instances
func (suite *UpdateTestSuite) TestGetInstancesToProcessForUpdateWithRemovedInstances() {
	commandValue := "entrypoint.sh"
	taskConfig := &pbtask.TaskConfig{
		Command: &mesosv1.CommandInfo{
			Value: &commandValue,
		},
	}

	testJob := &job{
		runtime: &pbjob.RuntimeInfo{
			Revision: &peloton.ChangeLog{
				Version: 1,
			},
		},
		jobFactory: &jobFactory{
			jobStore:  suite.jobStore,
			taskStore: suite.taskStore,
		},
		tasks: make(map[uint32]*task),
		id:    suite.jobID,
	}

	prevInstanceCount := uint32(10)
	prevJobConfig := &pbjob.JobConfig{
		InstanceCount: prevInstanceCount,
		DefaultConfig: taskConfig,
	}

	instanceCount := uint32(8)
	jobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		DefaultConfig: taskConfig,
	}

	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < prevInstanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State:                pbtask.TaskState_RUNNING,
			ConfigVersion:        3,
			DesiredConfigVersion: 3,
		}
		taskRuntimes[i] = runtime
	}

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(taskRuntimes, nil)

	suite.taskConfigV2Ops.EXPECT().
		GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(taskConfig, nil, nil).
		Times(int(instanceCount))

	instancesAdded, instancesUpdated, instancesRemoved, instancesUnchanged, err :=
		GetInstancesToProcessForUpdate(
			context.Background(),
			testJob.ID(),
			prevJobConfig,
			jobConfig,
			suite.taskStore,
			suite.taskConfigV2Ops,
		)

	suite.NoError(err)
	suite.Empty(instancesAdded)
	suite.Len(instancesRemoved, int(prevInstanceCount-instanceCount))
	suite.Len(instancesUnchanged, int(instanceCount))
	suite.Empty(instancesUpdated)
}

// TestGetInstancesToProcessForUpdateWithMssingConfig tests updating instances
// whose current task config is not found in the DB.
func (suite *UpdateTestSuite) TestGetInstancesToProcessForUpdateWithMssingConfig() {
	commandValue := "entrypoint.sh"
	taskConfig := &pbtask.TaskConfig{
		Command: &mesosv1.CommandInfo{
			Value: &commandValue,
		},
	}

	testJob := &job{
		runtime: &pbjob.RuntimeInfo{
			Revision: &peloton.ChangeLog{
				Version: 1,
			},
		},
		jobFactory: &jobFactory{
			jobStore:  suite.jobStore,
			taskStore: suite.taskStore,
		},
		tasks: make(map[uint32]*task),
		id:    suite.jobID,
	}

	instanceCount := uint32(10)
	jobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		DefaultConfig: taskConfig,
	}

	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State:                pbtask.TaskState_RUNNING,
			ConfigVersion:        3,
			DesiredConfigVersion: 3,
		}
		taskRuntimes[i] = runtime
	}

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(taskRuntimes, nil)

	suite.taskConfigV2Ops.EXPECT().
		GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil, yarpcerrors.NotFoundErrorf("not-found")).
		Times(int(instanceCount))

	instancesAdded, instancesUpdated, instancesRemoved, instancesUnchanged, err :=
		GetInstancesToProcessForUpdate(
			context.Background(),
			testJob.ID(),
			jobConfig,
			jobConfig,
			suite.taskStore,
			suite.taskConfigV2Ops,
		)

	suite.NoError(err)
	suite.Empty(instancesAdded)
	suite.Empty(instancesRemoved)
	suite.Len(instancesUpdated, int(instanceCount))
	suite.Empty(instancesUnchanged)
}

// TestGetInstancesToProcessForUpdateWithUpdateInstances tests updating instances
// which are unchanged but the config and desired config version are different.
func (suite *UpdateTestSuite) TestGetInstancesToProcessForUpdateWithUpdateInstances() {
	commandValue := "entrypoint.sh"
	taskConfig := &pbtask.TaskConfig{
		Command: &mesosv1.CommandInfo{
			Value: &commandValue,
		},
	}

	testJob := &job{
		runtime: &pbjob.RuntimeInfo{
			Revision: &peloton.ChangeLog{
				Version: 1,
			},
		},
		jobFactory: &jobFactory{
			jobStore:  suite.jobStore,
			taskStore: suite.taskStore,
		},
		tasks: make(map[uint32]*task),
		id:    suite.jobID,
	}

	instanceCount := uint32(10)
	jobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		DefaultConfig: taskConfig,
	}

	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State:                pbtask.TaskState_RUNNING,
			ConfigVersion:        3,
			DesiredConfigVersion: 4,
		}
		taskRuntimes[i] = runtime
	}

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(taskRuntimes, nil)

	suite.taskConfigV2Ops.EXPECT().
		GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(taskConfig, nil, nil).
		Times(int(instanceCount))

	instancesAdded, instancesUpdated, instancesRemoved, instancesUnchanged, err :=
		GetInstancesToProcessForUpdate(
			context.Background(),
			testJob.ID(),
			jobConfig,
			jobConfig,
			suite.taskStore,
			suite.taskConfigV2Ops,
		)

	suite.NoError(err)
	suite.Empty(instancesAdded)
	suite.Empty(instancesRemoved)
	suite.Len(instancesUpdated, int(instanceCount))
	suite.Empty(instancesUnchanged)
}

func (suite *UpdateTestSuite) TestGetInstancesToProcessForUpdateRuntimeError() {
	instanceCount := uint32(10)
	prevJobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		Labels: []*peloton.Label{
			{Key: "key1", Value: "val1"},
			{Key: "key2", Value: "val2"},
		},
	}
	jobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		Labels: []*peloton.Label{
			{Key: "key1", Value: "val1"},
			{Key: "key2", Value: "val2-1"},
		},
	}

	testJob := &job{
		id: suite.jobID,
	}

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(nil, fmt.Errorf("fake db error"))

	_, _, _, _, err :=
		GetInstancesToProcessForUpdate(
			context.Background(),
			testJob.ID(),
			prevJobConfig,
			jobConfig,
			suite.taskStore,
			suite.taskConfigV2Ops,
		)

	suite.Error(err)
}

func (suite *UpdateTestSuite) TestGetInstancesToProcessForUpdateConfigError() {
	commandValue := "entrypoint.sh"
	taskConfig := &pbtask.TaskConfig{
		Command: &mesosv1.CommandInfo{
			Value: &commandValue,
		},
	}

	commandValueNew := "new.sh"
	taskConfigNew := &pbtask.TaskConfig{
		Command: &mesosv1.CommandInfo{
			Value: &commandValueNew,
		},
	}

	testJob := &job{
		runtime: &pbjob.RuntimeInfo{
			Revision: &peloton.ChangeLog{
				Version: 1,
			},
		},
		jobFactory: &jobFactory{
			jobStore:  suite.jobStore,
			taskStore: suite.taskStore,
		},
		tasks: make(map[uint32]*task),
		id:    suite.jobID,
	}

	instanceCount := uint32(10)
	prevJobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		DefaultConfig: taskConfig,
	}

	jobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		DefaultConfig: taskConfigNew,
	}

	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State:                pbtask.TaskState_RUNNING,
			ConfigVersion:        3,
			DesiredConfigVersion: 3,
		}
		taskRuntimes[i] = runtime
	}

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(taskRuntimes, nil)

	for i := uint32(0); i < instanceCount; i++ {
		if i%2 == 0 {
			suite.taskConfigV2Ops.EXPECT().
				GetTaskConfig(gomock.Any(), suite.jobID, i, gomock.Any()).
				Return(nil, nil, fmt.Errorf("fake db error")).
				MaxTimes(1)
		} else {
			suite.taskConfigV2Ops.EXPECT().
				GetTaskConfig(gomock.Any(), suite.jobID, i, gomock.Any()).
				Return(taskConfig, nil, nil).
				MaxTimes(1)
		}
	}

	_, _, _, _, err :=
		GetInstancesToProcessForUpdate(
			context.Background(),
			testJob.ID(),
			prevJobConfig,
			jobConfig,
			suite.taskStore,
			suite.taskConfigV2Ops,
		)

	suite.Error(err)
}

// TestUpdateCreateSuccess tests the success case of creating update
func (suite *UpdateTestSuite) TestUpdateCreateSuccess() {
	var instancesAdded []uint32
	var instnacesRemoved []uint32
	instancesUpdated := []uint32{0, 1, 2}
	opaque := "test"

	workflowType := models.WorkflowType_START
	updateConfig := &pbupdate.UpdateConfig{
		BatchSize: 10,
	}

	prevConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{Version: 1},
	}
	jobConfig := prevConfig
	configAddOn := &models.ConfigAddOn{}

	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_INITIALIZED).
		Return(nil)

	suite.updateStore.EXPECT().
		CreateUpdate(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateInfo *models.UpdateModel) {
			suite.Equal(updateInfo.GetJobConfigVersion(),
				jobConfig.GetChangeLog().GetVersion())
			suite.Equal(updateInfo.GetPrevJobConfigVersion(),
				prevConfig.GetChangeLog().GetVersion())
			suite.Equal(updateInfo.GetState(), pbupdate.State_INITIALIZED)
			suite.Equal(updateInfo.GetPrevState(), pbupdate.State_INVALID)
			suite.Equal(updateInfo.GetJobID(), suite.jobID)
			suite.Equal(updateInfo.GetInstancesAdded(), instancesAdded)
			suite.Equal(updateInfo.GetInstancesUpdated(), instancesUpdated)
			suite.Equal(updateInfo.GetInstancesRemoved(), instnacesRemoved)
			suite.Equal(updateInfo.GetType(), workflowType)
			suite.Equal(updateInfo.GetUpdateConfig(), updateConfig)
			suite.Equal(updateInfo.GetOpaqueData().GetData(), opaque)
		}).
		Return(nil)
	for _, instance := range instancesUpdated {
		suite.updateStore.EXPECT().
			AddWorkflowEvent(
				gomock.Any(),
				gomock.Any(),
				instance,
				workflowType,
				pbupdate.State_INITIALIZED).Return(nil)
	}

	suite.NoError(suite.update.Create(
		context.Background(),
		suite.jobID,
		jobConfig,
		prevConfig,
		configAddOn,
		instancesAdded,
		instancesUpdated,
		instnacesRemoved,
		workflowType,
		updateConfig,
		&peloton.OpaqueData{Data: opaque},
	))
}

// TestUpdateCreatePausedSuccess tests the success case of creating update
func (suite *UpdateTestSuite) TestUpdateCreatePausedSuccess() {
	var instancesAdded []uint32
	var instnacesRemoved []uint32
	instancesUpdated := []uint32{0, 1, 2}

	workflowType := models.WorkflowType_START
	updateConfig := &pbupdate.UpdateConfig{
		BatchSize:   10,
		StartPaused: true,
	}

	prevConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{Version: 1},
	}
	jobConfig := prevConfig
	configAddOn := &models.ConfigAddOn{}

	suite.jobUpdateEventsOps.EXPECT().
		Create(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			pbupdate.State_PAUSED).
		Return(nil)

	suite.updateStore.EXPECT().
		CreateUpdate(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateInfo *models.UpdateModel) {
			suite.Equal(updateInfo.GetJobConfigVersion(),
				jobConfig.GetChangeLog().GetVersion())
			suite.Equal(updateInfo.GetPrevJobConfigVersion(),
				prevConfig.GetChangeLog().GetVersion())
			suite.Equal(updateInfo.GetState(), pbupdate.State_PAUSED)
			suite.Equal(updateInfo.GetPrevState(), pbupdate.State_INITIALIZED)
			suite.Equal(updateInfo.GetJobID(), suite.jobID)
			suite.Equal(updateInfo.GetInstancesAdded(), instancesAdded)
			suite.Equal(updateInfo.GetInstancesUpdated(), instancesUpdated)
			suite.Equal(updateInfo.GetInstancesRemoved(), instnacesRemoved)
			suite.Equal(updateInfo.GetType(), workflowType)
			suite.Equal(updateInfo.GetUpdateConfig(), updateConfig)
		}).
		Return(nil)

	for _, instance := range instancesUpdated {
		suite.updateStore.EXPECT().
			AddWorkflowEvent(
				gomock.Any(),
				gomock.Any(),
				instance,
				workflowType,
				pbupdate.State_PAUSED).Return(nil)
	}

	suite.NoError(suite.update.Create(
		context.Background(),
		suite.jobID,
		jobConfig,
		prevConfig,
		configAddOn,
		instancesAdded,
		instancesUpdated,
		instnacesRemoved,
		workflowType,
		updateConfig,
		nil,
	))
}

func (suite *UpdateTestSuite) TestUpdateGetJobID() {
	suite.Equal(suite.update.JobID(), suite.update.jobID)
}
