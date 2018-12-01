package cached

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	mesosv1 "code.uber.internal/infra/peloton/.gen/mesos/v1"
	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	pbupdate "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

type UpdateTestSuite struct {
	suite.Suite

	ctrl        *gomock.Controller
	jobStore    *storemocks.MockJobStore
	taskStore   *storemocks.MockTaskStore
	updateStore *storemocks.MockUpdateStore
	jobID       *peloton.JobID
	updateID    *peloton.UpdateID
	update      *update
}

func TestUpdate(t *testing.T) {
	suite.Run(t, new(UpdateTestSuite))
}

func (suite *UpdateTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.updateStore = storemocks.NewMockUpdateStore(suite.ctrl)
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.updateID = &peloton.UpdateID{Value: uuid.NewRandom().String()}
	suite.update = initializeUpdate(
		suite.jobStore,
		suite.taskStore,
		suite.updateStore,
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
	updateID *peloton.UpdateID) *update {
	jobFactory := &jobFactory{
		mtx:         NewMetrics(tally.NoopScope),
		jobStore:    jobStore,
		taskStore:   taskStore,
		updateStore: updateStore,
		jobs:        map[string]*job{},
		running:     true,
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

	suite.update.state = pbupdate.State_ROLLING_FORWARD

	suite.updateStore.EXPECT().
		ModifyUpdate(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateModel *models.UpdateModel) {
			suite.Equal(suite.updateID, updateModel.UpdateID)
			suite.Equal(instancesAdded, updateModel.InstancesAdded)
			suite.Equal(instancesUpdated, updateModel.InstancesUpdated)
			suite.Equal(instancesRemoved, updateModel.InstancesRemoved)
		}).Return(nil)

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
	instancesDone := []uint32{0, 1, 2, 3}
	instancesCurrent := []uint32{4, 5}
	instanceFailed := []uint32{}

	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	suite.update.state = pbupdate.State_INITIALIZED
	suite.NoError(suite.update.WriteProgress(
		context.Background(),
		pbupdate.State_ROLLING_FORWARD,
		instancesDone,
		instancesCurrent,
		instanceFailed,
	))
	suite.Equal(suite.update.prevState, pbupdate.State_INITIALIZED)
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_FORWARD)

	// prev state should still be INITIALIZED after we write
	// State_ROLLING_FORWARD twice
	suite.NoError(suite.update.WriteProgress(
		context.Background(),
		pbupdate.State_ROLLING_FORWARD,
		instancesDone,
		instancesCurrent,
		instanceFailed,
	))
	suite.Equal(suite.update.prevState, pbupdate.State_INITIALIZED)
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_FORWARD)

	suite.NoError(suite.update.WriteProgress(
		context.Background(),
		pbupdate.State_ROLLING_FORWARD,
		instancesDone,
		instancesCurrent,
		instanceFailed,
	))
	suite.Equal(suite.update.prevState, pbupdate.State_INITIALIZED)
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_FORWARD)

	suite.NoError(suite.update.WriteProgress(
		context.Background(),
		pbupdate.State_ROLLING_BACKWARD,
		instancesDone,
		instancesCurrent,
		instanceFailed,
	))
	suite.Equal(suite.update.prevState, pbupdate.State_ROLLING_FORWARD)
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_BACKWARD)
}

// TestPauseSuccess tests successfully pause an update
func (suite *UpdateTestSuite) TestPauseSuccess() {
	suite.update.state = pbupdate.State_ROLLING_FORWARD
	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateModel *models.UpdateModel) {
			suite.Equal(suite.updateID, updateModel.UpdateID)
			suite.Equal(pbupdate.State_PAUSED, updateModel.State)
		}).
		Return(nil)

	suite.NoError(suite.update.Pause(context.Background()))
}

// TestPauseRecoverFail tests the failure case of
// pause an update due to recover failure
func (suite *UpdateTestSuite) TestPauseRecoverFail() {
	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.update.Pause(context.Background()))
}

// TestPausePausedUpdate tests pause an update already paused
func (suite *UpdateTestSuite) TestPausePausedUpdate() {
	suite.update.state = pbupdate.State_PAUSED
	suite.NoError(suite.update.Pause(context.Background()))
}

// TestPauseResumeRollingForwardUpdate tests pause and resume a rolling
// forward update
func (suite *UpdateTestSuite) TestPauseResumeRollingForwardUpdate() {
	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	suite.update.state = pbupdate.State_ROLLING_FORWARD
	suite.NoError(suite.update.Pause(context.Background()))
	suite.NoError(suite.update.Resume(context.Background()))
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_FORWARD)
}

// TestPauseResumeRollingForwardUpdate tests pause and resume a rolling
// backward update
func (suite *UpdateTestSuite) TestPauseResumeRollingBackwardUpdate() {
	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	suite.update.state = pbupdate.State_ROLLING_BACKWARD
	suite.NoError(suite.update.Pause(context.Background()))
	suite.NoError(suite.update.Resume(context.Background()))
	suite.Equal(suite.update.state, pbupdate.State_ROLLING_BACKWARD)
}

// TestPauseResumeInitializedUpdate tests pause and resume an
// initialized update
func (suite *UpdateTestSuite) TestPauseResumeInitializedUpdate() {
	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	suite.update.state = pbupdate.State_INITIALIZED
	suite.NoError(suite.update.Pause(context.Background()))
	suite.NoError(suite.update.Resume(context.Background()))
	suite.Equal(suite.update.state, pbupdate.State_INITIALIZED)
}

// TestResumeRecoverFail tests the failure case of
// resume an update due to recover failure
func (suite *UpdateTestSuite) TestResumeRecoverFail() {
	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.update.Resume(context.Background()))
}

// TestCancelValid tests successfully canceling a job update
func (suite *UpdateTestSuite) TestCancelValid() {
	instancesDone := []uint32{1, 2, 3, 4, 5}
	instancesCurrent := []uint32{6, 7}
	suite.update.state = pbupdate.State_INITIALIZED
	suite.update.instancesDone = instancesDone
	suite.update.instancesCurrent = instancesCurrent

	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, updateModel *models.UpdateModel) {
			suite.Equal(suite.updateID, updateModel.UpdateID)
			suite.Equal(pbupdate.State_ABORTED, updateModel.State)
			suite.Equal(uint32(len(instancesDone)), updateModel.InstancesDone)
			suite.Equal(instancesCurrent, updateModel.InstancesCurrent)
		}).
		Return(nil)

	err := suite.update.Cancel(context.Background())
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

	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := suite.update.Cancel(context.Background())
	suite.EqualError(err, "fake db error")
	suite.Equal(pbupdate.State_INVALID, suite.update.state)
}

// TestCancelRecoverFail tests the failure case of
// cancel an update due to recover failure
func (suite *UpdateTestSuite) TestCancelRecoverFail() {
	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), suite.updateID).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.update.Cancel(context.Background()))
}

// TestCancelTerminatedUpdate tests canceling a terminated update
func (suite *UpdateTestSuite) TestCancelTerminatedUpdate() {
	suite.update.state = pbupdate.State_SUCCEEDED

	err := suite.update.Cancel(context.Background())
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
		Labels:        []*peloton.Label{{"test-key", "test-value"}},
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
		Labels:        []*peloton.Label{{"test-key", "test-value"}},
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
		Labels:        []*peloton.Label{{"test-key", "test-value"}},
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
		Labels:        []*peloton.Label{{"test-key", "test-value"}},
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
		Labels:        []*peloton.Label{{"test-key", "test-value"}},
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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(newJobConfig, &models.ConfigAddOn{}, nil).AnyTimes()

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
			{"key1", "val1"},
			{"key2", "val2"},
		},
	}
	jobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		Labels: []*peloton.Label{
			{"key1", "val1"},
			{"key2", "val2-1"},
			{"key3", "val3"},
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

	instancesAdded, instancesUpdated, instancesRemoved, err :=
		GetInstancesToProcessForUpdate(
			context.Background(),
			testJob.ID(),
			prevJobConfig,
			jobConfig,
			suite.taskStore,
		)

	suite.NoError(err)
	suite.Len(instancesUpdated, int(instanceCount))
	suite.Empty(instancesRemoved)
	suite.Empty(instancesAdded)
}

// TestGetInstancesToProcessForUpdateWithLabelUpdated tests
// GetInstancesToProcessForUpdate, when config have labels updated
func (suite *UpdateTestSuite) TestGetInstancesToProcessForUpdateWithLabelUpdated() {
	instanceCount := uint32(10)
	prevJobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		Labels: []*peloton.Label{
			{"key1", "val1"},
			{"key2", "val2"},
		},
	}
	jobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		Labels: []*peloton.Label{
			{"key1", "val1"},
			{"key2", "val2-1"},
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

	instancesAdded, instancesUpdated, instancesRemoved, err :=
		GetInstancesToProcessForUpdate(
			context.Background(),
			testJob.ID(),
			prevJobConfig,
			jobConfig,
			suite.taskStore,
		)

	suite.NoError(err)
	suite.Len(instancesUpdated, int(instanceCount))
	suite.Empty(instancesRemoved)
	suite.Empty(instancesAdded)
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

	suite.taskStore.EXPECT().
		GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(taskConfig, nil, nil).
		Times(int(prevInstanceCount))

	instancesAdded, instancesUpdated, instancesRemoved, err :=
		GetInstancesToProcessForUpdate(
			context.Background(),
			testJob.ID(),
			prevJobConfig,
			jobConfig,
			suite.taskStore,
		)

	suite.NoError(err)
	suite.Len(instancesAdded, int(instanceCount-prevInstanceCount))
	suite.Empty(instancesRemoved)
	suite.Len(instancesUpdated, int(prevInstanceCount))
}

// TestGetInstancesToProcessForUpdateWithRemovedInstances tests removing instances
func (suite *UpdateTestSuite) TestGetInstancesToProcessForUpdateWithRemovedInstances() {
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

	instanceCount := uint32(8)
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

	suite.taskStore.EXPECT().
		GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil, yarpcerrors.NotFoundErrorf("not-found")).
		Times(int(instanceCount))

	instancesAdded, instancesUpdated, instancesRemoved, err :=
		GetInstancesToProcessForUpdate(
			context.Background(),
			testJob.ID(),
			prevJobConfig,
			jobConfig,
			suite.taskStore,
		)

	suite.NoError(err)
	suite.Empty(instancesAdded)
	suite.Len(instancesRemoved, int(prevInstanceCount-instanceCount))
	suite.Len(instancesUpdated, int(instanceCount))
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

	suite.taskStore.EXPECT().
		GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(taskConfig, nil, nil).
		Times(int(instanceCount))

	instancesAdded, instancesUpdated, instancesRemoved, err :=
		GetInstancesToProcessForUpdate(
			context.Background(),
			testJob.ID(),
			jobConfig,
			jobConfig,
			suite.taskStore,
		)

	suite.NoError(err)
	suite.Empty(instancesAdded)
	suite.Empty(instancesRemoved)
	suite.Len(instancesUpdated, int(instanceCount))
}

func (suite *UpdateTestSuite) TestGetInstancesToProcessForUpdateRuntimeError() {
	instanceCount := uint32(10)
	prevJobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		Labels: []*peloton.Label{
			{"key1", "val1"},
			{"key2", "val2"},
		},
	}
	jobConfig := &pbjob.JobConfig{
		InstanceCount: instanceCount,
		Labels: []*peloton.Label{
			{"key1", "val1"},
			{"key2", "val2-1"},
		},
	}

	testJob := &job{
		id: suite.jobID,
	}

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), suite.jobID, nil).
		Return(nil, fmt.Errorf("fake db error"))

	_, _, _, err :=
		GetInstancesToProcessForUpdate(
			context.Background(),
			testJob.ID(),
			prevJobConfig,
			jobConfig,
			suite.taskStore,
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

	suite.taskStore.EXPECT().
		GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil, fmt.Errorf("fake db error"))

	_, _, _, err :=
		GetInstancesToProcessForUpdate(
			context.Background(),
			testJob.ID(),
			prevJobConfig,
			jobConfig,
			suite.taskStore,
		)

	suite.Error(err)
}

// TestUpdateCreateSuccess tests the success case of creating update
func (suite *UpdateTestSuite) TestUpdateCreateSuccess() {
	var instancesAdded []uint32
	var instnacesRemoved []uint32
	instancesUpdated := []uint32{0, 1, 2}

	workflowType := models.WorkflowType_START
	updateConfig := &pbupdate.UpdateConfig{
		BatchSize: 10,
	}

	prevConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{Version: 1},
	}
	jobConfig := prevConfig
	configAddOn := &models.ConfigAddOn{}

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
		}).
		Return(nil)

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
	))
}

// TestUpdateCreateSuccess tests the success case of creating update
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
	))
}

func (suite *UpdateTestSuite) TestUpdateGetJobID() {
	suite.Equal(suite.update.JobID(), suite.update.jobID)
}
