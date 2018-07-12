package update

import (
	"context"
	"fmt"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"

	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	goalstatemocks "code.uber.internal/infra/peloton/jobmgr/goalstate/mocks"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
)

type UpdateJobUtilTestSuite struct {
	suite.Suite

	ctrl            *gomock.Controller
	updateStore     *storemocks.MockUpdateStore
	updateFactory   *cachedmocks.MockUpdateFactory
	goalStateDriver *goalstatemocks.MockDriver
	cachedUpdate    *cachedmocks.MockUpdate
	updateID        *peloton.UpdateID
}

func TestUpdateJobUtil(t *testing.T) {
	suite.Run(t, new(UpdateJobUtilTestSuite))
}

func (suite *UpdateJobUtilTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.updateStore = storemocks.NewMockUpdateStore(suite.ctrl)
	suite.updateFactory = cachedmocks.NewMockUpdateFactory(suite.ctrl)
	suite.goalStateDriver = goalstatemocks.NewMockDriver(suite.ctrl)
	suite.cachedUpdate = cachedmocks.NewMockUpdate(suite.ctrl)
	suite.updateID = &peloton.UpdateID{Value: uuid.NewRandom().String()}
}

func (suite *UpdateJobUtilTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestAbortGetProgressFail tests failing to get update progress
// while trying to abort the update
func (suite *UpdateJobUtilTestSuite) TestAbortGetProgressFail() {
	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), suite.updateID).
		Return(nil, fmt.Errorf("fake db error"))

	err := AbortPreviousJobUpdate(
		context.Background(),
		suite.updateID,
		suite.updateStore,
		suite.updateFactory,
		suite.goalStateDriver,
	)
	suite.EqualError(err, "fake db error")
}

// TestAbortUpdateStateTerminal tests trying to abort a terminated update
func (suite *UpdateJobUtilTestSuite) TestAbortUpdateStateTerminal() {
	updateModel := &models.UpdateModel{
		State: update.State_SUCCEEDED,
	}

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), suite.updateID).
		Return(updateModel, nil)

	err := AbortPreviousJobUpdate(
		context.Background(),
		suite.updateID,
		suite.updateStore,
		suite.updateFactory,
		suite.goalStateDriver,
	)
	suite.NoError(err)
}

// TestAbortRecoverFail tests failing to recover an update
// while trying to abort it
func (suite *UpdateJobUtilTestSuite) TestAbortRecoverFail() {
	updateModel := &models.UpdateModel{
		State: update.State_ROLLING_FORWARD,
	}

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), suite.updateID).
		Return(updateModel, nil)

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(nil)

	suite.updateFactory.EXPECT().
		AddUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		Recover(gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := AbortPreviousJobUpdate(
		context.Background(),
		suite.updateID,
		suite.updateStore,
		suite.updateFactory,
		suite.goalStateDriver,
	)
	suite.EqualError(err, "fake db error")
}

// TestAbortCancelFail tests failing to cancel an
// update while trying to abort it
func (suite *UpdateJobUtilTestSuite) TestAbortCancelFail() {
	updateModel := &models.UpdateModel{
		State: update.State_ROLLING_FORWARD,
	}

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), suite.updateID).
		Return(updateModel, nil)

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(nil)

	suite.updateFactory.EXPECT().
		AddUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		Recover(gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		Cancel(gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := AbortPreviousJobUpdate(
		context.Background(),
		suite.updateID,
		suite.updateStore,
		suite.updateFactory,
		suite.goalStateDriver,
	)
	suite.EqualError(err, "fake db error")
}

// TestAbortSuccess tests being able to successully abort an update
func (suite *UpdateJobUtilTestSuite) TestAbortSuccess() {
	updateModel := &models.UpdateModel{
		State: update.State_ROLLING_FORWARD,
	}

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), suite.updateID).
		Return(updateModel, nil)

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(nil)

	suite.updateFactory.EXPECT().
		AddUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		Recover(gomock.Any()).
		Return(nil)

	suite.cachedUpdate.EXPECT().
		Cancel(gomock.Any()).
		Return(nil)

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(suite.updateID, gomock.Any())

	err := AbortPreviousJobUpdate(
		context.Background(),
		suite.updateID,
		suite.updateStore,
		suite.updateFactory,
		suite.goalStateDriver,
	)
	suite.NoError(err)
}
