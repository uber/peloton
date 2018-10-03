package cached

import (
	"context"
	"fmt"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbupdate "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"

	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type CachedUtilTestSuite struct {
	suite.Suite

	ctrl          *gomock.Controller
	updateStore   *storemocks.MockUpdateStore
	updateFactory *updateFactory
	jobFactory    *jobFactory
	cachedUpdate  *update
	updateID      *peloton.UpdateID
}

func TestUpdateBusiness(t *testing.T) {
	suite.Run(t, new(CachedUtilTestSuite))
}

func (suite *CachedUtilTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.updateStore = storemocks.NewMockUpdateStore(suite.ctrl)
	suite.updateID = &peloton.UpdateID{Value: uuid.NewRandom().String()}
	suite.jobFactory = InitJobFactory(
		nil,
		nil,
		nil,
		tally.NoopScope).(*jobFactory)
	suite.updateFactory = InitUpdateFactory(
		nil,
		nil,
		suite.updateStore,
		suite.jobFactory,
		tally.NoopScope).(*updateFactory)
	suite.updateFactory.AddUpdate(suite.updateID)
}

func (suite *CachedUtilTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestAbortGetProgressFail tests failing to get update progress
// while trying to abort the update
func (suite *CachedUtilTestSuite) TestAbortGetProgressFail() {
	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), suite.updateID).
		Return(nil, fmt.Errorf("fake db error"))

	err := AbortJobUpdate(
		context.Background(),
		suite.updateID,
		suite.updateStore,
		suite.updateFactory,
	)
	suite.EqualError(err, "fake db error")
}

// TestAbortUpdateStateTerminal tests trying to abort a terminated update
func (suite *CachedUtilTestSuite) TestAbortUpdateStateTerminal() {
	updateModel := &models.UpdateModel{
		State: pbupdate.State_SUCCEEDED,
	}

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), suite.updateID).
		Return(updateModel, nil)

	err := AbortJobUpdate(
		context.Background(),
		suite.updateID,
		suite.updateStore,
		suite.updateFactory,
	)
	suite.NoError(err)
}

// TestAbortRecoverFail tests failing to recover an update
// while trying to abort it
func (suite *CachedUtilTestSuite) TestAbortRecoverFail() {
	suite.updateFactory.updates = make(map[string]*update)
	updateModel := &models.UpdateModel{
		State: pbupdate.State_ROLLING_FORWARD,
	}

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), suite.updateID).
		Return(updateModel, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("fake db error"))

	err := AbortJobUpdate(
		context.Background(),
		suite.updateID,
		suite.updateStore,
		suite.updateFactory,
	)
	suite.EqualError(err, "fake db error")
}

// TestAbortCancelFail tests failing to cancel an
// update while trying to abort it
func (suite *CachedUtilTestSuite) TestAbortCancelFail() {
	updateModel := &models.UpdateModel{
		State: pbupdate.State_ROLLING_FORWARD,
	}

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), suite.updateID).
		Return(updateModel, nil)

	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := AbortJobUpdate(
		context.Background(),
		suite.updateID,
		suite.updateStore,
		suite.updateFactory,
	)
	suite.EqualError(err, "fake db error")
}

// TestAbortSuccess tests being able to successfully abort an update
func (suite *CachedUtilTestSuite) TestAbortSuccess() {
	updateModel := &models.UpdateModel{
		State: pbupdate.State_ROLLING_FORWARD,
	}

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), suite.updateID).
		Return(updateModel, nil)

	suite.updateStore.EXPECT().
		WriteUpdateProgress(gomock.Any(), gomock.Any()).
		Return(nil)

	err := AbortJobUpdate(
		context.Background(),
		suite.updateID,
		suite.updateStore,
		suite.updateFactory,
	)
	suite.NoError(err)
}
