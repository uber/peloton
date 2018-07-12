package goalstate

import (
	"context"
	"fmt"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbupdate "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"

	"code.uber.internal/infra/peloton/common/goalstate"
	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

type UpdateActionsTestSuite struct {
	suite.Suite
	ctrl                  *gomock.Controller
	updateFactory         *cachedmocks.MockUpdateFactory
	updateGoalStateEngine *goalstatemocks.MockEngine
	goalStateDriver       *driver
	updateID              *peloton.UpdateID
	updateEnt             *updateEntity
	cachedUpdate          *cachedmocks.MockUpdate
}

func TestUpdateActions(t *testing.T) {
	suite.Run(t, new(UpdateActionsTestSuite))
}

func (suite *UpdateActionsTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.updateFactory = cachedmocks.NewMockUpdateFactory(suite.ctrl)
	suite.updateGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.goalStateDriver = &driver{
		updateFactory: suite.updateFactory,
		updateEngine:  suite.updateGoalStateEngine,
		mtx:           NewMetrics(tally.NoopScope),
		cfg:           &Config{},
	}
	suite.goalStateDriver.cfg.normalize()

	suite.updateID = &peloton.UpdateID{Value: uuid.NewRandom().String()}
	suite.updateEnt = &updateEntity{
		id:     suite.updateID,
		driver: suite.goalStateDriver,
	}

	suite.cachedUpdate = cachedmocks.NewMockUpdate(suite.ctrl)
}

func (suite *UpdateActionsTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *UpdateActionsTestSuite) TestUpdateReload() {
	suite.updateFactory.EXPECT().
		AddUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		Recover(gomock.Any()).
		Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(updateEntity goalstate.Entity, deadline time.Time) {
			suite.Equal(suite.updateID.GetValue(), updateEntity.GetID())
		})

	err := UpdateReload(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

func (suite *UpdateActionsTestSuite) TestUpdateReloadNotExists() {
	suite.updateFactory.EXPECT().
		AddUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		Recover(gomock.Any()).
		Return(yarpcerrors.NotFoundErrorf("update not found"))

	suite.updateGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Do(func(updateEntity goalstate.Entity) {
			suite.Equal(suite.updateID.GetValue(), updateEntity.GetID())
		})

	suite.updateFactory.EXPECT().
		ClearUpdate(suite.updateID)

	err := UpdateReload(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

func (suite *UpdateActionsTestSuite) TestUpdateComplete() {
	instancesTotal := []uint32{2, 3, 4, 5}

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_SUCCEEDED,
			instancesTotal,
			[]uint32{}).
		Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(updateEntity goalstate.Entity, deadline time.Time) {
			suite.Equal(suite.updateID.GetValue(), updateEntity.GetID())
		})

	err := UpdateComplete(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

func (suite *UpdateActionsTestSuite) TestUpdateCompleteMissingInCache() {
	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(nil)

	err := UpdateComplete(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

func (suite *UpdateActionsTestSuite) TestUpdateCompleteDBError() {
	instancesTotal := []uint32{2, 3, 4, 5}

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_SUCCEEDED,
			instancesTotal,
			[]uint32{}).
		Return(fmt.Errorf("fake db error"))

	err := UpdateComplete(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

func (suite *UpdateActionsTestSuite) TestUpdateUntrack() {
	suite.updateGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Do(func(updateEntity goalstate.Entity) {
			suite.Equal(suite.updateID.GetValue(), updateEntity.GetID())
		})

	suite.updateFactory.EXPECT().
		ClearUpdate(suite.updateID)

	err := UpdateUntrack(context.Background(), suite.updateEnt)
	suite.NoError(err)
}
