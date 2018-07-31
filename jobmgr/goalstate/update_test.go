package goalstate

import (
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"

	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type UpdateGoalStateTestSuite struct {
	suite.Suite
	ctrl            *gomock.Controller
	updateFactory   *cachedmocks.MockUpdateFactory
	goalStateDriver *driver
	jobID           *peloton.JobID
	updateID        *peloton.UpdateID
	updateEnt       *updateEntity
}

func TestUpdateGoalState(t *testing.T) {
	suite.Run(t, new(UpdateGoalStateTestSuite))
}

func (suite *UpdateGoalStateTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.updateFactory = cachedmocks.NewMockUpdateFactory(suite.ctrl)
	suite.goalStateDriver = &driver{
		updateFactory: suite.updateFactory,
		mtx:           NewMetrics(tally.NoopScope),
		cfg:           &Config{},
	}
	suite.goalStateDriver.cfg.normalize()
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.updateID = &peloton.UpdateID{Value: uuid.NewRandom().String()}
	suite.updateEnt = &updateEntity{
		id:     suite.updateID,
		jobID:  suite.jobID,
		driver: suite.goalStateDriver,
	}
}

func (suite *UpdateGoalStateTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestUpdateStateAndGoalState tests fetching the state
// and goal state of a job update.
func (suite *UpdateGoalStateTestSuite) TestUpdateStateAndGoalState() {
	cachedUpdate := cachedmocks.NewMockUpdate(suite.ctrl)

	// Test fetching the entity ID
	suite.Equal(suite.jobID.GetValue(), suite.updateEnt.GetID())

	// Test fetching the entity state
	updateState := &cached.UpdateStateVector{
		State:     update.State_ROLLING_FORWARD,
		Instances: []uint32{2, 3, 4, 5, 6},
	}

	suite.updateFactory.EXPECT().
		AddUpdate(suite.updateID).
		Return(cachedUpdate)

	cachedUpdate.EXPECT().
		GetState().
		Return(updateState)

	actState := suite.updateEnt.GetState()
	suite.Equal(updateState.State, actState.(*cached.UpdateStateVector).State)
	suite.Equal(updateState.Instances,
		actState.(*cached.UpdateStateVector).Instances)

	// Test fetching the entity goal state
	updateGoalState := &cached.UpdateStateVector{
		Instances: []uint32{2, 3, 4, 5, 6},
	}

	suite.updateFactory.EXPECT().
		AddUpdate(suite.updateID).
		Return(cachedUpdate)

	cachedUpdate.EXPECT().
		GetGoalState().
		Return(updateGoalState)

	actGoalState := suite.updateEnt.GetGoalState()
	suite.Equal(updateGoalState.Instances,
		actGoalState.(*cached.UpdateStateVector).Instances)
}

// TestUpdateGetActionList tests getting the action list for a given
// state and goal state of a job update.
func (suite *UpdateGoalStateTestSuite) TestUpdateGetActionList() {
	updateState := &cached.UpdateStateVector{
		State:     update.State_ROLLING_FORWARD,
		Instances: []uint32{2, 3, 4, 5, 6},
	}

	updateGoalState := &cached.UpdateStateVector{
		Instances: []uint32{1, 2, 3, 4, 5, 6},
	}

	tt := []struct {
		state        update.State
		lengthAction int
	}{
		{
			state:        update.State_ROLLING_FORWARD,
			lengthAction: 2,
		},
		{
			state:        update.State_PAUSED,
			lengthAction: 2,
		},
		{
			state:        update.State_ABORTED,
			lengthAction: 1,
		},
	}

	for _, test := range tt {
		updateState.State = test.state
		_, _, actions := suite.updateEnt.GetActionList(
			updateState,
			updateGoalState,
		)
		suite.Equal(test.lengthAction, len(actions))
	}
}

// TestUpdateSuggestAction tests the determination of actions
// for a given update state and goal state.
func (suite *UpdateGoalStateTestSuite) TestUpdateSuggestAction() {
	tt := []struct {
		state          update.State
		instancesDone  []uint32
		instancesTotal []uint32
		action         UpdateAction
	}{
		{
			state:          update.State_ROLLING_FORWARD,
			instancesDone:  []uint32{2, 3, 4, 5, 6},
			instancesTotal: []uint32{1, 2, 3, 4, 5, 6},
			action:         RunUpdateAction,
		},
		{
			state:          update.State_INVALID,
			instancesDone:  []uint32{2, 3, 4, 5, 6},
			instancesTotal: []uint32{1, 2, 3, 4, 5, 6},
			action:         ReloadUpdateAction,
		},
		{
			state:          update.State_INITIALIZED,
			instancesDone:  []uint32{2, 3, 4, 5, 6},
			instancesTotal: []uint32{1, 2, 3, 4, 5, 6},
			action:         StartUpdateAction,
		},
		{
			state:          update.State_SUCCEEDED,
			instancesDone:  []uint32{2, 3, 4, 5, 6},
			instancesTotal: []uint32{1, 2, 3, 4, 5, 6},
			action:         ClearUpdateAction,
		},
		{
			state:          update.State_ABORTED,
			instancesDone:  []uint32{2, 3, 4, 5, 6},
			instancesTotal: []uint32{1, 2, 3, 4, 5, 6},
			action:         ClearUpdateAction,
		},
		{
			state:          update.State_ROLLING_FORWARD,
			instancesDone:  []uint32{1, 2, 3, 4, 5, 6},
			instancesTotal: []uint32{1, 2, 3, 4, 5, 6},
			action:         CompleteUpdateAction,
		},
	}

	for _, test := range tt {
		updateState := &cached.UpdateStateVector{
			State:     test.state,
			Instances: test.instancesDone,
		}

		updateGoalState := &cached.UpdateStateVector{
			Instances: test.instancesTotal,
		}

		a := suite.updateEnt.suggestUpdateAction(updateState, updateGoalState)
		suite.Equal(test.action, a)
	}
}
