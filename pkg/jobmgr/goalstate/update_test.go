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
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/update"

	"github.com/uber/peloton/pkg/jobmgr/cached"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type UpdateGoalStateTestSuite struct {
	suite.Suite
	ctrl            *gomock.Controller
	jobFactory      *cachedmocks.MockJobFactory
	cachedJob       *cachedmocks.MockJob
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
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.goalStateDriver = &driver{
		mtx:        NewMetrics(tally.NoopScope),
		cfg:        &Config{},
		jobFactory: suite.jobFactory,
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

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
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

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AddWorkflow(suite.updateID).
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
			state:          update.State_ROLLING_BACKWARD,
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
		{
			state:          update.State_ROLLING_BACKWARD,
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
