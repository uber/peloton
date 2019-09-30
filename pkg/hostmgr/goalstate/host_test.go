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
	"errors"
	"testing"

	pbhost "github.com/uber/peloton/.gen/peloton/api/v0/host"

	"github.com/uber/peloton/pkg/common/goalstate"
	goalstatemocks "github.com/uber/peloton/pkg/common/goalstate/mocks"
	mock_mpb "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
	orm_mocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type HostTestSuite struct {
	suite.Suite
	ctrl                  *gomock.Controller
	mockHostEngine        *goalstatemocks.MockEngine
	mockMesosMasterClient *mock_mpb.MockMasterOperatorClient
	mockHostInfoOps       *orm_mocks.MockHostInfoOps
	goalStateDriver       *driver
	hostname              string
	hostEntity            goalstate.Entity
}

func TestHost(t *testing.T) {
	suite.Run(t, new(HostTestSuite))
}

func (suite *HostTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.mockHostEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.mockMesosMasterClient = mock_mpb.NewMockMasterOperatorClient(suite.ctrl)
	suite.mockHostInfoOps = orm_mocks.NewMockHostInfoOps(suite.ctrl)
	suite.goalStateDriver = &driver{
		hostEngine:        suite.mockHostEngine,
		mesosMasterClient: suite.mockMesosMasterClient,
		hostInfoOps:       suite.mockHostInfoOps,
		scope:             tally.NoopScope,
		cfg:               &Config{},
	}
	suite.goalStateDriver.cfg.normalize()
	suite.goalStateDriver.setState(stopped)
	suite.hostname = "hostname"
	suite.hostEntity = &hostEntity{
		hostname: suite.hostname,
		driver:   suite.goalStateDriver,
	}
}

func (suite *HostTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestNewHostEntity tests NewHostEntity
func (suite *HostTestSuite) TestNewHostEntity() {
	suite.Equal(
		suite.hostEntity,
		NewHostEntity(suite.hostname, suite.goalStateDriver),
	)
}

// TestGetID tests GetID
func (suite *HostTestSuite) TestGetID() {
	suite.Equal(
		suite.hostname,
		suite.hostEntity.GetID(),
	)
}

// TestGetState tests GetState
func (suite *HostTestSuite) TestGetState() {
	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(&pbhost.HostInfo{
			Hostname: suite.hostname,
			State:    pbhost.HostState_HOST_STATE_DOWN}, nil)

	suite.Equal(
		pbhost.HostState_HOST_STATE_DOWN,
		suite.hostEntity.GetState().(*hostEntityState).hostState,
	)
}

// TestGetStateFailure tests GetState with DB failure
func (suite *HostTestSuite) TestGetStateFailure() {
	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("some error"))

	suite.Equal(
		pbhost.HostState_HOST_STATE_INVALID,
		suite.hostEntity.GetState().(*hostEntityState).hostState,
	)
}

// TestGetGoalState tests GetGoalState
func (suite *HostTestSuite) TestGetGoalState() {
	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(&pbhost.HostInfo{
			Hostname:  suite.hostname,
			GoalState: pbhost.HostState_HOST_STATE_DOWN}, nil)

	suite.Equal(
		pbhost.HostState_HOST_STATE_DOWN,
		suite.hostEntity.GetGoalState().(*hostEntityState).hostState,
	)
}

// TestGetGoalStateFailure tests GetGoalState with DB failure
func (suite *HostTestSuite) TestGetGoalStateFailure() {
	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("some error"))

	suite.Equal(
		pbhost.HostState_HOST_STATE_INVALID,
		suite.hostEntity.GetGoalState().(*hostEntityState).hostState,
	)
}

// TestGetActionList tests GetActionList
func (suite *HostTestSuite) TestGetActionList() {
	// Validate INVALID -> DOWN
	_, _, actions := suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_INVALID,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DOWN,
			hostPool:  "p1",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		RequeueAction,
		actions[0].Name,
	)

	// Validate UP -> DOWN
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_UP,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DOWN,
			hostPool:  "p1",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		DrainAction,
		actions[0].Name,
	)

	// Validate DRAINING -> DOWN
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DRAINING,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DOWN,
			hostPool:  "p1",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		DrainAction,
		actions[0].Name,
	)

	// Validate DRAINED -> DOWN
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DRAINED,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DOWN,
			hostPool:  "p1",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		DownAction,
		actions[0].Name,
	)

	// Validate DOWN -> DOWN
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DOWN,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DOWN,
			hostPool:  "p1",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		UntrackAction,
		actions[0].Name,
	)

	// Validate INVALID -> UP
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_INVALID,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_UP,
			hostPool:  "p1",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		RequeueAction,
		actions[0].Name,
	)

	// Validate UP -> UP
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_UP,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_UP,
			hostPool:  "p1",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		UntrackAction,
		actions[0].Name,
	)

	// Validate DOWN -> UP
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DOWN,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_UP,
			hostPool:  "p1",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		UpAction,
		actions[0].Name,
	)

	// Validate INVALID -> INVALID
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_INVALID,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_INVALID,
			hostPool:  "p1",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		RequeueAction,
		actions[0].Name,
	)

	// Validate UP -> INVALID
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_UP,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_INVALID,
			hostPool:  "p1",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		RequeueAction,
		actions[0].Name,
	)

	// Validate DRAINING -> INVALID
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DRAINING,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_INVALID,
			hostPool:  "p1",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		RequeueAction,
		actions[0].Name,
	)

	// Validate DRAINED -> INVALID
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DRAINED,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_INVALID,
			hostPool:  "p1",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		RequeueAction,
		actions[0].Name,
	)

	// Validate DOWN -> INVALID
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DOWN,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_INVALID,
			hostPool:  "p1",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		RequeueAction,
		actions[0].Name,
	)

	// Validate UP -> UNKNOWN
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_UP,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_UNKNOWN,
			hostPool:  "p1",
		},
	)
	suite.Len(actions, 0)

	// Validate DOWN -> UNKNOWN
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DOWN,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_UNKNOWN,
			hostPool:  "p1",
		},
	)
	suite.Len(actions, 0)
}

func (suite *HostTestSuite) TestGetActionListDifferentPool() {
	// Validate INVALID -> DOWN
	_, _, actions := suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_INVALID,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DOWN,
			hostPool:  "p2",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		RequeueAction,
		actions[0].Name,
	)

	// Validate UP -> DOWN
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_UP,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DOWN,
			hostPool:  "p2",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		DrainAction,
		actions[0].Name,
	)

	// Validate DRAINING -> DOWN
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DRAINING,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DOWN,
			hostPool:  "p2",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		DrainAction,
		actions[0].Name,
	)

	// Validate DRAINED -> DOWN
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DRAINED,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DOWN,
			hostPool:  "p2",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		DownAction,
		actions[0].Name,
	)

	// Validate INVALID -> UP
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_INVALID,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_UP,
			hostPool:  "p2",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		RequeueAction,
		actions[0].Name,
	)

	// Validate INVALID -> INVALID
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_INVALID,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_INVALID,
			hostPool:  "p2",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		RequeueAction,
		actions[0].Name,
	)

	// Validate UP -> INVALID
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_UP,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_INVALID,
			hostPool:  "p2",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		RequeueAction,
		actions[0].Name,
	)

	// Validate DRAINING -> INVALID
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DRAINING,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_INVALID,
			hostPool:  "p2",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		RequeueAction,
		actions[0].Name,
	)

	// Validate DRAINED -> INVALID
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DRAINED,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_INVALID,
			hostPool:  "p2",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		RequeueAction,
		actions[0].Name,
	)

	// Validate DOWN -> INVALID
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DOWN,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_INVALID,
			hostPool:  "p2",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		RequeueAction,
		actions[0].Name,
	)
	// Validate INVALID -> DOWN
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_INVALID,
			hostPool:  "p1",
		},

		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DOWN,
			hostPool:  "p2",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		RequeueAction,
		actions[0].Name,
	)

	// Validate Up -> UP
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_UP,
			hostPool:  "p1",
		},

		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_UP,
			hostPool:  "p2",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		StartMaintenanceAction,
		actions[0].Name,
	)

	// Validate Up -> DOWN
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DOWN,
			hostPool:  "p1",
		},

		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_UP,
			hostPool:  "p2",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		ChangePoolAction,
		actions[0].Name,
	)

	// Validate DOWN -> DOWN
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DOWN,
			hostPool:  "p1",
		},

		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DOWN,
			hostPool:  "p2",
		},
	)
	suite.Len(actions, 1)
	suite.EqualValues(
		ChangePoolAction,
		actions[0].Name,
	)

	// Validate UP -> UNKNOWN
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_UP,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_UNKNOWN,
			hostPool:  "p2",
		},
	)
	suite.Len(actions, 0)

	// Validate DOWN -> UNKNOWN
	_, _, actions = suite.hostEntity.GetActionList(
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_DOWN,
			hostPool:  "p1",
		},
		&hostEntityState{
			hostState: pbhost.HostState_HOST_STATE_UNKNOWN,
			hostPool:  "p2",
		},
	)
	suite.Len(actions, 0)
}
