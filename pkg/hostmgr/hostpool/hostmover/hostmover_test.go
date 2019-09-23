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

package hostmover

import (
	"context"
	"errors"
	"testing"

	hpb "github.com/uber/peloton/.gen/peloton/api/v0/host"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	resmocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"

	goalmocks "github.com/uber/peloton/pkg/hostmgr/goalstate/mocks"
	poolmocks "github.com/uber/peloton/pkg/hostmgr/hostpool/manager/mocks"
	ormmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/peloton/pkg/common"
)

// HostPoolTestSuite is test suite for host pool.
type hostMoverTestSuite struct {
	suite.Suite
	mover               HostMover
	mockCtrl            *gomock.Controller
	mockHostPoolManager *poolmocks.MockHostPoolManager
	mockresMgrClient    *resmocks.MockResourceManagerServiceYARPCClient
	mockHostInfoOps     *ormmocks.MockHostInfoOps
	mockGoalStateDriver *goalmocks.MockDriver
}

func (suite *hostMoverTestSuite) SetupSuite() {
}

func (suite *hostMoverTestSuite) SetupTest() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockHostPoolManager = poolmocks.NewMockHostPoolManager(suite.mockCtrl)
	suite.mockresMgrClient = resmocks.NewMockResourceManagerServiceYARPCClient(suite.mockCtrl)
	suite.mockHostInfoOps = ormmocks.NewMockHostInfoOps(suite.mockCtrl)
	suite.mockGoalStateDriver = goalmocks.NewMockDriver(suite.mockCtrl)
	suite.mover = NewHostMover(
		suite.mockHostPoolManager,
		suite.mockHostInfoOps,
		suite.mockGoalStateDriver,
		tally.NoopScope,
		suite.mockresMgrClient,
	)
}

// TestHostMoverTestSuite runs HostMoverTestSuite.
func TestHostMoverTestSuite(t *testing.T) {
	suite.Run(t, new(hostMoverTestSuite))
}

// TestMoveHosts tests the move hosts api from host mover interface
// it tests the without error path.
func (suite *hostMoverTestSuite) TestMoveHosts() {

	var hostInfos []*hpb.HostInfo

	hostInfo1 := &hpb.HostInfo{
		Hostname:    "host1",
		CurrentPool: "p2",
		DesiredPool: "p2",
	}

	hostInfo2 := &hpb.HostInfo{
		Hostname:    "host1",
		CurrentPool: common.SharedHostPoolID,
		DesiredPool: common.SharedHostPoolID,
	}

	hostInfo3 := &hpb.HostInfo{
		Hostname:    "host1",
		CurrentPool: common.SharedHostPoolID,
		DesiredPool: common.SharedHostPoolID,
	}

	hostInfo4 := &hpb.HostInfo{
		Hostname:    "host1",
		CurrentPool: common.SharedHostPoolID,
		DesiredPool: common.SharedHostPoolID,
	}

	hostInfos = append(hostInfos, hostInfo1)
	hostInfos = append(hostInfos, hostInfo2)
	hostInfos = append(hostInfos, hostInfo3)
	hostInfos = append(hostInfos, hostInfo4)

	suite.mockHostPoolManager.EXPECT().
		GetPool(gomock.Any()).Return(nil, nil).Times(2)

	suite.mockHostInfoOps.EXPECT().GetAll(gomock.Any()).Return(
		hostInfos, nil)

	suite.mockresMgrClient.EXPECT().GetHostsByScores(gomock.Any(),
		gomock.Any()).Return(
		&resmgrsvc.GetHostsByScoresResponse{
			Hosts: []string{"host1"},
		}, nil)

	suite.mockHostPoolManager.EXPECT().UpdateDesiredPool(
		"host1",
		"p2").
		Return(nil)

	suite.mockGoalStateDriver.EXPECT().
		EnqueueHost(gomock.Any(), gomock.Any())

	suite.NoError(suite.mover.MoveHosts(
		context.Background(), common.SharedHostPoolID, 2, "p2", 2))
}

// TestMoveHostsError tests the error scenario when get pool is a error
func (suite *hostMoverTestSuite) TestMoveHostsError() {
	suite.mockHostPoolManager.EXPECT().
		GetPool(common.SharedHostPoolID).Return(nil, errSourcePoolNotExists)
	suite.Error(suite.mover.MoveHosts(context.Background(),
		common.SharedHostPoolID, 1, "p1", 1))

	suite.mockHostPoolManager.EXPECT().
		GetPool(gomock.Any()).Return(nil, nil)
	suite.mockHostPoolManager.EXPECT().
		GetPool("p1").Return(nil, errDestPoolNotExists)
	suite.Error(suite.mover.MoveHosts(context.Background(),
		common.SharedHostPoolID, 1, "p1", 1))
}

// TestMoveHostsDBError tests the error condition for DB
func (suite *hostMoverTestSuite) TestMoveHostsDBError() {
	suite.mockHostPoolManager.EXPECT().
		GetPool(gomock.Any()).Return(nil, nil).Times(2)

	suite.mockHostInfoOps.EXPECT().GetAll(gomock.Any()).Return(
		nil, errors.New("error getting hosts"))
	suite.Error(suite.mover.MoveHosts(context.Background(),
		common.SharedHostPoolID, 1, "p1", 1))
}

// TestNotEnoughHostsError tests when there is not enough hosts to move
func (suite *hostMoverTestSuite) TestNotEnoughHostsError() {
	var hostInfos []*hpb.HostInfo

	hostInfo1 := &hpb.HostInfo{
		Hostname:    "host1",
		CurrentPool: common.SharedHostPoolID,
		DesiredPool: "p2",
	}

	hostInfo2 := &hpb.HostInfo{
		Hostname:    "host1",
		CurrentPool: common.SharedHostPoolID,
		DesiredPool: "p2",
	}

	hostInfos = append(hostInfos, hostInfo1)
	hostInfos = append(hostInfos, hostInfo2)

	suite.mockHostPoolManager.EXPECT().
		GetPool(gomock.Any()).Return(nil, nil).AnyTimes()

	suite.mockHostInfoOps.EXPECT().GetAll(gomock.Any()).Return(
		hostInfos, nil).AnyTimes()
	suite.NoError(suite.mover.MoveHosts(context.Background(),
		common.SharedHostPoolID, 1, "p2", 1))
	suite.Error(suite.mover.MoveHosts(context.Background(),
		"p2", 1, "p1", 3))
}

// TestScorerError tests the error from resmgr scorer
func (suite *hostMoverTestSuite) TestScorerError() {
	var hostInfos []*hpb.HostInfo

	hostInfo1 := &hpb.HostInfo{
		Hostname:    "host1",
		CurrentPool: "p1",
		DesiredPool: common.SharedHostPoolID,
	}

	hostInfo2 := &hpb.HostInfo{
		Hostname:    "host1",
		CurrentPool: "p1",
		DesiredPool: common.SharedHostPoolID,
	}

	hostInfos = append(hostInfos, hostInfo1)
	hostInfos = append(hostInfos, hostInfo2)

	suite.mockHostPoolManager.EXPECT().
		GetPool(gomock.Any()).Return(nil, nil).AnyTimes()

	suite.mockHostInfoOps.EXPECT().GetAll(gomock.Any()).Return(
		hostInfos, nil).AnyTimes()

	suite.mockresMgrClient.EXPECT().GetHostsByScores(
		gomock.Any(), gomock.Any()).Return(
		nil, errors.New("error in scorer"))

	suite.Error(suite.mover.MoveHosts(context.Background(),
		common.SharedHostPoolID, 1, "p1", 1))

	suite.mockresMgrClient.EXPECT().GetHostsByScores(
		gomock.Any(), gomock.Any()).Return(
		&resmgrsvc.GetHostsByScoresResponse{
			Hosts: []string{"host1"},
		}, nil)
	suite.Error(suite.mover.MoveHosts(context.Background(),
		common.SharedHostPoolID, 1, "p1", 2))
}

// TestUpdatePoolError tests when updating the pool info from host pool manager
func (suite *hostMoverTestSuite) TestUpdatePoolError() {
	var hostInfos []*hpb.HostInfo

	hostInfo1 := &hpb.HostInfo{
		Hostname:    "host1",
		CurrentPool: "p1",
		DesiredPool: common.SharedHostPoolID,
	}

	hostInfo2 := &hpb.HostInfo{
		Hostname:    "host1",
		CurrentPool: "p1",
		DesiredPool: common.SharedHostPoolID,
	}

	hostInfos = append(hostInfos, hostInfo1)
	hostInfos = append(hostInfos, hostInfo2)

	suite.mockHostPoolManager.EXPECT().
		GetPool(gomock.Any()).Return(nil, nil).AnyTimes()

	suite.mockHostInfoOps.EXPECT().GetAll(gomock.Any()).Return(
		hostInfos, nil).AnyTimes()

	suite.mockresMgrClient.EXPECT().GetHostsByScores(
		gomock.Any(), gomock.Any()).Return(
		&resmgrsvc.GetHostsByScoresResponse{
			Hosts: []string{"host1"},
		}, nil)

	suite.mockHostPoolManager.EXPECT().UpdateDesiredPool(
		gomock.Any(), gomock.Any()).Return(errors.New("error"))

	suite.Error(suite.mover.MoveHosts(context.Background(),
		common.SharedHostPoolID, 1, "p1", 1))
}
