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
	"errors"
	"testing"
	"time"

	pbhost "github.com/uber/peloton/.gen/peloton/api/v0/host"

	"github.com/uber/peloton/pkg/common/goalstate"
	goalstatemocks "github.com/uber/peloton/pkg/common/goalstate/mocks"
	mock_mpb "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
	orm_mocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type driverTestSuite struct {
	suite.Suite
	ctrl                  *gomock.Controller
	mockHostEngine        *goalstatemocks.MockEngine
	mockMesosMasterClient *mock_mpb.MockMasterOperatorClient
	mockHostInfoOps       *orm_mocks.MockHostInfoOps
	goalStateDriver       *driver
}

func TestDriver(t *testing.T) {
	suite.Run(t, new(driverTestSuite))
}

func (suite *driverTestSuite) SetupTest() {
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
}

func (suite *driverTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestNewDriver tests NewDriver
func (suite *driverTestSuite) TestNewDriver() {
	dr := NewDriver(
		suite.mockHostInfoOps,
		suite.mockMesosMasterClient,
		tally.NoopScope,
		Config{},
		nil,
	)
	suite.NotNil(dr)
}

// TestStartStop tests Start and Stop
func (suite *driverTestSuite) TestStartStop() {
	// Test Start
	suite.mockHostInfoOps.EXPECT().
		GetAll(gomock.Any()).
		Return([]*pbhost.HostInfo{}, nil)
	suite.mockHostEngine.EXPECT().Start()

	suite.False(suite.goalStateDriver.Started())
	suite.goalStateDriver.Start()
	suite.True(suite.goalStateDriver.Started())

	//Starting the driver again should be a noop
	suite.goalStateDriver.Start()
	suite.True(suite.goalStateDriver.Started())

	// Test Stop
	// 1 host to delete from goal state engine
	suite.mockHostInfoOps.EXPECT().
		GetAll(gomock.Any()).
		Return([]*pbhost.HostInfo{
			{
				Hostname: "host",
				State:    pbhost.HostState_HOST_STATE_DRAINING,
			},
		}, nil)
	suite.mockHostEngine.EXPECT().Delete(gomock.Any()).
		Do(func(hostEntity goalstate.Entity) {
			suite.Equal("host", hostEntity.GetID())
		})

	suite.mockHostEngine.EXPECT().Stop()

	suite.goalStateDriver.Stop()
	suite.False(suite.goalStateDriver.Started())

	// Stopping the driver again should be a noop
	suite.goalStateDriver.Stop()
	suite.False(suite.goalStateDriver.Started())
}

// TestSyncHostsFromDB tests syncHostsFromDB
func (suite *driverTestSuite) TestSyncHostsFromDB() {
	hostInfosInDB := []*pbhost.HostInfo{
		{
			Hostname: "host1",
			State:    pbhost.HostState_HOST_STATE_DRAINING,
		},
		{
			Hostname: "host2",
			State:    pbhost.HostState_HOST_STATE_DRAINING,
		},
	}

	suite.mockHostInfoOps.EXPECT().
		GetAll(gomock.Any()).
		Return(hostInfosInDB, nil)

	gomock.InOrder(
		suite.mockHostEngine.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
			Do(func(hostEntity goalstate.Entity, deadline time.Time) {
				suite.Equal("host1", hostEntity.GetID())
			}),
		suite.mockHostEngine.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
			Do(func(hostEntity goalstate.Entity, deadline time.Time) {
				suite.Equal("host2", hostEntity.GetID())
			}),
	)

	suite.NoError(suite.goalStateDriver.syncHostsFromDB(context.Background()))
}

// TestSyncHostsFromDBFailed tests syncHostsFromDB with DB failure
func (suite *driverTestSuite) TestSyncHostsFromDBFailed() {
	suite.mockHostInfoOps.EXPECT().
		GetAll(gomock.Any()).
		Return(nil, errors.New("some error"))

	suite.Error(suite.goalStateDriver.syncHostsFromDB(context.Background()))
}

// TestEnqueueHost tests EnqueueHost
func (suite *driverTestSuite) TestEnqueueHost() {
	suite.mockHostEngine.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		Do(func(hostEntity goalstate.Entity, deadline time.Time) {
			suite.Equal("host", hostEntity.GetID())
		})

	suite.goalStateDriver.EnqueueHost("host", time.Now())
}

// TestDeleteHost tests DeleteHost
func (suite *driverTestSuite) TestDeleteHost() {
	suite.mockHostEngine.EXPECT().Delete(gomock.Any()).
		Do(func(hostEntity goalstate.Entity) {
			suite.Equal("host", hostEntity.GetID())
		})

	suite.goalStateDriver.DeleteHost("host")
}
