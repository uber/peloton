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

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	mesos_maintenance "github.com/uber/peloton/.gen/mesos/v1/maintenance"
	mesos_master "github.com/uber/peloton/.gen/mesos/v1/master"
	pbhost "github.com/uber/peloton/.gen/peloton/api/v0/host"

	"github.com/uber/peloton/pkg/common/goalstate"
	goalstatemocks "github.com/uber/peloton/pkg/common/goalstate/mocks"
	"github.com/uber/peloton/pkg/hostmgr/common"
	hpoolmocks "github.com/uber/peloton/pkg/hostmgr/hostpool/manager/mocks"
	mpb_mocks "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
	orm_mocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type actionTestSuite struct {
	suite.Suite
	mockCtrl                 *gomock.Controller
	mockHostEngine           *goalstatemocks.MockEngine
	mockMasterOperatorClient *mpb_mocks.MockMasterOperatorClient
	mockHostInfoOps          *orm_mocks.MockHostInfoOps
	mockHostPoolmgr          *hpoolmocks.MockHostPoolManager
	hostname                 string
	IP                       string
	ctx                      context.Context
	goalStateDriver          *driver
	hostEntity               goalstate.Entity
}

func (suite *actionTestSuite) SetupTest() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockHostEngine = goalstatemocks.NewMockEngine(suite.mockCtrl)
	suite.mockMasterOperatorClient = mpb_mocks.NewMockMasterOperatorClient(suite.mockCtrl)
	suite.mockHostInfoOps = orm_mocks.NewMockHostInfoOps(suite.mockCtrl)
	suite.mockHostPoolmgr = hpoolmocks.NewMockHostPoolManager(suite.mockCtrl)
	suite.hostname = "hostname"
	suite.IP = "IP"
	suite.ctx = context.Background()
	suite.goalStateDriver = &driver{
		hostEngine:        suite.mockHostEngine,
		mesosMasterClient: suite.mockMasterOperatorClient,
		hostInfoOps:       suite.mockHostInfoOps,
		scope:             tally.NoopScope,
		cfg:               &Config{},
		hostPoolMgr:       suite.mockHostPoolmgr,
	}
	suite.hostEntity = &hostEntity{
		hostname: suite.hostname,
		driver:   suite.goalStateDriver,
	}
}

func (suite *actionTestSuite) TearDownTest() {
	suite.mockCtrl.Finish()
}

func TestAction(t *testing.T) {
	suite.Run(t, new(actionTestSuite))
}

// TestHostUntrack tests HostUntrack
func (suite *actionTestSuite) TestHostUntrack() {
	suite.mockHostEngine.EXPECT().
		Delete(gomock.Any())

	HostUntrack(suite.ctx, suite.hostEntity)
}

// TestHostRequeue tests HostRequeue
func (suite *actionTestSuite) TestHostRequeue() {
	suite.mockHostEngine.EXPECT().
		Delete(gomock.Any())
	suite.mockHostEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	HostRequeue(suite.ctx, suite.hostEntity)
}

// TestHostDrain tests HostDrain
func (suite *actionTestSuite) TestHostDrain() {
	hostInfo := &pbhost.HostInfo{
		Hostname:  suite.hostname,
		Ip:        suite.IP,
		State:     pbhost.HostState_HOST_STATE_UP,
		GoalState: pbhost.HostState_HOST_STATE_DOWN,
	}

	hostInfoDiff := common.HostInfoDiff{
		common.StateField: pbhost.HostState_HOST_STATE_DRAINING,
	}

	compareFields := map[string]interface{}{
		common.StateField:     hostInfo.GetState(),
		common.GoalStateField: hostInfo.GetGoalState(),
	}

	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(hostInfo, nil).
		Times(3)
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceSchedule().
		Return(&mesos_master.Response_GetMaintenanceSchedule{
			Schedule: &mesos_maintenance.Schedule{},
		}, nil)
	suite.mockMasterOperatorClient.EXPECT().
		UpdateMaintenanceSchedule(gomock.Any()).
		Return(nil)
	suite.mockHostInfoOps.EXPECT().
		CompareAndSet(gomock.Any(), hostInfo.GetHostname(), gomock.Any(), gomock.Any()).
		Do(func(
			_ context.Context,
			_ string,
			diff common.HostInfoDiff,
			compare map[string]interface{},
		) {
			suite.Len(diff, len(hostInfoDiff))
			for k, v := range diff {
				suite.Equal(hostInfoDiff[k], v)
			}

			suite.Len(compare, len(compareFields))
			for k, v := range compare {
				suite.Equal(compareFields[k], v)
			}
		}).Return(nil)

	suite.NoError(HostDrain(suite.ctx, suite.hostEntity))
}

// TestHostDrainFailure tests HostDrain with failures to read from DB
func (suite *actionTestSuite) TestHostDrainFailureDBRead() {
	// Failure to read from DB
	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("some error")).
		Times(3)

	suite.Error(HostDrain(suite.ctx, suite.hostEntity))
}

// TestHostDrainFailure tests HostDrain with failures to update Mesos Master
func (suite *actionTestSuite) TestHostDrainFailureWithMesosMaster() {
	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(&pbhost.HostInfo{
			Hostname: suite.hostname,
			Ip:       suite.IP,
		}, nil).
		Times(3)
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceSchedule().
		Return(&mesos_master.Response_GetMaintenanceSchedule{
			Schedule: &mesos_maintenance.Schedule{},
		}, nil)
	suite.mockMasterOperatorClient.EXPECT().
		UpdateMaintenanceSchedule(gomock.Any()).
		Return(errors.New("some error"))

	suite.Error(HostDrain(suite.ctx, suite.hostEntity))
}

// TestHostDrainFailure tests HostDrain with failures to write to DB
func (suite *actionTestSuite) TestHostDrainFailureDBWrite() {
	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(&pbhost.HostInfo{
			Hostname: suite.hostname,
			Ip:       suite.IP,
		}, nil).
		Times(3)
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceSchedule().
		Return(&mesos_master.Response_GetMaintenanceSchedule{
			Schedule: &mesos_maintenance.Schedule{},
		}, nil)
	suite.mockMasterOperatorClient.EXPECT().
		UpdateMaintenanceSchedule(gomock.Any()).
		Return(nil)
	suite.mockHostInfoOps.EXPECT().
		CompareAndSet(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("some error"))

	suite.Error(HostDrain(suite.ctx, suite.hostEntity))
}

// TestHostDown tests HostDown
func (suite *actionTestSuite) TestHostDown() {
	hostInfo := &pbhost.HostInfo{
		Hostname:  suite.hostname,
		Ip:        suite.IP,
		State:     pbhost.HostState_HOST_STATE_DRAINED,
		GoalState: pbhost.HostState_HOST_STATE_DOWN,
	}

	hostInfoDiff := common.HostInfoDiff{
		common.StateField: pbhost.HostState_HOST_STATE_DOWN,
	}

	compareFields := map[string]interface{}{
		common.StateField:     hostInfo.GetState(),
		common.GoalStateField: hostInfo.GetGoalState(),
	}

	// Host is part of the draining machines in the cluster status
	clusterStatusAsDraining := &mesos_maintenance.ClusterStatus{
		DrainingMachines: []*mesos_maintenance.ClusterStatus_DrainingMachine{
			{
				Id: &mesos.MachineID{
					Hostname: &suite.hostname,
					Ip:       &suite.IP,
				},
			},
		},
		DownMachines: []*mesos.MachineID{},
	}

	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(hostInfo, nil).
		Times(3)

	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&mesos_master.Response_GetMaintenanceStatus{
			Status: clusterStatusAsDraining,
		}, nil)
	suite.mockMasterOperatorClient.EXPECT().
		StartMaintenance(gomock.Any()).
		Return(nil)

	suite.mockHostInfoOps.EXPECT().
		CompareAndSet(gomock.Any(), hostInfo.GetHostname(), gomock.Any(), gomock.Any()).
		Do(func(
			_ context.Context,
			_ string,
			diff common.HostInfoDiff,
			compare map[string]interface{},
		) {
			suite.Len(diff, len(hostInfoDiff))
			for k, v := range diff {
				suite.Equal(hostInfoDiff[k], v)
			}

			suite.Len(compare, len(compareFields))
			for k, v := range compare {
				suite.Equal(compareFields[k], v)
			}
		}).Return(nil)

	suite.mockHostEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	suite.NoError(HostDown(suite.ctx, suite.hostEntity))
}

// TestHostDownFailure tests HostDown with failure to read from DB
func (suite *actionTestSuite) TestHostDownFailureDBRead() {
	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("some error")).
		Times(3)

	suite.Error(HostDown(suite.ctx, suite.hostEntity))
}

// TestHostDownFailure tests HostDown with failure to read from Mesos Master
func (suite *actionTestSuite) TestHostDownFailureMesosMasterRead() {
	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(&pbhost.HostInfo{
			Hostname: suite.hostname,
			Ip:       suite.IP,
		}, nil).
		Times(3)
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(nil, errors.New("some error"))

	suite.Error(HostDown(suite.ctx, suite.hostEntity))
}

// TestHostDownFailure tests HostDown with failure to write to DB
func (suite *actionTestSuite) TestHostDownFailureDBWrite() {
	// Host is part of the draining machines in the cluster status
	clusterStatusAsDraining := &mesos_maintenance.ClusterStatus{
		DrainingMachines: []*mesos_maintenance.ClusterStatus_DrainingMachine{
			{
				Id: &mesos.MachineID{
					Hostname: &suite.hostname,
					Ip:       &suite.IP,
				},
			},
		},
		DownMachines: []*mesos.MachineID{},
	}

	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(&pbhost.HostInfo{
			Hostname: suite.hostname,
			Ip:       suite.IP,
		}, nil).
		Times(3)
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&mesos_master.Response_GetMaintenanceStatus{
			Status: clusterStatusAsDraining,
		}, nil)
	suite.mockMasterOperatorClient.EXPECT().
		StartMaintenance(gomock.Any()).
		Return(nil)
	suite.mockHostInfoOps.EXPECT().
		CompareAndSet(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("some error"))

	suite.Error(HostDown(suite.ctx, suite.hostEntity))
}

// TestHostUp tests HostUp
func (suite *actionTestSuite) TestHostUp() {
	hostInfo := &pbhost.HostInfo{
		Hostname:  suite.hostname,
		Ip:        suite.IP,
		State:     pbhost.HostState_HOST_STATE_DOWN,
		GoalState: pbhost.HostState_HOST_STATE_UP,
	}

	hostInfoDiff := common.HostInfoDiff{
		common.StateField: pbhost.HostState_HOST_STATE_UP,
	}

	compareFields := map[string]interface{}{
		common.StateField:     hostInfo.GetState(),
		common.GoalStateField: hostInfo.GetGoalState(),
	}

	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(hostInfo, nil).
		Times(3)
	suite.mockMasterOperatorClient.EXPECT().
		StopMaintenance(gomock.Any()).
		Return(nil)
	suite.mockHostInfoOps.EXPECT().
		CompareAndSet(
			gomock.Any(),
			suite.hostname,
			gomock.Any(),
			gomock.Any(),
		).Do(func(
		_ context.Context,
		_ string,
		diff common.HostInfoDiff,
		compare map[string]interface{},
	) {
		suite.Len(diff, len(hostInfoDiff))
		for k, v := range diff {
			suite.Equal(hostInfoDiff[k], v)
		}

		suite.Len(compare, len(compareFields))
		for k, v := range compare {
			suite.Equal(compareFields[k], v)
		}
	}).Return(nil)
	suite.mockHostEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	suite.NoError(HostUp(suite.ctx, suite.hostEntity))
}

// TestHostUpFailure tests HostUp with failure to read from DB
func (suite *actionTestSuite) TestHostUpFailureDBRead() {
	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("some error")).
		Times(3)

	suite.Error(HostUp(suite.ctx, suite.hostEntity))
}

// TestHostUpFailure tests HostUp with failure to read from Mesos Master
func (suite *actionTestSuite) TestHostUpFailureMesosMasterRead() {
	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(&pbhost.HostInfo{
			Hostname: suite.hostname,
			Ip:       suite.IP,
		}, nil).
		Times(3)
	suite.mockMasterOperatorClient.EXPECT().
		StopMaintenance(gomock.Any()).
		Return(errors.New("some error"))

	suite.Error(HostUp(suite.ctx, suite.hostEntity))
}

// TestHostUpFailure tests HostUp with failure to write to DB
func (suite *actionTestSuite) TestHostUpFailureDBWrite() {
	// Failure to write to DB
	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(&pbhost.HostInfo{
			Hostname: suite.hostname,
			Ip:       suite.IP,
		}, nil).
		Times(3)
	suite.mockMasterOperatorClient.EXPECT().
		StopMaintenance(gomock.Any()).
		Return(nil)
	suite.mockHostInfoOps.EXPECT().
		CompareAndSet(gomock.Any(), suite.hostname, gomock.Any(), gomock.Any()).
		Return(errors.New("test error"))

	suite.Error(HostUp(suite.ctx, suite.hostEntity))
}

// TestHostTriggerMaintenanceAction will test the TriggerMaintainance Action
// In the host actions. It will mock DB call with update goal state
func (suite *actionTestSuite) TestHostTriggerMaintenanceAction() {
	hostInfo := &pbhost.HostInfo{
		Hostname:    suite.hostname,
		Ip:          suite.IP,
		State:       pbhost.HostState_HOST_STATE_UP,
		GoalState:   pbhost.HostState_HOST_STATE_UP,
		CurrentPool: "pool1",
		DesiredPool: "pool2",
	}

	hostInfoDiff := common.HostInfoDiff{
		common.GoalStateField: pbhost.HostState_HOST_STATE_DOWN,
	}

	compareFields := map[string]interface{}{
		common.StateField:       hostInfo.GetState(),
		common.GoalStateField:   hostInfo.GetGoalState(),
		common.CurrentPoolField: hostInfo.GetCurrentPool(),
		common.DesiredPoolField: hostInfo.GetDesiredPool(),
	}

	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(hostInfo, nil).
		Times(2)
	suite.mockHostInfoOps.EXPECT().
		CompareAndSet(
			gomock.Any(),
			hostInfo.GetHostname(),
			gomock.Any(),
			gomock.Any(),
		).Do(func(
		_ context.Context,
		_ string,
		diff common.HostInfoDiff,
		compare map[string]interface{},
	) {
		suite.Len(diff, len(hostInfoDiff))
		for k, v := range diff {
			suite.Equal(hostInfoDiff[k], v)
		}

		suite.Len(compare, len(compareFields))
		for k, v := range compare {
			suite.Equal(compareFields[k], v)
		}
	}).Return(nil)
	suite.mockHostEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	suite.NoError(HostTriggerMaintenance(suite.ctx, suite.hostEntity))
}

// TestHostTriggerMaintenanceActionError will test the
// error in TriggerMaintainance Action
func (suite *actionTestSuite) TestHostTriggerMaintenanceActionError() {
	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(&pbhost.HostInfo{
			Hostname: suite.hostname,
			Ip:       suite.IP,
		}, nil).
		Times(2)
	suite.mockHostInfoOps.EXPECT().
		CompareAndSet(gomock.Any(), suite.hostname, gomock.Any(), gomock.Any()).
		Return(errors.New("error"))
	suite.Error(HostTriggerMaintenance(suite.ctx, suite.hostEntity))
}

// TestHostChangePool will test the ChangePool Action
// In the host actions. It will mocj host pool manager for update
// the desired pool.
func (suite *actionTestSuite) TestHostChangePool() {
	hostInfo := &pbhost.HostInfo{
		Hostname:    suite.hostname,
		Ip:          suite.IP,
		State:       pbhost.HostState_HOST_STATE_DOWN,
		GoalState:   pbhost.HostState_HOST_STATE_DOWN,
		DesiredPool: "p1",
		CurrentPool: "p2",
	}

	hostInfoDiff := common.HostInfoDiff{
		common.GoalStateField: pbhost.HostState_HOST_STATE_UP,
	}

	compareFields := map[string]interface{}{
		common.StateField:       hostInfo.GetState(),
		common.GoalStateField:   hostInfo.GetGoalState(),
		common.CurrentPoolField: hostInfo.GetCurrentPool(),
		common.DesiredPoolField: hostInfo.GetDesiredPool(),
	}

	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(hostInfo, nil).
		Times(2)

	suite.mockHostPoolmgr.EXPECT().ChangeHostPool(hostInfo.Hostname,
		hostInfo.CurrentPool, hostInfo.DesiredPool).Return(nil)
	suite.mockHostInfoOps.EXPECT().
		CompareAndSet(
			gomock.Any(),
			hostInfo.GetHostname(),
			gomock.Any(),
			gomock.Any(),
		).Do(func(
		_ context.Context,
		_ string,
		diff common.HostInfoDiff,
		compare map[string]interface{},
	) {
		suite.Len(diff, len(hostInfoDiff))
		for k, v := range diff {
			suite.Equal(hostInfoDiff[k], v)
		}

		suite.Len(compare, len(compareFields))
		for k, v := range compare {
			suite.Equal(compareFields[k], v)
		}
	}).Return(nil)
	suite.mockHostEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())
	suite.NoError(HostChangePool(suite.ctx, suite.hostEntity))
}

// TestHostChangePoolHostPoolManagerError tests the failure case of
// ChangePool Action due to error while setting current pool to desired pool
func (suite *actionTestSuite) TestHostChangePoolHostPoolManagerError() {
	hostInfo := &pbhost.HostInfo{
		Hostname:    suite.hostname,
		Ip:          suite.IP,
		State:       pbhost.HostState_HOST_STATE_DOWN,
		GoalState:   pbhost.HostState_HOST_STATE_DOWN,
		DesiredPool: "p1",
		CurrentPool: "p2",
	}

	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(hostInfo, nil).
		Times(2)
	suite.mockHostPoolmgr.EXPECT().ChangeHostPool(hostInfo.Hostname,
		hostInfo.CurrentPool, hostInfo.DesiredPool).
		Return(errors.New("error"))
	suite.Error(HostChangePool(suite.ctx, suite.hostEntity))
}

// TestHostChangePoolDBWriteError tests the failure case of
// ChangePool Action due to error while writing to DB
func (suite *actionTestSuite) TestHostChangePoolDBWriteError() {
	hostInfo := &pbhost.HostInfo{
		Hostname:    suite.hostname,
		Ip:          suite.IP,
		State:       pbhost.HostState_HOST_STATE_DOWN,
		GoalState:   pbhost.HostState_HOST_STATE_DOWN,
		DesiredPool: "p1",
		CurrentPool: "p2",
	}

	suite.mockHostInfoOps.EXPECT().
		Get(gomock.Any(), gomock.Any()).
		Return(hostInfo, nil).
		Times(2)
	suite.mockHostPoolmgr.EXPECT().ChangeHostPool(hostInfo.Hostname,
		hostInfo.CurrentPool, hostInfo.DesiredPool).Return(nil)
	suite.mockHostInfoOps.EXPECT().
		CompareAndSet(gomock.Any(), hostInfo.GetHostname(), gomock.Any(), gomock.Any()).
		Return(errors.New("error"))

	suite.Error(HostChangePool(suite.ctx, suite.hostEntity))
}
