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

package hostmgr

import (
	"fmt"
	"testing"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	mesos_maintenance "github.com/uber/peloton/.gen/mesos/v1/maintenance"
	mesos_master "github.com/uber/peloton/.gen/mesos/v1/master"
	hpb "github.com/uber/peloton/.gen/peloton/api/v0/host"

	"github.com/uber/peloton/hostmgr/host"
	"github.com/uber/peloton/hostmgr/queue/mocks"
	mpb_mocks "github.com/uber/peloton/yarpc/encoding/mpb/mocks"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type RecoveryTestSuite struct {
	suite.Suite
	mockCtrl                 *gomock.Controller
	recoveryHandler          RecoveryHandler
	mockMaintenanceQueue     *mocks.MockMaintenanceQueue
	mockMasterOperatorClient *mpb_mocks.MockMasterOperatorClient
	drainingMachines         []*mesos.MachineID
	downMachines             []*mesos.MachineID
	maintenanceHostInfoMap   host.MaintenanceHostInfoMap
}

func (suite *RecoveryTestSuite) SetupSuite() {
	drainingHostname := "draininghost"
	drainingIP := "172.17.0.6"
	drainingMachine := &mesos.MachineID{
		Hostname: &drainingHostname,
		Ip:       &drainingIP,
	}
	suite.drainingMachines = append(suite.drainingMachines, drainingMachine)

	downHostname := "downhost"
	downIP := "172.17.0.7"
	downMachine := &mesos.MachineID{
		Hostname: &downHostname,
		Ip:       &downIP,
	}

	suite.downMachines = append(suite.downMachines, downMachine)
}

func (suite *RecoveryTestSuite) SetupTest() {
	log.Info("setting up test")
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockMaintenanceQueue = mocks.NewMockMaintenanceQueue(suite.mockCtrl)
	suite.mockMasterOperatorClient = mpb_mocks.NewMockMasterOperatorClient(suite.mockCtrl)

	suite.maintenanceHostInfoMap = host.NewMaintenanceHostInfoMap()
	suite.recoveryHandler = NewRecoveryHandler(tally.NoopScope,
		suite.mockMaintenanceQueue,
		suite.mockMasterOperatorClient,
		suite.maintenanceHostInfoMap)
}

func (suite *RecoveryTestSuite) TearDownTest() {
	log.Info("tearing down test")
	suite.mockCtrl.Finish()
}

func TestHostmgrRecovery(t *testing.T) {
	suite.Run(t, new(RecoveryTestSuite))
}

func (suite *RecoveryTestSuite) TestStart() {
	var clusterDrainingMachines []*mesos_maintenance.ClusterStatus_DrainingMachine
	for _, drainingMachine := range suite.drainingMachines {
		clusterDrainingMachines = append(clusterDrainingMachines,
			&mesos_maintenance.ClusterStatus_DrainingMachine{
				Id: drainingMachine,
			})
	}

	clusterStatus := &mesos_maintenance.ClusterStatus{
		DrainingMachines: clusterDrainingMachines,
		DownMachines:     suite.downMachines,
	}

	suite.mockMaintenanceQueue.EXPECT().Clear()
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().Return(&mesos_master.Response_GetMaintenanceStatus{
		Status: clusterStatus,
	}, nil)

	var drainingHostnames []string
	for _, machine := range suite.drainingMachines {
		drainingHostnames = append(drainingHostnames, machine.GetHostname())
	}
	suite.mockMaintenanceQueue.EXPECT().
		Enqueue(gomock.Any()).
		Return(nil).Do(func(hostnames []string) {
		suite.EqualValues(drainingHostnames, hostnames)
	})
	err := suite.recoveryHandler.Start()
	suite.NoError(err)

	drainingHostInfoMap := make(map[string]*hpb.HostInfo)
	for _, hostInfo := range suite.maintenanceHostInfoMap.GetDrainingHostInfos([]string{}) {
		drainingHostInfoMap[hostInfo.GetHostname()] = hostInfo
	}
	for _, drainingMachine := range suite.drainingMachines {
		hostInfo := drainingHostInfoMap[drainingMachine.GetHostname()]
		suite.NotNil(hostInfo)
		suite.Equal(drainingMachine.GetHostname(), hostInfo.GetHostname())
		suite.Equal(drainingMachine.GetIp(), hostInfo.GetIp())
		suite.Equal(hpb.HostState_HOST_STATE_DRAINING, hostInfo.GetState())
	}

	downHostInfoMap := make(map[string]*hpb.HostInfo)
	for _, hostInfo := range suite.maintenanceHostInfoMap.GetDownHostInfos([]string{}) {
		downHostInfoMap[hostInfo.GetHostname()] = hostInfo
	}
	for _, downMachine := range suite.downMachines {
		hostInfo := downHostInfoMap[downMachine.GetHostname()]
		suite.NotNil(hostInfo)
		suite.Equal(downMachine.GetHostname(), hostInfo.GetHostname())
		suite.Equal(downMachine.GetIp(), hostInfo.GetIp())
		suite.Equal(hpb.HostState_HOST_STATE_DOWN, hostInfo.GetState())
	}
}

func (suite *RecoveryTestSuite) TestStart_Error() {
	suite.mockMaintenanceQueue.EXPECT().Clear()
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(nil, fmt.Errorf("Fake GetMaintenance error"))

	err := suite.recoveryHandler.Start()
	suite.Error(err)
}

func (suite *RecoveryTestSuite) TestStop() {
	err := suite.recoveryHandler.Stop()
	suite.NoError(err)
}
