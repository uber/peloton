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

package host

import (
	"fmt"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	mesos_maintenance "github.com/uber/peloton/.gen/mesos/v1/maintenance"
	mesos_master "github.com/uber/peloton/.gen/mesos/v1/master"
	host "github.com/uber/peloton/.gen/peloton/api/v0/host"

	"github.com/uber/peloton/common/lifecycle"
	mq_mocks "github.com/uber/peloton/hostmgr/queue/mocks"
	mpb_mocks "github.com/uber/peloton/yarpc/encoding/mpb/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

const (
	drainerPeriod = 100 * time.Millisecond
)

type DrainerTestSuite struct {
	suite.Suite
	drainer                  *drainer
	mockCtrl                 *gomock.Controller
	mockMasterOperatorClient *mpb_mocks.MockMasterOperatorClient
	mockMaintenanceQueue     *mq_mocks.MockMaintenanceQueue
	drainingMachines         []*mesos.MachineID
	downMachines             []*mesos.MachineID
}

func (suite *DrainerTestSuite) SetupSuite() {
	testDownMachines := []struct {
		host string
		ip   string
	}{
		{
			host: "host2",
			ip:   "172.17.0.6",
		},
	}
	for _, test := range testDownMachines {
		suite.downMachines = append(suite.downMachines, &mesos.MachineID{
			Hostname: &test.host,
			Ip:       &test.ip,
		})
	}

	testDrainingMachines := []struct {
		host string
		ip   string
	}{
		{
			host: "host3",
			ip:   "172.17.0.7",
		},
	}
	for _, test := range testDrainingMachines {
		suite.drainingMachines = append(
			suite.drainingMachines,
			&mesos.MachineID{
				Hostname: &test.host,
				Ip:       &test.ip,
			})
	}
}

func (suite *DrainerTestSuite) SetupTest() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockMasterOperatorClient = mpb_mocks.NewMockMasterOperatorClient(suite.mockCtrl)
	suite.mockMaintenanceQueue = mq_mocks.NewMockMaintenanceQueue(suite.mockCtrl)
	suite.drainer = &drainer{
		drainerPeriod:          drainerPeriod,
		masterOperatorClient:   suite.mockMasterOperatorClient,
		maintenanceQueue:       suite.mockMaintenanceQueue,
		lifecycle:              lifecycle.NewLifeCycle(),
		maintenanceHostInfoMap: NewMaintenanceHostInfoMap(),
	}
}

func (suite *DrainerTestSuite) TearDownTest() {
	suite.mockCtrl.Finish()
}

func TestDrainer(t *testing.T) {
	suite.Run(t, new(DrainerTestSuite))
}

//TestNewDrainer test creation of new host drainer
func (suite *DrainerTestSuite) TestNewDrainer() {
	drainer := NewDrainer(drainerPeriod,
		suite.mockMasterOperatorClient,
		suite.mockMaintenanceQueue,
		NewMaintenanceHostInfoMap())
	suite.NotNil(drainer)
}

// TestStart tests starting the host drainer
func (suite *DrainerTestSuite) TestStart() {
	response := mesos_master.Response_GetMaintenanceStatus{
		Status: &mesos_maintenance.ClusterStatus{
			DrainingMachines: []*mesos_maintenance.ClusterStatus_DrainingMachine{},
			DownMachines:     suite.downMachines,
		},
	}
	for _, drainingMachine := range suite.drainingMachines {
		response.Status.DrainingMachines = append(
			response.Status.DrainingMachines,
			&mesos_maintenance.ClusterStatus_DrainingMachine{
				Id: &mesos.MachineID{
					Hostname: drainingMachine.Hostname,
					Ip:       drainingMachine.Ip,
				},
			})
	}
	var drainingHostnames []string
	for _, machine := range suite.drainingMachines {
		drainingHostnames = append(drainingHostnames, machine.GetHostname())
	}
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&response, nil).
		MinTimes(1).
		MaxTimes(2)
	suite.mockMaintenanceQueue.EXPECT().
		Enqueue(drainingHostnames).
		Return(nil).
		MinTimes(1).
		MaxTimes(2)
	suite.drainer.Start()
	// Starting drainer again should be no-op
	suite.drainer.Start()
	time.Sleep(2 * drainerPeriod)
	drainingHostInfoMap := make(map[string]*host.HostInfo)
	for _, hostInfo := range suite.drainer.maintenanceHostInfoMap.GetDrainingHostInfos([]string{}) {
		drainingHostInfoMap[hostInfo.GetHostname()] = hostInfo
	}
	for _, drainingMachine := range suite.drainingMachines {
		hostInfo := drainingHostInfoMap[drainingMachine.GetHostname()]
		suite.NotNil(hostInfo)
		suite.Equal(drainingMachine.GetHostname(), hostInfo.GetHostname())
		suite.Equal(drainingMachine.GetIp(), hostInfo.GetIp())
		suite.Equal(host.HostState_HOST_STATE_DRAINING, hostInfo.GetState())
	}

	downHostInfoMap := make(map[string]*host.HostInfo)
	for _, hostInfo := range suite.drainer.maintenanceHostInfoMap.GetDownHostInfos([]string{}) {
		downHostInfoMap[hostInfo.GetHostname()] = hostInfo
	}
	for _, downMachine := range suite.downMachines {
		hostInfo := downHostInfoMap[downMachine.GetHostname()]
		suite.NotNil(hostInfo)
		suite.Equal(downMachine.GetHostname(), hostInfo.GetHostname())
		suite.Equal(downMachine.GetIp(), hostInfo.GetIp())
		suite.Equal(host.HostState_HOST_STATE_DOWN, hostInfo.GetState())
	}
	suite.drainer.Stop()

	// Test GetMaintenanceStatus error
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(nil, fmt.Errorf("Fake GetMaintenanceStatus error")).
		MinTimes(1).
		MaxTimes(2)
	suite.drainer.Start()
	time.Sleep(2 * drainerPeriod)
	suite.drainer.Stop()

	// Test Enqueue error
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&response, nil).
		MinTimes(1).
		MaxTimes(2)
	suite.mockMaintenanceQueue.EXPECT().
		Enqueue(drainingHostnames).
		Return(fmt.Errorf("Fake Enqueue error")).
		MinTimes(1).
		MaxTimes(2)
	suite.drainer.Start()
	time.Sleep(2 * drainerPeriod)
	suite.drainer.Stop()
}

// TestStop tests stopping the host drainer
func (suite *DrainerTestSuite) TestStop() {
	suite.drainer.Stop()
	<-suite.drainer.lifecycle.StopCh()
}
