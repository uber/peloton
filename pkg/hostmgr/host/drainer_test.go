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

	"github.com/uber/peloton/pkg/common/lifecycle"
	host_mocks "github.com/uber/peloton/pkg/hostmgr/host/mocks"
	mpb_mocks "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
	mq_mocks "github.com/uber/peloton/pkg/hostmgr/queue/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

const (
	drainerPeriod = 100 * time.Millisecond
)

type drainerTestSuite struct {
	suite.Suite
	drainer                  *drainer
	mockCtrl                 *gomock.Controller
	mockMasterOperatorClient *mpb_mocks.MockMasterOperatorClient
	mockMaintenanceQueue     *mq_mocks.MockMaintenanceQueue
	mockMaintenanceMap       *host_mocks.MockMaintenanceHostInfoMap
	drainingMachines         []*mesos.MachineID
	downMachines             []*mesos.MachineID
	hostInfos                []*host.HostInfo
}

func (suite *drainerTestSuite) SetupSuite() {
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

	for _, drainingMachine := range suite.drainingMachines {
		suite.hostInfos = append(suite.hostInfos,
			&host.HostInfo{
				Hostname: drainingMachine.GetHostname(),
				Ip:       drainingMachine.GetIp(),
				State:    host.HostState_HOST_STATE_DRAINING,
			})
	}

	for _, downMachine := range suite.downMachines {
		suite.hostInfos = append(suite.hostInfos,
			&host.HostInfo{
				Hostname: downMachine.GetHostname(),
				Ip:       downMachine.GetIp(),
				State:    host.HostState_HOST_STATE_DOWN,
			})
	}
}

func (suite *drainerTestSuite) SetupTest() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockMasterOperatorClient = mpb_mocks.NewMockMasterOperatorClient(suite.mockCtrl)
	suite.mockMaintenanceQueue = mq_mocks.NewMockMaintenanceQueue(suite.mockCtrl)
	suite.mockMaintenanceMap = host_mocks.NewMockMaintenanceHostInfoMap(suite.mockCtrl)

	suite.drainer = &drainer{
		drainerPeriod:          drainerPeriod,
		masterOperatorClient:   suite.mockMasterOperatorClient,
		maintenanceQueue:       suite.mockMaintenanceQueue,
		lifecycle:              lifecycle.NewLifeCycle(),
		maintenanceHostInfoMap: suite.mockMaintenanceMap,
	}
}

func (suite *drainerTestSuite) TearDownTest() {
	suite.mockCtrl.Finish()
}

func TestDrainer(t *testing.T) {
	suite.Run(t, new(drainerTestSuite))
}

//TestNewDrainer test creation of new host drainer
func (suite *drainerTestSuite) TestDrainerNewDrainer() {
	drainer := NewDrainer(drainerPeriod,
		suite.mockMasterOperatorClient,
		suite.mockMaintenanceQueue,
		host_mocks.NewMockMaintenanceHostInfoMap(suite.mockCtrl))
	suite.NotNil(drainer)
}

// TestDrainerStartSuccess tests the success case of starting the host drainer
func (suite *drainerTestSuite) TestDrainerStartSuccess() {
	response := mesos_master.Response_GetMaintenanceStatus{
		Status: &mesos_maintenance.ClusterStatus{
			DrainingMachines: []*mesos_maintenance.ClusterStatus_DrainingMachine{},
			DownMachines:     suite.downMachines,
		},
	}

	var drainingHostnames []string
	for _, drainingMachine := range suite.drainingMachines {
		response.Status.DrainingMachines = append(
			response.Status.DrainingMachines,
			&mesos_maintenance.ClusterStatus_DrainingMachine{
				Id: &mesos.MachineID{
					Hostname: drainingMachine.Hostname,
					Ip:       drainingMachine.Ip,
				},
			})

		drainingHostnames = append(drainingHostnames, drainingMachine.GetHostname())
	}

	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&response, nil).
		MinTimes(1).
		MaxTimes(2)

	suite.mockMaintenanceMap.EXPECT().
		ClearAndFillMap(suite.hostInfos).
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
	suite.drainer.Stop()
}

// TestDrainerStartGetMaintenanceStatusFailure tests the failure case of
// starting the host drainer due to error while getting maintenance status
func (suite *drainerTestSuite) TestDrainerStartGetMaintenanceStatusFailure() {
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(nil, fmt.Errorf("Fake GetMaintenanceStatus error")).
		MinTimes(1).
		MaxTimes(2)

	suite.drainer.Start()
	time.Sleep(2 * drainerPeriod)
	suite.drainer.Stop()
}

// TestDrainerStartEnqueueFailure tests the failure case of starting the
// host drainer due to error while enqueuing hostnames into maintenance queue
func (suite *drainerTestSuite) TestDrainerStartEnqueueFailure() {
	var drainingHostnames []string
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

		drainingHostnames = append(drainingHostnames,
			drainingMachine.GetHostname())
	}

	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&response, nil).
		MinTimes(1).
		MaxTimes(2)

	suite.mockMaintenanceMap.EXPECT().
		ClearAndFillMap(suite.hostInfos).
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
func (suite *drainerTestSuite) TestStop() {
	suite.drainer.Stop()
	<-suite.drainer.lifecycle.StopCh()
}
