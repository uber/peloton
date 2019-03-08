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

package mpb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	mesos_maintenance "github.com/uber/peloton/.gen/mesos/v1/maintenance"
	mesos_master "github.com/uber/peloton/.gen/mesos/v1/master"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/util"

	"go.uber.org/yarpc/api/transport"
	transport_mocks "go.uber.org/yarpc/api/transport/transporttest"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber/peloton/.gen/mesos/v1/quota"
)

const (
	mockCaller = "testCall"
	mockSvc    = "testSvc"
)

type masterOperatorClientTestSuite struct {
	suite.Suite

	ctrl                 *gomock.Controller
	masterOperatorClient MasterOperatorClient
	mockUnaryOutbound    *transport_mocks.MockUnaryOutbound
	mockClientCfg        *transport_mocks.MockClientConfig
	defaultEncoding      string
}

func (suite *masterOperatorClientTestSuite) SetupTest() {
	log.Debug("setup")
	suite.ctrl = gomock.NewController(suite.T())
	suite.mockUnaryOutbound = transport_mocks.NewMockUnaryOutbound(suite.ctrl)
	mockClientCfg := transport_mocks.NewMockClientConfig(suite.ctrl)
	suite.mockClientCfg = mockClientCfg
	suite.defaultEncoding = ContentTypeProtobuf
	suite.masterOperatorClient = NewMasterOperatorClient(suite.mockClientCfg, suite.defaultEncoding)
}

func (suite *masterOperatorClientTestSuite) TearDownTest() {
	log.Debug("tear down")
	suite.ctrl.Finish()
}

func mesosResource(name string, value float64) *mesos.Resource {
	scalerType := mesos.Value_SCALAR

	return &mesos.Resource{
		Name: &name,
		Type: &scalerType,
		Scalar: &mesos.Value_Scalar{
			Value: &value,
		},
	}
}

func mesosRole(
	name string,
	weight float64,
	resources []*mesos.Resource,
	frameworkIDs []*mesos.FrameworkID) *mesos.Role {

	return &mesos.Role{
		Name:       &name,
		Weight:     &weight,
		Resources:  resources,
		Frameworks: frameworkIDs,
	}
}

func (suite *masterOperatorClientTestSuite) TestMasterOperatorClient_AllocatedResources() {
	mockValidValue := new(string)
	*mockValidValue = uuid.NIL.String()
	mockNotExistValue := new(string)
	*mockNotExistValue = "do_not_exist"

	mockValidFrameWorkID := &mesos.FrameworkID{
		Value: mockValidValue,
	}

	mockNotExistFrameWorkID := &mesos.FrameworkID{
		Value: mockNotExistValue,
	}

	tests := []struct {
		call        bool
		frameworkID string
		encoding    string
		callResp    proto.Message
		headers     transport.Headers
		errMsg      string
		callErr     bool
	}{
		{
			call:        false,
			frameworkID: *mockValidValue,
			encoding:    "unknown",
			errMsg:      "failed to marshal Call_MasterOperator call: Unsupported contentType unknown",
		},
		{
			call:        false,
			frameworkID: "",
			encoding:    suite.defaultEncoding,
			errMsg:      "frameworkID cannot be empty",
		},
		{
			call:        true,
			frameworkID: *mockValidValue,
			encoding:    suite.defaultEncoding,
			errMsg:      "error making call Call_MasterOperator: connection error",
			callErr:     true,
		},
		{
			call:        true,
			frameworkID: *mockValidValue,
			encoding:    suite.defaultEncoding,
			callResp: &mesos_master.Response{
				GetRoles: &mesos_master.Response_GetRoles{
					Roles: []*mesos.Role{
						mesosRole(
							"peloton",
							1,
							[]*mesos.Resource{
								mesosResource(
									"cpus",
									200,
								),
							},
							[]*mesos.FrameworkID{
								mockValidFrameWorkID,
							},
						),
					},
				},
			},
			headers: transport.NewHeaders().With("a", "b"),
			errMsg:  "",
			callErr: false,
		},
		{
			call:        true,
			frameworkID: *mockValidValue,
			encoding:    suite.defaultEncoding,
			callResp: &mesos_master.Response{
				GetRoles: &mesos_master.Response_GetRoles{
					Roles: []*mesos.Role{
						mesosRole(
							"peloton",
							1,
							[]*mesos.Resource{
								mesosResource(
									"cpus",
									200,
								),
							},
							[]*mesos.FrameworkID{
								mockNotExistFrameWorkID,
							},
						),
					},
				},
			},
			headers: transport.NewHeaders().With("a", "b"),
			errMsg:  "no resources configured",
			callErr: false,
		},
		{
			call:        true,
			frameworkID: *mockValidValue,
			encoding:    suite.defaultEncoding,
			callResp: &mesos_master.Response{
				GetRoles: nil,
			},
			headers: transport.NewHeaders().With("a", "b"),
			errMsg:  "no resources fetched",
			callErr: false,
		},
	}

	for _, tt := range tests {

		// Check YARPC call is needed
		if tt.call {
			// Set expectations
			var err error
			var response *transport.Response

			if tt.callErr {
				err = errors.New("connection error")
			} else {

				wireData, err := proto.Marshal(tt.callResp)
				suite.NoError(err)

				response = &transport.Response{
					Body: ioutil.NopCloser(
						bytes.NewReader(wireData),
					),
					Headers: transport.Headers(tt.headers),
				}
			}

			gomock.InOrder(
				suite.mockClientCfg.EXPECT().Caller().Return(mockCaller),
				suite.mockClientCfg.EXPECT().Service().Return(mockSvc),
				suite.mockClientCfg.EXPECT().GetUnaryOutbound().Return(
					suite.mockUnaryOutbound,
				),

				suite.mockUnaryOutbound.EXPECT().Call(
					gomock.Any(),
					gomock.Any(),
				).Return(
					response,
					err,
				),
			)
		}

		mOClient := NewMasterOperatorClient(suite.mockClientCfg, tt.encoding)
		resources, err := mOClient.AllocatedResources(tt.frameworkID)

		if tt.errMsg != "" {
			suite.EqualError(err, tt.errMsg)
			suite.Nil(resources)
		} else {
			suite.NoError(err)
			suite.NotNil(resources)
		}
	}
}

func (suite *masterOperatorClientTestSuite) TestMasterOperatorClient_TaskAllocation() {

	var response *transport.Response
	frameworkName := "peloton"
	isActive := true

	allocatedResources := []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName(common.MesosCPU).
			WithValue(8).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(common.MesosCPU).
			WithValue(8).
			WithRevocable(&mesos.Resource_RevocableInfo{}).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(common.MesosMem).
			WithValue(20).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(common.MesosDisk).
			WithValue(20).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(common.MesosGPU).
			WithValue(4).
			Build(),
	}

	offeredResources := []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName(common.MesosCPU).
			WithValue(4).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(common.MesosCPU).
			WithValue(4).
			WithRevocable(&mesos.Resource_RevocableInfo{}).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(common.MesosMem).
			WithValue(10).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(common.MesosDisk).
			WithValue(10).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(common.MesosGPU).
			WithValue(2).
			Build(),
	}

	resp := &mesos_master.Response{
		GetFrameworks: &mesos_master.Response_GetFrameworks{
			Frameworks: []*mesos_master.Response_GetFrameworks_Framework{
				{
					FrameworkInfo: &mesos.FrameworkInfo{
						User: &frameworkName,
						Name: &frameworkName,
						Id: &mesos.FrameworkID{
							Value: &frameworkName,
						},
					},
					Active:             &isActive,
					Connected:          &isActive,
					Recovered:          &isActive,
					AllocatedResources: allocatedResources,
					OfferedResources:   offeredResources,
				},
			},
		},
	}

	wireData, err := proto.Marshal(resp)
	suite.NoError(err)

	response = &transport.Response{
		Body: ioutil.NopCloser(
			bytes.NewReader(wireData),
		),
		Headers: transport.Headers(transport.NewHeaders().With("a", "b")),
	}

	gomock.InOrder(
		suite.mockClientCfg.EXPECT().Caller().Return(mockCaller),
		suite.mockClientCfg.EXPECT().Service().Return(mockSvc),
		suite.mockClientCfg.EXPECT().GetUnaryOutbound().Return(
			suite.mockUnaryOutbound,
		),

		suite.mockUnaryOutbound.EXPECT().Call(
			gomock.Any(),
			gomock.Any(),
		).Return(
			response,
			nil,
		),
	)

	mOClient := NewMasterOperatorClient(suite.mockClientCfg, suite.defaultEncoding)
	allocated, offered, err := mOClient.GetTasksAllocation("peloton")
	suite.Equal(len(allocatedResources), len(allocated))
	suite.Equal(len(offeredResources), len(offered))
	suite.NoError(err)
}

func (suite *masterOperatorClientTestSuite) TestMasterOperatorClient_GetMaintenanceSchedule() {
	testHost := "hostname"
	testIP := "0.0.0.0"
	testMachines := []*mesos.MachineID{
		{
			Hostname: &testHost,
			Ip:       &testIP,
		},
	}

	nanos := int64(time.Now().Nanosecond())
	startTime := &mesos.TimeInfo{
		Nanoseconds: &nanos,
	}
	windows := []*mesos_maintenance.Window{
		{
			MachineIds: testMachines,
			Unavailability: &mesos.Unavailability{
				Start: startTime,
			},
		},
	}

	schedule := &mesos_maintenance.Schedule{
		Windows: windows,
	}

	callResp := &mesos_master.Response{
		GetMaintenanceSchedule: &mesos_master.Response_GetMaintenanceSchedule{
			Schedule: schedule,
		},
	}
	wireData, err := proto.Marshal(callResp)
	suite.NoError(err)

	response := &transport.Response{
		Body: ioutil.NopCloser(
			bytes.NewReader(wireData),
		),
		Headers: transport.NewHeaders().With("a", "b"),
	}
	gomock.InOrder(
		suite.mockClientCfg.EXPECT().Caller().Return(mockCaller),
		suite.mockClientCfg.EXPECT().Service().Return(mockSvc),
		suite.mockClientCfg.EXPECT().GetUnaryOutbound().Return(
			suite.mockUnaryOutbound,
		),

		suite.mockUnaryOutbound.EXPECT().Call(
			gomock.Any(),
			gomock.Any(),
		).Return(
			response,
			nil,
		),
	)
	responseGetMaintenanceSchedule, err := suite.masterOperatorClient.GetMaintenanceSchedule()
	suite.NoError(err)
	suite.NotNil(responseGetMaintenanceSchedule)
	responseSchedule := responseGetMaintenanceSchedule.GetSchedule()
	suite.Equal(schedule, responseSchedule)

	// Test error
	gomock.InOrder(
		suite.mockClientCfg.EXPECT().Caller().Return(mockCaller),
		suite.mockClientCfg.EXPECT().Service().Return(mockSvc),
		suite.mockClientCfg.EXPECT().GetUnaryOutbound().Return(
			suite.mockUnaryOutbound,
		),

		suite.mockUnaryOutbound.EXPECT().Call(
			gomock.Any(),
			gomock.Any(),
		).Return(
			nil,
			fmt.Errorf("fake Call error"),
		),
	)
	responseGetMaintenanceSchedule, err = suite.masterOperatorClient.GetMaintenanceSchedule()
	suite.Error(err)
	suite.Nil(responseGetMaintenanceSchedule)
}

func (suite *masterOperatorClientTestSuite) TestMasterOperatorClient_GetMaintenanceStatus() {
	testHost1 := "hostname"
	testIP1 := "0.0.0.0"
	drainingMachines := []*mesos_maintenance.ClusterStatus_DrainingMachine{
		{
			Id: &mesos.MachineID{
				Hostname: &testHost1,
				Ip:       &testIP1,
			},
		},
	}

	testHost2 := "testhost"
	testIP2 := "172.17.0.6"
	downMachines := []*mesos.MachineID{
		{
			Hostname: &testHost2,
			Ip:       &testIP2,
		},
	}

	clusterStatus := &mesos_maintenance.ClusterStatus{
		DrainingMachines: drainingMachines,
		DownMachines:     downMachines,
	}

	callResp := &mesos_master.Response{
		GetMaintenanceStatus: &mesos_master.Response_GetMaintenanceStatus{
			Status: clusterStatus,
		},
	}
	wireData, err := proto.Marshal(callResp)
	suite.NoError(err)

	response := &transport.Response{
		Body: ioutil.NopCloser(
			bytes.NewReader(wireData),
		),
		Headers: transport.NewHeaders().With("a", "b"),
	}
	gomock.InOrder(
		suite.mockClientCfg.EXPECT().Caller().Return(mockCaller),
		suite.mockClientCfg.EXPECT().Service().Return(mockSvc),
		suite.mockClientCfg.EXPECT().GetUnaryOutbound().Return(
			suite.mockUnaryOutbound,
		),

		suite.mockUnaryOutbound.EXPECT().Call(
			gomock.Any(),
			gomock.Any(),
		).Return(
			response,
			nil,
		),
	)
	responseGetMaintenanceStatus, err := suite.masterOperatorClient.GetMaintenanceStatus()
	suite.NoError(err)
	suite.NotNil(responseGetMaintenanceStatus)
	responseMaintenanceStatus := responseGetMaintenanceStatus.GetStatus()
	suite.Equal(clusterStatus, responseMaintenanceStatus)

	// Test error
	gomock.InOrder(
		suite.mockClientCfg.EXPECT().Caller().Return(mockCaller),
		suite.mockClientCfg.EXPECT().Service().Return(mockSvc),
		suite.mockClientCfg.EXPECT().GetUnaryOutbound().Return(
			suite.mockUnaryOutbound,
		),

		suite.mockUnaryOutbound.EXPECT().Call(
			gomock.Any(),
			gomock.Any(),
		).Return(
			nil,
			fmt.Errorf("fake Call error"),
		),
	)
	responseGetMaintenanceStatus, err = suite.masterOperatorClient.GetMaintenanceStatus()
	suite.Error(err)
	suite.Nil(responseGetMaintenanceStatus)
}

func (suite *masterOperatorClientTestSuite) TestMasterOperatorClient_StartMaintenance() {
	testMachines := []struct {
		host string
		ip   string
	}{
		{
			host: "host1",
			ip:   "172.17.0.6",
		},
	}

	var testMachineIDs []*mesos.MachineID
	for _, machine := range testMachines {
		testMachineIDs = append(testMachineIDs, &mesos.MachineID{
			Hostname: &machine.host,
			Ip:       &machine.ip,
		})
	}

	wireData := []byte{}

	response := &transport.Response{
		Body: ioutil.NopCloser(
			bytes.NewReader(wireData),
		),
		Headers: transport.NewHeaders().With("a", "b"),
	}
	gomock.InOrder(
		suite.mockClientCfg.EXPECT().Caller().Return(mockCaller),
		suite.mockClientCfg.EXPECT().Service().Return(mockSvc),
		suite.mockClientCfg.EXPECT().GetUnaryOutbound().Return(
			suite.mockUnaryOutbound,
		),

		suite.mockUnaryOutbound.EXPECT().Call(
			gomock.Any(),
			gomock.Any(),
		).Return(
			response,
			nil,
		),
	)
	err := suite.masterOperatorClient.StartMaintenance(testMachineIDs)
	suite.NoError(err)

	// Test error
	gomock.InOrder(
		suite.mockClientCfg.EXPECT().Caller().Return(mockCaller),
		suite.mockClientCfg.EXPECT().Service().Return(mockSvc),
		suite.mockClientCfg.EXPECT().GetUnaryOutbound().Return(
			suite.mockUnaryOutbound,
		),

		suite.mockUnaryOutbound.EXPECT().Call(
			gomock.Any(),
			gomock.Any(),
		).Return(
			nil,
			fmt.Errorf("fake Call error"),
		),
	)
	err = suite.masterOperatorClient.StartMaintenance(testMachineIDs)
	suite.Error(err)
}

func (suite *masterOperatorClientTestSuite) TestMasterOperatorClient_StopMaintenance() {
	testMachines := []struct {
		host string
		ip   string
	}{
		{
			host: "host1",
			ip:   "172.17.0.6",
		},
	}

	var testMachineIDs []*mesos.MachineID
	for _, machine := range testMachines {
		testMachineIDs = append(testMachineIDs, &mesos.MachineID{
			Hostname: &machine.host,
			Ip:       &machine.ip,
		})
	}

	wireData := []byte{}

	response := &transport.Response{
		Body: ioutil.NopCloser(
			bytes.NewReader(wireData),
		),
		Headers: transport.NewHeaders().With("a", "b"),
	}
	gomock.InOrder(
		suite.mockClientCfg.EXPECT().Caller().Return(mockCaller),
		suite.mockClientCfg.EXPECT().Service().Return(mockSvc),
		suite.mockClientCfg.EXPECT().GetUnaryOutbound().Return(
			suite.mockUnaryOutbound,
		),

		suite.mockUnaryOutbound.EXPECT().Call(
			gomock.Any(),
			gomock.Any(),
		).Return(
			response,
			nil,
		),
	)
	err := suite.masterOperatorClient.StopMaintenance(testMachineIDs)
	suite.NoError(err)

	// Test error
	gomock.InOrder(
		suite.mockClientCfg.EXPECT().Caller().Return(mockCaller),
		suite.mockClientCfg.EXPECT().Service().Return(mockSvc),
		suite.mockClientCfg.EXPECT().GetUnaryOutbound().Return(
			suite.mockUnaryOutbound,
		),

		suite.mockUnaryOutbound.EXPECT().Call(
			gomock.Any(),
			gomock.Any(),
		).Return(
			nil,
			fmt.Errorf("fake Call error"),
		),
	)
	err = suite.masterOperatorClient.StopMaintenance(testMachineIDs)
	suite.Error(err)
}

func (suite *masterOperatorClientTestSuite) TestMasterOperatorClient_UpdateMaintenanceSchedule() {
	testMachines := []struct {
		host string
		ip   string
	}{
		{
			host: "host1",
			ip:   "172.17.0.6",
		},
	}

	var testMachineIDs []*mesos.MachineID
	for _, machine := range testMachines {
		testMachineIDs = append(testMachineIDs, &mesos.MachineID{
			Hostname: &machine.host,
			Ip:       &machine.ip,
		})
	}

	nano := int64(time.Now().Nanosecond())
	schedule := &mesos_maintenance.Schedule{
		Windows: []*mesos_maintenance.Window{
			{
				MachineIds: testMachineIDs,
				Unavailability: &mesos.Unavailability{
					Start: &mesos.TimeInfo{
						Nanoseconds: &nano,
					},
				},
			},
		},
	}
	wireData := []byte{}

	response := &transport.Response{
		Body: ioutil.NopCloser(
			bytes.NewReader(wireData),
		),
		Headers: transport.NewHeaders().With("a", "b"),
	}
	gomock.InOrder(
		suite.mockClientCfg.EXPECT().Caller().Return(mockCaller),
		suite.mockClientCfg.EXPECT().Service().Return(mockSvc),
		suite.mockClientCfg.EXPECT().GetUnaryOutbound().Return(
			suite.mockUnaryOutbound,
		),

		suite.mockUnaryOutbound.EXPECT().Call(
			gomock.Any(),
			gomock.Any(),
		).Return(
			response,
			nil,
		),
	)
	err := suite.masterOperatorClient.UpdateMaintenanceSchedule(schedule)
	suite.NoError(err)

	// Test error
	gomock.InOrder(
		suite.mockClientCfg.EXPECT().Caller().Return(mockCaller),
		suite.mockClientCfg.EXPECT().Service().Return(mockSvc),
		suite.mockClientCfg.EXPECT().GetUnaryOutbound().Return(
			suite.mockUnaryOutbound,
		),

		suite.mockUnaryOutbound.EXPECT().Call(
			gomock.Any(),
			gomock.Any(),
		).Return(
			nil,
			fmt.Errorf("fake Call error"),
		),
	)
	err = suite.masterOperatorClient.UpdateMaintenanceSchedule(schedule)
	suite.Error(err)
}

func (suite *masterOperatorClientTestSuite) TestMasterOperatorClient_GetQuota() {
	testRole := "testrole"
	callResp := &mesos_master.Response{
		GetQuota: &mesos_master.Response_GetQuota{
			Status: &mesos_v1_quota.QuotaStatus{
				Infos: []*mesos_v1_quota.QuotaInfo{
					{
						Role:      &testRole,
						Guarantee: []*mesos.Resource{},
					},
				},
			},
		},
	}
	wireData, err := proto.Marshal(callResp)
	suite.NoError(err)

	response := &transport.Response{
		Body: ioutil.NopCloser(
			bytes.NewReader(wireData),
		),
		Headers: transport.NewHeaders().With("a", "b"),
	}
	gomock.InOrder(
		suite.mockClientCfg.EXPECT().Caller().Return(mockCaller),
		suite.mockClientCfg.EXPECT().Service().Return(mockSvc),
		suite.mockClientCfg.EXPECT().GetUnaryOutbound().Return(
			suite.mockUnaryOutbound,
		),

		suite.mockUnaryOutbound.EXPECT().Call(
			gomock.Any(),
			gomock.Any(),
		).Return(
			response,
			nil,
		),
	)
	resources, err := suite.masterOperatorClient.GetQuota(testRole)
	suite.NoError(err)
	suite.Nil(resources)
}

func TestMasterOperatorClientTestSuite(t *testing.T) {
	suite.Run(t, new(masterOperatorClientTestSuite))
}
