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

package apiproxy

import (
	"testing"

	pb_api_v0_host_svc "github.com/uber/peloton/.gen/peloton/api/v0/host/svc"
	"github.com/uber/peloton/pkg/hostmgr/hostsvc"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/api/transport/transporttest"
)

// ProceduresTestSuite is test suite for procedures.
type ProceduresTestSuite struct {
	suite.Suite

	ctrl              *gomock.Controller
	mockUnaryOutbound *transporttest.MockUnaryOutbound

	hostSvcHandler pb_api_v0_host_svc.HostServiceYARPCServer
}

// SetupTest is setup function for each test in this suite.
func (suite *ProceduresTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.mockUnaryOutbound = transporttest.NewMockUnaryOutbound(suite.ctrl)
	suite.hostSvcHandler = hostsvc.NewTestServiceHandler()
}

// TearDownTest is teardown function for each test in this suite.
func (suite *ProceduresTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestProceduresTestSuite runs ProceduresTestSuite.
func TestProceduresTestSuite(t *testing.T) {
	suite.Run(t, new(ProceduresTestSuite))
}

// TestBuildHostServiceProcedures tests building Peloton host service procedures.
func (suite *ProceduresTestSuite) TestBuildHostServiceProcedures() {
	expectedProcedures := pb_api_v0_host_svc.BuildHostServiceYARPCProcedures(suite.hostSvcHandler)

	procedures := BuildHostServiceProcedures(suite.mockUnaryOutbound)

	suite.Equal(len(expectedProcedures), len(procedures))

	expectedService := ""

	epMap := map[string]map[transport.Encoding]struct{}{}
	for _, p := range expectedProcedures {
		if expectedService == "" {
			expectedService = p.Service
		}
		epMap[p.Name] = map[transport.Encoding]struct{}{
			p.Encoding: {},
		}
	}

	pMap := map[string]map[transport.Encoding]struct{}{}
	for _, p := range procedures {
		suite.Equal(expectedService, p.Service)
		pMap[p.Name] = map[transport.Encoding]struct{}{
			p.Encoding: {},
		}
	}

	suite.EqualValues(epMap, pMap)
}
