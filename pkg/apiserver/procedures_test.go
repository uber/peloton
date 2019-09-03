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

package apiserver

import (
	"testing"

	pbv0hostsvc "github.com/uber/peloton/.gen/peloton/api/v0/host/svc"
	pbv0jobmgr "github.com/uber/peloton/.gen/peloton/api/v0/job"
	pbv0jobsvc "github.com/uber/peloton/.gen/peloton/api/v0/job/svc"
	pbv0resmgr "github.com/uber/peloton/.gen/peloton/api/v0/respool"
	pbv0respoolsvc "github.com/uber/peloton/.gen/peloton/api/v0/respool/svc"
	pbv0taskmgr "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbv0tasksvc "github.com/uber/peloton/.gen/peloton/api/v0/task/svc"
	pbv0updatesvc "github.com/uber/peloton/.gen/peloton/api/v0/update/svc"
	pbv0volumesvc "github.com/uber/peloton/.gen/peloton/api/v0/volume/svc"
	pbv1alphaadminsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/admin/svc"
	pbv1alphahostsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/host/svc"
	pbv1alphajobstatelesssvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	pbv1alphapodsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc"
	pbv1alpharespoolsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/respool/svc"
	pbv1alphawatchsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/watch/svc"
	pbprivateeventstreamsvc "github.com/uber/peloton/.gen/peloton/private/eventstream/v1alpha/eventstreamsvc"
	pbprivatehostsvc "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	pbprivatehostmgrsvc "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha/svc"
	pbprivatejobmgrsvc "github.com/uber/peloton/.gen/peloton/private/jobmgrsvc"
	pbprivatetaskqueue "github.com/uber/peloton/.gen/peloton/private/resmgr/taskqueue"
	pbprivateresmgrsvc "github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/api/transport/transporttest"
)

// ProceduresTestSuite is test suite for procedures.
type ProceduresTestSuite struct {
	suite.Suite

	ctrl               *gomock.Controller
	mockUnaryOutbound  *transporttest.MockUnaryOutbound
	mockStreamOutbound *transporttest.MockStreamOutbound
}

// SetupTest is setup function for each test in this suite.
func (suite *ProceduresTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.mockUnaryOutbound = transporttest.NewMockUnaryOutbound(suite.ctrl)
	suite.mockStreamOutbound = transporttest.NewMockStreamOutbound(suite.ctrl)
}

// TearDownTest is teardown function for each test in this suite.
func (suite *ProceduresTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestProceduresTestSuite runs ProceduresTestSuite.
func TestProceduresTestSuite(t *testing.T) {
	suite.Run(t, new(ProceduresTestSuite))
}

// TestBuildJobManagerProcedures tests building Peloton Host Manager procedures.
func (suite *ProceduresTestSuite) TestBuildJobManagerProcedures() {
	expectedProcedures :=
		pbv0jobmgr.BuildJobManagerYARPCProcedures(nil)
	expectedProcedures = append(
		expectedProcedures,
		pbv0jobsvc.BuildJobServiceYARPCProcedures(nil)...,
	)
	expectedProcedures = append(
		expectedProcedures,
		pbv0taskmgr.BuildTaskManagerYARPCProcedures(nil)...,
	)
	expectedProcedures = append(
		expectedProcedures,
		pbv0tasksvc.BuildTaskServiceYARPCProcedures(nil)...,
	)
	expectedProcedures = append(
		expectedProcedures,
		pbv0updatesvc.BuildUpdateServiceYARPCProcedures(nil)...,
	)
	expectedProcedures = append(
		expectedProcedures,
		pbv0volumesvc.BuildVolumeServiceYARPCProcedures(nil)...,
	)
	expectedProcedures = append(
		expectedProcedures,
		pbv1alphaadminsvc.BuildAdminServiceYARPCProcedures(nil)...,
	)
	expectedProcedures = append(
		expectedProcedures,
		pbv1alphajobstatelesssvc.BuildJobServiceYARPCProcedures(nil)...,
	)
	expectedProcedures = append(
		expectedProcedures,
		pbv1alphapodsvc.BuildPodServiceYARPCProcedures(nil)...,
	)
	expectedProcedures = append(
		expectedProcedures,
		pbv1alphawatchsvc.BuildWatchServiceYARPCProcedures(nil)...,
	)
	expectedProcedures = append(
		expectedProcedures,
		pbprivatejobmgrsvc.BuildJobManagerServiceYARPCProcedures(nil)...,
	)

	procedures := BuildJobManagerProcedures(transport.Outbounds{
		Unary:  suite.mockUnaryOutbound,
		Stream: suite.mockStreamOutbound,
	})

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

// TestBuildHostManagerProcedures tests building Peloton Host Manager
// procedures.
func (suite *ProceduresTestSuite) TestBuildHostManagerProcedures() {
	expectedProcedures :=
		pbv0hostsvc.BuildHostServiceYARPCProcedures(nil)
	expectedProcedures = append(
		expectedProcedures,
		pbv1alphahostsvc.BuildHostServiceYARPCProcedures(nil)...,
	)
	expectedProcedures = append(
		expectedProcedures,
		pbprivateeventstreamsvc.BuildEventStreamServiceYARPCProcedures(nil)...,
	)
	expectedProcedures = append(
		expectedProcedures,
		pbprivatehostsvc.BuildInternalHostServiceYARPCProcedures(nil)...,
	)
	expectedProcedures = append(
		expectedProcedures,
		pbprivatehostmgrsvc.BuildHostManagerServiceYARPCProcedures(nil)...,
	)

	procedures := BuildHostManagerProcedures(transport.Outbounds{
		Unary:  suite.mockUnaryOutbound,
		Stream: suite.mockStreamOutbound,
	})

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

// TestBuildResourceManagerProcedures tests building Peloton Resource Manager
// procedures.
func (suite *ProceduresTestSuite) TestBuildResourceManagerProcedures() {
	expectedProcedures :=
		pbv0resmgr.BuildResourceManagerYARPCProcedures(nil)
	expectedProcedures = append(
		expectedProcedures,
		pbprivateresmgrsvc.BuildResourceManagerServiceYARPCProcedures(nil)...,
	)
	expectedProcedures = append(
		expectedProcedures,
		pbv0respoolsvc.BuildResourcePoolServiceYARPCProcedures(nil)...,
	)
	expectedProcedures = append(
		expectedProcedures,
		pbv1alpharespoolsvc.BuildResourcePoolServiceYARPCProcedures(nil)...,
	)
	expectedProcedures = append(
		expectedProcedures,
		pbprivatetaskqueue.BuildTaskQueueYARPCProcedures(nil)...,
	)

	procedures := BuildResourceManagerProcedures(transport.Outbounds{
		Unary:  suite.mockUnaryOutbound,
		Stream: suite.mockStreamOutbound,
	})

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
