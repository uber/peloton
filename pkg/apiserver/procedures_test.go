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
	pbv0resmgr "github.com/uber/peloton/.gen/peloton/api/v0/respool"
	pbv0taskmgr "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbv0updatesvc "github.com/uber/peloton/.gen/peloton/api/v0/update/svc"
	pbv0volumesvc "github.com/uber/peloton/.gen/peloton/api/v0/volume/svc"
	pbv1adminsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/admin/svc"
	pbv1jobstatelesssvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	pbv1podsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc"
	pbv1watchsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/watch/svc"
	pbprivateeventstreamsvc "github.com/uber/peloton/.gen/peloton/private/eventstream/v1alpha/eventstreamsvc"
	pbprivatehostsvc "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	pbprivatehostmgrsvc "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha/svc"
	pbprivatejobmgrsvc "github.com/uber/peloton/.gen/peloton/private/jobmgrsvc"
	pbprivateresmgrsvc "github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common/v1alpha/eventstream"
	"github.com/uber/peloton/pkg/hostmgr"
	"github.com/uber/peloton/pkg/hostmgr/hostsvc"
	"github.com/uber/peloton/pkg/hostmgr/p2k/hostmgrsvc"
	"github.com/uber/peloton/pkg/jobmgr/adminsvc"
	"github.com/uber/peloton/pkg/jobmgr/jobsvc"
	"github.com/uber/peloton/pkg/jobmgr/jobsvc/private"
	"github.com/uber/peloton/pkg/jobmgr/jobsvc/stateless"
	"github.com/uber/peloton/pkg/jobmgr/podsvc"
	"github.com/uber/peloton/pkg/jobmgr/tasksvc"
	"github.com/uber/peloton/pkg/jobmgr/updatesvc"
	"github.com/uber/peloton/pkg/jobmgr/volumesvc"
	"github.com/uber/peloton/pkg/jobmgr/watchsvc"
	"github.com/uber/peloton/pkg/resmgr"
	"github.com/uber/peloton/pkg/resmgr/respool/respoolsvc"

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

	adminSvcHandler         pbv1adminsvc.AdminServiceYARPCServer
	jobmgrHandler           pbv0jobmgr.JobManagerYARPCServer
	privatejobMgrSvcHandler pbprivatejobmgrsvc.JobManagerServiceYARPCServer
	statelessSvcHandler     pbv1jobstatelesssvc.JobServiceYARPCServer
	podSvcHandler           pbv1podsvc.PodServiceYARPCServer
	taskmgrHandler          pbv0taskmgr.TaskManagerYARPCServer
	updateSvcHandler        pbv0updatesvc.UpdateServiceYARPCServer
	volumeSvcHandler        pbv0volumesvc.VolumeServiceYARPCServer
	watchSvcHandler         pbv1watchsvc.WatchServiceYARPCServer
	hostSvcV0Handler        pbv0hostsvc.HostServiceYARPCServer
	eventstreamSvcHandler   pbprivateeventstreamsvc.EventStreamServiceYARPCServer
	privateHostSvcHandler   pbprivatehostsvc.InternalHostServiceYARPCServer
	hostMgrSvcHandler       pbprivatehostmgrsvc.HostManagerServiceYARPCServer

	resmgrHandler    pbv0resmgr.ResourceManagerYARPCServer
	resmgrSvcHandler pbprivateresmgrsvc.ResourceManagerServiceYARPCServer
}

// SetupTest is setup function for each test in this suite.
func (suite *ProceduresTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.mockUnaryOutbound = transporttest.NewMockUnaryOutbound(suite.ctrl)
	suite.adminSvcHandler = adminsvc.NewTestServiceHandler()
	suite.jobmgrHandler = jobsvc.NewTestServiceHandler()
	suite.privatejobMgrSvcHandler = private.NewTestServiceHandler()
	suite.statelessSvcHandler = stateless.NewTestServiceHandler()
	suite.podSvcHandler = podsvc.NewTestServiceHandler()
	suite.taskmgrHandler = tasksvc.NewTestServiceHandler()
	suite.updateSvcHandler = updatesvc.NewTestServiceHandler()
	suite.volumeSvcHandler = volumesvc.NewTestServiceHandler()
	suite.watchSvcHandler = watchsvc.NewTestServiceHandler()
	suite.hostSvcV0Handler = hostsvc.NewTestServiceHandler()
	suite.eventstreamSvcHandler = eventstream.NewTestHandler()
	suite.privateHostSvcHandler = hostmgr.NewTestServiceHandler()
	suite.hostMgrSvcHandler = hostmgrsvc.NewTestServiceHandler()
	suite.resmgrHandler = respoolsvc.NewTestServiceHandler()
	suite.resmgrSvcHandler = resmgr.NewTestServiceHandler()
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
		pbv0jobmgr.BuildJobManagerYARPCProcedures(suite.jobmgrHandler)
	expectedProcedures =
		append(
			expectedProcedures,
			pbv0taskmgr.BuildTaskManagerYARPCProcedures(
				suite.taskmgrHandler,
			)...,
		)
	expectedProcedures =
		append(expectedProcedures,
			pbv0updatesvc.BuildUpdateServiceYARPCProcedures(
				suite.updateSvcHandler,
			)...,
		)
	expectedProcedures =
		append(expectedProcedures,
			pbv0volumesvc.BuildVolumeServiceYARPCProcedures(
				suite.volumeSvcHandler)...,
		)
	expectedProcedures =
		append(expectedProcedures,
			pbv1adminsvc.BuildAdminServiceYARPCProcedures(
				suite.adminSvcHandler,
			)...,
		)
	expectedProcedures =
		append(expectedProcedures,
			pbv1jobstatelesssvc.BuildJobServiceYARPCProcedures(
				suite.statelessSvcHandler,
			)...,
		)
	expectedProcedures =
		append(expectedProcedures,
			pbv1podsvc.BuildPodServiceYARPCProcedures(
				suite.podSvcHandler,
			)...,
		)
	expectedProcedures =
		append(expectedProcedures,
			pbv1watchsvc.BuildWatchServiceYARPCProcedures(
				suite.watchSvcHandler,
			)...,
		)
	expectedProcedures = append(expectedProcedures,
		pbprivatejobmgrsvc.BuildJobManagerServiceYARPCProcedures(
			suite.privatejobMgrSvcHandler,
		)...,
	)

	procedures := BuildJobManagerProcedures(suite.mockUnaryOutbound)

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
		pbv0hostsvc.BuildHostServiceYARPCProcedures(suite.hostSvcV0Handler)
	expectedProcedures =
		append(
			expectedProcedures,
			pbprivateeventstreamsvc.BuildEventStreamServiceYARPCProcedures(
				suite.eventstreamSvcHandler,
			)...,
		)
	expectedProcedures =
		append(
			expectedProcedures,
			pbprivatehostsvc.BuildInternalHostServiceYARPCProcedures(
				suite.privateHostSvcHandler,
			)...,
		)
	expectedProcedures =
		append(
			expectedProcedures,
			pbprivatehostmgrsvc.BuildHostManagerServiceYARPCProcedures(
				suite.hostMgrSvcHandler,
			)...,
		)

	procedures := BuildHostManagerProcedures(suite.mockUnaryOutbound)

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
		pbv0resmgr.BuildResourceManagerYARPCProcedures(suite.resmgrHandler)
	expectedProcedures =
		append(
			expectedProcedures,
			pbprivateresmgrsvc.BuildResourceManagerServiceYARPCProcedures(
				suite.resmgrSvcHandler,
			)...,
		)

	procedures := BuildResourceManagerProcedures(suite.mockUnaryOutbound)

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
