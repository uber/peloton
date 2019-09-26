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

package stateless

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbupdate "github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	statelesssvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	statelesssvcmocks "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc/mocks"
	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	v1alphaquery "github.com/uber/peloton/.gen/peloton/api/v1alpha/query"
	v1alpharespool "github.com/uber/peloton/.gen/peloton/api/v1alpha/respool"
	"github.com/uber/peloton/.gen/peloton/private/models"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/util"
	versionutil "github.com/uber/peloton/pkg/common/util/entityversion"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	"github.com/uber/peloton/pkg/jobmgr/jobsvc"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	respoolmocks "github.com/uber/peloton/.gen/peloton/api/v0/respool/mocks"
	leadermocks "github.com/uber/peloton/pkg/common/leader/mocks"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	goalstatemocks "github.com/uber/peloton/pkg/jobmgr/goalstate/mocks"
	activermtaskmocks "github.com/uber/peloton/pkg/jobmgr/task/activermtask/mocks"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	testJobName              = "test-job"
	testJobID                = "481d565e-28da-457d-8434-f6bb7faa0e95"
	testUpdateID             = "941ff353-ba82-49fe-8f80-fb5bc649b04d"
	testEntityVersion        = "2-3-4"
	testSecretPath           = "/tmp/secret"
	testSecretStr            = "top-secret-token"
	testConfigurationVersion = uint64(2)
	testDesiredStateVersion  = uint64(3)
	testWorkflowVersion      = uint64(4)
)

var (
	testRespoolID = &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	testCmd               = "echo test"
	defaultResourceConfig = &pod.ResourceSpec{
		CpuLimit:    10,
		MemLimitMb:  10,
		DiskLimitMb: 10,
		FdLimit:     10,
	}
	testPrevMesosTaskID = "941ff353-ba82-49fe-8f80-fb5bc649b04d-1-2"
	testMesosTaskID     = "941ff353-ba82-49fe-8f80-fb5bc649b04d-1-3"
	testAgentID         = "agent-id"
	testPelotonJobID    = &peloton.JobID{Value: testJobID}
)

type statelessHandlerTestSuite struct {
	suite.Suite

	handler *serviceHandler

	ctrl               *gomock.Controller
	cachedJob          *cachedmocks.MockJob
	cachedWorkflow     *cachedmocks.MockUpdate
	jobFactory         *cachedmocks.MockJobFactory
	candidate          *leadermocks.MockCandidate
	respoolClient      *respoolmocks.MockResourceManagerYARPCClient
	goalStateDriver    *goalstatemocks.MockDriver
	jobStore           *storemocks.MockJobStore
	updateStore        *storemocks.MockUpdateStore
	listJobsServer     *statelesssvcmocks.MockJobServiceServiceListJobsYARPCServer
	listPodsServer     *statelesssvcmocks.MockJobServiceServiceListPodsYARPCServer
	taskStore          *storemocks.MockTaskStore
	jobIndexOps        *objectmocks.MockJobIndexOps
	jobConfigOps       *objectmocks.MockJobConfigOps
	jobNameToIDOps     *objectmocks.MockJobNameToIDOps
	jobRuntimeOps      *objectmocks.MockJobRuntimeOps
	secretInfoOps      *objectmocks.MockSecretInfoOps
	jobUpdateEventsOps *objectmocks.MockJobUpdateEventsOps
	taskConfigV2Ops    *objectmocks.MockTaskConfigV2Ops
	activeRMTasks      *activermtaskmocks.MockActiveRMTasks
}

func (suite *statelessHandlerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedWorkflow = cachedmocks.NewMockUpdate(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.candidate = leadermocks.NewMockCandidate(suite.ctrl)
	suite.goalStateDriver = goalstatemocks.NewMockDriver(suite.ctrl)
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.updateStore = storemocks.NewMockUpdateStore(suite.ctrl)
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.jobIndexOps = objectmocks.NewMockJobIndexOps(suite.ctrl)
	suite.jobConfigOps = objectmocks.NewMockJobConfigOps(suite.ctrl)
	suite.jobRuntimeOps = objectmocks.NewMockJobRuntimeOps(suite.ctrl)
	suite.jobNameToIDOps = objectmocks.NewMockJobNameToIDOps(suite.ctrl)
	suite.secretInfoOps = objectmocks.NewMockSecretInfoOps(suite.ctrl)
	suite.jobUpdateEventsOps = objectmocks.NewMockJobUpdateEventsOps(suite.ctrl)
	suite.taskConfigV2Ops = objectmocks.NewMockTaskConfigV2Ops(suite.ctrl)
	suite.respoolClient = respoolmocks.NewMockResourceManagerYARPCClient(suite.ctrl)
	suite.listJobsServer = statelesssvcmocks.NewMockJobServiceServiceListJobsYARPCServer(suite.ctrl)
	suite.listPodsServer = statelesssvcmocks.NewMockJobServiceServiceListPodsYARPCServer(suite.ctrl)
	suite.listJobsServer.EXPECT().Context().Return(context.Background()).AnyTimes()
	suite.listPodsServer.EXPECT().Context().Return(context.Background()).AnyTimes()
	suite.activeRMTasks = activermtaskmocks.NewMockActiveRMTasks(suite.ctrl)
	suite.handler = &serviceHandler{
		jobFactory:         suite.jobFactory,
		candidate:          suite.candidate,
		goalStateDriver:    suite.goalStateDriver,
		jobStore:           suite.jobStore,
		updateStore:        suite.updateStore,
		taskStore:          suite.taskStore,
		jobIndexOps:        suite.jobIndexOps,
		jobConfigOps:       suite.jobConfigOps,
		jobRuntimeOps:      suite.jobRuntimeOps,
		jobNameToIDOps:     suite.jobNameToIDOps,
		jobUpdateEventsOps: suite.jobUpdateEventsOps,
		taskConfigV2Ops:    suite.taskConfigV2Ops,
		secretInfoOps:      suite.secretInfoOps,
		respoolClient:      suite.respoolClient,
		rootCtx:            context.Background(),
		jobSvcCfg: jobsvc.Config{
			EnableSecrets:                true,
			MaxTasksPerJob:               100000,
			MedInstanceCount:             500,
			HighInstanceCount:            1000,
			LowGetWorkflowEventsWorkers:  25,
			MedGetWorkflowEventsWorkers:  50,
			HighGetWorkflowEventsWorkers: 100,
		},
		activeRMTasks: suite.activeRMTasks,
	}
}

func (suite *statelessHandlerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestGetJobWithSummarySuccess tests invoking
// GetJob API to get job summary
func (suite *statelessHandlerTestSuite) TestGetJobWithSummarySuccess() {
	suite.jobIndexOps.EXPECT().
		GetSummary(gomock.Any(), &peloton.JobID{Value: testJobID}).
		Return(&pbjob.JobSummary{
			Id:   &peloton.JobID{Value: testJobID},
			Name: "testjob",
			Runtime: &pbjob.RuntimeInfo{
				State:    pbjob.JobState_RUNNING,
				UpdateID: &peloton.UpdateID{Value: testUpdateID},
			},
		}, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(&models.UpdateModel{
			UpdateID: &peloton.UpdateID{Value: testUpdateID},
			State:    pbupdate.State_ROLLING_FORWARD,
		}, nil)

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId:       &v1alphapeloton.JobID{Value: testJobID},
			SummaryOnly: true,
		})

	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(
		resp.GetSummary().GetStatus().GetState(),
		stateless.JobState_JOB_STATE_RUNNING,
	)
	suite.Equal(
		resp.GetSummary().GetStatus().GetWorkflowStatus().GetState(),
		stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
	)
}

// TestGetJobWithSummaryGetConfigError tests invoking
// GetJob API to get job summary with DB error
func (suite *statelessHandlerTestSuite) TestGetJobWithSummaryGetConfigError() {
	suite.jobIndexOps.EXPECT().
		GetSummary(gomock.Any(), &peloton.JobID{Value: testJobID}).
		Return(nil, fmt.Errorf("fake db error"))

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId:       &v1alphapeloton.JobID{Value: testJobID},
			SummaryOnly: true,
		})

	suite.Error(err)
	suite.Nil(resp)
}

// TestGetJobWithSummaryGetUpdateError tests invoking
// GetJob API to get job summary with DB error when fetching update info
func (suite *statelessHandlerTestSuite) TestGetJobWithSummaryGetUpdateError() {
	suite.jobIndexOps.EXPECT().
		GetSummary(gomock.Any(), &peloton.JobID{Value: testJobID}).
		Return(&pbjob.JobSummary{
			Id:   &peloton.JobID{Value: testJobID},
			Name: "testjob",
			Runtime: &pbjob.RuntimeInfo{
				State:    pbjob.JobState_RUNNING,
				UpdateID: &peloton.UpdateID{Value: testUpdateID},
			},
		}, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(nil, fmt.Errorf("fake db error"))

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId:       &v1alphapeloton.JobID{Value: testJobID},
			SummaryOnly: true,
		})

	suite.Error(err)
	suite.Nil(resp)
}

// TestGetJobConfigVersionSuccess tests invoking
// GetJob API to get job configuration for a given version
func (suite *statelessHandlerTestSuite) TestGetJobConfigVersionSuccess() {
	version := uint64(3)
	instanceCount := uint32(5)

	suite.jobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
			version,
		).
		Return(&pbjob.JobConfig{
			InstanceCount: instanceCount,
		}, nil, nil)

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: versionutil.GetJobEntityVersion(version, 1, 1),
		})
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(
		resp.GetJobInfo().GetSpec().GetInstanceCount(),
		instanceCount,
	)
}

// TestGetJobConfigVersionError tests invoking
// GetJob API to get job configuration for a given version with DB error
func (suite *statelessHandlerTestSuite) TestGetJobConfigVersionError() {
	version := uint64(3)

	suite.jobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
			version,
		).
		Return(nil, nil, fmt.Errorf("fake db error"))

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: versionutil.GetJobEntityVersion(version, 1, 1),
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestGetJobSuccess tests invoking GetJob API to get job
// configuration, runtime and workflow information
func (suite *statelessHandlerTestSuite) TestGetJobSuccess() {
	instanceCount := uint32(5)

	suite.jobRuntimeOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
		).
		Return(&pbjob.RuntimeInfo{
			State:    pbjob.JobState_RUNNING,
			UpdateID: &peloton.UpdateID{Value: testUpdateID},
		}, nil)

	suite.jobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
			gomock.Any(),
		).
		Return(&pbjob.JobConfig{
			InstanceCount: instanceCount,
		}, nil, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(&models.UpdateModel{
			UpdateID:         &peloton.UpdateID{Value: testUpdateID},
			Type:             models.WorkflowType_UPDATE,
			State:            pbupdate.State_ROLLING_FORWARD,
			InstancesAdded:   []uint32{3, 4},
			InstancesUpdated: []uint32{0, 1, 2},
		}, nil)

	suite.jobUpdateEventsOps.EXPECT().
		GetAll(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return([]*stateless.WorkflowEvent{{
			Type:      stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
			State:     stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
			Timestamp: time.Now().Format(time.RFC3339),
		}}, nil)

	resp, err := suite.handler.GetJob(
		context.Background(),
		&statelesssvc.GetJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})

	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(
		resp.GetJobInfo().GetSpec().GetInstanceCount(),
		instanceCount,
	)
	suite.Equal(
		resp.GetJobInfo().GetStatus().GetState(),
		stateless.JobState_JOB_STATE_RUNNING,
	)
	suite.Equal(
		resp.GetJobInfo().GetStatus().GetWorkflowStatus().GetState(),
		stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
	)
	suite.Equal(uint32(0), resp.GetWorkflowInfo().GetInstancesUpdated()[0].GetFrom())
	suite.Equal(uint32(2), resp.GetWorkflowInfo().GetInstancesUpdated()[0].GetTo())
	suite.Equal(uint32(3), resp.GetWorkflowInfo().GetInstancesAdded()[0].GetFrom())
	suite.Equal(uint32(4), resp.GetWorkflowInfo().GetInstancesAdded()[0].GetTo())
}

// TestGetJobConfigGetError tests invoking GetJob API to get job
// configuration, runtime and workflow information with DB error
// when trying to fetch job configuration
func (suite *statelessHandlerTestSuite) TestGetJobConfigGetError() {
	suite.jobRuntimeOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
		).
		Return(&pbjob.RuntimeInfo{}, nil)

	suite.jobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
			gomock.Any(),
		).
		Return(nil, nil, fmt.Errorf("fake db error"))

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})

	suite.Error(err)
	suite.Nil(resp)
}

// TestGetJobRuntimeGetError tests invoking GetJob API to get job
// configuration, runtime and workflow information with DB error
// when trying to fetch job runtime
func (suite *statelessHandlerTestSuite) TestGetJobRuntimeGetError() {
	suite.jobRuntimeOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
		).
		Return(nil, fmt.Errorf("fake db error"))

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})

	suite.Error(err)
	suite.Nil(resp)
}

// TestGetJobUpdateGetError tests invoking GetJob API to get job
// configuration, runtime and workflow information with DB error
// when trying to fetch update information
func (suite *statelessHandlerTestSuite) TestGetJobUpdateGetError() {
	instanceCount := uint32(5)

	suite.jobRuntimeOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
		).
		Return(&pbjob.RuntimeInfo{
			State:    pbjob.JobState_RUNNING,
			UpdateID: &peloton.UpdateID{Value: testUpdateID},
		}, nil)

	suite.jobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
			gomock.Any(),
		).
		Return(&pbjob.JobConfig{
			InstanceCount: instanceCount,
		}, nil, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(nil, fmt.Errorf("fake db error"))

	suite.jobUpdateEventsOps.EXPECT().
		GetAll(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return([]*stateless.WorkflowEvent{{
			Type:      stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
			State:     stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
			Timestamp: time.Now().Format(time.RFC3339),
		}}, nil)

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})

	suite.Error(err)
	suite.Nil(resp)
}

// TestGetJobUpdateEventsGetError tests invoking GetJob API to get job
// configuration, runtime and workflow information with DB error
// when trying to fetch job update events information
func (suite *statelessHandlerTestSuite) TestGetJobUpdateEventsGetError() {
	instanceCount := uint32(5)

	suite.jobRuntimeOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
		).
		Return(&pbjob.RuntimeInfo{
			State:    pbjob.JobState_RUNNING,
			UpdateID: &peloton.UpdateID{Value: testUpdateID},
		}, nil)

	suite.jobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
			gomock.Any(),
		).
		Return(&pbjob.JobConfig{
			InstanceCount: instanceCount,
		}, nil, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(&models.UpdateModel{
			UpdateID:         &peloton.UpdateID{Value: testUpdateID},
			Type:             models.WorkflowType_UPDATE,
			State:            pbupdate.State_ROLLING_FORWARD,
			InstancesAdded:   []uint32{3, 4},
			InstancesUpdated: []uint32{0, 1, 2},
		}, nil)

	suite.jobUpdateEventsOps.EXPECT().
		GetAll(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(nil, errors.New("fake DB error"))

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})

	suite.Error(err)
	suite.Nil(resp)
}

// TestGetJobGetErrors tests invoking GetJob API to get job
// configuration, runtime and workflow information with DB error
// when trying to fetch job config, job update and job update events information
func (suite *statelessHandlerTestSuite) TestGetJobGetErrors() {

	suite.jobRuntimeOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
		).
		Return(&pbjob.RuntimeInfo{
			State:    pbjob.JobState_RUNNING,
			UpdateID: &peloton.UpdateID{Value: testUpdateID},
		}, nil)

	suite.jobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
			gomock.Any(),
		).
		Return(nil, nil, errors.New("fake DB error"))

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(nil, errors.New("fake DB error"))

	suite.jobUpdateEventsOps.EXPECT().
		GetAll(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(nil, errors.New("fake DB error"))

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})

	suite.Error(err)
	suite.Nil(resp)
}

// TestGetJobUpdateGetErrors tests invoking GetJob API to get job
// configuration, runtime and workflow information with DB error
// when trying to fetch job update and job update events information
func (suite *statelessHandlerTestSuite) TestGetJobUpdateGetErrors() {
	instanceCount := uint32(5)

	suite.jobRuntimeOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
		).
		Return(&pbjob.RuntimeInfo{
			State:    pbjob.JobState_RUNNING,
			UpdateID: &peloton.UpdateID{Value: testUpdateID},
		}, nil)

	suite.jobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
			gomock.Any(),
		).
		Return(&pbjob.JobConfig{
			InstanceCount: instanceCount,
		}, nil, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(nil, errors.New("fake DB error"))

	suite.jobUpdateEventsOps.EXPECT().
		GetAll(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(nil, errors.New("fake DB error"))

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})

	suite.Error(err)
	suite.Nil(resp)
}

// TestGetJobCacheWithUpdateSuccess test the success case of getting job
// cache which has update
func (suite *statelessHandlerTestSuite) TestGetJobCacheWithUpdateSuccess() {
	instanceCount := uint32(10)
	curJobVersion := uint64(1)
	targetJobVersion := uint64(2)
	instancesDone := []uint32{0, 1, 2}
	instancesFailed := []uint32{3, 4}
	instancesCurrent := []uint32{5}
	totalInstances := []uint32{0, 1, 2, 3, 4, 5, 6, 7}
	priority := uint32(2)
	preemptible := true
	revocable := false
	maximumUnavailableInstances := uint32(2)

	suite.jobFactory.EXPECT().
		GetJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			State:     pbjob.JobState_PENDING,
			GoalState: pbjob.JobState_RUNNING,
			UpdateID:  &peloton.UpdateID{Value: testUpdateID},
		}, nil)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&pbjob.JobConfig{
			InstanceCount: instanceCount,
			SLA: &pbjob.SlaConfig{
				Priority:                    priority,
				Preemptible:                 preemptible,
				Revocable:                   revocable,
				MaximumUnavailableInstances: maximumUnavailableInstances,
			},
		}, nil)

	suite.cachedJob.EXPECT().
		GetWorkflow(&peloton.UpdateID{Value: testUpdateID}).
		Return(suite.cachedWorkflow)

	suite.cachedWorkflow.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE)

	suite.cachedWorkflow.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedWorkflow.EXPECT().
		GetPrevState().
		Return(pbupdate.State_INITIALIZED)

	suite.cachedWorkflow.EXPECT().
		GetInstancesDone().
		Return(instancesDone).
		AnyTimes()

	suite.cachedWorkflow.EXPECT().
		GetInstancesFailed().
		Return(instancesFailed).
		AnyTimes()

	suite.cachedWorkflow.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: totalInstances,
		})

	suite.cachedWorkflow.EXPECT().
		GetInstancesCurrent().
		Return(instancesCurrent)

	suite.cachedWorkflow.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			JobVersion: curJobVersion,
		})

	suite.cachedWorkflow.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			JobVersion: targetJobVersion,
		})

	resp, err := suite.handler.GetJobCache(context.Background(),
		&statelesssvc.GetJobCacheRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})

	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(resp.GetSpec().GetInstanceCount(), instanceCount)
	suite.Equal(resp.GetSpec().GetSla().GetPriority(), priority)
	suite.Equal(resp.GetSpec().GetSla().GetPreemptible(), preemptible)
	suite.Equal(resp.GetSpec().GetSla().GetRevocable(), revocable)
	suite.Equal(resp.GetSpec().GetSla().GetMaximumUnavailableInstances(),
		maximumUnavailableInstances)

	suite.Equal(resp.GetStatus().GetState(), stateless.JobState_JOB_STATE_PENDING)
	suite.Equal(resp.GetStatus().GetDesiredState(), stateless.JobState_JOB_STATE_RUNNING)
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetState(),
		stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD)
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetPrevState(),
		stateless.WorkflowState_WORKFLOW_STATE_INITIALIZED)
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetType(),
		stateless.WorkflowType_WORKFLOW_TYPE_UPDATE)
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetNumInstancesCompleted(),
		uint32(len(instancesDone)))
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetNumInstancesFailed(),
		uint32(len(instancesFailed)))
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetNumInstancesRemaining(),
		uint32(len(totalInstances)-len(instancesFailed)-len(instancesDone)))
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetInstancesCurrent(),
		instancesCurrent)
	suite.Equal(
		resp.GetStatus().GetWorkflowStatus().GetPrevVersion().GetValue(),
		"1-0-0")
	suite.Equal(
		resp.GetStatus().GetWorkflowStatus().GetVersion().GetValue(),
		"2-0-0")
}

// TestGetJobCacheGetJobFail test the failure case of getting job cache due to
// failure of getting runtime
func (suite *statelessHandlerTestSuite) TestGetJobCacheGetRuntimeFail() {
	suite.jobFactory.EXPECT().
		GetJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.GetJobCache(context.Background(),
		&statelesssvc.GetJobCacheRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestGetJobCacheNotFound test the failure case of getting job cache due to
// job cache not found
func (suite *statelessHandlerTestSuite) TestGetJobCacheNotFound() {
	suite.jobFactory.EXPECT().
		GetJob(&peloton.JobID{Value: testJobID}).
		Return(nil)

	resp, err := suite.handler.GetJobCache(context.Background(),
		&statelesssvc.GetJobCacheRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})
	suite.Error(err)
	suite.Nil(resp)
	suite.True(yarpcerrors.IsNotFound(err))
}

// TestRefreshJobSuccess tests the case of successfully refreshing job
func (suite *statelessHandlerTestSuite) TestRefreshJobSuccess() {
	jobConfig := &pbjob.JobConfig{
		InstanceCount: 10,
	}
	configAddOn := &models.ConfigAddOn{}
	jobRuntime := &pbjob.RuntimeInfo{
		State: pbjob.JobState_RUNNING,
	}

	suite.candidate.EXPECT().
		IsLeader().
		Return(true)

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), testPelotonJobID).
		Return(jobRuntime, nil)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), testPelotonJobID, gomock.Any()).
		Return(jobConfig, configAddOn, nil)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		Update(gomock.Any(), &pbjob.JobInfo{
			Config:  jobConfig,
			Runtime: jobRuntime,
		}, configAddOn, nil, cached.UpdateCacheOnly).
		Return(nil)

	suite.goalStateDriver.EXPECT().
		EnqueueJob(&peloton.JobID{Value: testJobID}, gomock.Any())

	resp, err := suite.handler.RefreshJob(context.Background(), &statelesssvc.RefreshJobRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
	})
	suite.NotNil(resp)
	suite.NoError(err)
}

// TestGetWorkflowEventsFails tests the failure to fetch job runtime
// for update id.
func (suite *statelessHandlerTestSuite) TestGetWorkflowEventsJobRuntimeFail() {

	// invalid job UUID
	workflowEvents, err := suite.handler.GetWorkflowEvents(
		context.Background(), &statelesssvc.GetWorkflowEventsRequest{
			JobId: &v1alphapeloton.JobID{
				Value: testJobName,
			},
			InstanceId: 0,
		})
	suite.Error(err)
	suite.Nil(workflowEvents)

	getWorkflowEventsReq := &statelesssvc.GetWorkflowEventsRequest{
		JobId: &v1alphapeloton.JobID{
			Value: testJobID,
		},
		InstanceId: 0,
	}

	v0JobID := &peloton.JobID{
		Value: testJobID,
	}

	// No job runtime exists
	suite.jobFactory.EXPECT().
		AddJob(v0JobID).
		Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, errors.New("unable to get job runtime"))

	workflowEvents, err = suite.handler.GetWorkflowEvents(
		context.Background(),
		getWorkflowEventsReq)
	suite.Error(err)
	suite.Nil(workflowEvents)

	// no update id exists for the job
	jobRuntime := &pbjob.RuntimeInfo{
		State: pbjob.JobState_RUNNING,
	}
	suite.jobFactory.EXPECT().
		AddJob(v0JobID).
		Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	workflowEvents, err = suite.handler.GetWorkflowEvents(
		context.Background(),
		getWorkflowEventsReq)
	suite.Error(err)
	suite.Nil(workflowEvents)
}

// TestGetWorkflowEventsFail tests fail to fetch workflow events when job runtime exists
func (suite statelessHandlerTestSuite) TestGetWorkflowEventsFail() {
	getWorkflowEventsReq := &statelesssvc.GetWorkflowEventsRequest{
		JobId: &v1alphapeloton.JobID{
			Value: testJobID,
		},
		InstanceId: 0,
	}

	v0JobID := &peloton.JobID{
		Value: testJobID,
	}

	// no update id exists for the job
	jobRuntime := &pbjob.RuntimeInfo{
		State: pbjob.JobState_RUNNING,
	}

	suite.jobFactory.EXPECT().
		AddJob(v0JobID).
		Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	workflowEvents, err := suite.handler.GetWorkflowEvents(
		context.Background(),
		getWorkflowEventsReq)
	suite.Error(err)
	suite.Nil(workflowEvents)
}

// TestGetWorkflowEvents test for the successful case of fetching
// workflow events
func (suite *statelessHandlerTestSuite) TestGetWorkflowEvents() {
	getWorkflowEventsReq := &statelesssvc.GetWorkflowEventsRequest{
		JobId: &v1alphapeloton.JobID{
			Value: testJobID,
		},
		InstanceId: 0,
	}

	v0JobID := &peloton.JobID{
		Value: testJobID,
	}

	var events []*stateless.WorkflowEvent
	jobRuntime := &pbjob.RuntimeInfo{
		State: pbjob.JobState_RUNNING,
		UpdateID: &peloton.UpdateID{
			Value: testUpdateID,
		},
	}
	workflowEvent := &stateless.WorkflowEvent{
		Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
		State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD}
	events = append(events, workflowEvent)

	suite.jobFactory.EXPECT().
		AddJob(v0JobID).
		Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)
	suite.updateStore.EXPECT().
		GetWorkflowEvents(gomock.Any(), jobRuntime.GetUpdateID(), uint32(0), uint32(0)).
		Return(nil, errors.New("unable to get workflow events from DB"))
	_, err := suite.handler.GetWorkflowEvents(context.Background(), getWorkflowEventsReq)
	suite.Error(err)

	suite.jobFactory.EXPECT().
		AddJob(v0JobID).
		Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)
	suite.updateStore.EXPECT().
		GetWorkflowEvents(gomock.Any(), jobRuntime.GetUpdateID(), uint32(0), uint32(0)).
		Return(events, nil)
	workflowEvents, err := suite.handler.GetWorkflowEvents(context.Background(), getWorkflowEventsReq)
	suite.NoError(err)
	suite.Equal(1, len(workflowEvents.GetEvents()))
}

// TestGetWorkflowEventsWithLimit tests the case of fetching
// workflow events with limit
func (suite *statelessHandlerTestSuite) TestGetWorkflowEventsWithLimit() {
	getWorkflowEventsReq := &statelesssvc.GetWorkflowEventsRequest{
		JobId: &v1alphapeloton.JobID{
			Value: testJobID,
		},
		InstanceId: 0,
		Limit:      1,
	}

	v0JobID := &peloton.JobID{
		Value: testJobID,
	}

	var events []*stateless.WorkflowEvent
	jobRuntime := &pbjob.RuntimeInfo{
		State: pbjob.JobState_RUNNING,
		UpdateID: &peloton.UpdateID{
			Value: testUpdateID,
		},
	}
	workflowEvent := &stateless.WorkflowEvent{
		Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
		State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD}
	events = append(events, workflowEvent)

	suite.jobFactory.EXPECT().
		AddJob(v0JobID).
		Return(suite.cachedJob)
	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)
	suite.updateStore.EXPECT().
		GetWorkflowEvents(gomock.Any(), jobRuntime.GetUpdateID(), uint32(0), uint32(1)).
		Return(events, nil)
	workflowEvents, err := suite.handler.GetWorkflowEvents(context.Background(), getWorkflowEventsReq)
	suite.NoError(err)
	suite.Equal(1, len(workflowEvents.GetEvents()))
}

// TestRefreshJobFailNonLeader tests the failure case of refreshing job
// due to JobMgr is not leader
func (suite *statelessHandlerTestSuite) TestRefreshJobFailNonLeader() {
	suite.candidate.EXPECT().
		IsLeader().
		Return(false)
	resp, err := suite.handler.RefreshJob(context.Background(), &statelesssvc.RefreshJobRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
	})
	suite.Nil(resp)
	suite.Error(err)
}

// TestRefreshJobGetConfigFail tests the case of failure due to
// failure of getting job config
func (suite *statelessHandlerTestSuite) TestRefreshJobGetConfigFail() {
	suite.candidate.EXPECT().
		IsLeader().
		Return(true)

	suite.jobRuntimeOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
		).
		Return(&pbjob.RuntimeInfo{}, nil)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), testPelotonJobID, gomock.Any()).
		Return(nil, nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.RefreshJob(context.Background(), &statelesssvc.RefreshJobRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
	})
	suite.Nil(resp)
	suite.Error(err)
}

// TestRefreshJobGetRuntimeFail tests the case of failure due to
// failure of getting job runtime
func (suite *statelessHandlerTestSuite) TestRefreshJobGetRuntimeFail() {
	suite.candidate.EXPECT().
		IsLeader().
		Return(true)

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), testPelotonJobID).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.RefreshJob(context.Background(), &statelesssvc.RefreshJobRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
	})
	suite.Nil(resp)
	suite.Error(err)
}

// TestQueryJobsSuccess tests the success case of query jobs
func (suite *statelessHandlerTestSuite) TestQueryJobsSuccess() {
	pagination := &v1alphaquery.PaginationSpec{
		Offset: 0,
		Limit:  10,
		OrderBy: []*v1alphaquery.OrderBy{
			{
				Order:    v1alphaquery.OrderBy_ORDER_BY_ASC,
				Property: &v1alphaquery.PropertyPath{Value: "creation_time"},
			},
		},
		MaxLimit: 100,
	}
	labels := []*v1alphapeloton.Label{{Key: "k1", Value: "v1"}}
	keywords := []string{"key1", "key2"}
	jobstates := []stateless.JobState{stateless.JobState_JOB_STATE_RUNNING}
	respoolPath := &v1alpharespool.ResourcePoolPath{
		Value: "/testPath",
	}
	owner := "owner1"
	name := "test"
	respoolID := &peloton.ResourcePoolID{Value: "321d565e-28da-457d-8434-f6bb7faa0e95"}
	updateID := &peloton.UpdateID{Value: "322e122e-28da-457d-8434-f6bb7faa0e95"}
	jobSummary := &pbjob.JobSummary{
		Name:  name,
		Owner: owner,
		Runtime: &pbjob.RuntimeInfo{
			State:    pbjob.JobState_RUNNING,
			UpdateID: updateID,
		},
		Labels: []*peloton.Label{{
			Key:   labels[0].GetKey(),
			Value: labels[0].GetValue(),
		}},
	}
	timestamp, err := ptypes.TimestampProto(time.Now())
	suite.NoError(err)
	spec := &stateless.QuerySpec{
		Pagination: pagination,
		Labels:     labels,
		Keywords:   keywords,
		JobStates:  jobstates,
		Respool:    respoolPath,
		Owner:      owner,
		Name:       name,
		CreationTimeRange: &v1alphapeloton.TimeRange{
			Max: timestamp,
		},
		CompletionTimeRange: &v1alphapeloton.TimeRange{
			Max: timestamp,
		},
	}
	totalResult := uint32(1)

	suite.respoolClient.EXPECT().
		LookupResourcePoolID(gomock.Any(), &respool.LookupRequest{
			Path: &respool.ResourcePoolPath{Value: respoolPath.GetValue()},
		}).
		Return(&respool.LookupResponse{Id: respoolID}, nil)

	suite.jobStore.EXPECT().
		QueryJobs(gomock.Any(), respoolID, gomock.Any(), true).
		Return(nil, []*pbjob.JobSummary{jobSummary}, totalResult, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), updateID).
		Return(&models.UpdateModel{
			Type:                 models.WorkflowType_UPDATE,
			State:                pbupdate.State_ROLLING_FORWARD,
			InstancesTotal:       10,
			InstancesDone:        1,
			InstancesFailed:      6,
			InstancesCurrent:     []uint32{0, 1, 2},
			PrevJobConfigVersion: 1,
			JobConfigVersion:     2,
		}, nil)

	resp, err := suite.handler.QueryJobs(
		context.Background(),
		&statelesssvc.QueryJobsRequest{
			Spec: spec,
		},
	)
	suite.NotNil(resp)
	suite.Equal(resp.GetPagination(), &v1alphaquery.Pagination{
		Offset: pagination.GetOffset(),
		Limit:  pagination.GetLimit(),
		Total:  totalResult,
	})
	suite.Equal(resp.GetSpec(), spec)
	suite.Equal(resp.GetRecords()[0].GetOwner(), jobSummary.GetOwner())
	suite.Equal(resp.GetRecords()[0].GetOwningTeam(), jobSummary.GetOwningTeam())
	suite.Equal(
		resp.GetRecords()[0].GetLabels()[0].GetKey(),
		jobSummary.GetLabels()[0].GetKey(),
	)
	suite.Equal(
		resp.GetRecords()[0].GetLabels()[0].GetValue(),
		jobSummary.GetLabels()[0].GetValue(),
	)
	suite.Equal(
		resp.GetRecords()[0].GetStatus().GetState(),
		stateless.JobState_JOB_STATE_RUNNING,
	)
	suite.Equal(
		resp.GetRecords()[0].GetStatus().GetWorkflowStatus().GetState(),
		stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
	)
	suite.NoError(err)
}

// TestQueryJobsGetRespoolIDFail tests the failure case of query jobs
// due to get respool id
func (suite *statelessHandlerTestSuite) TestQueryJobsGetRespoolIdFail() {
	pagination := &v1alphaquery.PaginationSpec{
		Offset: 0,
		Limit:  10,
		OrderBy: []*v1alphaquery.OrderBy{
			{
				Order:    v1alphaquery.OrderBy_ORDER_BY_ASC,
				Property: &v1alphaquery.PropertyPath{Value: "creation_time"},
			},
		},
		MaxLimit: 100,
	}
	labels := []*v1alphapeloton.Label{{Key: "k1", Value: "v1"}}
	keywords := []string{"key1", "key2"}
	jobstates := []stateless.JobState{stateless.JobState_JOB_STATE_RUNNING}
	respoolPath := &v1alpharespool.ResourcePoolPath{
		Value: "/testPath",
	}
	owner := "owner1"
	name := "test"
	timestamp, err := ptypes.TimestampProto(time.Now())
	suite.NoError(err)
	spec := &stateless.QuerySpec{
		Pagination: pagination,
		Labels:     labels,
		Keywords:   keywords,
		JobStates:  jobstates,
		Respool:    respoolPath,
		Owner:      owner,
		Name:       name,
		CreationTimeRange: &v1alphapeloton.TimeRange{
			Max: timestamp,
		},
		CompletionTimeRange: &v1alphapeloton.TimeRange{
			Max: timestamp,
		},
	}

	suite.respoolClient.EXPECT().
		LookupResourcePoolID(gomock.Any(), &respool.LookupRequest{
			Path: &respool.ResourcePoolPath{Value: respoolPath.GetValue()},
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.QueryJobs(
		context.Background(),
		&statelesssvc.QueryJobsRequest{
			Spec: spec,
		},
	)
	suite.Nil(resp)
	suite.Error(err)
}

// TestQueryJobsErrorGettingUpdate tests the success case of querying jobs
// inspite of error while getting job update from store
func (suite *statelessHandlerTestSuite) TestQueryJobsErrorGettingUpdate() {
	pagination := &v1alphaquery.PaginationSpec{
		Offset: 0,
		Limit:  10,
		OrderBy: []*v1alphaquery.OrderBy{
			{
				Order:    v1alphaquery.OrderBy_ORDER_BY_ASC,
				Property: &v1alphaquery.PropertyPath{Value: "creation_time"},
			},
		},
		MaxLimit: 100,
	}
	labels := []*v1alphapeloton.Label{{Key: "k1", Value: "v1"}}
	keywords := []string{"key1", "key2"}
	jobstates := []stateless.JobState{stateless.JobState_JOB_STATE_RUNNING}
	respoolPath := &v1alpharespool.ResourcePoolPath{
		Value: "/testPath",
	}
	owner := "owner1"
	name := "test"
	respoolID := &peloton.ResourcePoolID{Value: "321d565e-28da-457d-8434-f6bb7faa0e95"}
	updateID := &peloton.UpdateID{Value: "322e122e-28da-457d-8434-f6bb7faa0e95"}
	jobSummary := &pbjob.JobSummary{
		Name:  name,
		Owner: owner,
		Runtime: &pbjob.RuntimeInfo{
			State:    pbjob.JobState_RUNNING,
			UpdateID: updateID,
		},
		Labels: []*peloton.Label{{
			Key:   labels[0].GetKey(),
			Value: labels[0].GetValue(),
		}},
	}
	timestamp, err := ptypes.TimestampProto(time.Now())
	suite.NoError(err)
	spec := &stateless.QuerySpec{
		Pagination: pagination,
		Labels:     labels,
		Keywords:   keywords,
		JobStates:  jobstates,
		Respool:    respoolPath,
		Owner:      owner,
		Name:       name,
		CreationTimeRange: &v1alphapeloton.TimeRange{
			Max: timestamp,
		},
		CompletionTimeRange: &v1alphapeloton.TimeRange{
			Max: timestamp,
		},
	}
	totalResult := uint32(1)

	suite.respoolClient.EXPECT().
		LookupResourcePoolID(gomock.Any(), &respool.LookupRequest{
			Path: &respool.ResourcePoolPath{Value: respoolPath.GetValue()},
		}).
		Return(&respool.LookupResponse{Id: respoolID}, nil)

	suite.jobStore.EXPECT().
		QueryJobs(gomock.Any(), respoolID, gomock.Any(), true).
		Return(nil, []*pbjob.JobSummary{jobSummary}, totalResult, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), updateID).
		Return(nil, fmt.Errorf("test error"))

	resp, err := suite.handler.QueryJobs(
		context.Background(),
		&statelesssvc.QueryJobsRequest{
			Spec: spec,
		},
	)
	suite.NotNil(resp)
	suite.Equal(resp.GetPagination(), &v1alphaquery.Pagination{
		Offset: pagination.GetOffset(),
		Limit:  pagination.GetLimit(),
		Total:  totalResult,
	})
	suite.Equal(resp.GetSpec(), spec)
	suite.Equal(resp.GetRecords()[0].GetOwner(), jobSummary.GetOwner())
	suite.Equal(resp.GetRecords()[0].GetOwningTeam(), jobSummary.GetOwningTeam())
	suite.Equal(
		resp.GetRecords()[0].GetLabels()[0].GetKey(),
		jobSummary.GetLabels()[0].GetKey(),
	)
	suite.Equal(
		resp.GetRecords()[0].GetLabels()[0].GetValue(),
		jobSummary.GetLabels()[0].GetValue(),
	)
	suite.Equal(
		resp.GetRecords()[0].GetStatus().GetState(),
		stateless.JobState_JOB_STATE_RUNNING,
	)
	suite.Nil(resp.GetRecords()[0].GetStatus().GetWorkflowStatus())
	suite.NoError(err)
}

// TestReplaceJobSuccess tests the success case of replacing job
func (suite *statelessHandlerTestSuite) TestReplaceJobSuccess() {
	configVersion := uint64(1)
	workflowVersion := uint64(1)
	desiredStateVersion := uint64(1)
	batchSize := uint32(1)
	opaque := "test"

	suite.candidate.EXPECT().
		IsLeader().
		Return(true)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			State:                pbjob.JobState_RUNNING,
			WorkflowVersion:      workflowVersion,
			ConfigurationVersion: configVersion,
		}, nil)

	suite.jobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
			configVersion,
		).Return(
		&pbjob.JobConfig{
			Type: pbjob.JobType_SERVICE,
		},
		&models.ConfigAddOn{
			SystemLabels: []*peloton.Label{
				{Key: common.SystemLabelResourcePool, Value: "/testRespool"},
			},
		},
		nil)

	suite.cachedJob.EXPECT().
		CreateWorkflow(
			gomock.Any(),
			models.WorkflowType_UPDATE,
			&pbupdate.UpdateConfig{
				BatchSize: batchSize,
			},
			versionutil.GetJobEntityVersion(configVersion, desiredStateVersion, workflowVersion),
			gomock.Any(),
		).
		Return(
			&peloton.UpdateID{
				Value: testUpdateID,
			},
			versionutil.GetJobEntityVersion(configVersion+1, desiredStateVersion, workflowVersion+1),
			nil)

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(&peloton.JobID{Value: testJobID}, &peloton.UpdateID{Value: testUpdateID}, gomock.Any()).
		Return()

	resp, err := suite.handler.ReplaceJob(
		context.Background(),
		&statelesssvc.ReplaceJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: versionutil.GetJobEntityVersion(configVersion, desiredStateVersion, workflowVersion),
			Spec:    &stateless.JobSpec{},
			UpdateSpec: &stateless.UpdateSpec{
				BatchSize: batchSize,
			},
			OpaqueData: &v1alphapeloton.OpaqueData{Data: opaque},
		},
	)
	suite.NoError(err)
	suite.Equal(resp.GetVersion(), versionutil.GetJobEntityVersion(configVersion+1, desiredStateVersion, workflowVersion+1))
}

// TestCreateJobFailNonLeader tests the failure case of creating job
// due to JobMgr is not leader
func (suite *statelessHandlerTestSuite) TestReplaceJobFailNonLeader() {
	suite.candidate.EXPECT().
		IsLeader().
		Return(false)

	resp, err := suite.handler.ReplaceJob(
		context.Background(),
		&statelesssvc.ReplaceJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})
	suite.Nil(resp)
	suite.Error(err)
}

// TestReplaceJobGetJobConfigFailure tests the failure case of replacing job
// due to not able to get job config
func (suite *statelessHandlerTestSuite) TestReplaceJobGetJobConfigFailure() {
	batchSize := uint32(1)

	suite.candidate.EXPECT().
		IsLeader().
		Return(true)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			State:                pbjob.JobState_RUNNING,
			WorkflowVersion:      testWorkflowVersion,
			ConfigurationVersion: testConfigurationVersion,
		}, nil)

	suite.jobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
			testConfigurationVersion,
		).Return(nil, nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.ReplaceJob(
		context.Background(),
		&statelesssvc.ReplaceJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: &v1alphapeloton.EntityVersion{Value: testEntityVersion},
			Spec:    &stateless.JobSpec{},
			UpdateSpec: &stateless.UpdateSpec{
				BatchSize: batchSize,
			},
		},
	)
	suite.Error(err)
	suite.Nil(resp)
}

// TestGetReplaceJobDiffSuccess tests the success case of getting the
// difference in configuration for ReplaceJob API
func (suite *statelessHandlerTestSuite) TestGetReplaceJobDiffSuccess() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: testEntityVersion}
	instanceCount := uint32(5)
	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State: pbtask.TaskState_RUNNING,
		}
		taskRuntimes[i] = runtime
	}

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			State:                pbjob.JobState_RUNNING,
			WorkflowVersion:      testWorkflowVersion,
			ConfigurationVersion: testConfigurationVersion,
		}, nil)

	suite.cachedJob.EXPECT().
		ValidateEntityVersion(gomock.Any(), entityVersion).
		Return(nil)

	suite.jobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
			testConfigurationVersion,
		).Return(
		&pbjob.JobConfig{
			Type:          pbjob.JobType_SERVICE,
			InstanceCount: instanceCount,
		},
		nil,
		nil)

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), gomock.Any(), nil).
		Return(taskRuntimes, nil)

	suite.taskConfigV2Ops.EXPECT().
		GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil, nil).
		Times(int(instanceCount))

	_, err := suite.handler.GetReplaceJobDiff(
		context.Background(),
		&statelesssvc.GetReplaceJobDiffRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
			Spec: &stateless.JobSpec{
				InstanceCount: instanceCount,
			},
		},
	)
	suite.NoError(err)
}

// TestGetReplaceJobDiffSuccess tests the failure case of DB error when
// fetching the job runtime when invoking GetReplaceJobDiff API
func (suite *statelessHandlerTestSuite) TestGetReplaceJobDiffRuntimeDBError() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: testEntityVersion}
	instanceCount := uint32(5)
	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State: pbtask.TaskState_RUNNING,
		}
		taskRuntimes[i] = runtime
	}

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	_, err := suite.handler.GetReplaceJobDiff(
		context.Background(),
		&statelesssvc.GetReplaceJobDiffRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
			Spec: &stateless.JobSpec{
				InstanceCount: instanceCount,
			},
		},
	)
	suite.Error(err)
}

// TestGetReplaceJobDiffValidateVersionFail failure case of entity
// version validation failure when invoking GetReplaceJobDiff API
func (suite *statelessHandlerTestSuite) TestGetReplaceJobDiffValidateVersionFail() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: testEntityVersion}
	instanceCount := uint32(5)
	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State: pbtask.TaskState_RUNNING,
		}
		taskRuntimes[i] = runtime
	}

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			State:                pbjob.JobState_RUNNING,
			WorkflowVersion:      testWorkflowVersion,
			ConfigurationVersion: testConfigurationVersion,
		}, nil)

	suite.cachedJob.EXPECT().
		ValidateEntityVersion(gomock.Any(), entityVersion).
		Return(yarpcerrors.InternalErrorf("test error"))

	_, err := suite.handler.GetReplaceJobDiff(
		context.Background(),
		&statelesssvc.GetReplaceJobDiffRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
			Spec: &stateless.JobSpec{
				InstanceCount: instanceCount,
			},
		},
	)
	suite.Error(err)
}

// TestGetReplaceJobDiffGetConfigError tests the failure case of DB error when
// fetching the job configuration when invoking GetReplaceJobDiff API
func (suite *statelessHandlerTestSuite) TestGetReplaceJobDiffGetConfigError() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: testEntityVersion}
	instanceCount := uint32(5)
	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State: pbtask.TaskState_RUNNING,
		}
		taskRuntimes[i] = runtime
	}

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			State:                pbjob.JobState_RUNNING,
			WorkflowVersion:      testWorkflowVersion,
			ConfigurationVersion: testConfigurationVersion,
		}, nil)

	suite.cachedJob.EXPECT().
		ValidateEntityVersion(gomock.Any(), entityVersion).
		Return(nil)

	suite.jobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
			testConfigurationVersion,
		).Return(
		nil, nil, yarpcerrors.InternalErrorf("test error"))

	_, err := suite.handler.GetReplaceJobDiff(
		context.Background(),
		&statelesssvc.GetReplaceJobDiffRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
			Spec: &stateless.JobSpec{
				InstanceCount: instanceCount,
			},
		},
	)
	suite.Error(err)
}

// TestResumeJobWorkflowFailNonLeader tests the failure case of resume workflow
// due to jobmgr is not leader
func (suite *statelessHandlerTestSuite) TestResumeJobWorkflowFailNonLeader() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1-1"}
	opaque := "test"

	suite.candidate.EXPECT().IsLeader().Return(false)

	resp, err := suite.handler.ResumeJobWorkflow(context.Background(),
		&statelesssvc.ResumeJobWorkflowRequest{
			JobId:      &v1alphapeloton.JobID{Value: testJobID},
			Version:    entityVersion,
			OpaqueData: &v1alphapeloton.OpaqueData{Data: opaque},
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestResumeJobWorkflowSuccess tests the success case of resume workflow
func (suite *statelessHandlerTestSuite) TestResumeJobWorkflowSuccess() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1-1"}
	newEntityVersion := &v1alphapeloton.EntityVersion{Value: "1-1-2"}
	opaque := "test"

	suite.candidate.EXPECT().IsLeader().Return(true)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		ResumeWorkflow(gomock.Any(), entityVersion, gomock.Any()).
		Return(&peloton.UpdateID{Value: testUpdateID}, newEntityVersion, nil)

	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: testJobID})

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(&peloton.JobID{Value: testJobID}, &peloton.UpdateID{Value: testUpdateID}, gomock.Any())

	resp, err := suite.handler.ResumeJobWorkflow(context.Background(),
		&statelesssvc.ResumeJobWorkflowRequest{
			JobId:      &v1alphapeloton.JobID{Value: testJobID},
			Version:    entityVersion,
			OpaqueData: &v1alphapeloton.OpaqueData{Data: opaque},
		})
	suite.NoError(err)
	suite.Equal(resp.GetVersion(), newEntityVersion)
}

// TestResumeJobWorkflowFailure tests the failure case of resume workflow
// due to fail to resume workflow
func (suite *statelessHandlerTestSuite) TestResumeJobWorkflowResumeWorkflowFailure() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1-1"}

	suite.candidate.EXPECT().IsLeader().Return(true)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		ResumeWorkflow(gomock.Any(), entityVersion, gomock.Any()).
		Return(nil, nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.ResumeJobWorkflow(context.Background(),
		&statelesssvc.ResumeJobWorkflowRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestAbortJobWorkflowSuccess tests the success case of abort workflow
func (suite *statelessHandlerTestSuite) TestAbortJobWorkflowSuccess() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1-1"}
	newEntityVersion := &v1alphapeloton.EntityVersion{Value: "1-1-2"}
	opaque := "test"

	suite.candidate.EXPECT().IsLeader().Return(true)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AbortWorkflow(gomock.Any(), entityVersion, gomock.Any()).
		Return(&peloton.UpdateID{Value: testUpdateID}, newEntityVersion, nil)

	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: testJobID})

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(&peloton.JobID{Value: testJobID}, &peloton.UpdateID{Value: testUpdateID}, gomock.Any())

	resp, err := suite.handler.AbortJobWorkflow(context.Background(),
		&statelesssvc.AbortJobWorkflowRequest{
			JobId:      &v1alphapeloton.JobID{Value: testJobID},
			Version:    entityVersion,
			OpaqueData: &v1alphapeloton.OpaqueData{Data: opaque},
		})
	suite.NoError(err)
	suite.Equal(resp.GetVersion(), newEntityVersion)
}

// TestAbortJobWorkflowAbortWorkflowFailure tests the failure case of abort workflow
// due to fail to abort workflow
func (suite *statelessHandlerTestSuite) TestAbortJobWorkflowAbortWorkflowFailure() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1-1"}

	suite.candidate.EXPECT().IsLeader().Return(true)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AbortWorkflow(gomock.Any(), entityVersion, gomock.Any()).
		Return(nil, nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.AbortJobWorkflow(context.Background(),
		&statelesssvc.AbortJobWorkflowRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestPauseJobWorkflowSuccess tests the success case of pause workflow
func (suite *statelessHandlerTestSuite) TestPauseJobWorkflowSuccess() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1-1"}
	newEntityVersion := &v1alphapeloton.EntityVersion{Value: "1-1-2"}
	opaque := "test"

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		PauseWorkflow(gomock.Any(), entityVersion, gomock.Any()).
		Return(&peloton.UpdateID{Value: testUpdateID}, newEntityVersion, nil)

	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: testJobID})

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(&peloton.JobID{Value: testJobID}, &peloton.UpdateID{Value: testUpdateID}, gomock.Any())

	resp, err := suite.handler.PauseJobWorkflow(context.Background(),
		&statelesssvc.PauseJobWorkflowRequest{
			JobId:      &v1alphapeloton.JobID{Value: testJobID},
			Version:    entityVersion,
			OpaqueData: &v1alphapeloton.OpaqueData{Data: opaque},
		})
	suite.NoError(err)
	suite.Equal(resp.GetVersion(), newEntityVersion)
}

// TestGetJobIDFromName tests the job name to job ids look up
func (suite *statelessHandlerTestSuite) TestGetJobIDFromName() {
	jobNameToIDs := []*ormobjects.JobNameToIDObject{
		{
			JobName: testJobName,
			JobID:   testJobID,
		},
	}

	suite.jobNameToIDOps.EXPECT().
		GetAll(gomock.Any(), testJobName).
		Return(nil, errors.New("failed to get job ids for job name"))

	_, err := suite.handler.GetJobIDFromJobName(context.Background(),
		&statelesssvc.GetJobIDFromJobNameRequest{
			JobName: testJobName,
		})
	suite.Error(err)

	suite.jobNameToIDOps.EXPECT().
		GetAll(gomock.Any(), testJobName).
		Return(jobNameToIDs, nil)
	resp, err := suite.handler.GetJobIDFromJobName(context.Background(),
		&statelesssvc.GetJobIDFromJobNameRequest{
			JobName: testJobName,
		})

	suite.Equal(len(jobNameToIDs), len(resp.GetJobId()))

	suite.jobNameToIDOps.EXPECT().
		GetAll(gomock.Any(), testJobName).
		Return([]*ormobjects.JobNameToIDObject{}, nil)
	_, err = suite.handler.GetJobIDFromJobName(context.Background(),
		&statelesssvc.GetJobIDFromJobNameRequest{
			JobName: testJobName,
		})
	suite.Error(err)
	suite.True(yarpcerrors.IsNotFound(err))
}

// TestPauseJobWorkflowPauseWorkflowFailure tests the failure case of pause workflow
// due to fail to pause workflow
func (suite *statelessHandlerTestSuite) TestPauseJobWorkflowPauseWorkflowFailure() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1-1"}

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		PauseWorkflow(gomock.Any(), entityVersion, gomock.Any()).
		Return(nil, nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.PauseJobWorkflow(context.Background(),
		&statelesssvc.PauseJobWorkflowRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		})
	suite.Error(err)
	suite.Nil(resp)
}

func (suite *statelessHandlerTestSuite) TestListPodsSuccess() {
	tasks := make(map[uint32]*pbtask.RuntimeInfo)
	instID := uint32(1)
	podName := fmt.Sprintf("%s-%d", testJobID, instID)
	mesosID := fmt.Sprintf("%s-%d", podName, 1)
	tasks[instID] = &pbtask.RuntimeInfo{
		State:       pbtask.TaskState_RUNNING,
		MesosTaskId: &mesos.TaskID{Value: &mesosID},
		Host:        "host1",
	}

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(
			gomock.Any(),
			&peloton.JobID{Value: testJobID},
			gomock.Any(),
		).
		Return(tasks, nil)

	suite.listPodsServer.EXPECT().
		Send(gomock.Any()).
		Do(func(resp *statelesssvc.ListPodsResponse) {
			suite.Equal(len(resp.GetPods()), 1)
			task := resp.GetPods()[0]
			suite.Equal(task.GetStatus().GetState(), pod.PodState_POD_STATE_RUNNING)
			suite.Equal(task.GetPodName().GetValue(), podName)
			suite.Equal(task.GetStatus().GetHost(), tasks[1].Host)
			suite.Equal(task.GetStatus().GetPodId().GetValue(), tasks[1].MesosTaskId.GetValue())
		}).Return(nil)

	suite.listJobsServer.EXPECT().Context().Return(context.Background()).AnyTimes()

	err := suite.handler.ListPods(
		&statelesssvc.ListPodsRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		},
		suite.listPodsServer,
	)
	suite.NoError(err)
}

func (suite *statelessHandlerTestSuite) TestListPodsTaskGetError() {
	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(
			gomock.Any(),
			&peloton.JobID{Value: testJobID},
			gomock.Any(),
		).
		Return(nil, fmt.Errorf("fake db error"))

	suite.listJobsServer.EXPECT().Context().Return(context.Background()).AnyTimes()

	err := suite.handler.ListPods(
		&statelesssvc.ListPodsRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		},
		suite.listPodsServer,
	)
	suite.Error(err)
}

func (suite *statelessHandlerTestSuite) TestListPodsSendError() {
	tasks := make(map[uint32]*pbtask.RuntimeInfo)
	instID := uint32(1)
	podName := fmt.Sprintf("%s-%d", testJobID, instID)
	mesosID := fmt.Sprintf("%s-%d", podName, 1)
	tasks[instID] = &pbtask.RuntimeInfo{
		State:       pbtask.TaskState_RUNNING,
		MesosTaskId: &mesos.TaskID{Value: &mesosID},
		Host:        "host1",
	}

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(
			gomock.Any(),
			&peloton.JobID{Value: testJobID},
			gomock.Any(),
		).
		Return(tasks, nil)

	suite.listPodsServer.EXPECT().
		Send(gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	suite.listJobsServer.EXPECT().Context().Return(context.Background()).AnyTimes()

	err := suite.handler.ListPods(
		&statelesssvc.ListPodsRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		},
		suite.listPodsServer,
	)
	suite.Error(err)
}

// TestListJobsSuccess tests invoking ListJobs API successfully
func (suite *statelessHandlerTestSuite) TestListJobsSuccess() {
	jobs := []*pbjob.JobSummary{
		{
			Id:   &peloton.JobID{Value: testJobID},
			Name: "testjob",
			Runtime: &pbjob.RuntimeInfo{
				State:    pbjob.JobState_RUNNING,
				UpdateID: &peloton.UpdateID{Value: testUpdateID},
			},
		},
	}

	suite.jobIndexOps.EXPECT().
		GetAll(gomock.Any()).
		Return(jobs, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(&models.UpdateModel{
			UpdateID: &peloton.UpdateID{Value: testUpdateID},
			State:    pbupdate.State_ROLLING_FORWARD,
		}, nil)

	suite.listJobsServer.EXPECT().
		Send(gomock.Any()).
		Do(func(resp *statelesssvc.ListJobsResponse) {
			suite.Equal(1, len(resp.GetJobs()))
			job := resp.GetJobs()[0]
			suite.Equal(job.GetName(), "testjob")
			suite.Equal(job.GetStatus().GetState(), stateless.JobState_JOB_STATE_RUNNING)
			suite.Equal(
				job.GetStatus().GetWorkflowStatus().GetState(),
				stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
			)
		}).
		Return(nil)

	suite.listJobsServer.EXPECT().Context().Return(context.Background()).AnyTimes()

	err := suite.handler.ListJobs(
		&statelesssvc.ListJobsRequest{},
		suite.listJobsServer,
	)
	suite.NoError(err)
}

// TestListJobsGetSummaryDBError tests getting DB error when fetching all
// job summaries from DB in the ListJobs API invocation
func (suite *statelessHandlerTestSuite) TestListJobsGetSummaryDBError() {
	suite.jobIndexOps.EXPECT().
		GetAll(gomock.Any()).
		Return(nil, fmt.Errorf("fake db error"))

	suite.listJobsServer.EXPECT().Context().Return(context.Background()).AnyTimes()

	err := suite.handler.ListJobs(
		&statelesssvc.ListJobsRequest{},
		suite.listJobsServer,
	)
	suite.Error(err)
}

// TestListJobsGetUpdateError tests getting DB error when fetching
// the update info from DB in the ListJobs API invocation
func (suite *statelessHandlerTestSuite) TestListJobsGetUpdateError() {
	jobs := []*pbjob.JobSummary{
		{
			Id:   &peloton.JobID{Value: testJobID},
			Name: "testjob",
			Runtime: &pbjob.RuntimeInfo{
				State:    pbjob.JobState_RUNNING,
				UpdateID: &peloton.UpdateID{Value: testUpdateID},
			},
		},
	}

	suite.jobIndexOps.EXPECT().
		GetAll(gomock.Any()).
		Return(jobs, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(nil, fmt.Errorf("fake db error"))

	suite.listJobsServer.EXPECT().
		Send(gomock.Any()).
		Do(func(resp *statelesssvc.ListJobsResponse) {
			suite.Equal(1, len(resp.GetJobs()))
			job := resp.GetJobs()[0]
			suite.Equal("testjob", job.GetName())
			suite.Equal(stateless.JobState_JOB_STATE_RUNNING, job.GetStatus().GetState())
			suite.Equal(
				stateless.WorkflowState_WORKFLOW_STATE_INVALID,
				job.GetStatus().GetWorkflowStatus().GetState(),
			)
		}).
		Return(nil)
	suite.listJobsServer.EXPECT().Context().Return(context.Background()).AnyTimes()

	err := suite.handler.ListJobs(
		&statelesssvc.ListJobsRequest{},
		suite.listJobsServer,
	)
	suite.NoError(err)
}

// TestListJobsSendError tests getting an error during Send to
// the stream in the ListJobs API invocation
func (suite *statelessHandlerTestSuite) TestListJobsSendError() {
	jobs := []*pbjob.JobSummary{
		{
			Id:   &peloton.JobID{Value: testJobID},
			Name: "testjob",
			Runtime: &pbjob.RuntimeInfo{
				State:    pbjob.JobState_RUNNING,
				UpdateID: &peloton.UpdateID{Value: testUpdateID},
			},
		},
	}

	suite.jobIndexOps.EXPECT().
		GetAll(gomock.Any()).
		Return(jobs, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(&models.UpdateModel{
			UpdateID: &peloton.UpdateID{Value: testUpdateID},
			State:    pbupdate.State_ROLLING_FORWARD,
		}, nil)

	suite.listJobsServer.EXPECT().
		Send(gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	suite.listJobsServer.EXPECT().Context().Return(context.Background()).AnyTimes()

	err := suite.handler.ListJobs(
		&statelesssvc.ListJobsRequest{},
		suite.listJobsServer,
	)
	suite.Error(err)
}

// TestCreateJobSuccess tests the success case of creating a job
func (suite *statelessHandlerTestSuite) TestCreateJobSuccess() {
	var testBatchSize uint32 = 10
	startPaused := true
	opaqueData := "test opaque data"

	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Command: &mesos.CommandInfo{Value: &testCmd},
			},
		},
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: defaultSpec,
		RespoolId:   testRespoolID,
	}

	request := &statelesssvc.CreateJobRequest{
		Spec:       jobSpec,
		OpaqueData: &v1alphapeloton.OpaqueData{Data: opaqueData},
		CreateSpec: &stateless.CreateSpec{
			BatchSize:   testBatchSize,
			StartPaused: startPaused,
		},
	}

	jobConfig, err := api.ConvertJobSpecToJobConfig(jobSpec)
	suite.NoError(err)

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),

		suite.jobFactory.EXPECT().
			AddJob(gomock.Any()).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			RollingCreate(
				gomock.Any(),
				jobConfig,
				gomock.Any(),
				jobSpec,
				&pbupdate.UpdateConfig{
					BatchSize:   testBatchSize,
					StartPaused: startPaused,
				},
				&peloton.OpaqueData{Data: opaqueData}).
			Return(nil),

		suite.goalStateDriver.EXPECT().
			EnqueueJob(
				gomock.Any(),
				gomock.Any(),
			),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbjob.RuntimeInfo{
				ConfigurationVersion: testConfigurationVersion,
				DesiredStateVersion:  testDesiredStateVersion,
				WorkflowVersion:      testWorkflowVersion,
			}, nil),
	)

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.NoError(err)
	suite.NotNil(response.GetJobId())
	suite.Equal(testEntityVersion, response.GetVersion().GetValue())
}

// TestCreateJobFailNonLeader tests the failure case of creating job
// due to JobMgr is not leader
func (suite *statelessHandlerTestSuite) TestCreateJobFailNonLeader() {
	suite.candidate.EXPECT().
		IsLeader().
		Return(false)

	resp, err := suite.handler.CreateJob(
		context.Background(),
		&statelesssvc.CreateJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})
	suite.Nil(resp)
	suite.Error(err)
}

// TestCreateJobFailInvalidJobID tests the failure case of creating job
// due to invalid JobID provided in the request
func (suite *statelessHandlerTestSuite) TestCreateJobFailInvalidJobID() {
	suite.candidate.EXPECT().IsLeader().Return(true)

	response, err := suite.handler.CreateJob(
		context.Background(),
		&statelesssvc.CreateJobRequest{
			JobId: &v1alphapeloton.JobID{Value: "invalid-Job-ID"},
		})
	suite.Nil(response)
	suite.Error(err)
}

// TestCreateJobFailNullResourcePool tests the failure case of creating job
// due to missing resource pool in the request
func (suite *statelessHandlerTestSuite) TestCreateJobFailNullResourcePool() {
	suite.candidate.EXPECT().IsLeader().Return(true)

	resp, err := suite.handler.CreateJob(
		context.Background(),
		&statelesssvc.CreateJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})
	suite.Nil(resp)
	suite.Error(err)
}

// TestCreateJobFailRootResourcePool tests the failure case of creating job
// due to root resource pool provided in the request
func (suite *statelessHandlerTestSuite) TestCreateJobFailRootResourcePool() {
	suite.candidate.EXPECT().IsLeader().Return(true)

	resp, err := suite.handler.CreateJob(
		context.Background(),
		&statelesssvc.CreateJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
			Spec: &stateless.JobSpec{
				RespoolId: &v1alphapeloton.ResourcePoolID{
					Value: common.RootResPoolID,
				},
			},
		})
	suite.Nil(resp)
	suite.Error(err)
}

// TestCreateJobFailGetResourcePoolFailure tests the failure case of creating job
// due to error while getting resource pool info
func (suite *statelessHandlerTestSuite) TestCreateJobFailGetResourcePoolFailure() {
	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(nil, yarpcerrors.InternalErrorf("test error")),
	)

	jobSpec := &stateless.JobSpec{
		RespoolId: testRespoolID,
	}
	request := &statelesssvc.CreateJobRequest{
		Spec: jobSpec,
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Error(err)
	suite.Nil(response)
}

// TestCreateJobFailResourcePoolNotFound tests the failure case of creating job
// due to resource pool not found error
func (suite *statelessHandlerTestSuite) TestCreateJobFailResourcePoolNotFound() {
	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(&respool.GetResponse{}, nil),
	)

	jobSpec := &stateless.JobSpec{
		RespoolId: testRespoolID,
	}
	request := &statelesssvc.CreateJobRequest{
		Spec: jobSpec,
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Error(err)
	suite.Nil(response)
}

// TestCreateJobFailResourcePoolNotFound tests the failure case of creating job
// due to non-leaf resource pool provided in the request
func (suite *statelessHandlerTestSuite) TestCreateJobFailNonLeafResourcePool() {
	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(&respool.GetResponse{
			Poolinfo: &respool.ResourcePoolInfo{
				Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				Children: []*peloton.ResourcePoolID{
					{
						Value: "child-respool1",
					},
				},
			},
		}, nil),
	)

	jobSpec := &stateless.JobSpec{
		RespoolId: testRespoolID,
	}
	request := &statelesssvc.CreateJobRequest{
		Spec: jobSpec,
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Error(err)
	suite.Nil(response)
}

// TestCreateJobFailJobSpecToJobConfigConversionFailure tests the failure case of creating job
// due to error while converting job spec to job config
func (suite *statelessHandlerTestSuite) TestCreateJobFailJobSpecToJobConfigConversionFailure() {
	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Command: &mesos.CommandInfo{Value: &testCmd},
			},
			{
				Command: &mesos.CommandInfo{Value: &testCmd},
			},
		},
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: defaultSpec,
		RespoolId:   testRespoolID,
	}
	request := &statelesssvc.CreateJobRequest{
		Spec: jobSpec,
	}

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),
	)

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Error(err)
	suite.Nil(response)
}

// TestCreateJobFailInvalidJobConfig tests the failure case of creating job
// due to invalid job config
func (suite *statelessHandlerTestSuite) TestCreateJobFailInvalidJobConfig() {
	jobSpec := &stateless.JobSpec{
		RespoolId:     testRespoolID,
		InstanceCount: suite.handler.jobSvcCfg.MaxTasksPerJob + 1,
	}
	request := &statelesssvc.CreateJobRequest{
		Spec: jobSpec,
	}

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),
	)

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Error(err)
	suite.Nil(response)
}

// TestCreateJobWithSecretsSuccess tests success scenario
// of creating a job with secrets
func (suite *statelessHandlerTestSuite) TestCreateJobWithSecretsSuccess() {
	testCmd := "echo test"
	mesosContainerizer := mesos.ContainerInfo_MESOS
	dockerContainerizer := mesos.ContainerInfo_DOCKER
	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Command:   &mesos.CommandInfo{Value: &testCmd},
				Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
			},
		},
	}
	respoolID := &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: defaultSpec,
		RespoolId:   respoolID,
	}

	jobSpec.InstanceSpec = make(map[uint32]*pod.PodSpec)
	for i := uint32(0); i < 25; i++ {
		jobSpec.InstanceSpec[i] = &pod.PodSpec{
			Containers: []*pod.ContainerSpec{
				{
					Resource:  defaultResourceConfig,
					Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
				},
			},
		}
	}
	// Create job where one instance is using docker containerizer.
	// The create should succeed and this instance will be
	// launched without secrets because container info in default
	// config is overridden.
	jobSpec.InstanceSpec[10].Containers[0].Container =
		&mesos.ContainerInfo{Type: &dockerContainerizer}

	jobConfig, err := api.ConvertJobSpecToJobConfig(jobSpec)
	suite.NoError(err)

	secret := &v1alphapeloton.Secret{
		Path: testSecretPath,
		Value: &v1alphapeloton.Secret_Value{
			Data: []byte(base64.StdEncoding.EncodeToString(
				[]byte(testSecretStr))),
		},
	}
	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),

		suite.secretInfoOps.EXPECT().CreateSecret(
			gomock.Any(),
			// jobID, now, secretID, secretString, secretPath
			testJobID, gomock.Any(), gomock.Any(), string(secret.Value.Data), testSecretPath).
			Return(nil),

		suite.jobFactory.EXPECT().
			AddJob(gomock.Any()).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			RollingCreate(
				gomock.Any(),
				jobConfig,
				gomock.Any(),
				jobSpec,
				gomock.Any(),
				gomock.Any()).
			Return(nil),

		suite.goalStateDriver.EXPECT().
			EnqueueJob(
				gomock.Any(),
				gomock.Any(),
			),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbjob.RuntimeInfo{
				ConfigurationVersion: testConfigurationVersion,
			}, nil),
	)

	// Create a job with a secret
	request := &statelesssvc.CreateJobRequest{
		JobId:   &v1alphapeloton.JobID{Value: testJobID},
		Spec:    jobSpec,
		Secrets: []*v1alphapeloton.Secret{secret},
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.NoError(err)
	suite.NotNil(response)
	suite.Equal(testJobID, response.GetJobId().GetValue())
}

// TestCreateJobWithSecretsFailureSecretsAddedToSpec tests failure scenario of
// creating a job with secrets when secret volumes are directly added to job spec
func (suite *statelessHandlerTestSuite) TestCreateJobWithSecretsFailureSecretsAddedToSpec() {
	testCmd := "echo test"
	mesosContainerizer := mesos.ContainerInfo_MESOS
	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Command:   &mesos.CommandInfo{Value: &testCmd},
				Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
			},
		},
	}
	respoolID := &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: defaultSpec,
		RespoolId:   respoolID,
	}

	secret := &peloton.Secret{
		Path: testSecretPath,
		Value: &peloton.Secret_Value{
			Data: []byte(base64.StdEncoding.EncodeToString(
				[]byte(testSecretStr))),
		},
	}

	jobSpec.GetDefaultSpec().GetContainers()[0].GetContainer().Volumes =
		append(
			jobSpec.GetDefaultSpec().GetContainers()[0].GetContainer().Volumes,
			util.CreateSecretVolume(secret.GetPath(),
				secret.GetId().GetValue()),
		)

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),
	)

	request := &statelesssvc.CreateJobRequest{
		JobId:   &v1alphapeloton.JobID{Value: testJobID},
		Spec:    jobSpec,
		Secrets: api.ConvertV0SecretsToV1Secrets([]*peloton.Secret{secret}),
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Nil(response)
	suite.Error(err)
}

// TestCreateJobWithSecretsCreateSecretsFailure tests failure scenario of
// creating a job with secrets due to db error
func (suite *statelessHandlerTestSuite) TestCreateJobWithSecretsCreateSecretsFailure() {
	testCmd := "echo test"
	mesosContainerizer := mesos.ContainerInfo_MESOS
	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Command:   &mesos.CommandInfo{Value: &testCmd},
				Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
			},
		},
	}
	respoolID := &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: defaultSpec,
		RespoolId:   respoolID,
	}
	secret := &v1alphapeloton.Secret{
		Path: testSecretPath,
		Value: &v1alphapeloton.Secret_Value{
			Data: []byte(base64.StdEncoding.EncodeToString(
				[]byte(testSecretStr))),
		},
	}
	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),

		suite.secretInfoOps.EXPECT().CreateSecret(
			gomock.Any(),
			// jobID, now, secretID, secretString, secretPath
			testJobID, gomock.Any(), gomock.Any(), string(secret.Value.Data), testSecretPath).
			Return(yarpcerrors.InternalErrorf("test error")),
	)

	request := &statelesssvc.CreateJobRequest{
		JobId:   &v1alphapeloton.JobID{Value: testJobID},
		Spec:    jobSpec,
		Secrets: []*v1alphapeloton.Secret{secret},
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Nil(response)
	suite.Error(err)
}

// TestCreateJobWithSecretsFailureNoContainerizer tests failure scenario of
// creating a job with secrets when default spec doesn't contain any containers
func (suite *statelessHandlerTestSuite) TestCreateJobWithSecretsFailureNoContainerizer() {
	testCmd := "echo test"
	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Command: &mesos.CommandInfo{Value: &testCmd},
			},
		},
	}
	respoolID := &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: defaultSpec,
		RespoolId:   respoolID,
	}

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),
	)

	secret := &v1alphapeloton.Secret{
		Path: testSecretPath,
		Value: &v1alphapeloton.Secret_Value{
			Data: []byte(base64.StdEncoding.EncodeToString(
				[]byte(testSecretStr))),
		},
	}

	request := &statelesssvc.CreateJobRequest{
		JobId:   &v1alphapeloton.JobID{Value: testJobID},
		Spec:    jobSpec,
		Secrets: []*v1alphapeloton.Secret{secret},
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Nil(response)
	suite.Error(err)
}

// TestCreateJobWithSecretsFailureEncodingError tests failure scenario of
// creating a job with secrets that are not base64 encoded
func (suite *statelessHandlerTestSuite) TestCreateJobWithSecretsFailureEncodingError() {
	testCmd := "echo test"
	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Command: &mesos.CommandInfo{Value: &testCmd},
			},
		},
	}
	respoolID := &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: defaultSpec,
		RespoolId:   respoolID,
	}

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),
	)

	secret := &v1alphapeloton.Secret{
		Path: testSecretPath,
		Value: &v1alphapeloton.Secret_Value{
			Data: []byte(testSecretStr),
		},
	}

	request := &statelesssvc.CreateJobRequest{
		JobId:   &v1alphapeloton.JobID{Value: testJobID},
		Spec:    jobSpec,
		Secrets: []*v1alphapeloton.Secret{secret},
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Nil(response)
	suite.Error(err)
}

// TestCreateJobWithSecretsFailureNonMesosContainerizer tests failure scenario of
// creating a job with secrets when default spec doesn't use Mesos containerizer
func (suite *statelessHandlerTestSuite) TestCreateJobWithSecretsFailureNonMesosContainerizer() {
	testCmd := "echo test"
	dockerContainerizer := mesos.ContainerInfo_DOCKER
	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Command:   &mesos.CommandInfo{Value: &testCmd},
				Container: &mesos.ContainerInfo{Type: &dockerContainerizer},
			},
		},
	}
	respoolID := &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: defaultSpec,
		RespoolId:   respoolID,
	}

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),
	)

	secret := &v1alphapeloton.Secret{
		Path: testSecretPath,
		Value: &v1alphapeloton.Secret_Value{
			Data: []byte(testSecretStr),
		},
	}

	request := &statelesssvc.CreateJobRequest{
		JobId:   &v1alphapeloton.JobID{Value: testJobID},
		Spec:    jobSpec,
		Secrets: []*v1alphapeloton.Secret{secret},
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Nil(response)
	suite.Error(err)
}

// TestCreateJobWithSecretsFailureSecretsDisabled tests failure scenario of
// creating a job with secrets when secrets are not enabled in JobSvcCfg
func (suite *statelessHandlerTestSuite) TestCreateJobWithSecretsFailureSecretsDisabled() {
	testCmd := "echo test"
	mesosContainerizer := mesos.ContainerInfo_MESOS
	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Command:   &mesos.CommandInfo{Value: &testCmd},
				Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
			},
		},
	}
	respoolID := &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: defaultSpec,
		RespoolId:   respoolID,
	}

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),
	)

	secret := &v1alphapeloton.Secret{
		Path: testSecretPath,
		Value: &v1alphapeloton.Secret_Value{
			Data: []byte(testSecretStr),
		},
	}

	request := &statelesssvc.CreateJobRequest{
		JobId:   &v1alphapeloton.JobID{Value: testJobID},
		Spec:    jobSpec,
		Secrets: []*v1alphapeloton.Secret{secret},
	}

	suite.handler.jobSvcCfg.EnableSecrets = false

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Nil(response)
	suite.Error(err)
}

// TestCreateJobWithSecretsFailureJobCacheCreateError tests failure scenario of
// creating a job with secrets due to error while creating job in cache
func (suite *statelessHandlerTestSuite) TestCreateJobFailureJobCacheCreateError() {
	respoolID := &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobSpec := &stateless.JobSpec{
		RespoolId: respoolID,
	}

	jobConfig, err := api.ConvertJobSpecToJobConfig(jobSpec)
	suite.NoError(err)

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),

		suite.jobFactory.EXPECT().
			AddJob(gomock.Any()).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			RollingCreate(
				gomock.Any(),
				jobConfig,
				gomock.Any(),
				jobSpec,
				gomock.Any(),
				gomock.Any()).
			Return(yarpcerrors.InternalErrorf("test error")),

		suite.goalStateDriver.EXPECT().
			EnqueueJob(
				gomock.Any(),
				gomock.Any(),
			),
	)

	request := &statelesssvc.CreateJobRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
		Spec:  jobSpec,
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Nil(response)
	suite.Error(err)
}

// TestCreateJobFailureGetJobRuntimeError tests failure scenario of
// creating a job due to error while getting job runtime
func (suite *statelessHandlerTestSuite) TestCreateJobFailureGetJobRuntimeError() {
	respoolID := &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobSpec := &stateless.JobSpec{
		RespoolId: respoolID,
	}

	jobConfig, err := api.ConvertJobSpecToJobConfig(jobSpec)
	suite.NoError(err)

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),

		suite.jobFactory.EXPECT().
			AddJob(gomock.Any()).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			RollingCreate(
				gomock.Any(),
				jobConfig,
				gomock.Any(),
				jobSpec,
				gomock.Any(),
				gomock.Any()).
			Return(nil),

		suite.goalStateDriver.EXPECT().
			EnqueueJob(
				gomock.Any(),
				gomock.Any(),
			),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(nil, yarpcerrors.InternalErrorf("test error")),
	)

	request := &statelesssvc.CreateJobRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
		Spec:  jobSpec,
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Nil(response)
	suite.Error(err)
}

// TestStopJobSuccess tests the success case of stopping job
func (suite *statelessHandlerTestSuite) TestStopJobSuccess() {
	jobRuntime := &pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		GoalState:            pbjob.JobState_RUNNING,
		ConfigurationVersion: testConfigurationVersion,
		DesiredStateVersion:  testDesiredStateVersion,
		WorkflowVersion:      testWorkflowVersion,
	}

	suite.candidate.EXPECT().IsLeader().Return(true)

	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: testJobID}).
		AnyTimes()

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.cachedJob.EXPECT().
		CompareAndSetRuntime(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, jobRuntime *pbjob.RuntimeInfo) {
			suite.Equal(jobRuntime.GetGoalState(), pbjob.JobState_KILLED)
			suite.Equal(jobRuntime.GetDesiredStateVersion(), testDesiredStateVersion+1)
		}).Return(&pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		GoalState:            pbjob.JobState_KILLED,
		ConfigurationVersion: testConfigurationVersion,
		DesiredStateVersion:  testDesiredStateVersion + 1,
		WorkflowVersion:      testWorkflowVersion,
	}, nil)

	suite.goalStateDriver.EXPECT().
		EnqueueJob(&peloton.JobID{Value: testJobID}, gomock.Any())

	resp, err := suite.handler.StopJob(
		context.Background(),
		&statelesssvc.StopJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: versionutil.GetJobEntityVersion(testConfigurationVersion, testDesiredStateVersion, testWorkflowVersion),
		})
	suite.NoError(err)
	suite.Equal(
		resp.GetVersion(),
		versionutil.GetJobEntityVersion(testConfigurationVersion, testDesiredStateVersion+1, testWorkflowVersion))
}

// TestStopJobNonLeaderFailure tests the failure case of stop
// a job due to jobmgr is not leader
func (suite *statelessHandlerTestSuite) TestStopJobNonLeaderFailure() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1-1"}

	suite.candidate.EXPECT().
		IsLeader().
		Return(false)

	resp, err := suite.handler.StopJob(
		context.Background(),
		&statelesssvc.StopJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		},
	)
	suite.Error(err)
	suite.Nil(resp)
}

// TestStopJobInvalidEntityVersionFailure tests the failure
// case of stopping job due to invalid entity version
func (suite *statelessHandlerTestSuite) TestStopJobInvalidEntityVersionFailure() {
	jobRuntime := &pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		GoalState:            pbjob.JobState_RUNNING,
		ConfigurationVersion: testConfigurationVersion,
		DesiredStateVersion:  testDesiredStateVersion,
		WorkflowVersion:      testWorkflowVersion,
	}

	suite.candidate.EXPECT().IsLeader().Return(true)

	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: testJobID}).
		AnyTimes()

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	resp, err := suite.handler.StopJob(
		context.Background(),
		&statelesssvc.StopJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: versionutil.GetJobEntityVersion(testConfigurationVersion, testDesiredStateVersion+1, testWorkflowVersion),
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestStopJobSuccessWithCompareAndSetRuntimeRetry tests the success
// case of stopping job after CompareAndSetRuntime retries due to UnexpectedVersionError
func (suite *statelessHandlerTestSuite) TestStopJobSuccessWithCompareAndSetRuntimeRetry() {
	suite.candidate.EXPECT().IsLeader().Return(true)

	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: testJobID}).
		AnyTimes()

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			State:                pbjob.JobState_RUNNING,
			GoalState:            pbjob.JobState_RUNNING,
			ConfigurationVersion: testConfigurationVersion,
			DesiredStateVersion:  testDesiredStateVersion,
			WorkflowVersion:      testWorkflowVersion,
		}, nil)

	suite.cachedJob.EXPECT().
		CompareAndSetRuntime(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, jobRuntime *pbjob.RuntimeInfo) {
			suite.Equal(jobRuntime.GetGoalState(), pbjob.JobState_KILLED)
			suite.Equal(jobRuntime.GetDesiredStateVersion(), testDesiredStateVersion+1)
		}).Return(nil, jobmgrcommon.UnexpectedVersionError)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			State:                pbjob.JobState_RUNNING,
			GoalState:            pbjob.JobState_RUNNING,
			ConfigurationVersion: testConfigurationVersion,
			DesiredStateVersion:  testDesiredStateVersion,
			WorkflowVersion:      testWorkflowVersion,
		}, nil)

	suite.cachedJob.EXPECT().
		CompareAndSetRuntime(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, jobRuntime *pbjob.RuntimeInfo) {
			suite.Equal(jobRuntime.GetGoalState(), pbjob.JobState_KILLED)
			suite.Equal(jobRuntime.GetDesiredStateVersion(), testDesiredStateVersion+1)
		}).Return(&pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		GoalState:            pbjob.JobState_KILLED,
		ConfigurationVersion: testConfigurationVersion,
		DesiredStateVersion:  testDesiredStateVersion + 1,
		WorkflowVersion:      testWorkflowVersion,
	}, nil)

	suite.goalStateDriver.EXPECT().
		EnqueueJob(&peloton.JobID{Value: testJobID}, gomock.Any())

	resp, err := suite.handler.StopJob(
		context.Background(),
		&statelesssvc.StopJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: versionutil.GetJobEntityVersion(testConfigurationVersion, testDesiredStateVersion, testWorkflowVersion),
		})
	suite.NoError(err)
	suite.Equal(
		resp.GetVersion(),
		versionutil.GetJobEntityVersion(testConfigurationVersion, testDesiredStateVersion+1, testWorkflowVersion))
}

// TestStopJobGetRuntimeFailure tests the failure case of stopping job
// due to runtime failure
func (suite *statelessHandlerTestSuite) TestStopJobGetRuntimeFailure() {
	suite.candidate.EXPECT().IsLeader().Return(true)

	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: testJobID}).
		AnyTimes()

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.StopJob(
		context.Background(),
		&statelesssvc.StopJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: versionutil.GetJobEntityVersion(testConfigurationVersion, testDesiredStateVersion, testWorkflowVersion),
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestStopJobCompareAndSetRuntimeFailure tests the failure case
// of stopping job due to CompareAndSetRuntime error
func (suite *statelessHandlerTestSuite) TestStopJobCompareAndSetRuntimeFailure() {
	jobRuntime := &pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		GoalState:            pbjob.JobState_RUNNING,
		ConfigurationVersion: testConfigurationVersion,
		DesiredStateVersion:  testDesiredStateVersion,
		WorkflowVersion:      testWorkflowVersion,
	}

	suite.candidate.EXPECT().IsLeader().Return(true)

	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: testJobID}).
		AnyTimes()

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.cachedJob.EXPECT().
		CompareAndSetRuntime(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, jobRuntime *pbjob.RuntimeInfo) {
			suite.Equal(jobRuntime.GetGoalState(), pbjob.JobState_KILLED)
			suite.Equal(jobRuntime.GetDesiredStateVersion(), testDesiredStateVersion+1)
		}).Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.goalStateDriver.EXPECT().
		EnqueueJob(&peloton.JobID{Value: testJobID}, gomock.Any())

	resp, err := suite.handler.StopJob(
		context.Background(),
		&statelesssvc.StopJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
			Version: versionutil.GetJobEntityVersion(
				testConfigurationVersion,
				testDesiredStateVersion,
				testWorkflowVersion,
			),
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestStopJobUpdateWithStartTasksRunningFailure tests the failure
// case of stopping job due to an update with StartTasks set is running
func (suite *statelessHandlerTestSuite) TestStopJobUpdateWithStartTasksRunningFailure() {
	jobRuntime := &pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		GoalState:            pbjob.JobState_RUNNING,
		ConfigurationVersion: testConfigurationVersion,
		DesiredStateVersion:  testDesiredStateVersion,
		WorkflowVersion:      testWorkflowVersion,
		UpdateID:             &peloton.UpdateID{Value: testUpdateID},
	}

	suite.candidate.EXPECT().IsLeader().Return(true)

	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: testJobID}).
		AnyTimes()

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(&models.UpdateModel{
			State: pbupdate.State_ROLLING_FORWARD,
			UpdateConfig: &pbupdate.UpdateConfig{
				StartTasks: true,
			},
		}, nil)

	resp, err := suite.handler.StopJob(
		context.Background(),
		&statelesssvc.StopJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: versionutil.GetJobEntityVersion(testConfigurationVersion, testDesiredStateVersion, testWorkflowVersion),
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestRestartJobSuccess tests the success case of restarting a job
func (suite *statelessHandlerTestSuite) TestRestartJobSuccess() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1-1"}
	newEntityVersion := &v1alphapeloton.EntityVersion{Value: "2-1-2"}
	batchSize := uint32(1)
	opaque := "test"
	ranges := []*pod.InstanceIDRange{
		{From: 0, To: 2},
		{From: 4, To: 6},
	}

	configVersion := uint64(2)

	suite.candidate.EXPECT().IsLeader().Return(true)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			ConfigurationVersion: configVersion,
		}, nil)

	suite.jobConfigOps.EXPECT().
		GetResult(
			gomock.Any(),
			testPelotonJobID,
			configVersion,
		).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig: &pbjob.JobConfig{
				ChangeLog: &peloton.ChangeLog{Version: configVersion},
			},
		}, nil)

	suite.cachedJob.EXPECT().
		CreateWorkflow(
			gomock.Any(),
			models.WorkflowType_RESTART,
			&pbupdate.UpdateConfig{
				BatchSize: batchSize,
				InPlace:   true,
			},
			entityVersion,
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Return(&peloton.UpdateID{Value: testUpdateID}, newEntityVersion, nil)

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(
			&peloton.JobID{Value: testJobID},
			&peloton.UpdateID{Value: testUpdateID},
			gomock.Any(),
		)

	resp, err := suite.handler.RestartJob(
		context.Background(),
		&statelesssvc.RestartJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
			RestartSpec: &stateless.RestartSpec{
				BatchSize: batchSize,
				Ranges:    ranges,
				InPlace:   true,
			},
			OpaqueData: &v1alphapeloton.OpaqueData{Data: opaque},
		},
	)
	suite.NoError(err)
	suite.Equal(resp.GetVersion().GetValue(), newEntityVersion.GetValue())
}

// TestStartJobNonLeaderFailure tests the failure case of start
// a job due to jobmgr is not leader
func (suite *statelessHandlerTestSuite) TestStartJobNonLeaderFailure() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1-1"}

	suite.candidate.EXPECT().
		IsLeader().
		Return(false)

	resp, err := suite.handler.StartJob(
		context.Background(),
		&statelesssvc.StartJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		},
	)
	suite.Error(err)
	suite.Nil(resp)
}

// TestRestartJobNonLeaderFailure tests the success case of fail to restart
// a job due to jobmgr is not leader
func (suite *statelessHandlerTestSuite) TestRestartJobNonLeaderFailure() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1-1"}
	batchSize := uint32(1)
	opaque := "test"
	ranges := []*pod.InstanceIDRange{
		{From: 0, To: 2},
		{From: 4, To: 6},
	}

	suite.candidate.EXPECT().
		IsLeader().
		Return(false)

	resp, err := suite.handler.RestartJob(
		context.Background(),
		&statelesssvc.RestartJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
			RestartSpec: &stateless.RestartSpec{
				BatchSize: batchSize,
				Ranges:    ranges,
			},
			OpaqueData: &v1alphapeloton.OpaqueData{Data: opaque},
		},
	)
	suite.Error(err)
	suite.Nil(resp)
}

// TestRestartJobGetRuntimeFailure tests the failure case of restarting a job
// due to get runtime error
func (suite *statelessHandlerTestSuite) TestRestartJobGetRuntimeFailure() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1-1"}
	batchSize := uint32(1)
	opaque := "test"
	ranges := []*pod.InstanceIDRange{
		{From: 0, To: 2},
		{From: 4, To: 6},
	}

	suite.candidate.EXPECT().
		IsLeader().
		Return(true)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.RestartJob(
		context.Background(),
		&statelesssvc.RestartJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
			RestartSpec: &stateless.RestartSpec{
				BatchSize: batchSize,
				Ranges:    ranges,
			},
			OpaqueData: &v1alphapeloton.OpaqueData{Data: opaque},
		},
	)
	suite.Error(err)
	suite.Nil(resp)
}

// TestRestartJobNoRangeSuccess tests the success case
// of restarting job when no range is provided
func (suite *statelessHandlerTestSuite) TestRestartJobNoRangeSuccess() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1-1"}
	newEntityVersion := &v1alphapeloton.EntityVersion{Value: "2-1-2"}
	batchSize := uint32(1)
	opaque := "test"
	ranges := []*pod.InstanceIDRange{
		{From: 0, To: 0},
	}
	instanceCount := uint32(10)

	configVersion := uint64(2)

	suite.candidate.EXPECT().
		IsLeader().
		Return(true)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			ConfigurationVersion: configVersion,
		}, nil)

	suite.jobConfigOps.EXPECT().
		GetResult(
			gomock.Any(),
			testPelotonJobID,
			configVersion,
		).
		Return(&ormobjects.JobConfigOpsResult{
			JobConfig: &pbjob.JobConfig{
				InstanceCount: instanceCount,
			},
		}, nil)

	suite.cachedJob.EXPECT().
		CreateWorkflow(
			gomock.Any(),
			models.WorkflowType_RESTART,
			&pbupdate.UpdateConfig{
				BatchSize: batchSize,
			},
			entityVersion,
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Do(
			func(
				_ context.Context,
				_ models.WorkflowType,
				_ *pbupdate.UpdateConfig,
				_ *v1alphapeloton.EntityVersion,
				option ...cached.Option,
			) {
				var instancesToUpdate []uint32
				for i := uint32(0); i < instanceCount; i++ {
					instancesToUpdate = append(instancesToUpdate, i)
				}
				suite.Equal(
					cached.WithInstanceToProcess(nil, instancesToUpdate, nil),
					option[0],
				)
			},
		).
		Return(&peloton.UpdateID{Value: testUpdateID}, newEntityVersion, nil)

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(
			&peloton.JobID{Value: testJobID},
			&peloton.UpdateID{Value: testUpdateID},
			gomock.Any(),
		)

	resp, err := suite.handler.RestartJob(
		context.Background(),
		&statelesssvc.RestartJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
			RestartSpec: &stateless.RestartSpec{
				BatchSize: batchSize,
				Ranges:    ranges,
			},
			OpaqueData: &v1alphapeloton.OpaqueData{Data: opaque},
		},
	)
	suite.NoError(err)
	suite.Equal(resp.GetVersion().GetValue(), newEntityVersion.GetValue())
}

// TestListJobWorkflowsSuccess tests the success case of list job updates
func (suite *statelessHandlerTestSuite) TestListJobWorkflowsSuccess() {
	testUpdateID1 := "941ff353-ba82-49fe-8f80-fb5bc649b04r"
	testUpdateID2 := "941ff353-ba82-49fe-8f80-fb5bc649b04p"
	workflowEvent1 := &stateless.WorkflowEvent{
		Type:      stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
		State:     stateless.WorkflowState_WORKFLOW_STATE_INITIALIZED,
		Timestamp: time.Now().Format(time.RFC3339),
	}
	workflowEvent2 := &stateless.WorkflowEvent{
		Type:      stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
		State:     stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	suite.updateStore.EXPECT().
		GetUpdatesForJob(gomock.Any(), testJobID).
		Return([]*peloton.UpdateID{
			{Value: testUpdateID1},
			{Value: testUpdateID2},
		}, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID1}).
		Return(&models.UpdateModel{
			UpdateID:             &peloton.UpdateID{Value: testUpdateID1},
			Type:                 models.WorkflowType_UPDATE,
			State:                pbupdate.State_ROLLING_FORWARD,
			InstancesDone:        1,
			InstancesFailed:      3,
			InstancesTotal:       20,
			InstancesCurrent:     []uint32{0, 1},
			JobConfigVersion:     2,
			PrevJobConfigVersion: 1,
			InstancesAdded:       []uint32{1},
			InstancesUpdated:     []uint32{0},
		}, nil)

	suite.jobUpdateEventsOps.EXPECT().
		GetAll(gomock.Any(), &peloton.UpdateID{Value: testUpdateID1}).
		Return([]*stateless.WorkflowEvent{workflowEvent2, workflowEvent1}, nil)

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), testPelotonJobID).
		Return(&pbjob.RuntimeInfo{
			State:    pbjob.JobState_RUNNING,
			UpdateID: &peloton.UpdateID{Value: testUpdateID},
		}, nil)

	suite.updateStore.EXPECT().
		GetWorkflowEvents(
			gomock.Any(),
			&peloton.UpdateID{Value: testUpdateID1},
			uint32(0),
			uint32(0),
		).
		Return([]*stateless.WorkflowEvent{workflowEvent2, workflowEvent1}, nil)

	suite.updateStore.EXPECT().
		GetWorkflowEvents(
			gomock.Any(),
			&peloton.UpdateID{Value: testUpdateID1},
			uint32(1),
			uint32(0),
		).
		Return([]*stateless.WorkflowEvent{workflowEvent2, workflowEvent1}, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID2}).
		Return(&models.UpdateModel{
			UpdateID:             &peloton.UpdateID{Value: testUpdateID2},
			Type:                 models.WorkflowType_UPDATE,
			State:                pbupdate.State_SUCCEEDED,
			InstancesDone:        10,
			InstancesFailed:      2,
			InstancesTotal:       20,
			InstancesCurrent:     []uint32{0, 1, 2},
			JobConfigVersion:     3,
			PrevJobConfigVersion: 2,
			InstancesRemoved:     []uint32{1},
		}, nil)

	suite.jobUpdateEventsOps.EXPECT().
		GetAll(gomock.Any(), &peloton.UpdateID{Value: testUpdateID2}).
		Return([]*stateless.WorkflowEvent{workflowEvent2, workflowEvent1}, nil)

	suite.updateStore.EXPECT().
		GetWorkflowEvents(gomock.Any(), &peloton.UpdateID{Value: testUpdateID2}, uint32(1), uint32(0)).
		Return([]*stateless.WorkflowEvent{workflowEvent2, workflowEvent1}, nil)

	resp, err := suite.handler.ListJobWorkflows(context.Background(), &statelesssvc.ListJobWorkflowsRequest{
		JobId:          &v1alphapeloton.JobID{Value: testJobID},
		UpdatesLimit:   4,
		InstanceEvents: true,
	})

	suite.NoError(err)
	suite.Equal(
		resp.GetWorkflowInfos()[0].GetStatus().GetState(),
		stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
	)
	suite.Equal(
		resp.GetWorkflowInfos()[0].GetStatus().GetNumInstancesCompleted(),
		uint32(1))
	suite.Equal(
		resp.GetWorkflowInfos()[0].GetStatus().GetNumInstancesFailed(),
		uint32(3))
	suite.Equal(
		resp.GetWorkflowInfos()[0].GetStatus().GetNumInstancesRemaining(),
		uint32(16))
	suite.Equal(
		resp.GetWorkflowInfos()[0].GetStatus().GetInstancesCurrent(),
		[]uint32{0, 1})

	suite.Equal(
		resp.GetWorkflowInfos()[1].GetStatus().GetState(),
		stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED)
	suite.Equal(
		resp.GetWorkflowInfos()[1].GetStatus().GetNumInstancesCompleted(),
		uint32(10))
	suite.Equal(
		resp.GetWorkflowInfos()[1].GetStatus().GetNumInstancesFailed(),
		uint32(2))
	suite.Equal(
		resp.GetWorkflowInfos()[1].GetStatus().GetNumInstancesRemaining(),
		uint32(8))
	suite.Equal(
		resp.GetWorkflowInfos()[1].GetStatus().GetInstancesCurrent(),
		[]uint32{0, 1, 2})
	suite.Equal(2, len(resp.GetWorkflowInfos()[0].GetInstanceEvents()))
	suite.Equal(1, len(resp.GetWorkflowInfos()[1].GetInstanceEvents()))
}

// TestListJobWorkflowsGetUpdatesLimitInstanceEvents tests
// the case of limiting the number of instance update events
func (suite *statelessHandlerTestSuite) TestListJobWorkflowsGetUpdatesLimitInstanceEvents() {
	testUpdateID1 := "941ff353-ba82-49fe-8f80-fb5bc649b04r"
	testUpdateID2 := "941ff353-ba82-49fe-8f80-fb5bc649b04p"
	workflowEvent1 := &stateless.WorkflowEvent{
		Type:      stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
		State:     stateless.WorkflowState_WORKFLOW_STATE_INITIALIZED,
		Timestamp: time.Now().Format(time.RFC3339),
	}
	workflowEvent2 := &stateless.WorkflowEvent{
		Type:      stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
		State:     stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	suite.updateStore.EXPECT().
		GetUpdatesForJob(gomock.Any(), testJobID).
		Return([]*peloton.UpdateID{
			{Value: testUpdateID1},
			{Value: testUpdateID2},
		}, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID1}).
		Return(&models.UpdateModel{
			UpdateID:             &peloton.UpdateID{Value: testUpdateID1},
			Type:                 models.WorkflowType_UPDATE,
			State:                pbupdate.State_ROLLING_FORWARD,
			InstancesDone:        1,
			InstancesFailed:      3,
			InstancesTotal:       20,
			InstancesCurrent:     []uint32{0, 1},
			JobConfigVersion:     2,
			PrevJobConfigVersion: 1,
			InstancesAdded:       []uint32{1},
			InstancesUpdated:     []uint32{0},
		}, nil)

	suite.jobUpdateEventsOps.EXPECT().
		GetAll(gomock.Any(), &peloton.UpdateID{Value: testUpdateID1}).
		Return([]*stateless.WorkflowEvent{workflowEvent2}, nil)

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), testPelotonJobID).
		Return(&pbjob.RuntimeInfo{
			State:    pbjob.JobState_RUNNING,
			UpdateID: &peloton.UpdateID{Value: testUpdateID},
		}, nil)

	suite.updateStore.EXPECT().
		GetWorkflowEvents(
			gomock.Any(),
			&peloton.UpdateID{Value: testUpdateID1},
			uint32(0),
			uint32(1),
		).
		Return([]*stateless.WorkflowEvent{workflowEvent1}, nil)

	suite.updateStore.EXPECT().
		GetWorkflowEvents(
			gomock.Any(),
			&peloton.UpdateID{Value: testUpdateID1},
			uint32(1),
			uint32(1),
		).
		Return([]*stateless.WorkflowEvent{workflowEvent2}, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID2}).
		Return(&models.UpdateModel{
			UpdateID:             &peloton.UpdateID{Value: testUpdateID2},
			Type:                 models.WorkflowType_UPDATE,
			State:                pbupdate.State_SUCCEEDED,
			InstancesDone:        10,
			InstancesFailed:      2,
			InstancesTotal:       20,
			InstancesCurrent:     []uint32{0, 1, 2},
			JobConfigVersion:     3,
			PrevJobConfigVersion: 2,
			InstancesRemoved:     []uint32{1},
		}, nil)

	suite.jobUpdateEventsOps.EXPECT().
		GetAll(gomock.Any(), &peloton.UpdateID{Value: testUpdateID2}).
		Return([]*stateless.WorkflowEvent{workflowEvent2}, nil)

	suite.updateStore.EXPECT().
		GetWorkflowEvents(
			gomock.Any(),
			&peloton.UpdateID{Value: testUpdateID2},
			uint32(1),
			uint32(1),
		).
		Return([]*stateless.WorkflowEvent{workflowEvent1}, nil)

	resp, err := suite.handler.ListJobWorkflows(context.Background(),
		&statelesssvc.ListJobWorkflowsRequest{
			JobId:               &v1alphapeloton.JobID{Value: testJobID},
			UpdatesLimit:        4,
			InstanceEvents:      true,
			InstanceEventsLimit: 1,
		})
	suite.NoError(err)
	suite.Equal(2, len(resp.GetWorkflowInfos()[0].GetInstanceEvents()))
	suite.Equal(1, len(resp.GetWorkflowInfos()[1].GetInstanceEvents()))

	suite.Equal(1, len(resp.GetWorkflowInfos()[0].GetInstanceEvents()[0].GetEvents()))
	suite.Equal(1, len(resp.GetWorkflowInfos()[1].GetInstanceEvents()[0].GetEvents()))
}

// TestListJobWorkflowsGetUpdatesFailure tests the failure
// case of getting job updates due to fail to read updates of a job
func (suite *statelessHandlerTestSuite) TestListJobWorkflowsGetUpdatesFailure() {
	testUpdateID1 := "941ff353-ba82-49fe-8f80-fb5bc649b04r"
	testUpdateID2 := "941ff353-ba82-49fe-8f80-fb5bc649b04p"
	workflowEvent1 := &stateless.WorkflowEvent{
		Type:      stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
		State:     stateless.WorkflowState_WORKFLOW_STATE_INITIALIZED,
		Timestamp: time.Now().Format(time.RFC3339),
	}
	workflowEvent2 := &stateless.WorkflowEvent{
		Type:      stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
		State:     stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	suite.updateStore.EXPECT().
		GetUpdatesForJob(gomock.Any(), testJobID).
		Return([]*peloton.UpdateID{
			{Value: testUpdateID1},
			{Value: testUpdateID2},
		}, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID1}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID2}).
		Return(&models.UpdateModel{
			UpdateID:             &peloton.UpdateID{Value: testUpdateID2},
			Type:                 models.WorkflowType_UPDATE,
			State:                pbupdate.State_ROLLING_FORWARD,
			InstancesDone:        1,
			InstancesFailed:      3,
			InstancesTotal:       20,
			InstancesCurrent:     []uint32{0, 1},
			JobConfigVersion:     2,
			PrevJobConfigVersion: 1,
			InstancesAdded:       []uint32{1},
			InstancesUpdated:     []uint32{0},
		}, nil)

	suite.jobUpdateEventsOps.EXPECT().
		GetAll(gomock.Any(), &peloton.UpdateID{Value: testUpdateID2}).
		Return([]*stateless.WorkflowEvent{workflowEvent2, workflowEvent1}, nil)

	suite.jobRuntimeOps.EXPECT().
		Get(gomock.Any(), testPelotonJobID).
		Return(&pbjob.RuntimeInfo{
			State:    pbjob.JobState_RUNNING,
			UpdateID: &peloton.UpdateID{Value: testUpdateID2},
		}, nil)

	suite.updateStore.EXPECT().
		GetWorkflowEvents(
			gomock.Any(),
			&peloton.UpdateID{Value: testUpdateID2},
			uint32(0),
			uint32(0),
		).
		Return([]*stateless.WorkflowEvent{workflowEvent2, workflowEvent1}, nil)

	suite.updateStore.EXPECT().
		GetWorkflowEvents(
			gomock.Any(),
			&peloton.UpdateID{Value: testUpdateID2},
			uint32(1),
			uint32(0),
		).
		Return([]*stateless.WorkflowEvent{workflowEvent2, workflowEvent1}, nil)

	resp, err := suite.handler.ListJobWorkflows(context.Background(), &statelesssvc.ListJobWorkflowsRequest{
		JobId:          &v1alphapeloton.JobID{Value: testJobID},
		InstanceEvents: true,
	})
	suite.Error(err)
	suite.Nil(resp)
}

// TestListJobWorkflowsGetRuntimeFailure tests the failure
// case of getting job updates due to failure for getting job runtime.
func (suite *statelessHandlerTestSuite) TestListJobWorkflowsGetRuntimeFailure() {
	testUpdateID1 := "941ff353-ba82-49fe-8f80-fb5bc649b04r"
	testUpdateID2 := "941ff353-ba82-49fe-8f80-fb5bc649b04p"

	suite.updateStore.EXPECT().
		GetUpdatesForJob(gomock.Any(), testJobID).
		Return([]*peloton.UpdateID{
			{Value: testUpdateID1},
			{Value: testUpdateID2},
		}, nil)

	suite.jobRuntimeOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
		).
		Return(nil, fmt.Errorf("fake db error"))

	resp, err := suite.handler.ListJobWorkflows(context.Background(), &statelesssvc.ListJobWorkflowsRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
	})
	suite.Error(err)
	suite.Nil(resp)
}

// TestDeleteJobSuccess tests the success case of deleting a job
func (suite *statelessHandlerTestSuite) TestDeleteJobSuccess() {
	jobRuntime := &pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		GoalState:            pbjob.JobState_RUNNING,
		ConfigurationVersion: testConfigurationVersion,
		DesiredStateVersion:  testDesiredStateVersion,
		WorkflowVersion:      testWorkflowVersion,
	}

	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = cachedTask

	suite.candidate.EXPECT().IsLeader().Return(true)

	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: testJobID}).
		AnyTimes()

	gomock.InOrder(
		suite.jobFactory.EXPECT().
			AddJob(&peloton.JobID{Value: testJobID}).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(jobRuntime, nil),

		suite.cachedJob.EXPECT().
			CompareAndSetRuntime(gomock.Any(), gomock.Any()).
			Do(func(ctx context.Context, runtime *pbjob.RuntimeInfo) {
				suite.Equal(pbjob.JobState_DELETED, runtime.GetGoalState())
				suite.Equal(testDesiredStateVersion+1, runtime.GetDesiredStateVersion())
			}).Return(&pbjob.RuntimeInfo{
			State:                pbjob.JobState_RUNNING,
			GoalState:            pbjob.JobState_DELETED,
			ConfigurationVersion: testConfigurationVersion,
			DesiredStateVersion:  testDesiredStateVersion + 1,
			WorkflowVersion:      testWorkflowVersion,
		}, nil),

		suite.goalStateDriver.EXPECT().
			EnqueueJob(&peloton.JobID{Value: testJobID}, gomock.Any()),
	)

	resp, err := suite.handler.DeleteJob(
		context.Background(),
		&statelesssvc.DeleteJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: &v1alphapeloton.EntityVersion{Value: testEntityVersion},
			Force:   true,
		},
	)
	suite.NoError(err)
	suite.NotNil(resp)
}

// TestDeleteJobFailNonLeader tests the failure case of delete job
// due to jobmgr is not leader
func (suite *statelessHandlerTestSuite) TestDeleteJobFailNonLeader() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1-1"}

	suite.candidate.EXPECT().IsLeader().Return(false)

	resp, err := suite.handler.DeleteJob(context.Background(),
		&statelesssvc.DeleteJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestDeleteJobGetRuntimeFailure tests the failure case of
// deleting a job due to error while getting job runtime
func (suite *statelessHandlerTestSuite) TestDeleteJobGetRuntimeFailure() {
	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.jobFactory.EXPECT().
			AddJob(&peloton.JobID{Value: testJobID}).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(nil, yarpcerrors.InternalErrorf("test error")),
	)

	resp, err := suite.handler.DeleteJob(
		context.Background(),
		&statelesssvc.DeleteJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: &v1alphapeloton.EntityVersion{Value: testEntityVersion},
		},
	)
	suite.Error(err)
	suite.Nil(resp)
}

// TestDeleteJobBadVersion tests failure case
// of deleting a job due to entity version mismatch
func (suite *statelessHandlerTestSuite) TestDeleteJobBadVersion() {
	jobRuntime := &pbjob.RuntimeInfo{
		ConfigurationVersion: testConfigurationVersion,
		DesiredStateVersion:  testDesiredStateVersion,
		WorkflowVersion:      testWorkflowVersion,
	}

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.jobFactory.EXPECT().
			AddJob(&peloton.JobID{Value: testJobID}).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(jobRuntime, nil),
	)

	resp, err := suite.handler.DeleteJob(
		context.Background(),
		&statelesssvc.DeleteJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: &v1alphapeloton.EntityVersion{Value: "1-2-3"},
		},
	)
	suite.Error(err)
	suite.Nil(resp)
}

// TestDeleteJobNonTerminalJobFailure tests failure case
// of deleting a job due to job being in non-terminal state
func (suite *statelessHandlerTestSuite) TestDeleteJobNonTerminalJobFailure() {
	jobRuntime := &pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		GoalState:            pbjob.JobState_RUNNING,
		ConfigurationVersion: testConfigurationVersion,
		DesiredStateVersion:  testDesiredStateVersion,
		WorkflowVersion:      testWorkflowVersion,
	}

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.jobFactory.EXPECT().
			AddJob(&peloton.JobID{Value: testJobID}).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(jobRuntime, nil),
	)

	resp, err := suite.handler.DeleteJob(
		context.Background(),
		&statelesssvc.DeleteJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: &v1alphapeloton.EntityVersion{Value: testEntityVersion},
		},
	)
	suite.Error(err)
	suite.Nil(resp)
}

// TestDeleteJobRuntimeUpdateFailure tests the failure case
// of deleting a job due to error while updating runtime
func (suite *statelessHandlerTestSuite) TestDeleteJobRuntimeUpdateFailure() {
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = cachedTask

	suite.candidate.EXPECT().IsLeader().Return(true)

	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: testJobID}).
		AnyTimes()

	gomock.InOrder(
		suite.jobFactory.EXPECT().
			AddJob(&peloton.JobID{Value: testJobID}).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbjob.RuntimeInfo{
				State:                pbjob.JobState_RUNNING,
				GoalState:            pbjob.JobState_RUNNING,
				ConfigurationVersion: testConfigurationVersion,
				DesiredStateVersion:  testDesiredStateVersion,
				WorkflowVersion:      testWorkflowVersion,
			}, nil),

		suite.cachedJob.EXPECT().
			CompareAndSetRuntime(gomock.Any(), gomock.Any()).
			Do(func(ctx context.Context, runtime *pbjob.RuntimeInfo) {
				suite.Equal(pbjob.JobState_DELETED, runtime.GetGoalState())
				suite.Equal(testDesiredStateVersion+1, runtime.GetDesiredStateVersion())
			}).Return(nil, jobmgrcommon.UnexpectedVersionError),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbjob.RuntimeInfo{
				State:                pbjob.JobState_RUNNING,
				GoalState:            pbjob.JobState_RUNNING,
				ConfigurationVersion: testConfigurationVersion,
				DesiredStateVersion:  testDesiredStateVersion,
				WorkflowVersion:      testWorkflowVersion,
			}, nil),

		suite.cachedJob.EXPECT().
			CompareAndSetRuntime(gomock.Any(), gomock.Any()).
			Do(func(ctx context.Context, runtime *pbjob.RuntimeInfo) {
				suite.Equal(pbjob.JobState_DELETED, runtime.GetGoalState())
				suite.Equal(testDesiredStateVersion+1, runtime.GetDesiredStateVersion())
			}).Return(nil, jobmgrcommon.UnexpectedVersionError),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbjob.RuntimeInfo{
				State:                pbjob.JobState_RUNNING,
				GoalState:            pbjob.JobState_RUNNING,
				ConfigurationVersion: testConfigurationVersion,
				DesiredStateVersion:  testDesiredStateVersion,
				WorkflowVersion:      testWorkflowVersion,
			}, nil),

		suite.cachedJob.EXPECT().
			CompareAndSetRuntime(gomock.Any(), gomock.Any()).
			Do(func(ctx context.Context, runtime *pbjob.RuntimeInfo) {
				suite.Equal(pbjob.JobState_DELETED, runtime.GetGoalState())
				suite.Equal(testDesiredStateVersion+1, runtime.GetDesiredStateVersion())
			}).Return(nil, jobmgrcommon.UnexpectedVersionError),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbjob.RuntimeInfo{
				State:                pbjob.JobState_RUNNING,
				GoalState:            pbjob.JobState_RUNNING,
				ConfigurationVersion: testConfigurationVersion,
				DesiredStateVersion:  testDesiredStateVersion,
				WorkflowVersion:      testWorkflowVersion,
			}, nil),

		suite.cachedJob.EXPECT().
			CompareAndSetRuntime(gomock.Any(), gomock.Any()).
			Do(func(ctx context.Context, runtime *pbjob.RuntimeInfo) {
				suite.Equal(pbjob.JobState_DELETED, runtime.GetGoalState())
				suite.Equal(testDesiredStateVersion+1, runtime.GetDesiredStateVersion())
			}).Return(nil, jobmgrcommon.UnexpectedVersionError),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbjob.RuntimeInfo{
				State:                pbjob.JobState_RUNNING,
				GoalState:            pbjob.JobState_RUNNING,
				ConfigurationVersion: testConfigurationVersion,
				DesiredStateVersion:  testDesiredStateVersion,
				WorkflowVersion:      testWorkflowVersion,
			}, nil),

		suite.cachedJob.EXPECT().
			CompareAndSetRuntime(gomock.Any(), gomock.Any()).
			Do(func(ctx context.Context, runtime *pbjob.RuntimeInfo) {
				suite.Equal(pbjob.JobState_DELETED, runtime.GetGoalState())
				suite.Equal(testDesiredStateVersion+1, runtime.GetDesiredStateVersion())
			}).Return(nil, jobmgrcommon.UnexpectedVersionError),

		suite.goalStateDriver.EXPECT().
			EnqueueJob(&peloton.JobID{Value: testJobID}, gomock.Any()),
	)

	resp, err := suite.handler.DeleteJob(
		context.Background(),
		&statelesssvc.DeleteJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: &v1alphapeloton.EntityVersion{Value: testEntityVersion},
			Force:   true,
		},
	)
	suite.Error(err)
	suite.Nil(resp)
}

// TestQueryPodsSuccess tests success case of querying pods of a job
func (suite *statelessHandlerTestSuite) TestQueryPodsSuccess() {
	pelotonJobID := &peloton.JobID{Value: testJobID}
	taskInfos := []*pbtask.TaskInfo{
		{
			Config: &pbtask.TaskConfig{
				Name: "peloton",
				Resource: &pbtask.ResourceConfig{
					CpuLimit:    4,
					MemLimitMb:  200,
					DiskLimitMb: 400,
					FdLimit:     100,
					GpuLimit:    10,
				},
				RestartPolicy: &pbtask.RestartPolicy{
					MaxFailures: 5,
				},
				Volume: &pbtask.PersistentVolumeConfig{
					ContainerPath: "test/container/path",
					SizeMB:        100,
				},
				PreemptionPolicy: &pbtask.PreemptionPolicy{
					Type:          pbtask.PreemptionPolicy_TYPE_NON_PREEMPTIBLE,
					KillOnPreempt: false,
				},
				Controller:             false,
				KillGracePeriodSeconds: 5,
				Revocable:              false,
			},
			Runtime: &pbtask.RuntimeInfo{
				State: pbtask.TaskState_RUNNING,
				MesosTaskId: &mesos.TaskID{
					Value: &testMesosTaskID,
				},
				GoalState: pbtask.TaskState_SUCCEEDED,
				AgentID: &mesos.AgentID{
					Value: &testAgentID,
				},
				Revision: &peloton.ChangeLog{
					Version:   1,
					CreatedAt: 2,
					UpdatedAt: 3,
					UpdatedBy: "peloton",
				},
				PrevMesosTaskId: &mesos.TaskID{
					Value: &testPrevMesosTaskID,
				},
				Healthy: pbtask.HealthState_HEALTHY,
				DesiredMesosTaskId: &mesos.TaskID{
					Value: &testMesosTaskID,
				},
			},
		},
		{
			Config: &pbtask.TaskConfig{
				Name: "test",
				Resource: &pbtask.ResourceConfig{
					CpuLimit:    2,
					MemLimitMb:  100,
					DiskLimitMb: 200,
					FdLimit:     50,
					GpuLimit:    5,
				},
			},
			Runtime: &pbtask.RuntimeInfo{
				State:     pbtask.TaskState_PENDING,
				GoalState: pbtask.TaskState_SUCCEEDED,
			},
		},
	}

	request := &statelesssvc.QueryPodsRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
		Spec: &pod.QuerySpec{
			PodStates: []pod.PodState{
				pod.PodState_POD_STATE_RUNNING,
				pod.PodState_POD_STATE_PENDING,
			},
		},
	}

	gomock.InOrder(
		suite.jobRuntimeOps.EXPECT().
			Get(gomock.Any(), testPelotonJobID).
			Return(&pbjob.RuntimeInfo{}, nil),

		suite.jobConfigOps.EXPECT().
			Get(gomock.Any(), testPelotonJobID, gomock.Any()).
			Return(&pbjob.JobConfig{}, nil, nil),

		suite.taskStore.EXPECT().
			QueryTasks(
				gomock.Any(),
				pelotonJobID,
				api.ConvertPodQuerySpecToTaskQuerySpec(request.GetSpec()),
			).Return(taskInfos, uint32(len(taskInfos)), nil),

		suite.activeRMTasks.EXPECT().
			GetTask(gomock.Any()).
			Return(&resmgrsvc.GetActiveTasksResponse_TaskEntry{
				Reason: "test reason",
			}),
	)

	pagination := &v1alphaquery.Pagination{
		Offset: request.GetPagination().GetOffset(),
		Limit:  request.GetPagination().GetLimit(),
		Total:  uint32(len(taskInfos)),
	}

	response, err := suite.handler.QueryPods(context.Background(), request)
	suite.NoError(err)
	suite.Len(response.GetPods(), len(taskInfos))
	for i, podInfo := range response.GetPods() {
		if podInfo.GetStatus().GetState() == pod.PodState_POD_STATE_PENDING {
			taskInfos[i].GetRuntime().Reason = podInfo.GetStatus().GetReason()
		}
	}
	suite.Equal(api.ConvertTaskInfosToPodInfos(taskInfos), response.GetPods())
	suite.Equal(pagination, response.GetPagination())
}

// TestQueryPodsFailureJobRuntimeError tests failure case of
// querying pods of a job due to error while getting job runtime
func (suite *statelessHandlerTestSuite) TestQueryPodsFailureJobRuntimeError() {
	request := &statelesssvc.QueryPodsRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
		Spec: &pod.QuerySpec{
			PodStates: []pod.PodState{
				pod.PodState_POD_STATE_RUNNING,
				pod.PodState_POD_STATE_PENDING,
			},
		},
	}

	suite.jobRuntimeOps.EXPECT().
		Get(
			gomock.Any(),
			testPelotonJobID,
		).
		Return(&pbjob.RuntimeInfo{}, nil)

	suite.jobConfigOps.EXPECT().
		Get(gomock.Any(), testPelotonJobID, gomock.Any()).
		Return(nil, nil, yarpcerrors.InternalErrorf("test error"))

	response, err := suite.handler.QueryPods(context.Background(), request)
	suite.Error(err)
	suite.Nil(response)
}

// TestQueryPodsFailureQueryTasksError tests failure case of
// querying pods of a job due to error while querying tasks from DB
func (suite *statelessHandlerTestSuite) TestQueryPodsFailureQueryTasksError() {
	pelotonJobID := &peloton.JobID{Value: testJobID}
	request := &statelesssvc.QueryPodsRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
		Spec: &pod.QuerySpec{
			PodStates: []pod.PodState{
				pod.PodState_POD_STATE_RUNNING,
				pod.PodState_POD_STATE_PENDING,
			},
		},
	}

	gomock.InOrder(
		suite.jobRuntimeOps.EXPECT().
			Get(gomock.Any(), testPelotonJobID).
			Return(&pbjob.RuntimeInfo{}, nil),

		suite.jobConfigOps.EXPECT().
			Get(gomock.Any(), testPelotonJobID, gomock.Any()).
			Return(&pbjob.JobConfig{}, nil, nil),

		suite.taskStore.EXPECT().
			QueryTasks(
				gomock.Any(),
				pelotonJobID,
				api.ConvertPodQuerySpecToTaskQuerySpec(request.GetSpec()),
			).Return(nil, uint32(0), yarpcerrors.InternalErrorf("test error")),
	)

	response, err := suite.handler.QueryPods(context.Background(), request)
	suite.Error(err)
	suite.Nil(response)
}

// TestStartJobSuccess tests the success case of starting a stateless job
func (suite *statelessHandlerTestSuite) TestStartJobSuccess() {
	configVersion := uint64(2)
	desiredStateVersion := uint64(3)
	workflowVersion := uint64(4)

	jobRuntime := &pbjob.RuntimeInfo{
		State:                pbjob.JobState_KILLED,
		GoalState:            pbjob.JobState_KILLED,
		ConfigurationVersion: configVersion,
		DesiredStateVersion:  desiredStateVersion,
		WorkflowVersion:      workflowVersion,
	}

	suite.candidate.EXPECT().IsLeader().Return(true)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.cachedJob.EXPECT().
		CompareAndSetRuntime(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, jobRuntime *pbjob.RuntimeInfo) {
			suite.Equal(jobRuntime.GetGoalState(), pbjob.JobState_RUNNING)
			suite.Equal(jobRuntime.GetDesiredStateVersion(), desiredStateVersion+1)
		}).Return(&pbjob.RuntimeInfo{
		State:                pbjob.JobState_KILLED,
		GoalState:            pbjob.JobState_RUNNING,
		ConfigurationVersion: configVersion,
		DesiredStateVersion:  desiredStateVersion + 1,
		WorkflowVersion:      workflowVersion,
	}, nil)

	suite.goalStateDriver.EXPECT().
		EnqueueJob(&peloton.JobID{Value: testJobID}, gomock.Any())

	resp, err := suite.handler.StartJob(
		context.Background(),
		&statelesssvc.StartJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: versionutil.GetJobEntityVersion(configVersion, desiredStateVersion, workflowVersion),
		})
	suite.NoError(err)
	suite.Equal(
		resp.GetVersion(),
		versionutil.GetJobEntityVersion(configVersion, desiredStateVersion+1, workflowVersion))
}

// TestStartJobGetRuntimeFailure tests the failure case of starting job
// due to runtime failure
func (suite *statelessHandlerTestSuite) TestStartJobGetRuntimeFailure() {
	configVersion := uint64(2)
	desiredStateVersion := uint64(3)
	workflowVersion := uint64(4)

	suite.candidate.EXPECT().IsLeader().Return(true)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.StartJob(
		context.Background(),
		&statelesssvc.StartJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: versionutil.GetJobEntityVersion(configVersion, desiredStateVersion, workflowVersion),
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestStartJobCompareAndSetRuntimeFailure tests the failure case
// of starting job due to CompareAndSetRuntime error
func (suite *statelessHandlerTestSuite) TestStartJobCompareAndSetRuntimeFailure() {
	configVersion := uint64(2)
	desiredStateVersion := uint64(3)
	workflowVersion := uint64(4)

	jobRuntime := &pbjob.RuntimeInfo{
		State:                pbjob.JobState_KILLED,
		GoalState:            pbjob.JobState_KILLED,
		ConfigurationVersion: configVersion,
		DesiredStateVersion:  desiredStateVersion,
		WorkflowVersion:      workflowVersion,
	}

	suite.candidate.EXPECT().IsLeader().Return(true)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.cachedJob.EXPECT().
		CompareAndSetRuntime(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, jobRuntime *pbjob.RuntimeInfo) {
			suite.Equal(jobRuntime.GetGoalState(), pbjob.JobState_RUNNING)
			suite.Equal(jobRuntime.GetDesiredStateVersion(), desiredStateVersion+1)
		}).Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.goalStateDriver.EXPECT().
		EnqueueJob(&peloton.JobID{Value: testJobID}, gomock.Any())

	resp, err := suite.handler.StartJob(
		context.Background(),
		&statelesssvc.StartJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: versionutil.GetJobEntityVersion(configVersion, desiredStateVersion, workflowVersion),
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestStartJobInvalidEntityVersionError tests the failure case
// of starting a job due to invalid entity version error
func (suite *statelessHandlerTestSuite) TestStartJobInvalidEntityVersionError() {
	configVersion := uint64(2)
	desiredStateVersion := uint64(3)
	workflowVersion := uint64(4)

	jobRuntime := &pbjob.RuntimeInfo{
		State:                pbjob.JobState_KILLED,
		GoalState:            pbjob.JobState_KILLED,
		ConfigurationVersion: configVersion,
		DesiredStateVersion:  desiredStateVersion,
		WorkflowVersion:      workflowVersion,
	}

	suite.candidate.EXPECT().IsLeader().Return(true)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	resp, err := suite.handler.StartJob(
		context.Background(),
		&statelesssvc.StartJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: versionutil.GetJobEntityVersion(configVersion+1, desiredStateVersion, workflowVersion),
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestStartJobConcurrencyErrorRetry tests the case
// of retrying the job start due to concurrency error
func (suite *statelessHandlerTestSuite) TestStartJobConcurrencyErrorRetry() {
	configVersion := uint64(2)
	desiredStateVersion := uint64(3)
	workflowVersion := uint64(4)

	jobRuntime := &pbjob.RuntimeInfo{
		State:                pbjob.JobState_KILLED,
		GoalState:            pbjob.JobState_KILLED,
		ConfigurationVersion: configVersion,
		DesiredStateVersion:  desiredStateVersion,
		WorkflowVersion:      workflowVersion,
	}

	suite.candidate.EXPECT().IsLeader().Return(true)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil).Times(2)

	suite.cachedJob.EXPECT().
		CompareAndSetRuntime(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, jobRuntime *pbjob.RuntimeInfo) {
			suite.Equal(jobRuntime.GetGoalState(), pbjob.JobState_RUNNING)
			suite.Equal(jobRuntime.GetDesiredStateVersion(), desiredStateVersion+1)
			jobRuntime.DesiredStateVersion--
		}).Return(nil, jobmgrcommon.UnexpectedVersionError)

	suite.cachedJob.EXPECT().
		CompareAndSetRuntime(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, jobRuntime *pbjob.RuntimeInfo) {
			suite.Equal(jobRuntime.GetGoalState(), pbjob.JobState_RUNNING)
			suite.Equal(jobRuntime.GetDesiredStateVersion(), desiredStateVersion+1)
		}).Return(&pbjob.RuntimeInfo{
		State:                pbjob.JobState_KILLED,
		GoalState:            pbjob.JobState_RUNNING,
		ConfigurationVersion: configVersion,
		DesiredStateVersion:  desiredStateVersion + 1,
		WorkflowVersion:      workflowVersion,
	}, nil)

	suite.goalStateDriver.EXPECT().
		EnqueueJob(&peloton.JobID{Value: testJobID}, gomock.Any())

	resp, err := suite.handler.StartJob(
		context.Background(),
		&statelesssvc.StartJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: versionutil.GetJobEntityVersion(configVersion, desiredStateVersion, workflowVersion),
		})
	suite.NoError(err)
	suite.Equal(
		resp.GetVersion(),
		versionutil.GetJobEntityVersion(configVersion, desiredStateVersion+1, workflowVersion))
}

func TestStatelessServiceHandler(t *testing.T) {
	suite.Run(t, new(statelessHandlerTestSuite))
}
