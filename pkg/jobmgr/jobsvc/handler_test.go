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

package jobsvc

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	apierrors "github.com/uber/peloton/.gen/peloton/api/v0/errors"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	respoolmocks "github.com/uber/peloton/.gen/peloton/api/v0/respool/mocks"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/private/models"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	resmocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"

	"github.com/uber/peloton/pkg/common"
	leadermocks "github.com/uber/peloton/pkg/common/leader/mocks"
	"github.com/uber/peloton/pkg/common/util"
	versionutil "github.com/uber/peloton/pkg/common/util/entityversion"
	taskutil "github.com/uber/peloton/pkg/common/util/task"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	cachedtest "github.com/uber/peloton/pkg/jobmgr/cached/test"
	goalstatemocks "github.com/uber/peloton/pkg/jobmgr/goalstate/mocks"
	jobmgrtask "github.com/uber/peloton/pkg/jobmgr/task"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	testInstanceCount    = 3
	testSecretPath       = "/tmp/secret"
	testSecretPathNew    = "/tmp/secret/new"
	testSecretStr        = "top-secret-token"
	testSecretStrUpdated = "top-secret-token-updated"
	testSecretStrNew     = "top-secret-token-new"
)

var (
	defaultResourceConfig = task.ResourceConfig{
		CpuLimit:    10,
		MemLimitMb:  10,
		DiskLimitMb: 10,
		FdLimit:     10,
	}
)

type JobHandlerTestSuite struct {
	suite.Suite
	context       context.Context
	handler       *serviceHandler
	testJobID     *peloton.JobID
	testJobConfig *job.JobConfig
	taskInfos     map[uint32]*task.TaskInfo
	testRespoolID *peloton.ResourcePoolID

	ctrl                  *gomock.Controller
	mockedCandidate       *leadermocks.MockCandidate
	mockedRespoolClient   *respoolmocks.MockResourceManagerYARPCClient
	mockedResmgrClient    *resmocks.MockResourceManagerServiceYARPCClient
	mockedJobFactory      *cachedmocks.MockJobFactory
	mockedCachedJob       *cachedmocks.MockJob
	mockedCachedUpdate    *cachedmocks.MockUpdate
	mockedGoalStateDriver *goalstatemocks.MockDriver
	mockedJobStore        *storemocks.MockJobStore
	mockedTaskStore       *storemocks.MockTaskStore
	mockedActiveJobsOps   *objectmocks.MockActiveJobsOps
	mockedJobIndexOps     *objectmocks.MockJobIndexOps
	mockedSecretInfoOps   *objectmocks.MockSecretInfoOps
	mockedJobConfigOps    *objectmocks.MockJobConfigOps
	mockedJobRuntimeOps   *objectmocks.MockJobRuntimeOps
}

// helper to initialize mocks in JobHandlerTestSuite
func (suite *JobHandlerTestSuite) initializeMocks() {
	// Initialize mocks for the test
	suite.ctrl = gomock.NewController(suite.T())
	suite.mockedJobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.mockedJobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.mockedCachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.mockedCachedUpdate = cachedmocks.NewMockUpdate(suite.ctrl)
	suite.mockedGoalStateDriver = goalstatemocks.NewMockDriver(suite.ctrl)
	suite.mockedRespoolClient = respoolmocks.NewMockResourceManagerYARPCClient(suite.ctrl)
	suite.mockedResmgrClient = resmocks.NewMockResourceManagerServiceYARPCClient(suite.ctrl)
	suite.mockedCandidate = leadermocks.NewMockCandidate(suite.ctrl)
	suite.mockedTaskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.mockedActiveJobsOps = objectmocks.NewMockActiveJobsOps(suite.ctrl)
	suite.mockedJobIndexOps = objectmocks.NewMockJobIndexOps(suite.ctrl)
	suite.mockedSecretInfoOps = objectmocks.NewMockSecretInfoOps(suite.ctrl)
	suite.mockedJobConfigOps = objectmocks.NewMockJobConfigOps(suite.ctrl)
	suite.mockedJobRuntimeOps = objectmocks.NewMockJobRuntimeOps(suite.ctrl)

	suite.handler.jobStore = suite.mockedJobStore
	suite.handler.taskStore = suite.mockedTaskStore
	suite.handler.activeJobsOps = suite.mockedActiveJobsOps
	suite.handler.jobIndexOps = suite.mockedJobIndexOps
	suite.handler.jobConfigOps = suite.mockedJobConfigOps
	suite.handler.jobRuntimeOps = suite.mockedJobRuntimeOps
	suite.handler.secretInfoOps = suite.mockedSecretInfoOps
	suite.handler.jobFactory = suite.mockedJobFactory
	suite.handler.goalStateDriver = suite.mockedGoalStateDriver
	suite.handler.respoolClient = suite.mockedRespoolClient
	suite.handler.resmgrClient = suite.mockedResmgrClient
	suite.handler.candidate = suite.mockedCandidate
	suite.handler.jobSvcCfg.EnableSecrets = true
}

// sets up generic mocks that are common to most tests
func (suite *JobHandlerTestSuite) setupMocks(
	jobID *peloton.JobID, respoolID *peloton.ResourcePoolID) {
	suite.mockedCandidate.EXPECT().IsLeader().Return(true).AnyTimes()
	suite.mockedJobFactory.EXPECT().AddJob(jobID).
		Return(suite.mockedCachedJob).AnyTimes()
	suite.mockedRespoolClient.EXPECT().
		GetResourcePool(gomock.Any(), gomock.Any()).Return(&respool.GetResponse{
		Poolinfo: &respool.ResourcePoolInfo{
			Id: respoolID,
		},
	}, nil).AnyTimes()
	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{State: job.JobState_PENDING}, nil).AnyTimes()
	suite.mockedJobConfigOps.EXPECT().
		Create(
			context.Background(),
			jobID,
			gomock.Any(),
			gomock.Any(),
			nil,
			gomock.Any(),
		).
		Return(nil).AnyTimes()
	suite.mockedCachedJob.EXPECT().
		CreateTaskRuntimes(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()
	suite.mockedCachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH).AnyTimes()
	suite.mockedJobFactory.EXPECT().AddJob(jobID).
		Return(suite.mockedCachedJob).AnyTimes()
	suite.mockedCachedJob.EXPECT().
		Update(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			nil,
			cached.UpdateCacheAndDB).
		Return(nil).AnyTimes()
	suite.mockedGoalStateDriver.EXPECT().JobRuntimeDuration(job.JobType_BATCH).
		Return(1 * time.Second).AnyTimes()
	suite.mockedGoalStateDriver.EXPECT().EnqueueJob(jobID, gomock.Any()).AnyTimes()
	suite.mockedGoalStateDriver.EXPECT().EnqueueTask(
		jobID, gomock.Any(), gomock.Any()).AnyTimes()
}

func (suite *JobHandlerTestSuite) SetupTest() {
	mtx := NewMetrics(tally.NoopScope)
	suite.handler = &serviceHandler{
		metrics:   mtx,
		rootCtx:   context.Background(),
		jobSvcCfg: Config{MaxTasksPerJob: _defaultMaxTasksPerJob},
	}
	suite.testJobID = &peloton.JobID{
		Value: uuid.New(),
	}
	suite.testJobConfig = &job.JobConfig{
		Name:          suite.testJobID.Value,
		InstanceCount: testInstanceCount,
		SLA: &job.SlaConfig{
			Preemptible:             true,
			Priority:                22,
			MaximumRunningInstances: 2,
			MinimumRunningInstances: 1,
		},
		Type: job.JobType_SERVICE,
	}
	suite.testRespoolID = &peloton.ResourcePoolID{
		Value: "test-respool",
	}
	var taskInfos = make(map[uint32]*task.TaskInfo)
	for i := uint32(0); i < testInstanceCount; i++ {
		taskInfos[i] = suite.createTestTaskInfo(
			task.TaskState_RUNNING, i)
	}
	suite.context = context.Background()
	suite.taskInfos = taskInfos
	suite.initializeMocks()
}

func (suite *JobHandlerTestSuite) TearDownTest() {
	log.Debug("tearing down")
	suite.ctrl.Finish()
}

func TestPelotonJobHandler(t *testing.T) {
	suite.Run(t, new(JobHandlerTestSuite))
}

func (suite *JobHandlerTestSuite) createTestTaskInfo(
	state task.TaskState,
	instanceID uint32) *task.TaskInfo {

	var taskID = fmt.Sprintf("%s-%d", suite.testJobID.Value, instanceID)
	return &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			MesosTaskId: &mesos.TaskID{Value: &taskID},
			State:       state,
			GoalState:   task.TaskState_SUCCEEDED,
		},
		Config: &task.TaskConfig{
			Name:     suite.testJobConfig.Name,
			Resource: &defaultResourceConfig,
		},
		InstanceId: instanceID,
		JobId:      suite.testJobID,
	}
}

// TestCreateJob_Success tests the success path of creating job
func (suite *JobHandlerTestSuite) TestCreateJob_Success() {
	// setup job config
	testCmd := "echo test"
	defaultConfig := &task.TaskConfig{
		Command: &mesos.CommandInfo{Value: &testCmd},
	}
	jobConfig := &job.JobConfig{
		DefaultConfig: defaultConfig,
		RespoolID:     suite.testRespoolID,
	}

	suite.setupMocks(suite.testJobID, suite.testRespoolID)
	// setup specific mocks throughout the test
	suite.mockedCachedJob.EXPECT().Create(
		gomock.Any(),
		jobConfig,
		gomock.Any(),
		nil,
	).Return(nil)

	req := &job.CreateRequest{
		Id:     suite.testJobID,
		Config: jobConfig,
	}
	// Job Create API for this request should pass
	resp, err := suite.handler.Create(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(suite.testJobID, resp.GetJobId())
}

// TestCreateJob_EmptyID tests create a job with empty uuid
func (suite *JobHandlerTestSuite) TestCreateJob_EmptyID() {
	testCmd := "echo test"
	defaultConfig := &task.TaskConfig{
		Command: &mesos.CommandInfo{Value: &testCmd},
	}
	jobConfig := &job.JobConfig{
		DefaultConfig: defaultConfig,
		RespoolID:     suite.testRespoolID,
	}
	suite.setupMocks(suite.testJobID, suite.testRespoolID)
	// test if handler generates UUID for empty job id
	suite.mockedJobFactory.EXPECT().AddJob(gomock.Any()).
		Return(suite.mockedCachedJob)
	suite.mockedCachedJob.EXPECT().Create(
		gomock.Any(),
		jobConfig,
		gomock.Any(),
		nil,
	).Return(nil)
	suite.mockedGoalStateDriver.EXPECT().EnqueueJob(gomock.Any(), gomock.Any())
	resp, err := suite.handler.Create(suite.context, &job.CreateRequest{
		Id:     nil,
		Config: jobConfig,
	})
	suite.NoError(err)
	suite.NotEqual(resp.GetJobId().GetValue(), "")
	suite.NotNil(uuid.Parse(resp.GetJobId().GetValue()))
}

// TestCreateJob_InternalError tests job create return internal error
func (suite *JobHandlerTestSuite) TestCreateJob_InternalError() {
	testCmd := "echo test"
	defaultConfig := &task.TaskConfig{
		Command: &mesos.CommandInfo{Value: &testCmd},
	}
	jobConfig := &job.JobConfig{
		DefaultConfig: defaultConfig,
		RespoolID:     suite.testRespoolID,
	}
	req := &job.CreateRequest{
		Id:     suite.testJobID,
		Config: jobConfig,
	}
	suite.setupMocks(suite.testJobID, suite.testRespoolID)
	// simulate cachedjob create failure
	suite.mockedCachedJob.EXPECT().Create(
		gomock.Any(),
		jobConfig,
		gomock.Any(),
		nil,
	).Return(errors.New("random error"))

	resp, err := suite.handler.Create(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	expectedErr := &job.CreateResponse_Error{
		AlreadyExists: &job.JobAlreadyExists{
			Id:      suite.testJobID,
			Message: "random error",
		},
	}
	suite.Equal(expectedErr, resp.GetError())
}

// TestCreateJob_JobAlreadyExistErr tests job create when the job has already
// existed
func (suite *JobHandlerTestSuite) TestCreateJob_JobAlreadyExistErr() {
	testCmd := "echo test"
	defaultConfig := &task.TaskConfig{
		Command: &mesos.CommandInfo{Value: &testCmd},
	}
	jobConfig := &job.JobConfig{
		DefaultConfig: defaultConfig,
		RespoolID:     suite.testRespoolID,
	}
	req := &job.CreateRequest{
		Id:     suite.testJobID,
		Config: jobConfig,
	}
	alreayExistErr := yarpcerrors.AlreadyExistsErrorf("job already exist")
	suite.setupMocks(suite.testJobID, suite.testRespoolID)
	suite.mockedCachedJob.EXPECT().Create(
		gomock.Any(),
		jobConfig,
		gomock.Any(),
		nil,
	).Return(alreayExistErr)

	resp, err := suite.handler.Create(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	expectedErr := &job.CreateResponse_Error{
		AlreadyExists: &job.JobAlreadyExists{
			Id:      suite.testJobID,
			Message: alreayExistErr.Error(),
		},
	}
	suite.Equal(expectedErr, resp.GetError())
}

// TestCreateJob_NilRespool tests job create with nil respool fail
func (suite *JobHandlerTestSuite) TestCreateJob_NilRespool() {
	testCmd := "echo test"
	defaultConfig := &task.TaskConfig{
		Command: &mesos.CommandInfo{Value: &testCmd},
	}
	jobConfig := &job.JobConfig{
		DefaultConfig: defaultConfig,
		RespoolID:     suite.testRespoolID,
	}
	req := &job.CreateRequest{
		Id:     suite.testJobID,
		Config: jobConfig,
	}
	suite.setupMocks(suite.testJobID, suite.testRespoolID)
	// Job Create with respool set to nil should fail
	jobConfig.RespoolID = nil
	resp, err := suite.handler.Create(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	expectedErr := &job.CreateResponse_Error{
		InvalidConfig: &job.InvalidJobConfig{
			Id:      suite.testJobID,
			Message: errNullResourcePoolID.Error(),
		},
	}
	suite.Equal(expectedErr, resp.GetError())
}

// TestCreateJob_BadJobID tests job_id set to random string should fail
func (suite *JobHandlerTestSuite) TestCreateJob_BadJobID() {
	testCmd := "echo test"
	defaultConfig := &task.TaskConfig{
		Command: &mesos.CommandInfo{Value: &testCmd},
	}
	jobConfig := &job.JobConfig{
		DefaultConfig: defaultConfig,
		RespoolID:     suite.testRespoolID,
	}

	badJobID := &peloton.JobID{
		Value: "bad-job-id-str",
	}
	suite.mockedCandidate.EXPECT().IsLeader().Return(true).AnyTimes()
	resp, err := suite.handler.Create(suite.context, &job.CreateRequest{
		Id:     badJobID,
		Config: jobConfig,
	})
	suite.NoError(err)
	suite.NotNil(resp)
	expectedErr := &job.CreateResponse_Error{
		InvalidJobId: &job.InvalidJobId{
			Id:      badJobID,
			Message: "JobID must be valid UUID",
		},
	}
	suite.Equal(expectedErr, resp.GetError())
}

// TestCreateJob_ValidationErr tests job create fails with bad config
func (suite *JobHandlerTestSuite) TestCreateJob_ValidationErr() {
	testCmd := "echo test"
	defaultConfig := &task.TaskConfig{
		Command: &mesos.CommandInfo{Value: &testCmd},
	}
	jobConfig := &job.JobConfig{
		DefaultConfig: defaultConfig,
		RespoolID:     suite.testRespoolID,
	}
	suite.setupMocks(suite.testJobID, suite.testRespoolID)
	// simulate a simple error validating task config just to test error
	// processing code path in Create()
	suite.handler.jobSvcCfg.MaxTasksPerJob = 1
	jobConfig.InstanceCount = 2
	resp, err := suite.handler.Create(suite.context, &job.CreateRequest{
		Id:     suite.testJobID,
		Config: jobConfig,
	})
	suite.NoError(err)
	suite.NotNil(resp)
	expectedErr := &job.CreateResponse_Error{
		InvalidConfig: &job.InvalidJobConfig{
			Id: suite.testJobID,
			Message: "code:invalid-argument message:Requested tasks: 2 for job is " +
				"greater than supported: 1 tasks/job",
		},
	}
	suite.Equal(expectedErr, resp.GetError())
}

func (suite *JobHandlerTestSuite) TestCreateJob_RootRespoolFail() {
	testCmd := "echo test"
	jobID := &peloton.JobID{
		Value: uuid.New(),
	}
	defaultConfig := &task.TaskConfig{
		Command: &mesos.CommandInfo{Value: &testCmd},
	}
	jobConfig := &job.JobConfig{
		DefaultConfig: defaultConfig,
	}
	// Job Create with respool set to root should fail
	jobConfig.RespoolID = &peloton.ResourcePoolID{
		Value: common.RootResPoolID,
	}
	req := &job.CreateRequest{
		Id:     jobID,
		Config: jobConfig,
	}
	suite.mockedCandidate.EXPECT().IsLeader().Return(true).AnyTimes()
	resp, err := suite.handler.Create(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	expectedErr := &job.CreateResponse_Error{
		InvalidConfig: &job.InvalidJobConfig{
			Id:      jobID,
			Message: errRootResourcePoolID.Error(),
		},
	}
	suite.Equal(expectedErr, resp.GetError())
}

// Test create job with one controller task as task 0
func (suite *JobHandlerTestSuite) TestCreateJob_ControllerTaskSucceed() {
	// setup job config
	testCmd := "echo test"
	// set controller task by defaultConfig
	defaultConfig := &task.TaskConfig{
		Command:    &mesos.CommandInfo{Value: &testCmd},
		Controller: true,
	}
	jobConfig := &job.JobConfig{
		DefaultConfig: defaultConfig,
		RespoolID:     suite.testRespoolID,
	}

	suite.setupMocks(suite.testJobID, suite.testRespoolID)
	// setup specific mocks throughout the test
	suite.mockedCachedJob.EXPECT().Create(
		gomock.Any(),
		jobConfig,
		gomock.Any(),
		nil,
	).Return(nil)

	req := &job.CreateRequest{
		Id:     suite.testJobID,
		Config: jobConfig,
	}
	// Job Create API for this request should pass
	resp, err := suite.handler.Create(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(suite.testJobID, resp.GetJobId())

	// set controller task by instance config
	defaultConfig = &task.TaskConfig{
		Command: &mesos.CommandInfo{Value: &testCmd},
	}
	jobConfig = &job.JobConfig{
		DefaultConfig: defaultConfig,
		InstanceCount: 3,
		RespoolID:     suite.testRespoolID,
		InstanceConfig: map[uint32]*task.TaskConfig{
			0: {
				Controller: true,
			},
		},
	}

	suite.setupMocks(suite.testJobID, suite.testRespoolID)
	// setup specific mocks throughout the test
	suite.mockedCachedJob.EXPECT().Create(
		gomock.Any(),
		jobConfig,
		gomock.Any(),
		nil,
	).Return(nil)

	req = &job.CreateRequest{
		Id:     suite.testJobID,
		Config: jobConfig,
	}
	// Job Create API for this request should pass
	resp, err = suite.handler.Create(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(suite.testJobID, resp.GetJobId())
}

// Test create job with more than one controller task
func (suite *JobHandlerTestSuite) TestCreateJob_MoreThanOneControllerTask() {
	// setup job config
	testCmd := "echo test"
	// set multiple controller task by defaultConfig
	defaultConfig := &task.TaskConfig{
		Command:    &mesos.CommandInfo{Value: &testCmd},
		Controller: true,
	}
	jobConfig := &job.JobConfig{
		DefaultConfig: defaultConfig,
		RespoolID:     suite.testRespoolID,
		InstanceCount: 3,
	}

	suite.setupMocks(suite.testJobID, suite.testRespoolID)

	req := &job.CreateRequest{
		Id:     suite.testJobID,
		Config: jobConfig,
	}
	// Job Create API for this request should fail
	resp, err := suite.handler.Create(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	expectedErr := &job.CreateResponse_Error{
		InvalidConfig: &job.InvalidJobConfig{
			Id:      req.Id,
			Message: "code:invalid-argument message:only task 0 can be controller task",
		},
	}
	suite.Equal(expectedErr, resp.GetError())
}

// Test create job with controller task not as task 0
func (suite *JobHandlerTestSuite) TestCreateJob_ControllerTaskAtNonZeroPosition() {
	// setup job config
	testCmd := "echo test"
	// set multiple controller task by defaultConfig
	defaultConfig := &task.TaskConfig{
		Command: &mesos.CommandInfo{Value: &testCmd},
	}
	jobConfig := &job.JobConfig{
		DefaultConfig: defaultConfig,
		RespoolID:     suite.testRespoolID,
		InstanceCount: 3,
		InstanceConfig: map[uint32]*task.TaskConfig{
			0: {
				Controller: false,
			},
			1: {
				Controller: true,
			},
		},
	}

	suite.setupMocks(suite.testJobID, suite.testRespoolID)

	req := &job.CreateRequest{
		Id:     suite.testJobID,
		Config: jobConfig,
	}
	// Job Create API for this request should fail
	resp, err := suite.handler.Create(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	expectedErr := &job.CreateResponse_Error{
		InvalidConfig: &job.InvalidJobConfig{
			Id:      req.Id,
			Message: "code:invalid-argument message:only task 0 can be controller task",
		},
	}
	suite.Equal(expectedErr, resp.GetError())
}

// Test Job Create/Update/Refresh going to non-leader
func (suite *JobHandlerTestSuite) TestNonLeader() {
	// setup the non-leader case
	suite.mockedCandidate.EXPECT().IsLeader().Return(false).AnyTimes()

	// Test Create
	createResp, err := suite.handler.Create(suite.context, &job.CreateRequest{})
	suite.Nil(createResp)
	suite.Error(err)
	suite.Equal(yarpcerrors.IsUnavailable(err), true)
	suite.Equal(yarpcerrors.ErrorMessage(err),
		"Job Create API not suppported on non-leader")

	// Test Update
	updateResp, err := suite.handler.Update(suite.context, &job.UpdateRequest{})
	suite.Nil(updateResp)
	suite.Error(err)
	suite.Equal(yarpcerrors.IsUnavailable(err), true)
	suite.Equal(yarpcerrors.ErrorMessage(err),
		"Job Update API not suppported on non-leader")

	// Test Refresh
	refreshResp, err := suite.handler.Refresh(suite.context, &job.RefreshRequest{})
	suite.Nil(refreshResp)
	suite.Error(err)
	suite.Equal(yarpcerrors.IsUnavailable(err), true)
	suite.Equal(yarpcerrors.ErrorMessage(err),
		"Job Refresh API not suppported on non-leader")
}

// TestCreateJobWithSecrets tests different success/failure scenarios
// for Job Create API for jobs that have secrets
func (suite *JobHandlerTestSuite) TestCreateJobWithSecrets() {
	// setup job config which uses defaultconfig which has
	// mesos containerizer
	testCmd := "echo test"
	jobID := &peloton.JobID{
		Value: uuid.New(),
	}
	secret := &peloton.Secret{
		Path: testSecretPath,
		Value: &peloton.Secret_Value{
			Data: []byte(base64.StdEncoding.EncodeToString(
				[]byte(testSecretStr))),
		},
	}
	mesosContainerizer := mesos.ContainerInfo_MESOS
	dockerContainerizer := mesos.ContainerInfo_DOCKER
	defaultConfig := &task.TaskConfig{
		Command:   &mesos.CommandInfo{Value: &testCmd},
		Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
	}
	jobConfig := &job.JobConfig{
		DefaultConfig: defaultConfig,
	}
	respoolID := &peloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobConfig.RespoolID = respoolID

	jobConfig.InstanceConfig = make(map[uint32]*task.TaskConfig)
	for i := uint32(0); i < 25; i++ {
		jobConfig.InstanceConfig[i] = &task.TaskConfig{
			Name:      suite.testJobConfig.Name,
			Resource:  &defaultResourceConfig,
			Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
		}
	}

	// setup mocks
	suite.setupMocks(jobID, respoolID)

	// setup mocks specific to this test
	suite.mockedSecretInfoOps.EXPECT().CreateSecret(
		gomock.Any(),
		jobID.Value,               // jobID
		gomock.Any(),              // now
		gomock.Any(),              // secretID
		string(secret.Value.Data), // secretString
		testSecretPath).           // secretPath
		Return(nil)

	suite.mockedCachedJob.EXPECT().Create(
		gomock.Any(), jobConfig, gomock.Any(), nil).Return(nil)

	// Create a job with a secret. This should pass
	req := &job.CreateRequest{
		Id:      jobID,
		Config:  jobConfig,
		Secrets: []*peloton.Secret{secret},
	}

	resp, err := suite.handler.Create(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(jobID, resp.GetJobId())

	// Create job where one instance is using docker containerizer.
	// The create should still succeed and this instance will be
	// launched without secrets because container info in default
	// config is overridden.
	_ = util.RemoveSecretVolumesFromJobConfig(jobConfig)
	jobConfig.InstanceConfig[10].Container =
		&mesos.ContainerInfo{Type: &dockerContainerizer}
	req = &job.CreateRequest{
		Id:      jobID,
		Config:  jobConfig,
		Secrets: []*peloton.Secret{secret},
	}
	suite.mockedSecretInfoOps.EXPECT().CreateSecret(
		gomock.Any(),
		jobID.Value,               // jobID
		gomock.Any(),              // now
		gomock.Any(),              // secretID
		string(secret.Value.Data), // secretString
		testSecretPath).           // secretPath
		Return(nil)
	suite.mockedCachedJob.EXPECT().Create(
		gomock.Any(), jobConfig, gomock.Any(), nil).Return(nil)
	resp, err = suite.handler.Create(suite.context, req)
	suite.Nil(err)

	// Negative tests begin

	// Now because the way test is setup, after Create succeeds, jobConfig will
	// now contain secret volumes (since it is a pointer. this will not happen
	// for a real client). We can leverage this same config to create job again,
	// and that should fail because we don't support directly adding secret
	// volumes to config in Create/Update
	resp, err = suite.handler.Create(suite.context, req)
	suite.Error(err)
	suite.Equal(yarpcerrors.IsInvalidArgument(err), true)
	suite.Equal(yarpcerrors.ErrorMessage(err),
		"adding secret volumes directly in config is not allowed")

	jobConfig.GetDefaultConfig().GetContainer().Volumes = nil
	// Simulate DB failure in CreateSecret
	suite.mockedSecretInfoOps.EXPECT().CreateSecret(
		gomock.Any(),
		jobID.Value,               // jobID
		gomock.Any(),              // now
		gomock.Any(),              // secretID
		string(secret.Value.Data), // secretString
		testSecretPath).           // secretPath
		Return(errors.New("DB error"))
	resp, err = suite.handler.Create(suite.context, req)
	suite.Error(err)

	// Create job with no containerizer info in default config

	// reset instance config to use mesos containerizer
	jobConfig.InstanceConfig[10].Container =
		&mesos.ContainerInfo{Type: &mesosContainerizer}
	jobConfig.DefaultConfig = &task.TaskConfig{
		Command: &mesos.CommandInfo{Value: &testCmd},
	}
	req = &job.CreateRequest{
		Id:      jobID,
		Config:  jobConfig,
		Secrets: []*peloton.Secret{secret},
	}
	resp, err = suite.handler.Create(suite.context, req)
	suite.Error(err)

	// Create job with secret that is not base64 encoded.
	jobConfig.DefaultConfig = defaultConfig
	secret = &peloton.Secret{
		Path: testSecretPath,
		Value: &peloton.Secret_Value{
			Data: []byte(testSecretStr),
		},
	}
	req.Secrets = []*peloton.Secret{secret}
	resp, err = suite.handler.Create(suite.context, req)
	suite.Error(err)

	// Create a job that uses docker containerizer in default config
	// but also has secrets. This should fail since we only support
	// secrets when using Mesos containerizer in default config.
	defaultConfig = &task.TaskConfig{
		Command:   &mesos.CommandInfo{Value: &testCmd},
		Container: &mesos.ContainerInfo{Type: &dockerContainerizer},
	}
	jobConfig = &job.JobConfig{
		DefaultConfig: defaultConfig,
	}
	resp, err = suite.handler.Create(suite.context, req)
	suite.Error(err)

	// Set EnableSecrets config to false and then create a job
	// with secrets. This should fail
	suite.handler.jobSvcCfg.EnableSecrets = false
	resp, err = suite.handler.Create(suite.context, req)
	suite.Error(err)
}

func (suite *JobHandlerTestSuite) TestSubmitTasksToResmgr() {
	var tasksInfo []*task.TaskInfo
	for _, v := range suite.taskInfos {
		tasksInfo = append(tasksInfo, v)
	}
	gangs := taskutil.ConvertToResMgrGangs(tasksInfo, suite.testJobConfig)
	var expectedGangs []*resmgrsvc.Gang
	gomock.InOrder(
		suite.mockedResmgrClient.EXPECT().
			EnqueueGangs(
				gomock.Any(),
				gomock.Eq(&resmgrsvc.EnqueueGangsRequest{
					Gangs: gangs,
				})).
			Do(func(_ context.Context, reqBody interface{}) {
				req := reqBody.(*resmgrsvc.EnqueueGangsRequest)
				for _, g := range req.Gangs {
					expectedGangs = append(expectedGangs, g)
				}
			}).
			Return(&resmgrsvc.EnqueueGangsResponse{}, nil),
	)

	jobmgrtask.EnqueueGangs(
		suite.handler.rootCtx,
		tasksInfo,
		suite.testJobConfig,
		suite.mockedResmgrClient)
	suite.Equal(gangs, expectedGangs)
}

func (suite *JobHandlerTestSuite) TestSubmitTasksToResmgrError() {
	var tasksInfo []*task.TaskInfo
	for _, v := range suite.taskInfos {
		tasksInfo = append(tasksInfo, v)
	}
	gangs := taskutil.ConvertToResMgrGangs(tasksInfo, suite.testJobConfig)
	var expectedGangs []*resmgrsvc.Gang
	gomock.InOrder(
		suite.mockedResmgrClient.EXPECT().
			EnqueueGangs(
				gomock.Any(),
				gomock.Eq(&resmgrsvc.EnqueueGangsRequest{
					Gangs: gangs,
				})).
			Do(func(_ context.Context, reqBody interface{}) {
				req := reqBody.(*resmgrsvc.EnqueueGangsRequest)
				for _, g := range req.Gangs {
					expectedGangs = append(expectedGangs, g)
				}
			}).
			Return(nil, errors.New("Resmgr Error")),
	)
	_, err := jobmgrtask.EnqueueGangs(
		suite.handler.rootCtx,
		tasksInfo,
		suite.testJobConfig,
		suite.mockedResmgrClient)
	suite.Error(err)
}

func (suite *JobHandlerTestSuite) TestValidateResourcePool_Failure() {

	tt := []struct {
		respoolID          string
		getRespoolResponse *respool.GetResponse
		getRespoolError    error
		errMsg             string
	}{
		{
			// tests resource pool not found in resource manager
			respoolID:          "respool11",
			getRespoolResponse: nil,
			getRespoolError:    errors.New("resource pool not found"),
			errMsg:             "resource pool not found",
		},
		{
			// tests submitting job to root resource pool
			respoolID:          common.RootResPoolID,
			getRespoolResponse: nil,
			getRespoolError:    nil,
			errMsg:             "cannot submit jobs to the `root` resource pool",
		},
		{
			// tests submitting job to a non leaf resource pool
			respoolID: "respool11",
			getRespoolResponse: &respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{
						Value: "respool11",
					},
					Children: []*peloton.ResourcePoolID{
						{
							Value: "respool111",
						},
					},
				},
			},
			getRespoolError: nil,
			errMsg:          "cannot submit jobs to a non leaf resource pool",
		},
	}

	for _, t := range tt {
		respoolID := &peloton.ResourcePoolID{
			Value: t.respoolID,
		}
		var request = &respool.GetRequest{
			Id: respoolID,
		}

		suite.mockedRespoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				gomock.Eq(request)).
			Return(t.getRespoolResponse, t.getRespoolError).MaxTimes(1)

		respoolPath, errResponse := suite.handler.validateResourcePool(respoolID)
		suite.Error(errResponse)
		suite.Equal(t.errMsg, errResponse.Error())
		suite.Nil(respoolPath)
	}
}

func (suite *JobHandlerTestSuite) TestJobScaleUp() {
	testCmd := "echo test"
	oldInstanceCount := uint32(3)
	newInstanceCount := uint32(4)
	jobID := &peloton.JobID{
		Value: "job0",
	}
	respoolID := &peloton.ResourcePoolID{
		Value: "test-respool",
	}
	defaultConfig := &task.TaskConfig{
		Command: &mesos.CommandInfo{Value: &testCmd},
	}
	oldJobConfig := &job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: oldInstanceCount,
		DefaultConfig: defaultConfig,
	}
	newJobConfig := &job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: newInstanceCount,
		DefaultConfig: defaultConfig,
	}
	// setup generic update mocks
	suite.setupMocks(jobID, respoolID)
	// setup specific update mocks
	suite.mockedJobConfigOps.EXPECT().
		Get(
			context.Background(),
			jobID,
			gomock.Any(),
		).
		Return(oldJobConfig, &models.ConfigAddOn{}, nil).
		AnyTimes()

	suite.mockedCachedJob.EXPECT().
		CompareAndSetConfig(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(&job.JobConfig{
			ChangeLog: &peloton.ChangeLog{
				Version: 2,
			},
		}, nil)
	req := &job.UpdateRequest{
		Id:     jobID,
		Config: newJobConfig,
	}
	resp, err := suite.handler.Update(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(jobID, resp.Id)
	suite.Equal("added 1 instances", resp.Message)
}

// TestJobUpdateControllerJob tests updated job config should contain both
// existing and new instance config
func (suite *JobHandlerTestSuite) TestJobUpdateInstanceConfig() {
	jobID := &peloton.JobID{
		Value: uuid.New(),
	}

	oldJobConfig := &job.JobConfig{
		OwningTeam:    "team6",
		InstanceCount: 1,
		Type:          job.JobType_BATCH,
		InstanceConfig: map[uint32]*task.TaskConfig{
			0: {Command: &mesos.CommandInfo{}, Controller: true},
		},
		ChangeLog: &peloton.ChangeLog{Version: 1},
	}

	newJobConfig := &job.JobConfig{
		OwningTeam:    "team6",
		InstanceCount: 2,
		Type:          job.JobType_BATCH,
		InstanceConfig: map[uint32]*task.TaskConfig{
			1: {Command: &mesos.CommandInfo{}, Controller: false},
		},
		ChangeLog: &peloton.ChangeLog{Version: 2},
	}

	configAddOn := &models.ConfigAddOn{}
	suite.mockedCandidate.EXPECT().IsLeader().Return(true)
	suite.mockedJobFactory.EXPECT().AddJob(jobID).
		Return(suite.mockedCachedJob)
	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{State: job.JobState_RUNNING}, nil)
	suite.mockedJobConfigOps.EXPECT().
		Get(context.Background(), jobID, gomock.Any()).
		Return(oldJobConfig, configAddOn, nil)
	suite.mockedCachedJob.EXPECT().
		CompareAndSetConfig(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			nil).
		Do(func(
			_ context.Context,
			jobConfig *job.JobConfig,
			addOn *models.ConfigAddOn,
			_ *stateless.JobSpec) {
			suite.NotNil(jobConfig.GetInstanceConfig()[0])
			suite.True(jobConfig.GetInstanceConfig()[0].Controller)
			suite.NotNil(jobConfig.GetInstanceConfig()[1])
		}).
		Return(&job.JobConfig{
			ChangeLog: &peloton.ChangeLog{Version: 2},
		}, nil)
	suite.mockedCachedJob.EXPECT().
		Update(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			nil,
			cached.UpdateCacheAndDB).
		Return(nil)
	suite.mockedGoalStateDriver.EXPECT().
		EnqueueJob(gomock.Any(), gomock.Any()).
		Return()
	req := &job.UpdateRequest{Id: jobID, Config: newJobConfig}
	_, err := suite.handler.Update(suite.context, req)
	suite.NoError(err)
}

// TestJobUpdateServiceJob tests updating a service job should fail
func (suite *JobHandlerTestSuite) TestJobUpdateServiceJob() {
	jobID := &peloton.JobID{
		Value: uuid.New(),
	}

	oldJobConfig := &job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: 5,
		Type:          job.JobType_SERVICE,
	}

	newJobConfig := &job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: 5,
		Type:          job.JobType_BATCH,
	}

	suite.mockedCandidate.EXPECT().IsLeader().Return(true)
	suite.mockedJobFactory.EXPECT().AddJob(jobID).
		Return(suite.mockedCachedJob)
	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{State: job.JobState_RUNNING}, nil)
	suite.mockedJobConfigOps.EXPECT().
		Get(context.Background(), jobID, gomock.Any()).
		Return(oldJobConfig, &models.ConfigAddOn{}, nil)
	req := &job.UpdateRequest{Id: jobID, Config: newJobConfig}
	_, err := suite.handler.Update(suite.context, req)
	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.Equal(yarpcerrors.ErrorMessage(err),
		"job update is only supported for batch jobs")
}

// TestJobUpdateFailure tests failure scenarios for Job Update API
func (suite *JobHandlerTestSuite) TestJobUpdateFailure() {
	jobID := &peloton.JobID{
		Value: uuid.New(),
	}
	respoolID := &peloton.ResourcePoolID{
		Value: "test-respool",
	}
	testCmd := "echo test"
	defaultConfig := &task.TaskConfig{
		Command: &mesos.CommandInfo{Value: &testCmd},
	}

	suite.mockedCandidate.EXPECT().IsLeader().Return(true).AnyTimes()
	suite.mockedJobFactory.EXPECT().AddJob(jobID).
		Return(suite.mockedCachedJob).AnyTimes()

	// simulate GetRuntime failure
	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(nil, errors.New("random error"))
	req := &job.UpdateRequest{Id: jobID, Config: &job.JobConfig{}}
	resp, err := suite.handler.Update(suite.context, req)
	suite.Error(err)
	suite.Nil(resp)
	suite.Equal(err.Error(), "random error")

	// simulate updating terminal job
	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{State: job.JobState_FAILED}, nil)
	resp, err = suite.handler.Update(suite.context, req)
	suite.Error(err)
	suite.Nil(resp)

	// simulate GetJobConfig failure
	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{State: job.JobState_RUNNING}, nil).AnyTimes()
	suite.mockedJobConfigOps.EXPECT().
		Get(gomock.Any(), jobID, gomock.Any()).
		Return(nil, nil, errors.New("DB error"))
	resp, err = suite.handler.Update(suite.context, req)
	suite.Error(err)
	suite.Nil(resp)
	suite.Equal(err.Error(), "DB error")

	// simulate a simple failure in ValidateUpdatedConfig
	suite.mockedJobConfigOps.EXPECT().
		Get(gomock.Any(), jobID, gomock.Any()).
		Return(&job.JobConfig{
			OwningTeam: "team-original",
			RespoolID:  respoolID,
		}, &models.ConfigAddOn{}, nil)
	resp, err = suite.handler.Update(suite.context, &job.UpdateRequest{
		Id: jobID,
		Config: &job.JobConfig{
			OwningTeam: "team-change",
		},
	})
	suite.Error(err)
	suite.Nil(resp)

	// simulate cachedJob Update failure
	suite.mockedJobConfigOps.EXPECT().
		Get(gomock.Any(), jobID, gomock.Any()).
		Return(&job.JobConfig{
			RespoolID:     respoolID,
			DefaultConfig: defaultConfig,
		}, &models.ConfigAddOn{}, nil)
	suite.mockedCachedJob.EXPECT().
		CompareAndSetConfig(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(nil, errors.New("random error"))
	resp, err = suite.handler.Update(suite.context, &job.UpdateRequest{
		Id: jobID,
		Config: &job.JobConfig{
			InstanceCount: 1,
			DefaultConfig: defaultConfig,
		},
	})
	suite.Error(err)
	suite.Nil(resp)
}

// TestGetJob tests success scenarios for Job Get API
func (suite *JobHandlerTestSuite) TestGetJob() {
	testCmd := "echo test"
	jobID := &peloton.JobID{
		Value: uuid.New(),
	}
	respoolID := &peloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobConfig := &job.JobConfig{
		DefaultConfig: &task.TaskConfig{
			Command: &mesos.CommandInfo{Value: &testCmd},
		},
	}

	// setup mocks
	suite.setupMocks(jobID, respoolID)
	// setup mocks specific to test
	suite.mockedJobFactory.EXPECT().GetJob(jobID).
		Return(suite.mockedCachedJob).AnyTimes()
	suite.mockedJobConfigOps.EXPECT().
		Get(context.Background(), jobID, gomock.Any()).
		Return(jobConfig, &models.ConfigAddOn{}, nil)

	resp, err := suite.handler.Get(suite.context, &job.GetRequest{Id: jobID})
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(0, len(resp.GetSecrets()))

	secretID := &peloton.SecretID{
		Value: uuid.New(),
	}
	mesosContainerizer := mesos.ContainerInfo_MESOS
	jobConfig = &job.JobConfig{
		DefaultConfig: &task.TaskConfig{
			Command: &mesos.CommandInfo{Value: &testCmd},
			Container: &mesos.ContainerInfo{
				Type: &mesosContainerizer,
				Volumes: []*mesos.Volume{util.CreateSecretVolume(
					testSecretPath, secretID.GetValue())},
			},
		},
	}
	suite.mockedJobConfigOps.EXPECT().
		Get(context.Background(), jobID, gomock.Any()).
		Return(jobConfig, &models.ConfigAddOn{}, nil)

	resp, err = suite.handler.Get(suite.context, &job.GetRequest{Id: jobID})
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(1, len(resp.GetSecrets()))
	suite.Equal(secretID, resp.GetSecrets()[0].GetId())
}

// TestGetJobFailure tests failure scenarios for Job Get API
func (suite *JobHandlerTestSuite) TestGetJobFailure() {
	// setup mocks specific to test
	jobID := &peloton.JobID{
		Value: uuid.New(),
	}
	// simulate GetJobConfig failure
	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(nil, nil)
	suite.mockedJobFactory.EXPECT().GetJob(jobID).
		Return(suite.mockedCachedJob)
	suite.mockedJobConfigOps.EXPECT().
		Get(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil, errors.New("DB error"))
	resp, err := suite.handler.Get(suite.context, &job.GetRequest{Id: jobID})
	suite.NoError(err)
	suite.NotNil(resp)
	expectedErr := &job.GetResponse_Error{
		NotFound: &apierrors.JobNotFound{
			Id:      jobID,
			Message: "DB error",
		},
	}
	suite.Equal(expectedErr, resp.GetError())

	// simulate GetRuntime failure
	suite.mockedJobFactory.EXPECT().GetJob(jobID).
		Return(suite.mockedCachedJob)
	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(nil, errors.New("random error"))
	resp, err = suite.handler.Get(suite.context, &job.GetRequest{Id: jobID})
	suite.NoError(err)
	suite.NotNil(resp)
	expectedErr = &job.GetResponse_Error{
		GetRuntimeFail: &apierrors.JobGetRuntimeFail{
			Id:      jobID,
			Message: "random error",
		},
	}
	suite.Equal(expectedErr, resp.GetError())
}

// TestUpdateJobWithSecrets tests different success/failure scenarios
// for Job Update API for jobs that have secrets
func (suite *JobHandlerTestSuite) TestUpdateJobWithSecrets() {
	testCmd := "echo test"
	jobID := &peloton.JobID{
		Value: uuid.New(),
	}
	respoolID := &peloton.ResourcePoolID{
		Value: "test-respool",
	}
	mesosContainerizer := mesos.ContainerInfo_MESOS
	oldJobConfig := &job.JobConfig{
		DefaultConfig: &task.TaskConfig{
			Command:   &mesos.CommandInfo{Value: &testCmd},
			Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
		},
	}
	newJobConfig := &job.JobConfig{
		DefaultConfig: &task.TaskConfig{
			Command:   &mesos.CommandInfo{Value: &testCmd},
			Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
		},
	}

	// create a secret
	// completely new secret
	secretID := &peloton.SecretID{
		Value: uuid.New(),
	}
	secret := jobmgrtask.CreateSecretProto(secretID.GetValue(),
		testSecretPath, []byte(testSecretStr))
	// create updated secret (same id same path) but with new data
	updatedSecret := jobmgrtask.CreateSecretProto(secretID.GetValue(),
		testSecretPath, []byte(testSecretStrUpdated))
	// create another secret
	addedSecretID := &peloton.SecretID{
		Value: uuid.New(),
	}
	addedSecret := jobmgrtask.CreateSecretProto(addedSecretID.GetValue(),
		testSecretPath, []byte(testSecretStrNew))

	// setup generic update mocks
	suite.setupMocks(jobID, respoolID)

	// Tests begin

	// no secret present in update request and oldConfig and newConfig are the
	// same. This should be a NOOP
	req := &job.UpdateRequest{
		Id:     jobID,
		Config: newJobConfig,
	}
	suite.mockedJobConfigOps.EXPECT().
		Get(context.Background(), jobID, gomock.Any()).
		Return(oldJobConfig, &models.ConfigAddOn{}, nil)

	resp, err := suite.handler.Update(suite.context, req)
	suite.NoError(err)
	suite.Nil(resp)

	// oldConfig does not contain existing secret volumes, request has one
	// secret, new secret should be created and config should be populated with
	// this new secret volume even if instance count remains unchanged.
	req.Secrets = []*peloton.Secret{secret}
	suite.mockedSecretInfoOps.EXPECT().CreateSecret(
		gomock.Any(),
		jobID.Value,               // jobID
		gomock.Any(),              // now
		gomock.Any(),              // secretID
		string(secret.Value.Data), // secretString
		testSecretPath).           // secretPath
		Return(nil)
	suite.mockedJobConfigOps.EXPECT().
		Get(context.Background(), jobID, gomock.Any()).
		Return(oldJobConfig, &models.ConfigAddOn{}, nil)
	suite.mockedCachedJob.EXPECT().
		CompareAndSetConfig(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(&job.JobConfig{
			ChangeLog: &peloton.ChangeLog{
				Version: 2,
			},
		}, nil)
	resp, err = suite.handler.Update(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(jobID, resp.GetId())
	suite.Equal("added 0 instances", resp.GetMessage())
	// Check if new secret volume is present in config and matches the secret
	// supplied in the request
	secretVolumes := util.RemoveSecretVolumesFromJobConfig(newJobConfig)
	suite.Equal(len(secretVolumes), 1)
	suite.Equal(secretID.GetValue(),
		string(secretVolumes[0].GetSource().GetSecret().GetValue().GetData()))

	// One secret present in oldConfig, request has same secret but with empty
	// secretID, and only path and data. We will look up this secret using path
	// and then update it. This should succeed and udated config should contain
	// 1 secret
	// add secret volumes to oldConfig
	req.Config = &job.JobConfig{
		DefaultConfig: &task.TaskConfig{
			Command:   &mesos.CommandInfo{Value: &testCmd},
			Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
		},
	}
	oldJobConfig.GetDefaultConfig().GetContainer().Volumes = []*mesos.Volume{
		util.CreateSecretVolume(testSecretPath, secretID.GetValue())}
	suite.mockedJobConfigOps.EXPECT().
		Get(context.Background(), jobID, gomock.Any()).
		Return(oldJobConfig, &models.ConfigAddOn{}, nil)
	suite.mockedCachedJob.EXPECT().
		CompareAndSetConfig(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(&job.JobConfig{
			ChangeLog: &peloton.ChangeLog{
				Version: 2,
			},
		}, nil)
	// request contains secret with same path, different data and empty ID
	req.Secrets = []*peloton.Secret{jobmgrtask.CreateSecretProto("",
		testSecretPath, []byte(testSecretStrUpdated))}
	// Even if ID is empty, the existing secret volume will contain same path as
	// supplied secret. So we will update the existing secret with new data.
	// This should result in once call to update updatedSecret in DB
	suite.mockedSecretInfoOps.EXPECT().UpdateSecretData(
		gomock.Any(),
		gomock.Any(),
		string(updatedSecret.Value.Data),
	).Return(nil)
	resp, err = suite.handler.Update(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(jobID, resp.GetId())
	suite.Equal("added 0 instances", resp.GetMessage())

	// One secret present in oldConfig, request has two secrets (1 update + 1 new),
	// This should succeed and udated config should contain 2 secrets
	// add secret volumes to oldConfig
	newJobConfig = &job.JobConfig{
		DefaultConfig: &task.TaskConfig{
			Command:   &mesos.CommandInfo{Value: &testCmd},
			Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
		},
	}
	req.Config = newJobConfig
	oldJobConfig.GetDefaultConfig().GetContainer().Volumes = []*mesos.Volume{
		util.CreateSecretVolume(testSecretPath, secretID.GetValue())}
	suite.mockedJobConfigOps.EXPECT().
		Get(context.Background(), jobID, gomock.Any()).
		Return(oldJobConfig, &models.ConfigAddOn{}, nil)
	// request contains one updated and one added secret
	req.Secrets = []*peloton.Secret{addedSecret, updatedSecret}
	// This should result in once call to create addedSecret in DB and one
	// call to update updatedSecret in DB
	suite.mockedSecretInfoOps.EXPECT().CreateSecret(
		gomock.Any(),
		jobID.Value,                       // jobID
		gomock.Any(),                      // now
		gomock.Any(),                      // secretID
		string(req.Secrets[0].Value.Data), // secretString
		testSecretPath).                   // secretPath
		Return(nil)
	suite.mockedSecretInfoOps.EXPECT().UpdateSecretData(
		gomock.Any(),
		gomock.Any(),
		string(updatedSecret.Value.Data)).
		Return(nil)
	suite.mockedCachedJob.EXPECT().
		CompareAndSetConfig(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			nil).
		Return(&job.JobConfig{
			ChangeLog: &peloton.ChangeLog{
				Version: 2,
			},
		}, nil)
	resp, err = suite.handler.Update(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(jobID, resp.GetId())
	suite.Equal("added 0 instances", resp.GetMessage())
	// Check if new secret volume is present in config and matches the secret
	// supplied in the request
	secretVolumes = util.RemoveSecretVolumesFromJobConfig(newJobConfig)
	suite.Equal(len(secretVolumes), 2)
	for _, volume := range secretVolumes {
		// since secret volumes may not be in the same order, still not totally
		// deterministic but good enough for this
		id := string(volume.GetSource().GetSecret().GetValue().GetData())
		suite.True(id == addedSecretID.GetValue() || id == secretID.GetValue())
	}

	// request contains a secret with blank secretID
	req.Secrets = []*peloton.Secret{jobmgrtask.CreateSecretProto(
		"", testSecretPath, []byte(testSecretStr))}
	req.Config = &job.JobConfig{
		DefaultConfig: &task.TaskConfig{
			Command:   &mesos.CommandInfo{Value: &testCmd},
			Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
		},
	}
	suite.mockedJobConfigOps.EXPECT().
		Get(context.Background(), jobID, gomock.Any()).
		Return(&job.JobConfig{
			DefaultConfig: &task.TaskConfig{
				Command:   &mesos.CommandInfo{Value: &testCmd},
				Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
			}}, &models.ConfigAddOn{},
			nil)
	suite.mockedCachedJob.EXPECT().
		CompareAndSetConfig(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			nil).Return(&job.JobConfig{
		ChangeLog: &peloton.ChangeLog{
			Version: 2,
		},
	}, nil)
	suite.mockedSecretInfoOps.EXPECT().CreateSecret(
		gomock.Any(),
		jobID.Value,               // jobID
		gomock.Any(),              // now
		gomock.Any(),              // secretID
		string(secret.Value.Data), // secretString
		testSecretPath).           // secretPath
		Return(nil)
	resp, err = suite.handler.Update(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(jobID, resp.GetId())
	suite.Equal("added 0 instances", resp.GetMessage())

	// Negative tests begin

	// newJobConfig contains secret volumes directly added to config.
	// This will be rejected and should result in error
	req.Config = &job.JobConfig{
		DefaultConfig: &task.TaskConfig{
			Command: &mesos.CommandInfo{Value: &testCmd},
			Container: &mesos.ContainerInfo{
				Type: &mesosContainerizer,
				Volumes: []*mesos.Volume{util.CreateSecretVolume(
					testSecretPath, secretID.GetValue())},
			},
		},
	}
	suite.mockedJobConfigOps.EXPECT().
		Get(context.Background(), jobID, gomock.Any()).
		Return(oldJobConfig, &models.ConfigAddOn{}, nil)
	_, err = suite.handler.Update(suite.context, req)
	suite.Error(err)
	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.Equal(
		yarpcerrors.ErrorMessage(err),
		"adding secret volumes directly in config is not allowed")

	// Request contains a secret with blank path
	req.Secrets = []*peloton.Secret{jobmgrtask.CreateSecretProto(
		secretID.GetValue(), "", []byte(testSecretStr))}
	req.Config = &job.JobConfig{
		DefaultConfig: &task.TaskConfig{
			Command:   &mesos.CommandInfo{Value: &testCmd},
			Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
		},
	}
	suite.mockedJobConfigOps.EXPECT().
		Get(context.Background(), jobID, gomock.Any()).
		Return(oldJobConfig, &models.ConfigAddOn{}, nil)
	_, err = suite.handler.Update(suite.context, req)
	suite.Error(err)
	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.Equal(yarpcerrors.ErrorMessage(err), "secret does not have a path")

	// Two secrets present in oldConfig, request has only one. This will fail.
	// Request should contain existing two and/or additional secrets
	oldJobConfig.GetDefaultConfig().GetContainer().Volumes = []*mesos.Volume{
		// insert non-secret volumes, will be ignored
		getVolume(),
		util.CreateSecretVolume(testSecretPath, addedSecretID.GetValue()),
		// insert non-secret volumes, will be ignored
		getVolume(),
		util.CreateSecretVolume(testSecretPath, secretID.GetValue()),
	}
	req.Config = &job.JobConfig{
		DefaultConfig: &task.TaskConfig{
			Command: &mesos.CommandInfo{Value: &testCmd},
			Container: &mesos.ContainerInfo{
				Type:    &mesosContainerizer,
				Volumes: []*mesos.Volume{getVolume(), getVolume()},
			},
		},
	}
	suite.mockedJobConfigOps.EXPECT().
		Get(context.Background(), jobID, gomock.Any()).
		Return(oldJobConfig, &models.ConfigAddOn{}, nil)
	// secret contains one updated and one added secret
	req.Secrets = []*peloton.Secret{addedSecret}
	resp, err = suite.handler.Update(suite.context, req)
	suite.Error(err)
	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.Equal(yarpcerrors.ErrorMessage(err),
		"number of secrets in request should be >= existing secrets")

	// request contains secret that doesn't match existing secret
	oldJobConfig.GetDefaultConfig().GetContainer().Volumes = []*mesos.Volume{
		util.CreateSecretVolume(testSecretPathNew, addedSecretID.GetValue()),
	}
	req.Config = &job.JobConfig{
		DefaultConfig: &task.TaskConfig{
			Command:   &mesos.CommandInfo{Value: &testCmd},
			Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
		},
	}
	suite.mockedJobConfigOps.EXPECT().
		Get(context.Background(), jobID, gomock.Any()).
		Return(oldJobConfig, &models.ConfigAddOn{}, nil)
	req.Secrets = []*peloton.Secret{secret}
	resp, err = suite.handler.Update(suite.context, req)
	suite.Error(err)
	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.Equal(yarpcerrors.ErrorMessage(err),
		fmt.Sprintf("request missing secret with id %v path %v",
			addedSecretID.GetValue(), testSecretPathNew))

	// test UpdateSecret failure
	oldJobConfig.GetDefaultConfig().GetContainer().Volumes = []*mesos.Volume{
		util.CreateSecretVolume(testSecretPath, addedSecretID.GetValue()),
	}
	req.Config = &job.JobConfig{
		DefaultConfig: &task.TaskConfig{
			Command:   &mesos.CommandInfo{Value: &testCmd},
			Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
		},
	}
	suite.mockedJobConfigOps.EXPECT().
		Get(context.Background(), jobID, gomock.Any()).
		Return(oldJobConfig, &models.ConfigAddOn{}, nil)
	req.Secrets = []*peloton.Secret{addedSecret}
	// Simulate DB failure in UpdateSecret
	suite.mockedSecretInfoOps.EXPECT().UpdateSecretData(
		gomock.Any(),
		gomock.Any(),
		string(addedSecret.Value.Data)).
		Return(errors.New("DB error"))
	resp, err = suite.handler.Update(suite.context, req)
	suite.Error(err)
	suite.Equal(yarpcerrors.ErrorMessage(err), "DB error")

	// test CreateSecret failure
	oldJobConfig.GetDefaultConfig().GetContainer().Volumes = nil
	req.Config = &job.JobConfig{
		DefaultConfig: &task.TaskConfig{
			Command:   &mesos.CommandInfo{Value: &testCmd},
			Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
		},
	}
	suite.mockedJobConfigOps.EXPECT().
		Get(context.Background(), jobID, gomock.Any()).
		Return(oldJobConfig, &models.ConfigAddOn{}, nil)
	req.Secrets = []*peloton.Secret{addedSecret}
	// Simulate DB failure in CreateSecret
	suite.mockedSecretInfoOps.EXPECT().CreateSecret(
		gomock.Any(),
		jobID.Value,                    // jobID
		gomock.Any(),                   // now
		gomock.Any(),                   // secretID
		string(addedSecret.Value.Data), // secretString
		testSecretPath).                // secretPath
		Return(errors.New("DB error"))

	resp, err = suite.handler.Update(suite.context, req)
	suite.Error(err)
	suite.Equal(yarpcerrors.FromError(err).Message(), "DB error")

	// Cluster does not support secrets but update request has secrets,
	// this will result in error
	suite.handler.jobSvcCfg.EnableSecrets = false
	suite.mockedJobConfigOps.EXPECT().
		Get(context.Background(), jobID, gomock.Any()).
		Return(oldJobConfig, &models.ConfigAddOn{}, nil)
	_, err = suite.handler.Update(suite.context, req)
	suite.Error(err)
}

// helper function to generate random volume
func getVolume() *mesos.Volume {
	volumeMode := mesos.Volume_RO
	testPath := "/test"
	return &mesos.Volume{
		Mode:          &volumeMode,
		ContainerPath: &testPath,
	}
}

// TestJobQuery tests success case for Job Query API
// This is fairly minimal, all interesting test cases are in the unit tests
// for store.QueryJobs()
func (suite *JobHandlerTestSuite) TestJobQuery() {
	// TODO: add more inputs
	suite.mockedJobStore.EXPECT().QueryJobs(suite.context, nil, nil, false)
	resp, err := suite.handler.Query(suite.context, &job.QueryRequest{})
	suite.NoError(err)
	suite.NotNil(resp)
}

// TestJobQuery tests failure case for Job Query API
// This is fairly minimal, all interesting test cases are in the unit tests
// for store.QueryJobs()
func (suite *JobHandlerTestSuite) TestJobQueryFailure() {
	// TODO: add more inputs
	suite.mockedJobStore.EXPECT().QueryJobs(suite.context, nil, nil, false).
		Return(nil, nil, uint32(0), errors.New("DB error"))
	resp, err := suite.handler.Query(suite.context, &job.QueryRequest{})
	suite.NoError(err)
	suite.NotNil(resp)
	expectedErr := &job.QueryResponse_Error{
		Err: &apierrors.UnknownError{
			Message: "DB error",
		},
	}
	suite.Equal(expectedErr, resp.GetError())
}

func (suite *JobHandlerTestSuite) TestJobDelete() {
	id := &peloton.JobID{
		Value: "my-job",
	}

	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	taskMap := make(map[uint32]cached.Task)
	taskMap[0] = cachedTask

	suite.mockedJobFactory.EXPECT().GetJob(id).
		Return(suite.mockedCachedJob).Times(2)

	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{State: job.JobState_SUCCEEDED}, nil)

	suite.mockedJobStore.EXPECT().
		DeleteJob(context.Background(), id.GetValue()).
		Return(nil)
	suite.mockedJobIndexOps.EXPECT().
		Delete(context.Background(), id).
		Return(nil)

	suite.mockedCachedJob.EXPECT().
		GetAllTasks().
		Return(taskMap)

	suite.mockedGoalStateDriver.EXPECT().
		DeleteTask(id, uint32(0)).
		Return()

	suite.mockedGoalStateDriver.EXPECT().
		DeleteJob(id).
		Return()

	suite.mockedJobFactory.EXPECT().
		ClearJob(id).
		Return()

	res, err := suite.handler.Delete(suite.context, &job.DeleteRequest{Id: id})
	suite.Equal(&job.DeleteResponse{}, res)
	suite.NoError(err)

	suite.mockedJobFactory.EXPECT().GetJob(id).
		Return(suite.mockedCachedJob)

	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{State: job.JobState_PENDING}, nil)

	res, err = suite.handler.Delete(suite.context, &job.DeleteRequest{Id: id})
	suite.Nil(res)
	suite.Error(err)
	expectedErr := yarpcerrors.InternalErrorf("Job is not in a terminal state: PENDING")
	suite.Equal(expectedErr, err)

	suite.mockedJobFactory.EXPECT().GetJob(id).
		Return(suite.mockedCachedJob)

	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(nil, fmt.Errorf("fake db error"))

	res, err = suite.handler.Delete(suite.context, &job.DeleteRequest{Id: id})
	suite.Nil(res)
	suite.Error(err)
	expectedErr = yarpcerrors.NotFoundErrorf("job not found")
	suite.Equal(expectedErr, err)

	// Test JobDelete failure
	suite.mockedJobFactory.EXPECT().GetJob(id).
		Return(suite.mockedCachedJob)

	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{State: job.JobState_SUCCEEDED}, nil)

	suite.mockedJobStore.EXPECT().
		DeleteJob(context.Background(), id.GetValue()).
		Return(yarpcerrors.InternalErrorf("fake db error"))

	res, err = suite.handler.Delete(suite.context, &job.DeleteRequest{Id: id})
	suite.Nil(res)
	suite.Error(err)
	expectedErr = yarpcerrors.InternalErrorf("fake db error")
	suite.Equal(expectedErr, err)

	// Test JobIndex delete failure
	suite.mockedJobFactory.EXPECT().GetJob(id).
		Return(suite.mockedCachedJob)
	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{State: job.JobState_SUCCEEDED}, nil)
	suite.mockedJobStore.EXPECT().
		DeleteJob(context.Background(), id.GetValue()).
		Return(nil)
	suite.mockedJobIndexOps.EXPECT().
		Delete(context.Background(), id).
		Return(yarpcerrors.InternalErrorf("fake db error: job_index"))

	res, err = suite.handler.Delete(suite.context, &job.DeleteRequest{Id: id})
	suite.Nil(res)
	suite.Error(err)
	expectedErr = yarpcerrors.InternalErrorf("fake db error: job_index")
}

func (suite *JobHandlerTestSuite) TestJobRefresh() {
	id := &peloton.JobID{
		Value: "my-job",
	}
	jobConfig := &job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: 4,
	}
	jobRuntime := &job.RuntimeInfo{State: job.JobState_RUNNING}
	jobInfo := &job.JobInfo{
		Config:  jobConfig,
		Runtime: jobRuntime,
	}

	suite.mockedCandidate.EXPECT().IsLeader().Return(true)
	suite.mockedJobRuntimeOps.EXPECT().
		Get(context.Background(), id).
		Return(jobRuntime, nil)
	suite.mockedJobConfigOps.EXPECT().
		Get(context.Background(), id, gomock.Any()).
		Return(jobConfig, &models.ConfigAddOn{}, nil)
	suite.mockedJobFactory.EXPECT().AddJob(id).Return(suite.mockedCachedJob)
	suite.mockedCachedJob.EXPECT().
		Update(
			gomock.Any(),
			jobInfo,
			gomock.Any(),
			nil,
			cached.UpdateCacheOnly).
		Return(nil)
	suite.mockedGoalStateDriver.EXPECT().EnqueueJob(id, gomock.Any())
	_, err := suite.handler.Refresh(suite.context, &job.RefreshRequest{Id: id})
	suite.NoError(err)

	suite.mockedCandidate.EXPECT().IsLeader().Return(true)
	suite.mockedJobRuntimeOps.EXPECT().
		Get(context.Background(), id).
		Return(jobRuntime, nil)
	suite.mockedJobConfigOps.EXPECT().
		Get(context.Background(), id, gomock.Any()).
		Return(nil, nil, fmt.Errorf("fake db error"))
	_, err = suite.handler.Refresh(suite.context, &job.RefreshRequest{Id: id})
	suite.Error(err)

	suite.mockedCandidate.EXPECT().IsLeader().Return(true)
	suite.mockedJobRuntimeOps.EXPECT().
		Get(context.Background(), id).
		Return(nil, fmt.Errorf("fake db error"))
	_, err = suite.handler.Refresh(suite.context, &job.RefreshRequest{Id: id})
	suite.Error(err)
}

func (suite *JobHandlerTestSuite) TestJobGetCache_JobNotFound() {
	id := &peloton.JobID{
		Value: "my-job",
	}

	// Test job is not in cache
	suite.mockedJobFactory.EXPECT().
		GetJob(gomock.Any()).Return(nil)
	_, err := suite.handler.GetCache(context.Background(), &job.GetCacheRequest{Id: id})
	suite.Error(err)
}

func (suite *JobHandlerTestSuite) TestJobGetCache_FailToLoadRuntime() {
	id := &peloton.JobID{
		Value: "my-job",
	}

	// Test fail to get job runtime cache
	suite.mockedJobFactory.EXPECT().
		GetJob(gomock.Any()).Return(suite.mockedCachedJob)
	suite.mockedCachedJob.EXPECT().
		GetRuntime(gomock.Any()).Return(nil, fmt.Errorf("get runtime err"))
	_, err := suite.handler.GetCache(context.Background(), &job.GetCacheRequest{Id: id})
	suite.Error(err)
}

func (suite *JobHandlerTestSuite) TestJobGetCache_FailToGetJobConfig() {
	id := &peloton.JobID{
		Value: "my-job",
	}
	jobRuntime := &job.RuntimeInfo{State: job.JobState_RUNNING}

	// Test fail to get job config cache
	suite.mockedJobFactory.EXPECT().
		GetJob(gomock.Any()).Return(suite.mockedCachedJob)
	suite.mockedCachedJob.EXPECT().
		GetRuntime(gomock.Any()).Return(jobRuntime, nil)
	suite.mockedCachedJob.EXPECT().
		GetConfig(gomock.Any()).Return(nil, fmt.Errorf("get runtime err"))
	_, err := suite.handler.GetCache(context.Background(), &job.GetCacheRequest{Id: id})
	suite.Error(err)
}

func (suite *JobHandlerTestSuite) TestJobGetCache_SUCCESS() {
	id := &peloton.JobID{
		Value: "my-job",
	}
	jobConfig := &job.JobConfig{
		InstanceCount: 4,
		Type:          job.JobType_BATCH,
	}
	jobRuntime := &job.RuntimeInfo{State: job.JobState_RUNNING}

	// Test succeed path
	suite.mockedJobFactory.EXPECT().
		GetJob(gomock.Any()).Return(suite.mockedCachedJob)
	suite.mockedCachedJob.EXPECT().
		GetRuntime(gomock.Any()).Return(jobRuntime, nil)
	suite.mockedCachedJob.EXPECT().
		GetConfig(gomock.Any()).Return(cachedtest.NewMockJobConfig(suite.ctrl, jobConfig), nil)
	resp, err := suite.handler.GetCache(context.Background(), &job.GetCacheRequest{Id: id})
	suite.NoError(err)
	suite.Equal(resp.Config.Type, jobConfig.Type)
	suite.Equal(resp.Config.InstanceCount, jobConfig.InstanceCount)
	suite.Equal(resp.Runtime.State, jobRuntime.State)
}

// TestJobGetActiveJobsFail tests failure to get active jobs list from DB
func (suite *JobHandlerTestSuite) TestJobGetActiveJobsFail() {
	suite.mockedActiveJobsOps.EXPECT().GetAll(context.Background()).
		Return(nil, fmt.Errorf("get active jobs err"))
	_, err := suite.handler.GetActiveJobs(context.Background(),
		&job.GetActiveJobsRequest{})
	suite.Error(err)
}

// TestJobGetActiveJobs tests failure to get active jobs list from DB
func (suite *JobHandlerTestSuite) TestJobGetActiveJobs() {
	expectedJobIDs := []*peloton.JobID{
		{Value: "my-job-1"},
	}
	suite.mockedActiveJobsOps.EXPECT().
		GetAll(context.Background()).
		Return(expectedJobIDs, nil)

	resp, err := suite.handler.GetActiveJobs(context.Background(),
		&job.GetActiveJobsRequest{})
	suite.NoError(err)
	suite.Equal(resp.GetIds(), expectedJobIDs)
}

// TestRestartJobSuccess tests the success path of restarting job
func (suite *JobHandlerTestSuite) TestRestartJobSuccess() {
	var configurationVersion uint64 = 1
	var workflowVersion uint64 = 2
	var desiredStateVersion uint64 = 1
	var batchSize uint32 = 1

	suite.mockedCandidate.EXPECT().
		IsLeader().
		Return(true)

	suite.testJobConfig.ChangeLog =
		&peloton.ChangeLog{Version: configurationVersion}

	suite.mockedJobFactory.EXPECT().
		AddJob(suite.testJobID).
		Return(suite.mockedCachedJob)

	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{
			State:                job.JobState_PENDING,
			ConfigurationVersion: configurationVersion,
		}, nil)

	suite.mockedJobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			suite.testJobID,
			configurationVersion,
		).
		Return(suite.testJobConfig, &models.ConfigAddOn{}, nil)

	newConfig := *suite.testJobConfig
	newConfig.ChangeLog = &peloton.ChangeLog{Version: configurationVersion + 1}
	suite.mockedCachedJob.EXPECT().
		CreateWorkflow(
			gomock.Any(),
			models.WorkflowType_RESTART,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		Return(
			&peloton.UpdateID{Value: uuid.New()},
			versionutil.GetJobEntityVersion(configurationVersion+1, desiredStateVersion, workflowVersion),
			nil)

	suite.mockedGoalStateDriver.EXPECT().
		EnqueueUpdate(gomock.Any(), gomock.Any(), gomock.Any())

	suite.mockedCachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&newConfig, nil)

	req := &job.RestartRequest{
		Id:              suite.testJobID,
		ResourceVersion: configurationVersion,
		RestartConfig: &job.RestartConfig{
			BatchSize: batchSize,
		},
	}

	resp, err := suite.handler.Restart(context.Background(), req)
	suite.NoError(err)
	suite.NotNil(resp.GetUpdateID())
	suite.Equal(resp.GetResourceVersion(),
		newConfig.GetChangeLog().GetVersion())
}

// TestRestartJobSuccessWithRange tests the success path of restarting job with
// ranges
func (suite *JobHandlerTestSuite) TestRestartJobSuccessWithRange() {
	var configurationVersion uint64 = 1
	var workflowVersion uint64 = 2
	var desiredStateVersion uint64 = 1
	var batchSize uint32 = 1
	restartRanges := []*task.InstanceRange{
		{
			From: 0,
			To:   1,
		},
		{
			From: 2,
			To:   3,
		},
	}

	suite.mockedCandidate.EXPECT().
		IsLeader().
		Return(true)

	suite.testJobConfig.ChangeLog =
		&peloton.ChangeLog{Version: configurationVersion}

	suite.mockedJobFactory.EXPECT().
		AddJob(suite.testJobID).
		Return(suite.mockedCachedJob)

	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{
			State:                job.JobState_PENDING,
			ConfigurationVersion: configurationVersion,
		}, nil)

	suite.mockedJobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			suite.testJobID,
			configurationVersion,
		).
		Return(suite.testJobConfig, &models.ConfigAddOn{}, nil)

	newConfig := *suite.testJobConfig
	newConfig.ChangeLog = &peloton.ChangeLog{Version: configurationVersion + 1}
	suite.mockedCachedJob.EXPECT().
		CreateWorkflow(
			gomock.Any(),
			models.WorkflowType_RESTART,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		Return(
			&peloton.UpdateID{Value: uuid.New()},
			versionutil.GetJobEntityVersion(configurationVersion+1, desiredStateVersion, workflowVersion),
			nil)

	suite.mockedGoalStateDriver.EXPECT().
		EnqueueUpdate(gomock.Any(), gomock.Any(), gomock.Any())

	suite.mockedCachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&newConfig, nil)

	req := &job.RestartRequest{
		Id:              suite.testJobID,
		ResourceVersion: configurationVersion,
		Ranges:          restartRanges,
		RestartConfig: &job.RestartConfig{
			BatchSize: batchSize,
		},
	}

	resp, err := suite.handler.Restart(context.Background(), req)
	suite.NoError(err)
	suite.NotNil(resp.GetUpdateID())
	suite.Equal(resp.GetResourceVersion(),
		newConfig.GetChangeLog().GetVersion())
}

// TestRestartJobOutsideOfRangeSuccess tests the success path of restart
// with ranges out side of range
func (suite *JobHandlerTestSuite) TestRestartJobOutsideOfRangeSuccess() {
	var configurationVersion uint64 = 1
	var workflowVersion uint64 = 2
	var desiredStateVersion uint64 = 1
	var batchSize uint32 = 1
	restartRanges := []*task.InstanceRange{
		{
			From: 0,
			To:   1,
		},
		{
			From: 1,
			To:   10,
		},
	}

	suite.mockedCandidate.EXPECT().
		IsLeader().
		Return(true)

	suite.testJobConfig.ChangeLog =
		&peloton.ChangeLog{Version: configurationVersion}

	suite.mockedJobFactory.EXPECT().
		AddJob(suite.testJobID).
		Return(suite.mockedCachedJob)

	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{
			State:                job.JobState_PENDING,
			ConfigurationVersion: configurationVersion,
			WorkflowVersion:      workflowVersion,
		}, nil)

	suite.mockedJobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			suite.testJobID,
			configurationVersion,
		).
		Return(suite.testJobConfig, &models.ConfigAddOn{}, nil)

	newConfig := *suite.testJobConfig
	newConfig.ChangeLog = &peloton.ChangeLog{Version: configurationVersion + 1}
	suite.mockedCachedJob.EXPECT().
		CreateWorkflow(
			gomock.Any(),
			models.WorkflowType_RESTART,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		Return(
			&peloton.UpdateID{Value: uuid.New()},
			versionutil.GetJobEntityVersion(configurationVersion+1, desiredStateVersion, workflowVersion),
			nil)

	suite.mockedGoalStateDriver.EXPECT().
		EnqueueUpdate(gomock.Any(), gomock.Any(), gomock.Any())

	suite.mockedCachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&newConfig, nil)

	req := &job.RestartRequest{
		Id:              suite.testJobID,
		ResourceVersion: configurationVersion,
		Ranges:          restartRanges,
		RestartConfig: &job.RestartConfig{
			BatchSize: batchSize,
		},
	}

	resp, err := suite.handler.Restart(context.Background(), req)
	suite.NoError(err)
	suite.NotNil(resp.GetUpdateID())
	suite.Equal(resp.GetResourceVersion(),
		newConfig.GetChangeLog().GetVersion())
}

// TestRestartJobNonLeaderFailure tests the restart a job fails because jobmgr
// is not leader
func (suite *JobHandlerTestSuite) TestRestartJobNonLeaderFailure() {
	var configurationVersion uint64 = 1
	var batchSize uint32 = 1

	suite.mockedCandidate.EXPECT().
		IsLeader().
		Return(false)
	req := &job.RestartRequest{
		Id:              suite.testJobID,
		ResourceVersion: configurationVersion,
		RestartConfig: &job.RestartConfig{
			BatchSize: batchSize,
		},
	}

	resp, err := suite.handler.Restart(context.Background(), req)
	suite.Error(err)
	suite.Nil(resp)
}

// TestRestartJobGetRuntimeFailure tests restarting job fails because
// get runtime fails
func (suite *JobHandlerTestSuite) TestRestartJobGetRuntimeFailure() {
	var configurationVersion uint64 = 1
	var batchSize uint32 = 1

	suite.mockedCandidate.EXPECT().
		IsLeader().
		Return(true)

	suite.testJobConfig.ChangeLog =
		&peloton.ChangeLog{Version: configurationVersion}

	suite.mockedJobFactory.EXPECT().
		AddJob(suite.testJobID).
		Return(suite.mockedCachedJob)

	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(nil, fmt.Errorf("test error"))

	req := &job.RestartRequest{
		Id:              suite.testJobID,
		ResourceVersion: configurationVersion,
		RestartConfig: &job.RestartConfig{
			BatchSize: batchSize,
		},
	}

	resp, err := suite.handler.Restart(context.Background(), req)
	suite.Error(err)
	suite.Nil(resp)
}

// TestRestartJobGetConfigFailure tests restarting job fails due to get config
// fails
func (suite *JobHandlerTestSuite) TestRestartJobGetConfigFailure() {
	var configurationVersion uint64 = 1
	var batchSize uint32 = 1

	suite.mockedCandidate.EXPECT().
		IsLeader().
		Return(true)

	suite.testJobConfig.ChangeLog =
		&peloton.ChangeLog{Version: configurationVersion}

	suite.mockedJobFactory.EXPECT().
		AddJob(suite.testJobID).
		Return(suite.mockedCachedJob)

	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{
			State:                job.JobState_PENDING,
			ConfigurationVersion: configurationVersion,
		}, nil)

	suite.mockedJobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			suite.testJobID,
			configurationVersion,
		).
		Return(nil, nil, fmt.Errorf("test error"))

	req := &job.RestartRequest{
		Id:              suite.testJobID,
		ResourceVersion: configurationVersion,
		RestartConfig: &job.RestartConfig{
			BatchSize: batchSize,
		},
	}

	resp, err := suite.handler.Restart(context.Background(), req)
	suite.Error(err)
	suite.Nil(resp)
}

// TestRestartJobCreateUpdateFailure tests restart job fails due to update
// creation fails
func (suite *JobHandlerTestSuite) TestRestartJobCreateUpdateFailure() {
	var configurationVersion uint64 = 1
	var workflowVersion uint64 = 2
	var batchSize uint32 = 1

	suite.mockedCandidate.EXPECT().
		IsLeader().
		Return(true)

	suite.testJobConfig.ChangeLog =
		&peloton.ChangeLog{Version: configurationVersion}

	suite.mockedJobFactory.EXPECT().
		AddJob(suite.testJobID).
		Return(suite.mockedCachedJob)

	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{
			State:                job.JobState_PENDING,
			ConfigurationVersion: configurationVersion,
			WorkflowVersion:      workflowVersion,
		}, nil)

	suite.mockedJobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			suite.testJobID,
			configurationVersion,
		).
		Return(suite.testJobConfig, &models.ConfigAddOn{}, nil)

	newConfig := *suite.testJobConfig
	newConfig.ChangeLog = &peloton.ChangeLog{Version: configurationVersion + 1}
	suite.mockedCachedJob.EXPECT().
		CreateWorkflow(
			gomock.Any(),
			models.WorkflowType_RESTART,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		Return(&peloton.UpdateID{Value: uuid.New()}, nil, yarpcerrors.InternalErrorf("test error"))

	suite.mockedGoalStateDriver.EXPECT().
		EnqueueUpdate(gomock.Any(), gomock.Any(), gomock.Any())

	req := &job.RestartRequest{
		Id:              suite.testJobID,
		ResourceVersion: configurationVersion,
		RestartConfig: &job.RestartConfig{
			BatchSize: batchSize,
		},
	}

	resp, err := suite.handler.Restart(context.Background(), req)
	suite.Error(err)
	suite.Nil(resp)
}

// TestRestartJobGetCachedConfigFailure tests restarting job fails because
// get cached config failure
func (suite *JobHandlerTestSuite) TestRestartJobGetCachedConfigFailure() {
	var configurationVersion uint64 = 1
	var workflowVersion uint64 = 2
	var desiredStateVersion uint64 = 1
	var batchSize uint32 = 1

	suite.mockedCandidate.EXPECT().
		IsLeader().
		Return(true)

	suite.testJobConfig.ChangeLog =
		&peloton.ChangeLog{Version: configurationVersion}

	suite.mockedJobFactory.EXPECT().
		AddJob(suite.testJobID).
		Return(suite.mockedCachedJob)

	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{
			State:                job.JobState_PENDING,
			ConfigurationVersion: configurationVersion,
			WorkflowVersion:      workflowVersion,
		}, nil)

	suite.mockedJobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			suite.testJobID,
			configurationVersion,
		).
		Return(suite.testJobConfig, &models.ConfigAddOn{}, nil)

	newConfig := *suite.testJobConfig
	newConfig.ChangeLog = &peloton.ChangeLog{Version: configurationVersion + 1}
	suite.mockedCachedJob.EXPECT().
		CreateWorkflow(
			gomock.Any(),
			models.WorkflowType_RESTART,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		Return(
			&peloton.UpdateID{Value: uuid.New()},
			versionutil.GetJobEntityVersion(configurationVersion+1, desiredStateVersion, workflowVersion),
			nil)

	suite.mockedGoalStateDriver.EXPECT().
		EnqueueUpdate(gomock.Any(), gomock.Any(), gomock.Any())

	suite.mockedCachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(nil, fmt.Errorf("test error"))

	req := &job.RestartRequest{
		Id:              suite.testJobID,
		ResourceVersion: configurationVersion,
		RestartConfig: &job.RestartConfig{
			BatchSize: batchSize,
		},
	}

	resp, err := suite.handler.Restart(context.Background(), req)
	suite.Error(err)
	suite.Nil(resp)
}

// TestRestartNonServiceJobFailure tests restarting job fails because
// job is not of type service
func (suite *JobHandlerTestSuite) TestRestartNonServiceJobFailure() {
	var configurationVersion uint64 = 1
	var batchSize uint32 = 1

	suite.mockedCandidate.EXPECT().
		IsLeader().
		Return(true)

	suite.testJobConfig.ChangeLog =
		&peloton.ChangeLog{Version: configurationVersion}

	suite.testJobConfig.Type = job.JobType_BATCH

	suite.mockedJobFactory.EXPECT().
		AddJob(suite.testJobID).
		Return(suite.mockedCachedJob)

	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{
			State:                job.JobState_PENDING,
			ConfigurationVersion: configurationVersion,
		}, nil)

	suite.mockedJobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			suite.testJobID,
			configurationVersion,
		).
		Return(suite.testJobConfig, &models.ConfigAddOn{}, nil)

	req := &job.RestartRequest{
		Id:              suite.testJobID,
		ResourceVersion: configurationVersion,
		RestartConfig: &job.RestartConfig{
			BatchSize: batchSize,
		},
	}

	resp, err := suite.handler.Restart(context.Background(), req)
	suite.Error(err)
	suite.Nil(resp)
}

// TestStartJobSuccess tests the success path of starting job
func (suite *JobHandlerTestSuite) TestStartJobSuccess() {
	var configurationVersion uint64 = 1
	var workflowVersion uint64 = 2
	var desiredStateVersion uint64 = 1
	var batchSize uint32 = 1

	suite.mockedCandidate.EXPECT().
		IsLeader().
		Return(true)

	suite.testJobConfig.ChangeLog =
		&peloton.ChangeLog{Version: configurationVersion}

	suite.mockedJobFactory.EXPECT().
		AddJob(suite.testJobID).
		Return(suite.mockedCachedJob)

	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{
			State:                job.JobState_PENDING,
			ConfigurationVersion: configurationVersion,
		}, nil)

	suite.mockedJobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			suite.testJobID,
			configurationVersion,
		).
		Return(suite.testJobConfig, &models.ConfigAddOn{}, nil)

	newConfig := *suite.testJobConfig
	newConfig.ChangeLog = &peloton.ChangeLog{Version: configurationVersion + 1}

	suite.mockedCachedJob.EXPECT().
		CreateWorkflow(
			gomock.Any(),
			models.WorkflowType_START,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		Return(
			&peloton.UpdateID{Value: uuid.New()},
			versionutil.GetJobEntityVersion(configurationVersion+1, desiredStateVersion, workflowVersion),
			nil)

	suite.mockedGoalStateDriver.EXPECT().
		EnqueueUpdate(gomock.Any(), gomock.Any(), gomock.Any())

	suite.mockedCachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&newConfig, nil)

	req := &job.StartRequest{
		Id:              suite.testJobID,
		ResourceVersion: configurationVersion,
		StartConfig: &job.StartConfig{
			BatchSize: batchSize,
		},
	}

	resp, err := suite.handler.Start(context.Background(), req)
	suite.NoError(err)
	suite.NotNil(resp.GetUpdateID())
	suite.Equal(resp.GetResourceVersion(),
		newConfig.GetChangeLog().GetVersion())
}

// TestStopJobSuccess tests the success path of stopping job
func (suite *JobHandlerTestSuite) TestStopJobSuccess() {
	var configurationVersion uint64 = 1
	var workflowVersion uint64 = 2
	var desiredStateVersion uint64 = 1
	var batchSize uint32 = 1

	suite.mockedCandidate.EXPECT().
		IsLeader().
		Return(true)

	suite.testJobConfig.ChangeLog =
		&peloton.ChangeLog{Version: configurationVersion}

	suite.mockedJobFactory.EXPECT().
		AddJob(suite.testJobID).
		Return(suite.mockedCachedJob)

	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{
			State:                job.JobState_PENDING,
			ConfigurationVersion: configurationVersion,
		}, nil)

	suite.mockedJobConfigOps.EXPECT().
		Get(
			gomock.Any(),
			suite.testJobID,
			configurationVersion,
		).
		Return(suite.testJobConfig, &models.ConfigAddOn{}, nil)

	newConfig := *suite.testJobConfig
	newConfig.ChangeLog = &peloton.ChangeLog{Version: configurationVersion + 1}
	suite.mockedCachedJob.EXPECT().
		CreateWorkflow(
			gomock.Any(),
			models.WorkflowType_STOP,
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		Return(
			&peloton.UpdateID{Value: uuid.New()},
			versionutil.GetJobEntityVersion(configurationVersion+1, desiredStateVersion, workflowVersion),
			nil)

	suite.mockedGoalStateDriver.EXPECT().
		EnqueueUpdate(gomock.Any(), gomock.Any(), gomock.Any())

	suite.mockedCachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&newConfig, nil)

	req := &job.StopRequest{
		Id:              suite.testJobID,
		ResourceVersion: configurationVersion,
		StopConfig: &job.StopConfig{
			BatchSize: batchSize,
		},
	}

	resp, err := suite.handler.Stop(context.Background(), req)
	suite.NoError(err)
	suite.NotNil(resp.GetUpdateID())
	suite.Equal(resp.GetResourceVersion(),
		newConfig.GetChangeLog().GetVersion())
}
