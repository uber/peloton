package jobsvc

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	apierrors "code.uber.internal/infra/peloton/.gen/peloton/api/v0/errors"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool"
	respoolmocks "code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	resmocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	cachedtest "code.uber.internal/infra/peloton/jobmgr/cached/test"
	goalstatemocks "code.uber.internal/infra/peloton/jobmgr/goalstate/mocks"
	jobmgrtask "code.uber.internal/infra/peloton/jobmgr/task"
	leadermocks "code.uber.internal/infra/peloton/leader/mocks"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"go.uber.org/yarpc/yarpcerrors"
)

const (
	testInstanceCount    = 2
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

	ctrl                  *gomock.Controller
	mockedCandidate       *leadermocks.MockCandidate
	mockedRespoolClient   *respoolmocks.MockResourceManagerYARPCClient
	mockedResmgrClient    *resmocks.MockResourceManagerServiceYARPCClient
	mockedJobFactory      *cachedmocks.MockJobFactory
	mockedCachedJob       *cachedmocks.MockJob
	mockedGoalStateDriver *goalstatemocks.MockDriver
	mockedJobStore        *storemocks.MockJobStore
	mockedTaskStore       *storemocks.MockTaskStore
	mockedSecretStore     *storemocks.MockSecretStore
}

// helper to initialize mocks in JobHandlerTestSuite
func (suite *JobHandlerTestSuite) initializeMocks() {
	// Initialize mocks for the test
	suite.ctrl = gomock.NewController(suite.T())
	suite.mockedJobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.mockedJobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.mockedCachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.mockedGoalStateDriver = goalstatemocks.NewMockDriver(suite.ctrl)
	suite.mockedRespoolClient = respoolmocks.NewMockResourceManagerYARPCClient(suite.ctrl)
	suite.mockedResmgrClient = resmocks.NewMockResourceManagerServiceYARPCClient(suite.ctrl)
	suite.mockedCandidate = leadermocks.NewMockCandidate(suite.ctrl)
	suite.mockedSecretStore = storemocks.NewMockSecretStore(suite.ctrl)
	suite.mockedTaskStore = storemocks.NewMockTaskStore(suite.ctrl)

	suite.handler.jobStore = suite.mockedJobStore
	suite.handler.secretStore = suite.mockedSecretStore
	suite.handler.taskStore = suite.mockedTaskStore
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
	suite.mockedJobStore.EXPECT().
		UpdateJobRuntime(context.Background(), jobID, gomock.Any()).
		Return(nil).AnyTimes()
	suite.mockedJobStore.EXPECT().
		UpdateJobConfig(context.Background(), jobID, gomock.Any()).
		Return(nil).AnyTimes()
	suite.mockedTaskStore.EXPECT().
		CreateTaskConfigs(context.Background(), gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()
	suite.mockedCachedJob.EXPECT().
		CreateTasks(gomock.Any(), gomock.Any(), "peloton").
		Return(nil).AnyTimes()
	suite.mockedCachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH).AnyTimes()
	suite.mockedJobFactory.EXPECT().AddJob(jobID).
		Return(suite.mockedCachedJob).AnyTimes()
	suite.mockedCachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Return(nil).AnyTimes()
	suite.mockedGoalStateDriver.EXPECT().JobRuntimeDuration(job.JobType_BATCH).
		Return(1 * time.Second).AnyTimes()
	suite.mockedGoalStateDriver.EXPECT().EnqueueJob(jobID, gomock.Any()).AnyTimes()
	suite.mockedGoalStateDriver.EXPECT().EnqueueTask(
		jobID, gomock.Any(), gomock.Any()).AnyTimes()
	suite.mockedTaskStore.EXPECT().
		GetTaskStateSummaryForJob(context.Background(), gomock.Any()).
		Return(map[string]uint32{}, nil).AnyTimes()
}

func (suite *JobHandlerTestSuite) SetupTest() {
	mtx := NewMetrics(tally.NoopScope)
	suite.handler = &serviceHandler{
		metrics:   mtx,
		rootCtx:   context.Background(),
		jobSvcCfg: Config{MaxTasksPerJob: _defaultMaxTasksPerJob},
	}
	suite.testJobID = &peloton.JobID{
		Value: "test_job",
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

// TestCreateJobs tests different success/failure scenarios
// for Job Create API
func (suite *JobHandlerTestSuite) TestCreateJobs() {
	// setup job config
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
	respoolID := &peloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobConfig.RespoolID = respoolID

	// setup mocks
	suite.setupMocks(jobID, respoolID)

	// setup specific mocks throughout the test
	suite.mockedCachedJob.EXPECT().Create(gomock.Any(), jobConfig, "peloton").
		Return(nil)

	req := &job.CreateRequest{
		Id:     jobID,
		Config: jobConfig,
	}
	// Job Create API for this request should pass
	resp, err := suite.handler.Create(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(jobID, resp.GetJobId())

	// test if handler generates UUID for empty job id
	suite.mockedJobFactory.EXPECT().AddJob(gomock.Any()).
		Return(suite.mockedCachedJob)
	suite.mockedCachedJob.EXPECT().Create(gomock.Any(), jobConfig, "peloton").
		Return(nil)
	suite.mockedGoalStateDriver.EXPECT().EnqueueJob(gomock.Any(), gomock.Any())
	resp, err = suite.handler.Create(suite.context, &job.CreateRequest{
		Id:     nil,
		Config: jobConfig,
	})
	suite.NoError(err)
	suite.NotEqual(resp.GetJobId().GetValue(), "")
	suite.NotNil(uuid.Parse(resp.GetJobId().GetValue()))

	// Negative tests begin

	// simulate cachedjob create failure
	suite.mockedCachedJob.EXPECT().Create(gomock.Any(), jobConfig, "peloton").
		Return(errors.New("random error"))
	suite.mockedJobFactory.EXPECT().ClearJob(jobID)
	suite.mockedJobStore.EXPECT().DeleteJob(gomock.Any(), jobID)

	resp, err = suite.handler.Create(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	expectedErr := &job.CreateResponse_Error{
		AlreadyExists: &job.JobAlreadyExists{
			Id:      jobID,
			Message: "random error",
		},
	}
	suite.Equal(expectedErr, resp.GetError())

	// Job Create with respool set to nil should fail
	jobConfig.RespoolID = nil
	resp, err = suite.handler.Create(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	expectedErr = &job.CreateResponse_Error{
		InvalidConfig: &job.InvalidJobConfig{
			Id:      jobID,
			Message: errNullResourcePoolID.Error(),
		},
	}
	suite.Equal(expectedErr, resp.GetError())

	// Job Create with job_id set to random string should fail
	badJobID := &peloton.JobID{
		Value: "bad-job-id-str",
	}
	resp, err = suite.handler.Create(suite.context, &job.CreateRequest{
		Id:     badJobID,
		Config: jobConfig,
	})
	suite.NoError(err)
	suite.NotNil(resp)
	expectedErr = &job.CreateResponse_Error{
		InvalidJobId: &job.InvalidJobId{
			Id:      badJobID,
			Message: "JobID must be valid UUID",
		},
	}
	suite.Equal(expectedErr, resp.GetError())

	// simulate a simple error validating task config just to test error
	// processing code path in Create()
	suite.handler.jobSvcCfg.MaxTasksPerJob = 1
	jobConfig.InstanceCount = 2
	jobConfig.RespoolID = respoolID
	resp, err = suite.handler.Create(suite.context, &job.CreateRequest{
		Id:     jobID,
		Config: jobConfig,
	})
	suite.NoError(err)
	suite.NotNil(resp)
	expectedErr = &job.CreateResponse_Error{
		InvalidConfig: &job.InvalidJobConfig{
			Id: jobID,
			Message: "Requested tasks: 2 for job is " +
				"greater than supported: 1 tasks/job",
		},
	}
	suite.Equal(expectedErr, resp.GetError())

	// Job Create with respool set to root should fail
	jobConfig.RespoolID = &peloton.ResourcePoolID{
		Value: common.RootResPoolID,
	}
	resp, err = suite.handler.Create(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	expectedErr = &job.CreateResponse_Error{
		InvalidConfig: &job.InvalidJobConfig{
			Id:      jobID,
			Message: errRootResourcePoolID.Error(),
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
	suite.mockedSecretStore.EXPECT().CreateSecret(
		gomock.Any(), gomock.Any(), jobID).Return(nil)
	suite.mockedCachedJob.EXPECT().Create(
		gomock.Any(), jobConfig, "peloton").Return(nil)

	secret := &peloton.Secret{
		Path: testSecretPath,
		Value: &peloton.Secret_Value{
			Data: []byte(base64.StdEncoding.EncodeToString(
				[]byte(testSecretStr))),
		},
	}

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
	suite.mockedSecretStore.EXPECT().CreateSecret(
		gomock.Any(), gomock.Any(), jobID).
		Return(errors.New("DB error"))
	resp, err = suite.handler.Create(suite.context, req)
	suite.Error(err)

	// Create job where one instance is using docker containerizer.
	jobConfig.InstanceConfig[10].Container =
		&mesos.ContainerInfo{Type: &dockerContainerizer}
	req = &job.CreateRequest{
		Id:      jobID,
		Config:  jobConfig,
		Secrets: []*peloton.Secret{secret},
	}
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
	gangs := util.ConvertToResMgrGangs(tasksInfo, suite.testJobConfig.GetSLA())
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
		suite.testJobConfig.SLA,
		suite.testJobConfig.RespoolID,
		suite.mockedResmgrClient)
	suite.Equal(gangs, expectedGangs)
}

func (suite *JobHandlerTestSuite) TestSubmitTasksToResmgrError() {
	var tasksInfo []*task.TaskInfo
	for _, v := range suite.taskInfos {
		tasksInfo = append(tasksInfo, v)
	}
	gangs := util.ConvertToResMgrGangs(tasksInfo, suite.testJobConfig.GetSLA())
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
	err := jobmgrtask.EnqueueGangs(
		suite.handler.rootCtx,
		tasksInfo,
		suite.testJobConfig.SLA,
		suite.testJobConfig.RespoolID,
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

		errResponse := suite.handler.validateResourcePool(respoolID)
		suite.Error(errResponse)
		suite.Equal(t.errMsg, errResponse.Error())
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
	suite.mockedJobStore.EXPECT().
		GetJobConfig(context.Background(), jobID).Return(oldJobConfig, nil).
		AnyTimes()
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
	suite.NoError(err)
	suite.NotNil(resp)
	expectedErr := &job.UpdateResponse_Error{
		InvalidJobId: &job.InvalidJobId{
			Id:      jobID,
			Message: "Job is in a terminal state:FAILED",
		},
	}
	suite.Equal(expectedErr, resp.GetError())

	// simulate GetJobConfig failure
	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{State: job.JobState_RUNNING}, nil).AnyTimes()
	suite.mockedJobStore.EXPECT().
		GetJobConfig(gomock.Any(), jobID).
		Return(nil, errors.New("DB error"))
	resp, err = suite.handler.Update(suite.context, req)
	suite.Error(err)
	suite.Nil(resp)
	suite.Equal(err.Error(), "DB error")

	// simulate a simple failure in ValidateUpdatedConfig
	suite.mockedJobStore.EXPECT().
		GetJobConfig(gomock.Any(), jobID).
		Return(&job.JobConfig{
			OwningTeam: "team-original",
			RespoolID:  respoolID,
		}, nil)
	resp, err = suite.handler.Update(suite.context, &job.UpdateRequest{
		Id: jobID,
		Config: &job.JobConfig{
			OwningTeam: "team-change",
		},
	})
	suite.NoError(err)
	suite.NotNil(resp)

	expectedErr = &job.UpdateResponse_Error{
		InvalidConfig: &job.InvalidJobConfig{
			Id: jobID,
			Message: `1 error occurred:

* updating OwningTeam not supported`,
		},
	}
	suite.Equal(expectedErr, resp.GetError())

	// simulate cachedJob Update failure
	suite.mockedJobStore.EXPECT().
		GetJobConfig(gomock.Any(), jobID).
		Return(&job.JobConfig{
			RespoolID:     respoolID,
			DefaultConfig: defaultConfig,
		}, nil)
	suite.mockedCachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Return(errors.New("random error"))
	resp, err = suite.handler.Update(suite.context, &job.UpdateRequest{
		Id: jobID,
		Config: &job.JobConfig{
			InstanceCount: 1,
			DefaultConfig: defaultConfig,
		},
	})
	suite.NoError(err)
	suite.NotNil(resp)
	expectedErr = &job.UpdateResponse_Error{
		InvalidConfig: &job.InvalidJobConfig{
			Id:      jobID,
			Message: "random error",
		},
	}
	suite.Equal(expectedErr, resp.GetError())

	// simulate failure for CreateTaskConfigs
	suite.mockedJobStore.EXPECT().
		GetJobConfig(gomock.Any(), jobID).
		Return(&job.JobConfig{
			RespoolID:     respoolID,
			DefaultConfig: defaultConfig,
		}, nil)
	suite.mockedCachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Return(nil)
	suite.mockedTaskStore.EXPECT().
		CreateTaskConfigs(context.Background(), gomock.Any(), gomock.Any()).
		Return(errors.New("random error")).AnyTimes()

	resp, err = suite.handler.Update(suite.context, &job.UpdateRequest{
		Id: jobID,
		Config: &job.JobConfig{
			InstanceCount: 1,
			DefaultConfig: defaultConfig,
		},
	})
	suite.Error(err)
	suite.Nil(resp)
	suite.Equal(err.Error(), "random error")
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
	suite.mockedJobStore.EXPECT().
		GetJobConfig(context.Background(), jobID).Return(jobConfig, nil)

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
				Volumes: []*mesos.Volume{jobmgrtask.CreateSecretVolume(
					testSecretPath, secretID.GetValue())},
			},
		},
	}
	suite.mockedJobStore.EXPECT().
		GetJobConfig(context.Background(), jobID).Return(jobConfig, nil)

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
	suite.mockedJobStore.EXPECT().
		GetJobConfig(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("DB error"))
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
	suite.mockedJobStore.EXPECT().
		GetJobConfig(gomock.Any(), gomock.Any()).
		Return(&job.JobConfig{}, nil)
	suite.mockedJobFactory.EXPECT().AddJob(jobID).
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
	suite.mockedJobStore.EXPECT().
		GetJobConfig(context.Background(), jobID).Return(oldJobConfig, nil)
	resp, err := suite.handler.Update(suite.context, req)
	suite.NoError(err)
	suite.Nil(resp)

	// oldConfig does not contain existing secret volumes, request has one
	// secret, new secret should be created and config should be populated with
	// this new secret volume even if instance count remains unchanged.
	req.Secrets = []*peloton.Secret{secret}
	suite.mockedSecretStore.EXPECT().CreateSecret(
		gomock.Any(), secret, jobID).Return(nil)
	suite.mockedJobStore.EXPECT().
		GetJobConfig(context.Background(), jobID).Return(oldJobConfig, nil)
	resp, err = suite.handler.Update(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(jobID, resp.GetId())
	suite.Equal("added 0 instances", resp.GetMessage())
	// Check if new secret volume is present in config and matches the secret
	// supplied in the request
	secretVolumes := jobmgrtask.RemoveSecretVolumesFromConfig(newJobConfig)
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
		jobmgrtask.CreateSecretVolume(testSecretPath, secretID.GetValue())}
	suite.mockedJobStore.EXPECT().
		GetJobConfig(context.Background(), jobID).Return(oldJobConfig, nil)
	// request contains secret with same path, different data and empty ID
	req.Secrets = []*peloton.Secret{jobmgrtask.CreateSecretProto("",
		testSecretPath, []byte(testSecretStrUpdated))}
	// Even if ID is empty, the existing secret volume will contain same path as
	// supplied secret. So we will update the existing secret with new data.
	// This should result in once call to update updatedSecret in DB
	suite.mockedSecretStore.EXPECT().UpdateSecret(
		gomock.Any(), updatedSecret).Return(nil)
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
		jobmgrtask.CreateSecretVolume(testSecretPath, secretID.GetValue())}
	suite.mockedJobStore.EXPECT().
		GetJobConfig(context.Background(), jobID).Return(oldJobConfig, nil)
	// request contains one updated and one added secret
	req.Secrets = []*peloton.Secret{addedSecret, updatedSecret}
	// This should result in once call to create addedSecret in DB and one
	// call to update updatedSecret in DB
	suite.mockedSecretStore.EXPECT().CreateSecret(
		gomock.Any(), addedSecret, jobID).Return(nil)
	suite.mockedSecretStore.EXPECT().UpdateSecret(
		gomock.Any(), updatedSecret).Return(nil)
	resp, err = suite.handler.Update(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(jobID, resp.GetId())
	suite.Equal("added 0 instances", resp.GetMessage())
	// Check if new secret volume is present in config and matches the secret
	// supplied in the request
	secretVolumes = jobmgrtask.RemoveSecretVolumesFromConfig(newJobConfig)
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
	suite.mockedJobStore.EXPECT().
		GetJobConfig(context.Background(), jobID).Return(&job.JobConfig{
		DefaultConfig: &task.TaskConfig{
			Command:   &mesos.CommandInfo{Value: &testCmd},
			Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
		}}, nil)
	suite.mockedSecretStore.EXPECT().CreateSecret(
		gomock.Any(), gomock.Any(), jobID).Return(nil)
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
				Volumes: []*mesos.Volume{jobmgrtask.CreateSecretVolume(
					testSecretPath, secretID.GetValue())},
			},
		},
	}
	suite.mockedJobStore.EXPECT().
		GetJobConfig(context.Background(), jobID).Return(oldJobConfig, nil)
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
	suite.mockedJobStore.EXPECT().
		GetJobConfig(context.Background(), jobID).Return(oldJobConfig, nil)
	_, err = suite.handler.Update(suite.context, req)
	suite.Error(err)
	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.Equal(yarpcerrors.ErrorMessage(err), "secret does not have a path")

	// Two secrets present in oldConfig, request has only one. This will fail.
	// Request should contain existing two and/or additional secrets
	oldJobConfig.GetDefaultConfig().GetContainer().Volumes = []*mesos.Volume{
		// insert non-secret volumes, will be ignored
		getVolume(),
		jobmgrtask.CreateSecretVolume(testSecretPath, addedSecretID.GetValue()),
		// insert non-secret volumes, will be ignored
		getVolume(),
		jobmgrtask.CreateSecretVolume(testSecretPath, secretID.GetValue()),
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
	suite.mockedJobStore.EXPECT().
		GetJobConfig(context.Background(), jobID).Return(oldJobConfig, nil)
	// secret contains one updated and one added secret
	req.Secrets = []*peloton.Secret{addedSecret}
	resp, err = suite.handler.Update(suite.context, req)
	suite.Error(err)
	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.Equal(yarpcerrors.ErrorMessage(err),
		"number of secrets in request should be >= existing secrets")

	// request contains secret that doesn't match existing secret
	oldJobConfig.GetDefaultConfig().GetContainer().Volumes = []*mesos.Volume{
		jobmgrtask.CreateSecretVolume(testSecretPathNew, addedSecretID.GetValue()),
	}
	req.Config = &job.JobConfig{
		DefaultConfig: &task.TaskConfig{
			Command:   &mesos.CommandInfo{Value: &testCmd},
			Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
		},
	}
	suite.mockedJobStore.EXPECT().
		GetJobConfig(context.Background(), jobID).Return(oldJobConfig, nil)
	req.Secrets = []*peloton.Secret{secret}
	resp, err = suite.handler.Update(suite.context, req)
	suite.Error(err)
	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.Equal(yarpcerrors.ErrorMessage(err),
		fmt.Sprintf("request missing secret with id %v path %v",
			addedSecretID.GetValue(), testSecretPathNew))

	// test UpdateSecret failure
	oldJobConfig.GetDefaultConfig().GetContainer().Volumes = []*mesos.Volume{
		jobmgrtask.CreateSecretVolume(testSecretPath, addedSecretID.GetValue()),
	}
	req.Config = &job.JobConfig{
		DefaultConfig: &task.TaskConfig{
			Command:   &mesos.CommandInfo{Value: &testCmd},
			Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
		},
	}
	suite.mockedJobStore.EXPECT().
		GetJobConfig(context.Background(), jobID).Return(oldJobConfig, nil)
	req.Secrets = []*peloton.Secret{addedSecret}
	// Simulate DB failure in UpdateSecret
	suite.mockedSecretStore.EXPECT().UpdateSecret(
		gomock.Any(), addedSecret).
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
	suite.mockedJobStore.EXPECT().
		GetJobConfig(context.Background(), jobID).Return(oldJobConfig, nil)
	req.Secrets = []*peloton.Secret{addedSecret}
	// Simulate DB failure in CreateSecret
	suite.mockedSecretStore.EXPECT().CreateSecret(
		gomock.Any(), addedSecret, jobID).
		Return(errors.New("DB error"))

	resp, err = suite.handler.Update(suite.context, req)
	suite.Error(err)
	suite.Equal(yarpcerrors.ErrorMessage(err), "DB error")

	// Cluster does not support secrets but update request has secrets,
	// this will result in error
	suite.handler.jobSvcCfg.EnableSecrets = false
	suite.mockedJobStore.EXPECT().
		GetJobConfig(context.Background(), jobID).Return(oldJobConfig, nil)
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

	suite.mockedJobFactory.EXPECT().AddJob(id).
		Return(suite.mockedCachedJob)

	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{State: job.JobState_SUCCEEDED}, nil)

	suite.mockedJobStore.EXPECT().DeleteJob(context.Background(), id).Return(nil)

	res, err := suite.handler.Delete(suite.context, &job.DeleteRequest{Id: id})
	suite.Equal(&job.DeleteResponse{}, res)
	suite.NoError(err)

	suite.mockedJobFactory.EXPECT().AddJob(id).
		Return(suite.mockedCachedJob)

	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{State: job.JobState_PENDING}, nil)

	res, err = suite.handler.Delete(suite.context, &job.DeleteRequest{Id: id})
	suite.Nil(res)
	suite.Error(err)
	expectedErr := yarpcerrors.InternalErrorf("Job is not in a terminal state: PENDING")
	suite.Equal(expectedErr, err)

	suite.mockedJobFactory.EXPECT().AddJob(id).
		Return(suite.mockedCachedJob)

	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(nil, fmt.Errorf("fake db error"))

	res, err = suite.handler.Delete(suite.context, &job.DeleteRequest{Id: id})
	suite.Nil(res)
	suite.Error(err)
	expectedErr = yarpcerrors.NotFoundErrorf("job not found")
	suite.Equal(expectedErr, err)

	suite.mockedJobFactory.EXPECT().AddJob(id).
		Return(suite.mockedCachedJob)

	suite.mockedCachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(&job.RuntimeInfo{State: job.JobState_SUCCEEDED}, nil)
	suite.mockedJobStore.EXPECT().DeleteJob(context.Background(), id).
		Return(yarpcerrors.InternalErrorf("fake db error"))
	res, err = suite.handler.Delete(suite.context, &job.DeleteRequest{Id: id})
	suite.Nil(res)
	suite.Error(err)
	expectedErr = yarpcerrors.InternalErrorf("fake db error")
	suite.Equal(expectedErr, err)
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
	suite.mockedJobStore.EXPECT().GetJobConfig(context.Background(), id).
		Return(jobConfig, nil)
	suite.mockedJobStore.EXPECT().GetJobRuntime(context.Background(), id).
		Return(jobRuntime, nil)
	suite.mockedJobFactory.EXPECT().AddJob(id).Return(suite.mockedCachedJob)
	suite.mockedCachedJob.EXPECT().Update(gomock.Any(), jobInfo, cached.UpdateCacheOnly).Return(nil)
	suite.mockedGoalStateDriver.EXPECT().EnqueueJob(id, gomock.Any())
	_, err := suite.handler.Refresh(suite.context, &job.RefreshRequest{Id: id})
	suite.NoError(err)

	suite.mockedCandidate.EXPECT().IsLeader().Return(true)
	suite.mockedJobStore.EXPECT().GetJobConfig(context.Background(), id).
		Return(nil, fmt.Errorf("fake db error"))
	_, err = suite.handler.Refresh(suite.context, &job.RefreshRequest{Id: id})
	suite.Error(err)

	suite.mockedCandidate.EXPECT().IsLeader().Return(true)
	suite.mockedJobStore.EXPECT().GetJobConfig(context.Background(), id).
		Return(jobConfig, nil)
	suite.mockedJobStore.EXPECT().GetJobRuntime(context.Background(), id).
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
