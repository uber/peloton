package jobsvc

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	respoolmocks "code.uber.internal/infra/peloton/.gen/peloton/api/respool/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
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
	testInstanceCount = 2
	testSecretPath    = "/tmp/secret"
	testSecretStr     = "top-secret-token"
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

	ctrl                 *gomock.Controller
	mockedCandidate      *leadermocks.MockCandidate
	mockedRespoolClient  *respoolmocks.MockResourceManagerYARPCClient
	mockedResmgrClient   *resmocks.MockResourceManagerServiceYARPCClient
	mockedJobFactory     *cachedmocks.MockJobFactory
	mockedCachedJob      *cachedmocks.MockJob
	mockedGoalStateDrive *goalstatemocks.MockDriver
	mockedJobStore       *storemocks.MockJobStore
	mockedTaskStore      *storemocks.MockTaskStore
	mockedSecretStore    *storemocks.MockSecretStore
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
		Sla: &job.SlaConfig{
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

	suite.ctrl = gomock.NewController(suite.T())
	suite.mockedJobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.mockedJobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.mockedCachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.mockedGoalStateDrive = goalstatemocks.NewMockDriver(suite.ctrl)
	suite.mockedRespoolClient = respoolmocks.NewMockResourceManagerYARPCClient(suite.ctrl)
	suite.mockedResmgrClient = resmocks.NewMockResourceManagerServiceYARPCClient(suite.ctrl)
	suite.mockedCandidate = leadermocks.NewMockCandidate(suite.ctrl)
	suite.mockedSecretStore = storemocks.NewMockSecretStore(suite.ctrl)
	suite.mockedTaskStore = storemocks.NewMockTaskStore(suite.ctrl)

	suite.handler.jobStore = suite.mockedJobStore
	suite.handler.secretStore = suite.mockedSecretStore
	suite.handler.taskStore = suite.mockedTaskStore
	suite.handler.jobFactory = suite.mockedJobFactory
	suite.handler.goalStateDriver = suite.mockedGoalStateDrive
	suite.handler.respoolClient = suite.mockedRespoolClient
	suite.handler.resmgrClient = suite.mockedResmgrClient
	suite.handler.candidate = suite.mockedCandidate
}

func (suite *JobHandlerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
	log.Debug("tearing down")
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
	getRespoolResponse := &respool.GetResponse{
		Poolinfo: &respool.ResourcePoolInfo{
			Id: respoolID,
		},
	}

	// setup mocks
	suite.mockedCandidate.EXPECT().IsLeader().Return(true).AnyTimes()
	suite.mockedRespoolClient.EXPECT().
		GetResourcePool(
			gomock.Any(),
			gomock.Any()).
		Return(getRespoolResponse, nil).AnyTimes()
	suite.mockedJobFactory.EXPECT().AddJob(jobID).Return(suite.mockedCachedJob)
	suite.mockedCachedJob.EXPECT().Create(
		gomock.Any(),
		jobConfig,
		"peloton").
		Return(nil)
	suite.mockedGoalStateDrive.EXPECT().
		EnqueueJob(jobID, gomock.Any()).AnyTimes()

	req := &job.CreateRequest{
		Id:     jobID,
		Config: jobConfig,
	}

	// Job Create API for this request should pass
	resp, err := suite.handler.Create(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(jobID, resp.GetJobId())

	// Negative tests begin

	// Job Create with respool set to nil should fail
	jobConfig.RespoolID = nil
	resp, err = suite.handler.Create(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
	expectedErr := &job.CreateResponse_Error{
		InvalidConfig: &job.InvalidJobConfig{
			Id:      jobID,
			Message: errNullResourcePoolID.Error(),
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
	getRespoolResponse := &respool.GetResponse{
		Poolinfo: &respool.ResourcePoolInfo{
			Id: respoolID,
		},
	}

	jobConfig.InstanceConfig = make(map[uint32]*task.TaskConfig)
	for i := uint32(0); i < 25; i++ {
		jobConfig.InstanceConfig[i] = &task.TaskConfig{
			Name:      suite.testJobConfig.Name,
			Resource:  &defaultResourceConfig,
			Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
		}
	}

	suite.handler.jobSvcCfg.EnableSecrets = true

	// setup mocks
	suite.mockedCandidate.EXPECT().IsLeader().Return(true).AnyTimes()

	suite.mockedRespoolClient.EXPECT().
		GetResourcePool(
			gomock.Any(),
			gomock.Any()).
		Return(getRespoolResponse, nil).AnyTimes()

	suite.mockedSecretStore.EXPECT().CreateSecret(
		gomock.Any(),
		gomock.Any(),
		jobID).
		Return(nil)

	suite.mockedCachedJob.EXPECT().Create(
		gomock.Any(),
		jobConfig,
		"peloton").
		Return(nil)

	suite.mockedJobFactory.EXPECT().AddJob(jobID).Return(suite.mockedCachedJob)

	suite.mockedGoalStateDrive.EXPECT().EnqueueJob(jobID, gomock.Any()).AnyTimes()

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
	gangs := util.ConvertToResMgrGangs(tasksInfo, suite.testJobConfig.GetSla())
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
		suite.testJobConfig.Sla,
		suite.testJobConfig.RespoolID,
		suite.mockedResmgrClient)
	suite.Equal(gangs, expectedGangs)
}

func (suite *JobHandlerTestSuite) TestSubmitTasksToResmgrError() {
	var tasksInfo []*task.TaskInfo
	for _, v := range suite.taskInfos {
		tasksInfo = append(tasksInfo, v)
	}
	gangs := util.ConvertToResMgrGangs(tasksInfo, suite.testJobConfig.GetSla())
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
		suite.testJobConfig.Sla,
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
	var jobRuntime = job.RuntimeInfo{
		State: job.JobState_PENDING,
	}

	suite.mockedCandidate.EXPECT().IsLeader().Return(true)
	suite.mockedJobFactory.EXPECT().
		AddJob(jobID).
		Return(suite.mockedCachedJob)
	suite.mockedCachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&jobRuntime, nil)
	suite.mockedJobStore.EXPECT().
		GetJobConfig(gomock.Any(), jobID).
		Return(oldJobConfig, nil)
	suite.mockedCachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Return(nil)
	suite.mockedTaskStore.EXPECT().
		CreateTaskConfigs(context.Background(), gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	suite.mockedCachedJob.EXPECT().
		CreateTasks(gomock.Any(), gomock.Any(), "peloton").Return(nil).AnyTimes()
	suite.mockedGoalStateDrive.EXPECT().EnqueueTask(jobID, gomock.Any(), gomock.Any()).AnyTimes()
	suite.mockedCachedJob.EXPECT().
		GetJobType().Return(job.JobType_BATCH)
	suite.mockedGoalStateDrive.EXPECT().
		JobRuntimeDuration(job.JobType_BATCH).
		Return(1 * time.Second)
	suite.mockedGoalStateDrive.EXPECT().EnqueueJob(jobID, gomock.Any())

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

func (suite *JobHandlerTestSuite) TestJobQuery() {
	// TODO: add more inputs
	suite.mockedJobStore.EXPECT().QueryJobs(context.Background(), nil, nil, false)
	req := &job.QueryRequest{}
	resp, err := suite.handler.Query(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
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
	suite.mockedGoalStateDrive.EXPECT().EnqueueJob(id, gomock.Any())
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
