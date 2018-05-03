package jobsvc

import (
	"context"
	"fmt"
	"testing"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	respool_mocks "code.uber.internal/infra/peloton/.gen/peloton/api/respool/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	goalstatemocks "code.uber.internal/infra/peloton/jobmgr/goalstate/mocks"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"go.uber.org/yarpc/yarpcerrors"
)

const (
	testInstanceCount = 2
)

const (
	_newConfig        = "testdata/new_config.yaml"
	_oldConfig        = "testdata/old_config.yaml"
	_invalidNewConfig = "testdata/invalid_new_config.yaml"
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
}

func (suite *JobHandlerTestSuite) TearDownTest() {
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

func (suite *JobHandlerTestSuite) TestSubmitTasksToResmgr() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	suite.handler.resmgrClient = mockResmgrClient
	var tasksInfo []*task.TaskInfo
	for _, v := range suite.taskInfos {
		tasksInfo = append(tasksInfo, v)
	}
	gangs := util.ConvertToResMgrGangs(tasksInfo, suite.testJobConfig)
	var expectedGangs []*resmgrsvc.Gang
	gomock.InOrder(
		mockResmgrClient.EXPECT().
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

	jobmgr_task.EnqueueGangs(suite.handler.rootCtx, tasksInfo, suite.testJobConfig, mockResmgrClient)
	suite.Equal(gangs, expectedGangs)
}

func (suite *JobHandlerTestSuite) TestSubmitTasksToResmgrError() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	suite.handler.resmgrClient = mockResmgrClient
	var tasksInfo []*task.TaskInfo
	for _, v := range suite.taskInfos {
		tasksInfo = append(tasksInfo, v)
	}
	gangs := util.ConvertToResMgrGangs(tasksInfo, suite.testJobConfig)
	var expectedGangs []*resmgrsvc.Gang
	gomock.InOrder(
		mockResmgrClient.EXPECT().
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
	err := jobmgr_task.EnqueueGangs(suite.handler.rootCtx, tasksInfo, suite.testJobConfig, mockResmgrClient)
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

	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	for _, t := range tt {
		mockRespoolClient := respool_mocks.NewMockResourceManagerYARPCClient(ctrl)
		suite.handler.respoolClient = mockRespoolClient

		respoolID := &peloton.ResourcePoolID{
			Value: t.respoolID,
		}
		var request = &respool.GetRequest{
			Id: respoolID,
		}

		mockRespoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				gomock.Eq(request)).
			Return(t.getRespoolResponse, t.getRespoolError).AnyTimes()

		errResponse := suite.handler.validateResourcePool(respoolID)
		suite.Error(errResponse)
		suite.Equal(t.errMsg, errResponse.Error())
	}
}

func (suite *JobHandlerTestSuite) TestJobScaleUp() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

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

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	goalStateDriver := goalstatemocks.NewMockDriver(ctrl)

	suite.handler.resmgrClient = mockResmgrClient
	suite.handler.jobStore = mockJobStore
	suite.handler.taskStore = mockTaskStore
	suite.handler.jobFactory = jobFactory
	suite.handler.goalStateDriver = goalStateDriver

	mockJobStore.EXPECT().
		GetJobConfig(context.Background(), jobID).
		Return(oldJobConfig, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		GetJobRuntime(context.Background(), jobID).
		Return(&jobRuntime, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		UpdateJobRuntime(context.Background(), jobID, gomock.Any()).
		Return(nil).
		AnyTimes()
	mockJobStore.EXPECT().
		UpdateJobConfig(context.Background(), jobID, gomock.Any()).
		Return(nil).
		AnyTimes()
	jobFactory.EXPECT().AddJob(jobID).Return(cachedJob)
	cachedJob.EXPECT().Update(gomock.Any(), gomock.Any(), cached.UpdateCacheOnly).Return(nil)
	mockTaskStore.EXPECT().
		CreateTaskConfigs(context.Background(), gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	mockTaskStore.EXPECT().
		CreateTaskRuntime(context.Background(), jobID, uint32(3), gomock.Any(), "peloton").
		Return(nil).
		AnyTimes()
	cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheOnly).Return(nil).AnyTimes()
	goalStateDriver.EXPECT().EnqueueTask(jobID, gomock.Any(), gomock.Any()).AnyTimes()
	mockTaskStore.EXPECT().
		GetTaskStateSummaryForJob(context.Background(), gomock.Any()).
		Return(map[string]uint32{}, nil).
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

func (suite *JobHandlerTestSuite) TestJobQuery() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	// TODO: add more inputs
	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore

	mockJobStore.EXPECT().QueryJobs(context.Background(), nil, nil, false)
	req := &job.QueryRequest{}
	resp, err := suite.handler.Query(suite.context, req)
	suite.NoError(err)
	suite.NotNil(resp)
}

func (suite *JobHandlerTestSuite) TestJobDelete() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	id := &peloton.JobID{
		Value: "my-job",
	}

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore

	mockJobStore.EXPECT().GetJobRuntime(context.Background(), id).
		Return(&job.RuntimeInfo{State: job.JobState_SUCCEEDED}, nil)
	mockJobStore.EXPECT().DeleteJob(context.Background(), id).Return(nil)

	res, err := suite.handler.Delete(suite.context, &job.DeleteRequest{Id: id})
	suite.Equal(&job.DeleteResponse{}, res)
	suite.NoError(err)

	mockJobStore.EXPECT().GetJobRuntime(context.Background(), id).
		Return(&job.RuntimeInfo{State: job.JobState_PENDING}, nil)

	res, err = suite.handler.Delete(suite.context, &job.DeleteRequest{Id: id})
	suite.Nil(res)
	suite.Error(err)
	expectedErr := yarpcerrors.InternalErrorf("Job is not in a terminal state: PENDING")
	suite.Equal(expectedErr, err)

	mockJobStore.EXPECT().GetJobRuntime(context.Background(), id).
		Return(nil, fmt.Errorf("fake db error"))
	res, err = suite.handler.Delete(suite.context, &job.DeleteRequest{Id: id})
	suite.Nil(res)
	suite.Error(err)
	expectedErr = yarpcerrors.NotFoundErrorf("job not found")
	suite.Equal(expectedErr, err)

	mockJobStore.EXPECT().GetJobRuntime(context.Background(), id).
		Return(&job.RuntimeInfo{State: job.JobState_SUCCEEDED}, nil)
	mockJobStore.EXPECT().DeleteJob(context.Background(), id).
		Return(yarpcerrors.InternalErrorf("fake db error"))
	res, err = suite.handler.Delete(suite.context, &job.DeleteRequest{Id: id})
	suite.Nil(res)
	suite.Error(err)
	expectedErr = yarpcerrors.InternalErrorf("fake db error")
	suite.Equal(expectedErr, err)
}

func (suite *JobHandlerTestSuite) TestJobRefresh() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

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
		Id:      id,
		Config:  jobConfig,
		Runtime: jobRuntime,
	}

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	suite.handler.jobStore = mockJobStore
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	goalStateDriver := goalstatemocks.NewMockDriver(ctrl)
	suite.handler.jobFactory = jobFactory
	suite.handler.goalStateDriver = goalStateDriver

	mockJobStore.EXPECT().GetJobConfig(context.Background(), id).
		Return(jobConfig, nil)
	mockJobStore.EXPECT().GetJobRuntime(context.Background(), id).
		Return(jobRuntime, nil)
	jobFactory.EXPECT().AddJob(id).Return(cachedJob)
	cachedJob.EXPECT().Update(gomock.Any(), jobInfo, cached.UpdateCacheOnly).Return(nil)
	goalStateDriver.EXPECT().EnqueueJob(id, gomock.Any())
	_, err := suite.handler.Refresh(suite.context, &job.RefreshRequest{Id: id})
	suite.NoError(err)

	mockJobStore.EXPECT().GetJobConfig(context.Background(), id).
		Return(nil, fmt.Errorf("fake db error"))
	_, err = suite.handler.Refresh(suite.context, &job.RefreshRequest{Id: id})
	suite.Error(err)

	mockJobStore.EXPECT().GetJobConfig(context.Background(), id).
		Return(jobConfig, nil)
	mockJobStore.EXPECT().GetJobRuntime(context.Background(), id).
		Return(nil, fmt.Errorf("fake db error"))
	_, err = suite.handler.Refresh(suite.context, &job.RefreshRequest{Id: id})
	suite.Error(err)
}
