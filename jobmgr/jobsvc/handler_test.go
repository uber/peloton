package jobsvc

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	respool_mocks "code.uber.internal/infra/peloton/.gen/peloton/api/respool/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"

	jobmgr_job "code.uber.internal/infra/peloton/jobmgr/job"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"
)

const (
	testInstanceCount = 2
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
		metrics: mtx,
		rootCtx: context.Background(),
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

	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYarpcClient(ctrl)
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

	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYarpcClient(ctrl)
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

func (suite *JobHandlerTestSuite) TestValidateResourcePool() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockRespoolClient := respool_mocks.NewMockResourceManagerYarpcClient(ctrl)
	suite.handler.respoolClient = mockRespoolClient
	respoolID := &respool.ResourcePoolID{
		Value: "respool11",
	}
	var request = &respool.GetRequest{
		Id: respoolID,
	}

	gomock.InOrder(
		mockRespoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				gomock.Eq(request)).
			Return(nil, errors.New("Respool Not found")),
	)
	errResponse := suite.handler.validateResourcePool(respoolID)
	suite.Error(errResponse)
}

func (suite *JobHandlerTestSuite) TestJobScaleUp() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	oldInstanceCount := uint32(3)
	newInstanceCount := uint32(4)
	jobID := &peloton.JobID{
		Value: "job0",
	}
	oldJobConfig := job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: oldInstanceCount,
	}
	newJobConfig := job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: newInstanceCount,
	}
	var jobRuntime = job.RuntimeInfo{
		State: job.JobState_PENDING,
	}

	mockJobStore := store_mocks.NewMockJobStore(ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)
	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYarpcClient(ctrl)
	suite.handler.resmgrClient = mockResmgrClient
	suite.handler.jobStore = mockJobStore
	suite.handler.taskStore = mockTaskStore
	updater := jobmgr_job.NewJobRuntimeUpdater(mockJobStore, mockTaskStore, nil, tally.NoopScope)
	updater.Start()
	suite.handler.runtimeUpdater = updater

	mockJobStore.EXPECT().
		GetJobConfig(context.Background(), jobID).
		Return(&oldJobConfig, nil).
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
	mockTaskStore.EXPECT().
		CreateTasks(context.Background(), jobID, gomock.Any(), "peloton").
		Return(nil).
		AnyTimes()
	mockTaskStore.EXPECT().
		GetTasksForJobAndState(context.Background(), jobID, gomock.Any()).
		Return(map[uint32]*task.TaskInfo{}, nil).
		AnyTimes()
	mockResmgrClient.EXPECT().EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil).
		AnyTimes()

	req := &job.UpdateRequest{
		Id:     jobID,
		Config: &newJobConfig,
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

	mockJobStore.EXPECT().QueryJobs(context.Background(), nil, nil)
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
	suite.EqualError(err, "Job is not in a terminal state: PENDING")
}
