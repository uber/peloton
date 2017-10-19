package jobsvc

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	respool_mocks "code.uber.internal/infra/peloton/.gen/peloton/api/respool/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"
	tracked_mocks "code.uber.internal/infra/peloton/jobmgr/tracked/mocks"

	jobmgr_job "code.uber.internal/infra/peloton/jobmgr/job"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"
	"github.com/pborman/uuid"
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
	}
	suite.testJobID = &peloton.JobID{
		Value: "test_job",
	}
	suite.testJobConfig = &job.JobConfig{
		Name:          suite.testJobID.Value,
		InstanceCount: testInstanceCount,
		Sla: &job.SlaConfig{
			Preemptible:               true,
			Priority:                  22,
			MaximumRunningInstances:   2,
			MinimumSchedulingUnitSize: 1,
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

	mtID := util.BuildMesosTaskID(util.BuildTaskID(suite.testJobID, instanceID), uuid.NewUUID().String())

	return &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			MesosTaskId: mtID,
			State:       state,
			GoalState:   task.TaskGoalState_SUCCEED,
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
	gangs := util.ConvertToResMgrGangs(tasksInfo, suite.testJobConfig.GetSla())
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

	jobmgr_task.EnqueueGangs(context.Background(), tasksInfo, suite.testJobConfig.GetRespoolID(), suite.testJobConfig.GetSla(), mockResmgrClient)
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
	gangs := util.ConvertToResMgrGangs(tasksInfo, suite.testJobConfig.GetSla())
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
	err := jobmgr_task.EnqueueGangs(context.Background(), tasksInfo, suite.testJobConfig.GetRespoolID(), suite.testJobConfig.GetSla(), mockResmgrClient)
	suite.Error(err)
}

func (suite *JobHandlerTestSuite) TestValidateResourcePool() {
	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	mockRespoolClient := respool_mocks.NewMockResourceManagerYARPCClient(ctrl)
	suite.handler.respoolClient = mockRespoolClient
	respoolID := &peloton.ResourcePoolID{
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
	errResponse := suite.handler.validateResourcePool(context.Background(), respoolID)
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
	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	mockTrackedManager := tracked_mocks.NewMockManager(ctrl)
	suite.handler.resmgrClient = mockResmgrClient
	suite.handler.jobStore = mockJobStore
	suite.handler.taskStore = mockTaskStore
	suite.handler.trackedManager = mockTrackedManager
	updater := jobmgr_job.NewJobRuntimeUpdater(nil, mockJobStore, mockTaskStore, jobmgr_job.Config{}, tally.NoopScope)
	updater.Start()
	suite.handler.runtimeUpdater = updater

	mockJobStore.EXPECT().
		GetJob(context.Background(), jobID).
		Return(&job.JobInfo{
			Runtime: &jobRuntime,
			Config:  &oldJobConfig,
		}, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		CreateJobConfig(context.Background(), jobID, gomock.Any()).
		Return(nil).
		AnyTimes()
	mockJobStore.EXPECT().
		UpdateJobRuntime(context.Background(), jobID, gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	mockTaskStore.EXPECT().
		CreateTaskConfigs(context.Background(), gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	mockTaskStore.EXPECT().
		CreateTaskRuntime(context.Background(), jobID, uint32(3), gomock.Any(), "peloton").
		Return(nil).
		AnyTimes()
	mockTrackedManager.EXPECT().SetTask(jobID, uint32(3), gomock.Any()).AnyTimes()
	mockTaskStore.EXPECT().
		GetTaskStateSummaryForJob(context.Background(), gomock.Any()).
		Return(map[string]uint32{}, nil).
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
