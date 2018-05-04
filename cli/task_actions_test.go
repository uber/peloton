package cli

import (
	"context"
	"testing"

	task_mocks "code.uber.internal/infra/peloton/.gen/peloton/api/task/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"

	"code.uber.internal/infra/peloton/.gen/peloton/api/errors"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/query"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

const (
	taskStartTime      = "2017-01-02T15:04:05.456789016Z"
	taskCompletionTime = "2017-01-03T18:04:05.987654447Z"
)

type taskActionsTestSuite struct {
	suite.Suite
	mockCtrl *gomock.Controller
	mockTask *task_mocks.MockTaskManagerYARPCClient
	ctx      context.Context
}

func TestTaskActions(t *testing.T) {
	suite.Run(t, new(taskActionsTestSuite))
}

func (suite *taskActionsTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockTask = task_mocks.NewMockTaskManagerYARPCClient(suite.mockCtrl)
	suite.ctx = context.Background()
}

func (suite *taskActionsTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
	suite.ctx.Done()
}

func (suite *taskActionsTestSuite) getListResult(
	jobID *peloton.JobID) *task.ListResponse_Result {

	result := task.ListResponse_Result{
		Value: map[uint32]*task.TaskInfo{
			0: {
				InstanceId: 0,
				JobId:      jobID,
				Config: &task.TaskConfig{
					Name: "Instance_0",
				},
				Runtime: &task.RuntimeInfo{
					StartTime:      "",
					CompletionTime: "",
					State:          task.TaskState_PENDING,
					Host:           "",
					Message:        "",
					Reason:         "",
				},
			},
			1: {
				InstanceId: 1,
				JobId:      jobID,
				Config: &task.TaskConfig{
					Name: "Instance_1",
				},
				Runtime: &task.RuntimeInfo{
					StartTime:      taskStartTime,
					CompletionTime: "",
					State:          task.TaskState_RUNNING,
					Host:           "mesos-slave-01",
					Message:        "",
					Reason:         "",
				},
			},
			2: {
				InstanceId: 2,
				JobId:      jobID,
				Config: &task.TaskConfig{
					Name: "Instance_2",
				},
				Runtime: &task.RuntimeInfo{
					StartTime:      taskStartTime,
					CompletionTime: taskCompletionTime,
					State:          task.TaskState_SUCCEEDED,
					Host:           "mesos-slave-02",
					Message:        "Container Exit 0",
					Reason:         "REASON",
				},
			},
		},
	}
	return &result
}

func (suite *taskActionsTestSuite) TestClient_TaskListAction() {
	c := Client{
		Debug:      false,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: uuid.New(),
	}

	tt := []struct {
		taskListRequest  *task.ListRequest
		taskListResponse *task.ListResponse
		listError        error
	}{
		{
			taskListRequest: &task.ListRequest{
				JobId: jobID,
				Range: nil,
			},
			taskListResponse: &task.ListResponse{
				Result: suite.getListResult(jobID),
			},
			listError: nil,
		},
		{
			taskListRequest: &task.ListRequest{
				JobId: jobID,
				Range: nil,
			},
			taskListResponse: &task.ListResponse{
				Result: nil,
				NotFound: &errors.JobNotFound{
					Id:      jobID,
					Message: "Job not found",
				},
			},
			listError: nil,
		},
	}
	for _, t := range tt {
		suite.withMockTaskListResponse(
			t.taskListRequest,
			t.taskListResponse,
			t.listError,
		)
		err := c.TaskListAction(jobID.Value, nil)
		if t.listError != nil {
			suite.EqualError(err, t.listError.Error())
		} else {
			suite.NoError(err)
		}
	}
}

func (suite *taskActionsTestSuite) withMockTaskListResponse(
	req *task.ListRequest,
	resp *task.ListResponse,
	err error) {

	suite.mockTask.EXPECT().List(suite.ctx, gomock.Eq(req)).
		Return(resp, err)
}

func (suite *taskActionsTestSuite) getQueryResult(
	jobID *peloton.JobID, states []task.TaskState) []*task.TaskInfo {

	result := []*task.TaskInfo{
		{
			InstanceId: 0,
			JobId:      jobID,
			Config: &task.TaskConfig{
				Name: "Instance_0",
			},
			Runtime: &task.RuntimeInfo{
				StartTime:      taskStartTime,
				CompletionTime: "",
				State:          task.TaskState_RUNNING,
				Host:           "mesos-slave-01",
				Message:        "",
				Reason:         "",
			},
		},
		{
			InstanceId: 1,
			JobId:      jobID,
			Config: &task.TaskConfig{
				Name: "Instance_1",
			},
			Runtime: &task.RuntimeInfo{
				StartTime:      taskStartTime,
				CompletionTime: "",
				State:          task.TaskState_RUNNING,
				Host:           "mesos-slave-01",
				Message:        "",
				Reason:         "",
			},
		},
		{
			InstanceId: 2,
			JobId:      jobID,
			Config: &task.TaskConfig{
				Name: "Instance_2",
			},
			Runtime: &task.RuntimeInfo{
				StartTime:      taskStartTime,
				CompletionTime: "",
				State:          task.TaskState_RUNNING,
				Host:           "mesos-slave-01",
				Message:        "",
				Reason:         "",
			},
		},
	}
	return result
}

func (suite *taskActionsTestSuite) TestClient_TaskQueryAction() {
	c := Client{
		Debug:      false,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: uuid.New(),
	}
	tests := []struct {
		taskQueryRequest  *task.QueryRequest
		taskQueryResponse *task.QueryResponse
		queryError        error
	}{
		{
			taskQueryRequest: &task.QueryRequest{
				JobId: jobID,
				Spec: &task.QuerySpec{
					Pagination: &query.PaginationSpec{
						Limit:  10,
						Offset: 0,
						OrderBy: []*query.OrderBy{
							{
								Order: query.OrderBy_DESC,
								Property: &query.PropertyPath{
									Value: "state",
								},
							},
						},
					},
					TaskStates: []task.TaskState{
						task.TaskState_RUNNING,
					},
					Hosts: []string{
						"taskHost",
					},
				},
			},
			taskQueryResponse: &task.QueryResponse{
				Records: suite.getQueryResult(jobID, []task.TaskState{task.TaskState_RUNNING}),
			},
			queryError: nil,
		},
		{
			taskQueryRequest: &task.QueryRequest{
				JobId: jobID,
				Spec: &task.QuerySpec{
					Pagination: &query.PaginationSpec{
						Limit:  10,
						Offset: 0,
						OrderBy: []*query.OrderBy{
							{
								Order: query.OrderBy_DESC,
								Property: &query.PropertyPath{
									Value: "state",
								},
							},
						},
					},
					TaskStates: []task.TaskState{
						task.TaskState_RUNNING,
					},
					Hosts: []string{
						"taskHost",
					},
				},
			},
			taskQueryResponse: &task.QueryResponse{
				Records: nil,
				Error: &task.QueryResponse_Error{
					NotFound: &errors.JobNotFound{
						Id:      jobID,
						Message: "Job not found",
					},
				},
			},
			queryError: nil,
		},
	}
	for _, t := range tests {
		suite.withMockTaskQueryResponse(
			t.taskQueryRequest,
			t.taskQueryResponse,
			t.queryError,
		)
		err := c.TaskQueryAction(jobID.Value, "RUNNING", "", "taskHost", 10, 0, "state", "DESC")
		if t.queryError != nil {
			suite.EqualError(err, t.queryError.Error())
		} else {
			suite.NoError(err)
		}
	}
}

func (suite *taskActionsTestSuite) withMockTaskQueryResponse(
	req *task.QueryRequest,
	resp *task.QueryResponse,
	err error) {

	suite.mockTask.EXPECT().Query(suite.ctx, gomock.Eq(req)).
		Return(resp, err)
}
