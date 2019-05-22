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

package cli

import (
	"context"
	"errors"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	taskmocks "github.com/uber/peloton/.gen/peloton/api/v0/task/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	pberr "github.com/uber/peloton/.gen/peloton/api/v0/errors"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/query"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
)

const (
	taskStartTime      = "2017-01-02T15:04:05.456789016Z"
	taskCompletionTime = "2017-01-03T18:04:05.987654447Z"
)

type taskActionsTestSuite struct {
	suite.Suite
	mockCtrl *gomock.Controller
	mockTask *taskmocks.MockTaskManagerYARPCClient
	ctx      context.Context
}

func TestTaskActions(t *testing.T) {
	suite.Run(t, new(taskActionsTestSuite))
}

func (suite *taskActionsTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockTask = taskmocks.NewMockTaskManagerYARPCClient(suite.mockCtrl)
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
					TerminationStatus: &task.TerminationStatus{
						Reason:   task.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_ON_REQUEST,
						ExitCode: 0,
						Signal:   "",
					},
				},
			},
		},
	}
	return &result
}

func (suite *taskActionsTestSuite) TestClientTaskListAction() {
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
		debug            bool
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
			debug: true,
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
				NotFound: &pberr.JobNotFound{
					Id:      jobID,
					Message: "Job not found",
				},
			},
			listError: nil,
		},
		{
			taskListRequest: &task.ListRequest{
				JobId: jobID,
				Range: nil,
			},
			taskListResponse: nil,
			listError:        errors.New("cannot execute task list"),
		},
	}
	for _, t := range tt {
		c.Debug = t.debug
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

func (suite *taskActionsTestSuite) TestClientTaskListPrint() {
	c := Client{
		Debug:      false,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}
	jobID := &peloton.JobID{
		Value: uuid.New(),
	}
	taskListResponse := &task.ListResponse{
		Result: suite.getListResult(jobID),
	}

	assert.NotPanics(suite.T(), func() { printTaskListResponse(taskListResponse, c.Debug) })
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

func (suite *taskActionsTestSuite) TestGetPodEvents() {
	c := Client{
		Debug:      false,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: uuid.New(),
	}
	runID := "taskid"
	prevRunID := "prevtaskid"
	req := &task.GetPodEventsRequest{
		JobId:      jobID,
		InstanceId: 0,
		Limit:      5,
		RunId:      runID,
	}

	suite.mockTask.EXPECT().GetPodEvents(context.Background(), req).
		Return(nil, errors.New("get pod events request failed"))
	err := c.PodGetEventsAction(jobID.GetValue(), 0, runID, 5)
	suite.Error(err)

	podEvent := &task.PodEvent{
		TaskId: &mesos.TaskID{
			Value: &runID,
		},
		PrevTaskId: &mesos.TaskID{
			Value: &prevRunID,
		},
		ActualState: "PENDING",
		GoalState:   "RUNNING",
	}
	var podEvents []*task.PodEvent
	podEvents = append(podEvents, podEvent)
	response := &task.GetPodEventsResponse{
		Result: podEvents,
	}
	suite.mockTask.EXPECT().GetPodEvents(context.Background(), req).
		Return(response, nil)
	err = c.PodGetEventsAction(jobID.GetValue(), 0, runID, 5)
	suite.NoError(err)

	response = &task.GetPodEventsResponse{
		Result: nil,
		Error: &task.GetPodEventsResponse_Error{
			Message: "get pod events read failed"},
	}
	suite.mockTask.EXPECT().GetPodEvents(context.Background(), req).
		Return(response, nil)
	err = c.PodGetEventsAction(jobID.GetValue(), 0, runID, 5)
	suite.NoError(err)

	c.Debug = true
	suite.mockTask.EXPECT().GetPodEvents(context.Background(), req).
		Return(response, nil)
	err = c.PodGetEventsAction(jobID.GetValue(), 0, runID, 5)
	suite.NoError(err)
}

func (suite *taskActionsTestSuite) TestClientTaskQueryAction() {
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
		debug             bool
		taskQueryRequest  *task.QueryRequest
		taskQueryResponse *task.QueryResponse
		queryError        error
		orderString       string
		names             string
	}{
		{
			// happy path
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
				Records: suite.getQueryResult(
					jobID,
					[]task.TaskState{task.TaskState_RUNNING},
				),
			},
			queryError:  nil,
			orderString: "DESC",
			names:       "",
		},
		{
			// json
			debug: true,
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
				Records: suite.getQueryResult(
					jobID,
					[]task.TaskState{task.TaskState_RUNNING},
				),
			},
			queryError:  nil,
			orderString: "DESC",
			names:       "",
		},
		{
			// no task returned
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
				Records: []*task.TaskInfo{},
			},
			queryError:  nil,
			orderString: "DESC",
			names:       "",
		},
		{
			// query error
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
				Records: []*task.TaskInfo{},
			},
			queryError:  errors.New("failed to query"),
			orderString: "DESC",
			names:       "",
		},
		{
			// ascending order
			taskQueryRequest: &task.QueryRequest{
				JobId: jobID,
				Spec: &task.QuerySpec{
					Pagination: &query.PaginationSpec{
						Limit:  10,
						Offset: 0,
						OrderBy: []*query.OrderBy{
							{
								Order: query.OrderBy_ASC,
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
					Names: []string{"task1"},
				},
			},
			taskQueryResponse: &task.QueryResponse{
				Records: suite.getQueryResult(
					jobID,
					[]task.TaskState{task.TaskState_RUNNING},
				),
			},
			queryError:  nil,
			orderString: "ASC",
			names:       "task1",
		},
		{
			// job not found
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
					NotFound: &pberr.JobNotFound{
						Id:      jobID,
						Message: "Job not found",
					},
				},
			},
			queryError:  nil,
			orderString: "DESC",
			names:       "",
		},
	}
	for _, t := range tests {
		c.Debug = t.debug
		suite.withMockTaskQueryResponse(
			t.taskQueryRequest,
			t.taskQueryResponse,
			t.queryError,
		)
		err := c.TaskQueryAction(
			jobID.Value, "RUNNING", t.names, "taskHost",
			10, 0, "state", t.orderString,
		)
		if t.queryError != nil {
			suite.EqualError(err, t.queryError.Error())
		} else {
			suite.NoError(err)
		}
	}
}

// TestClientTaskQueryActionInvalidOrder tests passing an
// invalid order in query spec
func (suite *taskActionsTestSuite) TestClientTaskQueryActionInvalidOrder() {
	c := Client{
		Debug:      false,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: uuid.New(),
	}

	suite.Error(c.TaskQueryAction(
		jobID.Value, "RUNNING", "", "taskHost", 10, 0, "state", "ABC"))
}

// TestClientTaskBrowseSandboxAction tests browsing sandbox
func (suite *taskActionsTestSuite) TestClientTaskBrowseSandboxAction() {
	c := Client{
		Debug:      false,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: uuid.New(),
	}
	instanceID := uint32(0)
	taskID := "task-1"

	tt := []struct {
		req  *task.BrowseSandboxRequest
		resp *task.BrowseSandboxResponse
		err  error
	}{
		{
			// happy path
			req: &task.BrowseSandboxRequest{
				JobId:      jobID,
				InstanceId: instanceID,
				TaskId:     taskID,
			},
			resp: &task.BrowseSandboxResponse{
				Hostname: "host1",
				Port:     "8000",
				Paths:    []string{"/do/not/get"},
			},
			err: nil,
		},
		{
			// error
			req: &task.BrowseSandboxRequest{
				JobId:      jobID,
				InstanceId: instanceID,
				TaskId:     taskID,
			},
			resp: &task.BrowseSandboxResponse{
				Hostname: "host1",
				Port:     "8000",
				Paths:    []string{"/do/not/get"},
			},
			err: errors.New("cannot fetch sandbox"),
		},
		{
			// not have the file in the path
			req: &task.BrowseSandboxRequest{
				JobId:      jobID,
				InstanceId: instanceID,
				TaskId:     taskID,
			},
			resp: &task.BrowseSandboxResponse{
				Hostname: "host1",
				Port:     "8000",
				Paths:    []string{"/do/not/fetch"},
			},
			err: nil,
		},
		{
			// sandbox error
			req: &task.BrowseSandboxRequest{
				JobId:      jobID,
				InstanceId: instanceID,
				TaskId:     taskID,
			},
			resp: &task.BrowseSandboxResponse{
				Error: &task.BrowseSandboxResponse_Error{
					Failure: &task.BrowseSandboxFailure{
						Message: "sandbox error",
					},
				},
			},
			err: nil,
		},
	}

	for _, t := range tt {
		suite.mockTask.EXPECT().
			BrowseSandbox(gomock.Any(), t.req).
			Return(t.resp, t.err)

		suite.Error(c.TaskLogsGetAction("get", jobID.Value, instanceID, taskID))
	}
}

func (suite *taskActionsTestSuite) TestClientTaskRefreshAction() {
	c := Client{
		Debug:      false,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: uuid.New(),
	}
	instanceRange := &task.InstanceRange{
		From: 0,
		To:   10,
	}

	suite.mockTask.EXPECT().
		Refresh(gomock.Any(), &task.RefreshRequest{
			JobId: jobID,
			Range: instanceRange,
		}).
		Return(&task.RefreshResponse{}, nil)
	suite.NoError(c.TaskRefreshAction(jobID.Value, instanceRange))
}

func (suite *taskActionsTestSuite) TestClientTaskGetAction() {
	c := Client{
		Debug:      false,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: uuid.New(),
	}
	instanceID := uint32(0)

	tt := []struct {
		debug bool
		req   *task.GetRequest
		resp  *task.GetResponse
		err   error
	}{
		{
			req: &task.GetRequest{
				JobId:      jobID,
				InstanceId: instanceID,
			},
			resp: &task.GetResponse{
				Result: &task.TaskInfo{
					InstanceId: instanceID,
					JobId:      jobID,
					Config: &task.TaskConfig{
						Name: "task",
					},
					Runtime: &task.RuntimeInfo{
						State:          task.TaskState_RUNNING,
						Host:           "host1",
						StartTime:      time.Now().UTC().Format(time.RFC3339Nano),
						CompletionTime: time.Now().UTC().Format(time.RFC3339Nano),
					},
				},
			},
			err: nil,
		},
		{
			debug: true,
			req: &task.GetRequest{
				JobId:      jobID,
				InstanceId: instanceID,
			},
			resp: &task.GetResponse{
				Result: &task.TaskInfo{
					InstanceId: instanceID,
					JobId:      jobID,
					Config: &task.TaskConfig{
						Name: "task",
					},
					Runtime: &task.RuntimeInfo{
						State:          task.TaskState_RUNNING,
						Host:           "host1",
						StartTime:      time.Now().UTC().Format(time.RFC3339Nano),
						CompletionTime: time.Now().UTC().Format(time.RFC3339Nano),
					},
				},
			},
			err: nil,
		},
		{
			req: &task.GetRequest{
				JobId:      jobID,
				InstanceId: instanceID,
			},
			resp: &task.GetResponse{},
			err:  errors.New("failed to get task"),
		},
		{
			req: &task.GetRequest{
				JobId:      jobID,
				InstanceId: instanceID,
			},
			resp: &task.GetResponse{
				Result: nil,
			},
			err: nil,
		},
		{
			req: &task.GetRequest{
				JobId:      jobID,
				InstanceId: instanceID,
			},
			resp: &task.GetResponse{
				NotFound: &pberr.JobNotFound{
					Id:      jobID,
					Message: "job not found",
				},
			},
			err: nil,
		},
		{
			req: &task.GetRequest{
				JobId:      jobID,
				InstanceId: instanceID,
			},
			resp: &task.GetResponse{
				OutOfRange: &task.InstanceIdOutOfRange{
					JobId:         jobID,
					InstanceCount: 10,
				},
			},
			err: nil,
		},
	}

	for _, t := range tt {
		c.Debug = t.debug
		suite.mockTask.EXPECT().
			Get(gomock.Any(), t.req).
			Return(t.resp, t.err)

		if t.err != nil {
			suite.Error(c.TaskGetAction(jobID.Value, instanceID))
		} else {
			suite.NoError(c.TaskGetAction(jobID.Value, instanceID))
		}
	}
}

func (suite *taskActionsTestSuite) TestClientTaskStartAction() {
	c := Client{
		Debug:      false,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: uuid.New(),
	}
	instanceRange := []*task.InstanceRange{
		{
			From: 0,
			To:   10,
		},
	}

	tt := []struct {
		debug bool
		req   *task.StartRequest
		resp  *task.StartResponse
		err   error
	}{
		{
			req: &task.StartRequest{
				JobId:  jobID,
				Ranges: instanceRange,
			},
			resp: &task.StartResponse{
				StartedInstanceIds: []uint32{0, 1, 2, 3, 4, 5, 6},
				InvalidInstanceIds: []uint32{7, 8, 9},
			},
			err: nil,
		},
		{
			debug: true,
			req: &task.StartRequest{
				JobId:  jobID,
				Ranges: instanceRange,
			},
			resp: &task.StartResponse{
				StartedInstanceIds: []uint32{0, 1, 2, 3, 4, 5, 6},
				InvalidInstanceIds: []uint32{7, 8, 9},
			},
			err: nil,
		},
		{
			req: &task.StartRequest{
				JobId:  jobID,
				Ranges: instanceRange,
			},
			resp: &task.StartResponse{
				StartedInstanceIds: []uint32{0, 1, 2, 3, 4, 5, 6},
				InvalidInstanceIds: []uint32{7, 8, 9},
			},
			err: errors.New("failed to start instances"),
		},
		{
			req: &task.StartRequest{
				JobId:  jobID,
				Ranges: instanceRange,
			},
			resp: &task.StartResponse{
				Error: &task.StartResponse_Error{
					NotFound: &pberr.JobNotFound{
						Id:      jobID,
						Message: "job not found",
					},
				},
			},
			err: nil,
		},
		{
			req: &task.StartRequest{
				JobId:  jobID,
				Ranges: instanceRange,
			},
			resp: &task.StartResponse{
				Error: &task.StartResponse_Error{
					OutOfRange: &task.InstanceIdOutOfRange{
						JobId:         jobID,
						InstanceCount: 10,
					},
				},
			},
			err: nil,
		},
		{
			req: &task.StartRequest{
				JobId:  jobID,
				Ranges: instanceRange,
			},
			resp: &task.StartResponse{
				Error: &task.StartResponse_Error{
					Failure: &task.TaskStartFailure{
						Message: "failed to start task",
					},
				},
			},
			err: nil,
		},
	}

	for _, t := range tt {
		c.Debug = t.debug
		suite.mockTask.EXPECT().
			Start(gomock.Any(), t.req).
			Return(t.resp, t.err)

		if t.err != nil {
			suite.Error(c.TaskStartAction(jobID.Value, instanceRange))
		} else {
			suite.NoError(c.TaskStartAction(jobID.Value, instanceRange))
		}
	}
}

func (suite *taskActionsTestSuite) TestClientTaskRestartAction() {
	c := Client{
		Debug:      false,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: uuid.New(),
	}
	instanceRange := []*task.InstanceRange{
		{
			From: 0,
			To:   10,
		},
	}

	tt := []struct {
		debug bool
		req   *task.RestartRequest
		resp  *task.RestartResponse
		err   error
	}{
		{
			req: &task.RestartRequest{
				JobId:  jobID,
				Ranges: instanceRange,
			},
			resp: &task.RestartResponse{},
			err:  nil,
		},
		{
			debug: true,
			req: &task.RestartRequest{
				JobId:  jobID,
				Ranges: instanceRange,
			},
			resp: &task.RestartResponse{},
			err:  nil,
		},
		{
			req: &task.RestartRequest{
				JobId:  jobID,
				Ranges: instanceRange,
			},
			resp: &task.RestartResponse{},
			err:  errors.New("failed to restart tasks"),
		},
		{
			req: &task.RestartRequest{
				JobId:  jobID,
				Ranges: instanceRange,
			},
			resp: &task.RestartResponse{
				NotFound: &pberr.JobNotFound{
					Id:      jobID,
					Message: "job not found",
				},
			},
			err: nil,
		},
		{
			req: &task.RestartRequest{
				JobId:  jobID,
				Ranges: instanceRange,
			},
			resp: &task.RestartResponse{
				OutOfRange: &task.InstanceIdOutOfRange{
					JobId:         jobID,
					InstanceCount: 10,
				},
			},
			err: nil,
		},
	}

	for _, t := range tt {
		c.Debug = t.debug
		suite.mockTask.EXPECT().
			Restart(gomock.Any(), t.req).
			Return(t.resp, t.err)

		if t.err != nil {
			suite.Error(c.TaskRestartAction(jobID.Value, instanceRange))
		} else {
			suite.NoError(c.TaskRestartAction(jobID.Value, instanceRange))
		}
	}
}

func (suite *taskActionsTestSuite) TestClientTaskStopAction() {
	c := Client{
		Debug:      false,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: uuid.New(),
	}
	instanceRange := []*task.InstanceRange{
		{
			From: 0,
			To:   10,
		},
	}

	tt := []struct {
		debug      bool
		req        *task.StopRequest
		resp       *task.StopResponse
		err        error
		secondResp *task.StopResponse
		secondErr  error
	}{
		{
			// happy path
			req: &task.StopRequest{
				JobId:  jobID,
				Ranges: instanceRange,
			},
			resp: &task.StopResponse{
				StoppedInstanceIds: []uint32{0, 1, 2, 3, 4, 5, 6},
				InvalidInstanceIds: []uint32{7, 8, 9},
			},
			err: nil,
			secondResp: &task.StopResponse{
				StoppedInstanceIds: []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
				InvalidInstanceIds: []uint32{},
			},
			secondErr: nil,
		},
		{
			// json
			debug: true,
			req: &task.StopRequest{
				JobId:  jobID,
				Ranges: instanceRange,
			},
			resp: &task.StopResponse{
				StoppedInstanceIds: []uint32{0, 1, 2, 3, 4, 5, 6},
				InvalidInstanceIds: []uint32{7, 8, 9},
			},
			err: nil,
			secondResp: &task.StopResponse{
				StoppedInstanceIds: []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
				InvalidInstanceIds: []uint32{},
			},
			secondErr: nil,
		},
		{
			// error
			req: &task.StopRequest{
				JobId:  jobID,
				Ranges: instanceRange,
			},
			resp: &task.StopResponse{
				StoppedInstanceIds: []uint32{0, 1, 2, 3, 4, 5, 6},
				InvalidInstanceIds: []uint32{7, 8, 9},
			},
			err:        errors.New("cannot not stop tasks"),
			secondResp: nil,
			secondErr:  nil,
		},
		{
			// job not found
			req: &task.StopRequest{
				JobId:  jobID,
				Ranges: instanceRange,
			},
			resp: &task.StopResponse{
				StoppedInstanceIds: []uint32{0, 1, 2, 3, 4, 5, 6},
				InvalidInstanceIds: []uint32{},
				Error: &task.StopResponse_Error{
					NotFound: &pberr.JobNotFound{
						Id:      jobID,
						Message: "job not found",
					},
				},
			},
			err:        nil,
			secondResp: nil,
			secondErr:  nil,
		},
		{
			// out of range
			req: &task.StopRequest{
				JobId:  jobID,
				Ranges: instanceRange,
			},
			resp: &task.StopResponse{
				StoppedInstanceIds: []uint32{0, 1, 2, 3, 4, 5, 6},
				InvalidInstanceIds: []uint32{},
				Error: &task.StopResponse_Error{
					OutOfRange: &task.InstanceIdOutOfRange{
						JobId:         jobID,
						InstanceCount: 10,
					},
				},
			},
			err:        nil,
			secondResp: nil,
			secondErr:  nil,
		},
		{
			// stop error
			req: &task.StopRequest{
				JobId:  jobID,
				Ranges: instanceRange,
			},
			resp: &task.StopResponse{
				StoppedInstanceIds: []uint32{0, 1, 2, 3, 4, 5, 6},
				InvalidInstanceIds: []uint32{},
				Error: &task.StopResponse_Error{
					UpdateError: &task.TaskUpdateError{
						Message: "cannot stop tasks",
					},
				},
			},
			err:        nil,
			secondResp: nil,
			secondErr:  nil,
		},
		{
			// error on second try to stop
			req: &task.StopRequest{
				JobId:  jobID,
				Ranges: instanceRange,
			},
			resp: &task.StopResponse{
				StoppedInstanceIds: []uint32{0, 1, 2, 3, 4, 5, 6},
				InvalidInstanceIds: []uint32{9},
			},
			err: nil,
			secondResp: &task.StopResponse{
				StoppedInstanceIds: []uint32{0, 1, 2, 3, 4, 5, 6},
				InvalidInstanceIds: []uint32{9},
			},
			secondErr: errors.New("cannot not stop tasks"),
		},
	}

	for _, t := range tt {
		c.Debug = t.debug
		if len(t.resp.InvalidInstanceIds) > 0 && t.err == nil {
			gomock.InOrder(
				suite.mockTask.EXPECT().
					Stop(gomock.Any(), t.req).
					Return(t.resp, t.err),
				suite.mockTask.EXPECT().
					Stop(gomock.Any(), t.req).
					Return(t.secondResp, t.secondErr),
			)
		} else {
			suite.mockTask.EXPECT().
				Stop(gomock.Any(), t.req).
				Return(t.resp, t.err)
		}

		if t.err == nil && t.secondErr == nil {
			suite.NoError(c.TaskStopAction(jobID.Value, instanceRange))
		} else {
			suite.Error(c.TaskStopAction(jobID.Value, instanceRange))
		}
	}
}

func (suite *taskActionsTestSuite) TestClientTaskGetCacheAction() {
	c := Client{
		Debug:      false,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: uuid.New(),
	}

	instanceID := uint32(0)

	tt := []struct {
		req *task.GetCacheRequest
		err error
	}{
		{
			req: &task.GetCacheRequest{
				JobId:      jobID,
				InstanceId: instanceID,
			},
			err: nil,
		},
		{
			req: &task.GetCacheRequest{
				JobId:      jobID,
				InstanceId: instanceID,
			},
			err: errors.New("unable to fetch task from cache"),
		},
	}

	for _, t := range tt {
		suite.mockTask.EXPECT().
			GetCache(gomock.Any(), t.req).
			Return(nil, t.err)
		if t.err != nil {
			suite.Error(c.TaskGetCacheAction(jobID.Value, instanceID))
		} else {
			suite.NoError(c.TaskGetCacheAction(jobID.Value, instanceID))
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
