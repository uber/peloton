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
	"fmt"
	"testing"

	mesos_v1 "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	res_mocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

type resmgrActionsTestSuite struct {
	suite.Suite
	mockCtrl *gomock.Controller
	mockRes  *res_mocks.MockResourceManagerServiceYARPCClient
	ctx      context.Context
}

func TestResmgrActions(t *testing.T) {
	suite.Run(t, new(resmgrActionsTestSuite))
}

func (suite *resmgrActionsTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockRes = res_mocks.NewMockResourceManagerServiceYARPCClient(suite.mockCtrl)
	suite.ctx = context.Background()
}

func (suite *resmgrActionsTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
	suite.ctx.Done()
}

func (suite *resmgrActionsTestSuite) TestClientGetActiveTasks() {
	c := Client{
		Debug:        false,
		resMgrClient: suite.mockRes,
		dispatcher:   nil,
		ctx:          suite.ctx,
	}

	pendingTasks := &resmgrsvc.GetActiveTasksResponse_TaskEntries{
		TaskEntry: make([]*resmgrsvc.GetActiveTasksResponse_TaskEntry, 1),
	}
	runningTasks := &resmgrsvc.GetActiveTasksResponse_TaskEntries{
		TaskEntry: make([]*resmgrsvc.GetActiveTasksResponse_TaskEntry, 2),
	}

	taskEntries := make(map[string]*resmgrsvc.GetActiveTasksResponse_TaskEntries)
	taskEntries[task.TaskState_PENDING.String()] = pendingTasks
	taskEntries[task.TaskState_RUNNING.String()] = runningTasks

	tt := []struct {
		debug bool
		resp  *resmgrsvc.GetActiveTasksResponse
		err   error
	}{
		{
			resp: &resmgrsvc.GetActiveTasksResponse{
				TasksByState: taskEntries,
			},
			err: nil,
		},
		{
			resp: &resmgrsvc.GetActiveTasksResponse{
				Error: &resmgrsvc.GetActiveTasksResponse_Error{
					Message: "cannot fetch tasks",
				},
			},
		},
		{
			resp: nil,
			err:  fmt.Errorf("fake res error"),
		},
		{
			debug: true,
			resp: &resmgrsvc.GetActiveTasksResponse{
				TasksByState: taskEntries,
			},
			err: nil,
		},
	}

	for _, t := range tt {
		c.Debug = t.debug
		suite.mockRes.EXPECT().
			GetActiveTasks(gomock.Any(), gomock.Any()).
			Return(t.resp, t.err)
		if t.err != nil {
			suite.Error(c.ResMgrGetActiveTasks("", "", ""))
		} else {
			suite.NoError(c.ResMgrGetActiveTasks("", "", "SUCCEEDED,FAILED"))
		}
	}
}

func (suite *resmgrActionsTestSuite) TestClientGetPendingTasks() {
	c := Client{
		Debug:        false,
		resMgrClient: suite.mockRes,
		dispatcher:   nil,
		ctx:          suite.ctx,
	}

	pGangs := make(map[string]*resmgrsvc.
		GetPendingTasksResponse_PendingGangs, 2)

	var pGang1 []*resmgrsvc.GetPendingTasksResponse_PendingGang
	pGang1 = append(
		pGang1,
		&resmgrsvc.GetPendingTasksResponse_PendingGang{
			TaskIDs: []string{"job-1-1", "job-1-2"},
		},
	)
	var pGang2 []*resmgrsvc.GetPendingTasksResponse_PendingGang
	pGang2 = append(
		pGang2,
		&resmgrsvc.GetPendingTasksResponse_PendingGang{
			TaskIDs: []string{"job-2-1", "job-2-2"},
		},
	)
	pGangs["pending"] = &resmgrsvc.GetPendingTasksResponse_PendingGangs{
		PendingGangs: pGang1,
	}
	pGangs["non-preemptible"] = &resmgrsvc.GetPendingTasksResponse_PendingGangs{
		PendingGangs: pGang1,
	}

	resp := &resmgrsvc.GetPendingTasksResponse{
		PendingGangsByQueue: pGangs,
	}

	suite.mockRes.EXPECT().
		GetPendingTasks(gomock.Any(), gomock.Any()).
		Return(resp, nil)

	err := c.ResMgrGetPendingTasks("respool-1", 10)
	suite.NoError(err)

	suite.mockRes.EXPECT().
		GetPendingTasks(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("fake res error"))

	err = c.ResMgrGetPendingTasks("respool-1", 10)
	suite.Error(err)

	c.Debug = true
	suite.mockRes.EXPECT().
		GetPendingTasks(gomock.Any(), gomock.Any()).
		Return(resp, nil)
	err = c.ResMgrGetPendingTasks("respool-1", 10)
	suite.NoError(err)
}

// TestClientGetOrphanTasks tests getting orphan tasks
func (suite *resmgrActionsTestSuite) TestClientGetOrphanTasks() {
	c := Client{
		Debug:        false,
		resMgrClient: suite.mockRes,
		dispatcher:   nil,
		ctx:          suite.ctx,
	}

	orphanTasks := []*resmgr.Task{
		{
			TaskId: &mesos_v1.TaskID{
				Value: &[]string{"test-task-1"}[0],
			},
			Hostname: "test-host-1",
			Resource: &task.ResourceConfig{
				CpuLimit:    1,
				GpuLimit:    0,
				DiskLimitMb: 1024,
				MemLimitMb:  512,
				FdLimit:     2,
			},
		},
		{
			TaskId: &mesos_v1.TaskID{
				Value: &[]string{"test-task-2"}[0],
			},
			Hostname: "test-host-2",
			Resource: &task.ResourceConfig{
				CpuLimit:    4,
				GpuLimit:    3,
				DiskLimitMb: 512,
				MemLimitMb:  1024,
				FdLimit:     0,
			},
		},
	}
	tt := []struct {
		debug bool
		resp  *resmgrsvc.GetOrphanTasksResponse
		err   error
	}{
		{
			resp: &resmgrsvc.GetOrphanTasksResponse{
				OrphanTasks: orphanTasks,
			},
			err: nil,
		},
		{
			resp: nil,
			err:  fmt.Errorf("fake res error"),
		},
		{
			debug: true,
			resp: &resmgrsvc.GetOrphanTasksResponse{
				OrphanTasks: orphanTasks,
			},
			err: nil,
		},
	}

	for _, t := range tt {
		c.Debug = t.debug
		suite.mockRes.EXPECT().
			GetOrphanTasks(gomock.Any(), gomock.Any()).
			Return(t.resp, t.err)
		if t.err != nil {
			suite.Error(c.ResMgrGetOrphanTasks(""))
		} else {
			suite.NoError(c.ResMgrGetOrphanTasks(""))
		}
	}
}
