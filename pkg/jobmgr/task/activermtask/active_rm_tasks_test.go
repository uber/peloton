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

package activermtask

import (
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	resmocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"
	"github.com/uber/peloton/pkg/jobmgr/cached"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

func TestPelotonActiveRMTasks(t *testing.T) {
	suite.Run(t, new(TestActiveRMTasks))
}

type TestActiveRMTasks struct {
	suite.Suite
	mockResmgr    *resmocks.MockResourceManagerServiceYARPCClient
	activeRMTasks activeRMTasks
	ctrl          *gomock.Controller
}

func (suite *TestActiveRMTasks) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.mockResmgr = resmocks.NewMockResourceManagerServiceYARPCClient(suite.ctrl)

	testScope := tally.NewTestScope("", map[string]string{})
	metrics := NewMetrics(testScope)
	testCache := make(map[string]*resmgrsvc.GetActiveTasksResponse_TaskEntry)

	for i := 0; i < 10; i++ {
		taskID := fmt.Sprintf("TASK_%v", i)
		testCache[taskID] = &resmgrsvc.GetActiveTasksResponse_TaskEntry{
			TaskID:    taskID,
			TaskState: "PENDING",
			Reason:    fmt.Sprintf("REASON_%v", i),
		}
	}

	suite.activeRMTasks = activeRMTasks{
		resmgrClient: suite.mockResmgr,
		metrics:      metrics,
		taskCache:    testCache,
	}
}

func (suite *TestActiveRMTasks) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *TestActiveRMTasks) TestGetActiveTasks() {
	taskEntry := suite.activeRMTasks.GetTask("TASK_3")
	assert.NotNil(suite.T(), taskEntry)
	assert.Equal(suite.T(), taskEntry.Reason, "REASON_3")

	taskEntry = suite.activeRMTasks.GetTask("TASK_11")
	assert.Nil(suite.T(), taskEntry)

	emptyActiveRMTasks := activeRMTasks{
		resmgrClient: suite.mockResmgr,
		metrics:      nil,
		taskCache:    nil,
	}
	taskEntry = emptyActiveRMTasks.GetTask("TASK_0")
	assert.Nil(suite.T(), taskEntry)
}

func (suite *TestActiveRMTasks) TestUpdateActiveTasks() {

	taskEntries := []*resmgrsvc.GetActiveTasksResponse_TaskEntry{
		{
			TaskID: "TASK_RESMGR",
			Reason: "REASON_RESMGR",
		},
	}
	states := cached.GetResourceManagerProcessingStates()
	sort.Strings(states)
	suite.mockResmgr.EXPECT().
		GetActiveTasks(gomock.Any(), &resmgrsvc.GetActiveTasksRequest{
			States: states,
		}).Return(&resmgrsvc.GetActiveTasksResponse{
		TasksByState: map[string]*resmgrsvc.GetActiveTasksResponse_TaskEntries{
			task.TaskState_PLACING.String(): {TaskEntry: taskEntries}},
	}, nil)

	suite.activeRMTasks.resmgrClient = suite.mockResmgr
	suite.activeRMTasks.UpdateActiveTasks()
}

func (suite *TestActiveRMTasks) TestUpdateActiveTasksError() {
	states := cached.GetResourceManagerProcessingStates()
	sort.Strings(states)
	suite.mockResmgr.EXPECT().
		GetActiveTasks(gomock.Any(), &resmgrsvc.GetActiveTasksRequest{
			States: states,
		}).Return(nil, errors.New("ResMgr Error"))
	// UpdateActiveTasks will not failed
	suite.activeRMTasks.UpdateActiveTasks()
}
