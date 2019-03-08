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

package tasktestutil

import (
	"fmt"
	"testing"
	"time"

	"github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	rmock "github.com/uber/peloton/pkg/resmgr/respool/mocks"
	"github.com/uber/peloton/pkg/resmgr/scalar"
	rm_task "github.com/uber/peloton/pkg/resmgr/task"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type TestUtilTestSuite struct {
	suite.Suite
}

// TestCreateConfig tests the create config
func (suite *TestUtilTestSuite) TestCreateConfig() {
	taskConfig := CreateTaskConfig()
	suite.Equal(taskConfig.PlacingTimeout, 1*time.Minute)
}

// TestValidateResources validates the resources
func (suite *TestUtilTestSuite) TestValidateResources() {
	res := &scalar.Resources{
		CPU:    33,
		GPU:    0,
		MEMORY: 333,
		DISK:   1000,
	}

	suite.True(ValidateResources(res,
		map[string]int64{"CPU": 33, "GPU": 0, "MEMORY": 333, "DISK": 1000}))

	res = &scalar.Resources{
		CPU:    33,
		GPU:    20,
		MEMORY: 333,
		DISK:   1000,
	}

	suite.False(ValidateResources(res,
		map[string]int64{"CPU": 33, "GPU": 0, "MEMORY": 333, "DISK": 1000}))
}

// TestValidateTransitions validates the transitions
func (suite *TestUtilTestSuite) TestValidateTransitions() {
	mockCtrl := gomock.NewController(suite.T())
	rm_task.InitTaskTracker(tally.NoopScope, CreateTaskConfig())
	rmTaskTracker := rm_task.GetTracker()
	mockResPool := rmock.NewMockResPool(mockCtrl)
	mockResPool.EXPECT().GetPath().Return("/mock/path")

	rmTaskTracker.AddTask(
		suite.pendingGang0().Tasks[0],
		nil,
		mockResPool,
		CreateTaskConfig())
	rmtask := rmTaskTracker.GetTask(suite.pendingGang0().Tasks[0].Id)

	err := rmtask.TransitTo(task.TaskState_PENDING.String())
	suite.NoError(err)
	ValidateStateTransitions(rmtask, []task.TaskState{
		task.TaskState_READY,
		task.TaskState_PLACING,
		task.TaskState_PLACED,
		task.TaskState_LAUNCHING})
}

func (suite *TestUtilTestSuite) pendingGang0() *resmgrsvc.Gang {
	var gang resmgrsvc.Gang
	uuidStr := "uuidstr-1"
	jobID := "job1"
	instance := 1
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, instance, uuidStr)
	gang.Tasks = []*resmgr.Task{
		{
			Name:     "job1-1",
			Priority: 0,
			JobId:    &peloton.JobID{Value: "job1"},
			Id:       &peloton.TaskID{Value: fmt.Sprintf("%s-%d", jobID, instance)},
			Resource: &task.ResourceConfig{
				CpuLimit:    1,
				DiskLimitMb: 10,
				GpuLimit:    0,
				MemLimitMb:  100,
			},
			TaskId: &mesos_v1.TaskID{
				Value: &mesosTaskID,
			},
			Preemptible:             true,
			PlacementTimeoutSeconds: 60,
			PlacementRetryCount:     1,
		},
	}
	return &gang
}

func TestTaskTestUtil(t *testing.T) {
	suite.Run(t, new(TestUtilTestSuite))
}
