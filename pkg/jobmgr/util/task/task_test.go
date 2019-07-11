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

package task

import (
	"context"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"

	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

type TaskTestSuite struct {
	suite.Suite
	ctrl            *gomock.Controller
	taskConfigV2Ops *objectmocks.MockTaskConfigV2Ops

	jobID *peloton.JobID
}

func TestTask(t *testing.T) {
	suite.Run(t, new(TaskTestSuite))
}

func (suite *TaskTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.taskConfigV2Ops = objectmocks.NewMockTaskConfigV2Ops(suite.ctrl)
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
}

func (suite *TaskTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestTasksRunInParallel tests an action taken on list of instances
// in parallel
func (suite *TaskTestSuite) TestTasksRunInParallel() {
	instances := []uint32{0, 1, 2, 3, 4}
	taskConfig := &pbtask.TaskConfig{
		Name: "test-instance",
		Resource: &pbtask.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
		},
	}

	createSingleTaskConfig := func(id uint32) error {
		return suite.taskConfigV2Ops.Create(
			context.Background(),
			suite.jobID,
			int64(id),
			taskConfig,
			nil,
			nil,
			1,
		)
	}

	for _, i := range instances {
		suite.taskConfigV2Ops.EXPECT().
			Create(
				gomock.Any(),
				suite.jobID,
				int64(i),
				taskConfig,
				nil,
				nil,
				uint64(1)).
			Return(nil)
	}

	RunInParallel(suite.jobID.GetValue(), instances, createSingleTaskConfig)
}

// TestTaskRunInParallelFail tests failure scenario for running task action
// in parallel
func (suite *TaskTestSuite) TestTaskRunInParallelFail() {
	instances := []uint32{0, 1, 2, 3, 4}
	taskConfig := &pbtask.TaskConfig{
		Name: "test-instance",
		Resource: &pbtask.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
		},
	}

	createSingleTaskConfig := func(id uint32) error {
		return suite.taskConfigV2Ops.Create(
			context.Background(),
			suite.jobID,
			int64(id),
			taskConfig,
			nil,
			nil,
			1,
		)
	}

	suite.taskConfigV2Ops.EXPECT().
		Create(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			taskConfig,
			nil,
			nil,
			uint64(1)).
		Return(yarpcerrors.AbortedErrorf("db error")).
		AnyTimes()

	suite.Error(RunInParallel(suite.jobID.GetValue(), instances, createSingleTaskConfig))
}
