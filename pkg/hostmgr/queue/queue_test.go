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

package queue

import (
	"fmt"
	"testing"
	"time"

	"github.com/uber/peloton/pkg/common/queue/mocks"
	"github.com/uber/peloton/pkg/common/stringset"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

type TaskQueueTestSuite struct {
	suite.Suite
	mockCtrl    *gomock.Controller
	maxWaitTime time.Duration
	testTaskIDs []string
}

func (suite *TaskQueueTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.maxWaitTime = 1
	suite.testTaskIDs = []string{"testTask1", "testTask2"}
}

func TestTaskQueue(t *testing.T) {
	suite.Run(t, new(TaskQueueTestSuite))
}

func (suite *TaskQueueTestSuite) TestTaskQueueEnqueueDequeue() {
	taskQueue := NewTaskQueue("test-task-queue")
	err := taskQueue.Enqueue(suite.testTaskIDs)
	suite.NoError(err)

	// Check length
	suite.Equal(len(suite.testTaskIDs), taskQueue.Length())

	// Test enqueuing duplicates
	err = taskQueue.Enqueue(suite.testTaskIDs)
	suite.NoError(err)

	// Length should remain the same
	suite.Equal(len(suite.testTaskIDs), taskQueue.Length())

	for _, taskID := range suite.testTaskIDs {
		t, err := taskQueue.Dequeue(suite.maxWaitTime * time.Second)
		suite.NoError(err)
		suite.Equal(taskID, t)
	}

}

func (suite *TaskQueueTestSuite) TestTaskQueueErrors() {
	queue := mocks.NewMockQueue(suite.mockCtrl)
	testTaskQueue := &taskQueue{
		queue:   queue,
		taskSet: stringset.New(),
	}
	// Test Enqueue error
	queue.EXPECT().Enqueue(gomock.Any()).
		Return(fmt.Errorf("fake enqueue error")).
		Times(len(suite.testTaskIDs))
	err := testTaskQueue.Enqueue(suite.testTaskIDs)
	suite.Error(err)

	// Test Dequeue error
	queue.EXPECT().Dequeue(gomock.Any()).
		Return(nil, fmt.Errorf("fake dequeue error"))
	taskID, err := testTaskQueue.
		Dequeue(suite.maxWaitTime * time.Second)
	suite.Error(err)
	suite.Equal("", taskID)
}

func (suite *TaskQueueTestSuite) TestTaskQueueClear() {
	queue := mocks.NewMockQueue(suite.mockCtrl)
	testTaskQueue := &taskQueue{
		queue: queue,
	}

	queue.EXPECT().Length().Return(len(suite.testTaskIDs))
	queue.EXPECT().
		Dequeue(gomock.Any()).
		Return(gomock.Any(), nil).
		Times(len(suite.testTaskIDs))
	testTaskQueue.Clear()

	// Test Dequeue error
	queue.EXPECT().Length().Return(len(suite.testTaskIDs))
	queue.EXPECT().
		Dequeue(gomock.Any()).
		Return(gomock.Any(), fmt.Errorf("fake dequeue error")).
		Times(len(suite.testTaskIDs))
	testTaskQueue.Clear()
}
