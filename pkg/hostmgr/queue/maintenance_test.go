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

type MaintenanceQueueTestSuite struct {
	suite.Suite
	mockCtrl     *gomock.Controller
	maxWaitTime  time.Duration
	testHostname string
}

func (suite *MaintenanceQueueTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.maxWaitTime = 1
	suite.testHostname = "testHostname"
}

func TestMaintenanceQueue(t *testing.T) {
	suite.Run(t, new(MaintenanceQueueTestSuite))
}

func (suite *MaintenanceQueueTestSuite) TestMaintenanceQueueEnqueueDequeue() {
	maintenanceQueue := NewMaintenanceQueue()
	err := maintenanceQueue.Enqueue(suite.testHostname)
	suite.NoError(err)

	// Check length
	suite.Equal(1, maintenanceQueue.Length())

	// Test enqueuing duplicates
	err = maintenanceQueue.Enqueue(suite.testHostname)
	suite.NoError(err)

	// Length should remain the same
	suite.Equal(1, maintenanceQueue.Length())

	h, err := maintenanceQueue.Dequeue(suite.maxWaitTime * time.Second)
	suite.NoError(err)
	suite.Equal(suite.testHostname, h)
}

func (suite *MaintenanceQueueTestSuite) TestMaintenanceQueueErrors() {
	queue := mocks.NewMockQueue(suite.mockCtrl)
	maintenanceQueue := &maintenanceQueue{
		queue:   queue,
		hostSet: stringset.New(),
	}
	// Test Enqueue error
	queue.EXPECT().Enqueue(gomock.Any()).
		Return(fmt.Errorf("fake enqueue error"))
	err := maintenanceQueue.Enqueue(suite.testHostname)
	suite.Error(err)

	// Test Dequeue error
	queue.EXPECT().Dequeue(gomock.Any()).
		Return(nil, fmt.Errorf("fake dequeue error"))
	hostname, err := maintenanceQueue.
		Dequeue(suite.maxWaitTime * time.Second)
	suite.Error(err)
	suite.Equal("", hostname)
}

func (suite *MaintenanceQueueTestSuite) TestMaintenanceQueueClear() {
	queue := mocks.NewMockQueue(suite.mockCtrl)
	maintenanceQueue := &maintenanceQueue{
		queue: queue,
	}

	queue.EXPECT().Length().Return(1)
	queue.EXPECT().
		Dequeue(gomock.Any()).
		Return(gomock.Any(), nil)
	maintenanceQueue.Clear()

	// Test Dequeue error
	queue.EXPECT().Length().Return(1)
	queue.EXPECT().
		Dequeue(gomock.Any()).
		Return(gomock.Any(), fmt.Errorf("fake dequeue error"))
	maintenanceQueue.Clear()
}
