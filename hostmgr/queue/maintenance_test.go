package queue

import (
	"fmt"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/common/queue/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

type MaintenanceQueueTestSuite struct {
	suite.Suite
	mockCtrl      *gomock.Controller
	maxWaitTime   time.Duration
	testHostnames []string
}

func (suite *MaintenanceQueueTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.maxWaitTime = 1
	suite.testHostnames = []string{"testHost1", "testHost2"}
}

func TestMaintenanceQueue(t *testing.T) {
	suite.Run(t, new(MaintenanceQueueTestSuite))
}

func (suite *MaintenanceQueueTestSuite) TestMaintenanceQueue_EnqueueDequeue() {
	maintenanceQueue := NewMaintenanceQueue()
	err := maintenanceQueue.Enqueue(suite.testHostnames)
	suite.NoError(err)

	// Check length
	suite.Equal(len(suite.testHostnames), maintenanceQueue.Length())

	hostnameMap := make(map[string]bool)
	for _, hostname := range suite.testHostnames {
		hostnameMap[hostname] = true
	}
	for range suite.testHostnames {
		h, err := maintenanceQueue.Dequeue(suite.maxWaitTime * time.Second)
		suite.NoError(err)
		suite.True(hostnameMap[h])
		delete(hostnameMap, h)
	}
	suite.Equal(0, len(hostnameMap))
}

func (suite *MaintenanceQueueTestSuite) TestMaintenanceQueue_QueueErrors() {
	queue := mocks.NewMockQueue(suite.mockCtrl)
	maintenanceQueue := &maintenanceQueue{
		queue: queue,
	}
	// Test Enqueue error
	queue.EXPECT().Enqueue(gomock.Any()).
		Return(fmt.Errorf("fake enqueue error")).
		Times(len(suite.testHostnames))
	err := maintenanceQueue.Enqueue(suite.testHostnames)
	suite.Error(err)

	// Test Dequeue error
	queue.EXPECT().Dequeue(gomock.Any()).
		Return(nil, fmt.Errorf("fake dequeue error"))
	hostname, err := maintenanceQueue.
		Dequeue(suite.maxWaitTime * time.Second)
	suite.Error(err)
	suite.Equal("", hostname)
}
