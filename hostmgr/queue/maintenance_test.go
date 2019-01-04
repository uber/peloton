package queue

import (
	"fmt"
	"testing"
	"time"

	"github.com/uber/peloton/common/queue/mocks"
	"github.com/uber/peloton/common/stringset"

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

func (suite *MaintenanceQueueTestSuite) TestMaintenanceQueueEnqueueDequeue() {
	maintenanceQueue := NewMaintenanceQueue()
	err := maintenanceQueue.Enqueue(suite.testHostnames)
	suite.NoError(err)

	// Check length
	suite.Equal(len(suite.testHostnames), maintenanceQueue.Length())

	// Test enqueuing duplicates
	err = maintenanceQueue.Enqueue(suite.testHostnames)
	suite.NoError(err)

	// Length should remain the same
	suite.Equal(len(suite.testHostnames), maintenanceQueue.Length())

	for _, hostname := range suite.testHostnames {
		h, err := maintenanceQueue.Dequeue(suite.maxWaitTime * time.Second)
		suite.NoError(err)
		suite.Equal(hostname, h)
	}

}

func (suite *MaintenanceQueueTestSuite) TestMaintenanceQueueErrors() {
	queue := mocks.NewMockQueue(suite.mockCtrl)
	maintenanceQueue := &maintenanceQueue{
		queue:   queue,
		hostSet: stringset.New(),
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

func (suite *MaintenanceQueueTestSuite) TestMaintenanceQueueClear() {
	queue := mocks.NewMockQueue(suite.mockCtrl)
	maintenanceQueue := &maintenanceQueue{
		queue: queue,
	}

	queue.EXPECT().Length().Return(len(suite.testHostnames))
	queue.EXPECT().
		Dequeue(gomock.Any()).
		Return(gomock.Any(), nil).
		Times(len(suite.testHostnames))
	maintenanceQueue.Clear()

	// Test Dequeue error
	queue.EXPECT().Length().Return(len(suite.testHostnames))
	queue.EXPECT().
		Dequeue(gomock.Any()).
		Return(gomock.Any(), fmt.Errorf("fake dequeue error")).
		Times(len(suite.testHostnames))
	maintenanceQueue.Clear()
}
