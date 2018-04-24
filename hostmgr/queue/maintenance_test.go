package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type MaintenanceQueueTestSuite struct {
	suite.Suite
	maintenanceQueue MaintenanceQueue
	maxWaitTime      time.Duration
}

func (suite *MaintenanceQueueTestSuite) SetupSuite() {
	suite.maintenanceQueue = NewMaintenanceQueue()
	suite.maxWaitTime = 1
}

func TestMaintenanceQueue(t *testing.T) {
	suite.Run(t, new(MaintenanceQueueTestSuite))
}

func (suite *MaintenanceQueueTestSuite) TestMaintenanceQueue_EnqueueDequeue() {
	testHostnames := []string{"testHost1", "testHost2"}
	err := suite.maintenanceQueue.Enqueue(testHostnames)
	suite.NoError(err)

	for i := range testHostnames {
		h, err := suite.maintenanceQueue.Dequeue(suite.maxWaitTime * time.Second)
		suite.NoError(err)
		suite.Equal(testHostnames[i], h)
	}

	// Test Dequeue error
	hostname, err := suite.maintenanceQueue.Dequeue(suite.maxWaitTime * time.Second)
	suite.Error(err)
	suite.Equal("", hostname)
}
