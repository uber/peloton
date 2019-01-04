package queue

import (
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/respool"

	"github.com/stretchr/testify/suite"
)

// QueueTestSuite is the struct for Queue Tests
type QueueTestSuite struct {
	suite.Suite
}

func TestQueue(t *testing.T) {
	suite.Run(t, new(QueueTestSuite))
}

// TestCreateQueue tests the Create Queue
func (suite *QueueTestSuite) TestCreateQueueSuccess() {
	q, err := CreateQueue(respool.SchedulingPolicy_PriorityFIFO, 100)
	suite.NoError(err)
	suite.NotNil(q)
}

// TestCreateQueue tests the Create Queue
func (suite *QueueTestSuite) TestCreateQueueError() {
	q, err := CreateQueue(2, 100)
	suite.Nil(q)
	suite.Error(err)
	suite.EqualError(err, "invalid queue type")
}
