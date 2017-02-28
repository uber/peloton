package queue

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"math"
	"peloton/api/job"
	"peloton/api/task"
	"testing"
)

type FifoQueueTestSuite struct {
	suite.Suite
	fq *PriorityQueue
}

func (suite *FifoQueueTestSuite) SetupTest() {
	suite.fq = NewPriorityQueue(math.MaxInt64)
	// TODO: Add tests for concurency behavior
	suite.AddTasks()
}

func (suite *FifoQueueTestSuite) AddTasks() {

	// Task -1
	taskInfo1 := task.TaskInfo{
		InstanceId: 1,
		JobId: &job.JobID{
			Value: "job1",
		},
	}
	enq1 := TaskItem{
		TaskInfo: &taskInfo1,
		TaskID:   fmt.Sprintf("%s-%d", taskInfo1.JobId.Value, taskInfo1.InstanceId),
		Priority: 0,
	}
	suite.fq.Enqueue(&enq1)

	// Task -2
	taskInfo2 := task.TaskInfo{
		InstanceId: 2,
		JobId: &job.JobID{
			Value: "job1",
		},
	}

	enq2 := TaskItem{
		TaskInfo: &taskInfo2,
		TaskID:   fmt.Sprintf("%s-%d", taskInfo2.JobId.Value, taskInfo2.InstanceId),
		Priority: 1,
	}
	suite.fq.Enqueue(&enq2)

	// Task -3
	taskInfo3 := task.TaskInfo{
		InstanceId: 1,
		JobId: &job.JobID{
			Value: "job2",
		},
	}

	enq3 := TaskItem{
		TaskInfo: &taskInfo3,
		TaskID:   fmt.Sprintf("%s-%d", taskInfo3.JobId.Value, taskInfo3.InstanceId),
		Priority: 2,
	}
	suite.fq.Enqueue(&enq3)

	// Task -4
	taskInfo4 := task.TaskInfo{
		InstanceId: 2,
		JobId: &job.JobID{
			Value: "job2",
		},
	}

	enq4 := TaskItem{
		TaskInfo: &taskInfo4,
		TaskID:   fmt.Sprintf("%s-%d", taskInfo4.JobId.Value, taskInfo4.InstanceId),
		Priority: 2,
	}
	suite.fq.Enqueue(&enq4)
}

func (suite *FifoQueueTestSuite) TearDownTest() {
	fmt.Println("tearing down")
}

func TestPelotonFifoQueue(t *testing.T) {
	suite.Run(t, new(FifoQueueTestSuite))
}

func (suite *FifoQueueTestSuite) TestLength() {
	assert.Equal(suite.T(), suite.fq.Len(0), 1, "Length should be 1")
	assert.Equal(suite.T(), suite.fq.Len(1), 1, "Length should be 1")
	assert.Equal(suite.T(), suite.fq.Len(2), 2, "Length should be 1")
}

func (suite *FifoQueueTestSuite) TestDequeue() {
	dqRes, err := suite.fq.Dequeue()
	if err != nil {
		assert.Fail(suite.T(), "Dequeue should not fail")
	}

	assert.Equal(suite.T(), dqRes.TaskInfo.JobId.Value, "job2", "Should get Job-2")
	dqRes, err = suite.fq.Dequeue()
	if err != nil {
		assert.Fail(suite.T(), "Dequeue should not fail")
	}
	assert.Equal(suite.T(), dqRes.TaskInfo.JobId.Value, "job2", "Should get Job-2")
	assert.Equal(suite.T(), dqRes.TaskInfo.InstanceId, uint32(2), "Should get Job-2 and Instance Id 2")

	dqRes, err = suite.fq.Dequeue()
	if err != nil {
		assert.Fail(suite.T(), "Dequeue should not fail")
	}
	assert.Equal(suite.T(), dqRes.TaskInfo.JobId.Value, "job1", "Should get Job-1")
	assert.Equal(suite.T(), dqRes.TaskInfo.InstanceId, uint32(2), "Should be instance 2")

	dqRes, err = suite.fq.Dequeue()
	if err != nil {
		assert.Fail(suite.T(), "Dequeue should not fail")
	}
	assert.Equal(suite.T(), dqRes.TaskInfo.JobId.Value, "job1", "Should get Job-1")
	assert.Equal(suite.T(), dqRes.TaskInfo.InstanceId, uint32(1), "Should get Job-1 and instance 1")
}
