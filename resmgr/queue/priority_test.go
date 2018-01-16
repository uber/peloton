package queue

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
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

	// Task - 1
	jobID1 := &peloton.JobID{
		Value: "job1",
	}
	taskID1 := &peloton.TaskID{
		Value: fmt.Sprintf("%s-%d", jobID1.Value, 1),
	}
	enq1 := resmgr.Task{
		Name:     "job1-1",
		Priority: 0,
		JobId:    jobID1,
		Id:       taskID1,
	}

	var gang1 resmgrsvc.Gang
	gang1.Tasks = append(gang1.Tasks, &enq1)
	suite.fq.Enqueue(&gang1)

	// Task - 2
	jobID2 := &peloton.JobID{
		Value: "job1",
	}
	taskID2 := &peloton.TaskID{
		Value: fmt.Sprintf("%s-%d", jobID2.Value, 2),
	}
	enq2 := resmgr.Task{
		Name:     "job1-2",
		Priority: 1,
		JobId:    jobID2,
		Id:       taskID2,
	}

	var gang2 resmgrsvc.Gang
	gang2.Tasks = append(gang2.Tasks, &enq2)
	suite.fq.Enqueue(&gang2)

	// Task - 3
	jobID3 := &peloton.JobID{
		Value: "job2",
	}
	taskID3 := &peloton.TaskID{
		Value: fmt.Sprintf("%s-%d", jobID3.Value, 1),
	}
	enq3 := resmgr.Task{
		Name:     "job2-1",
		Priority: 2,
		JobId:    jobID3,
		Id:       taskID3,
	}

	var gang3 resmgrsvc.Gang
	gang3.Tasks = append(gang3.Tasks, &enq3)
	suite.fq.Enqueue(&gang3)

	// Task - 4
	jobID4 := &peloton.JobID{
		Value: "job2",
	}
	taskID4 := &peloton.TaskID{
		Value: fmt.Sprintf("%s-%d", jobID4.Value, 2),
	}
	enq4 := resmgr.Task{
		Name:     "job2-2",
		Priority: 2,
		JobId:    jobID4,
		Id:       taskID4,
	}

	var gang4 resmgrsvc.Gang
	gang4.Tasks = append(gang4.Tasks, &enq4)
	suite.fq.Enqueue(&gang4)
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
	assert.Equal(suite.T(), suite.fq.Len(2), 2, "Length should be 2")
}

func (suite *FifoQueueTestSuite) TestSize() {
	suite.Equal(4, suite.fq.Size())
}

func (suite *FifoQueueTestSuite) TestDequeue() {
	gang, err := suite.fq.Dequeue()
	if err != nil {
		assert.Fail(suite.T(), "Dequeue should not fail")
	}
	if len(gang.Tasks) != 1 {
		assert.Fail(suite.T(), "Dequeue should return single task gang")
	}
	dqRes := gang.Tasks[0]
	assert.Equal(suite.T(), dqRes.JobId.Value, "job2", "Should get Job-2")

	gang, err = suite.fq.Dequeue()
	if err != nil {
		assert.Fail(suite.T(), "Dequeue should not fail")
	}
	if len(gang.Tasks) != 1 {
		assert.Fail(suite.T(), "Dequeue should return single task gang")
	}
	dqRes = gang.Tasks[0]
	assert.Equal(suite.T(), dqRes.JobId.Value, "job2", "Should get Job-2")
	assert.Equal(suite.T(), dqRes.Id.GetValue(), "job2-2", "Should get Job-2 and Instance Id 2")

	gang, err = suite.fq.Dequeue()
	if err != nil {
		assert.Fail(suite.T(), "Dequeue should not fail")
	}
	if len(gang.Tasks) != 1 {
		assert.Fail(suite.T(), "Dequeue should return single task gang")
	}
	dqRes = gang.Tasks[0]
	assert.Equal(suite.T(), dqRes.JobId.Value, "job1", "Should get Job-1")
	assert.Equal(suite.T(), dqRes.Id.GetValue(), "job1-2", "Should be instance 2")

	gang, err = suite.fq.Dequeue()
	if err != nil {
		assert.Fail(suite.T(), "Dequeue should not fail")
	}
	if len(gang.Tasks) != 1 {
		assert.Fail(suite.T(), "Dequeue should return single task gang")
	}
	dqRes = gang.Tasks[0]
	assert.Equal(suite.T(), dqRes.JobId.Value, "job1", "Should get Job-1")
	assert.Equal(suite.T(), dqRes.Id.GetValue(), "job1-1", "Should get Job-1 and instance 1")
}

func (suite *FifoQueueTestSuite) TestPeek() {
	gangs, err := suite.fq.Peek(1)
	suite.NoError(err)
	suite.Equal(len(gangs), 1)
	suite.Equal(len(gangs[0].Tasks), 1)
	dqRes := gangs[0].Tasks[0]
	suite.Equal(dqRes.JobId.Value, "job2", "Should get Job-2")
	suite.Equal(suite.fq.Len(2), 2, "Length should be 2")
}

func (suite *FifoQueueTestSuite) TestPeekWithLimit() {
	q := NewPriorityQueue(1000)

	// add 4 tasks with different priorities
	for i := 0; i < 4; i++ {
		var gang1 resmgrsvc.Gang
		// Task - 1
		jobID1 := &peloton.JobID{
			Value: fmt.Sprintf("job-%d", i),
		}
		taskID1 := &peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", jobID1.Value, 0),
		}
		enq1 := resmgr.Task{
			Name:     fmt.Sprintf("job-%d", i),
			Priority: uint32(i),
			JobId:    jobID1,
			Id:       taskID1,
		}
		gang1.Tasks = append(gang1.Tasks, &enq1)
		q.Enqueue(&gang1)
	}

	// peek 10
	gangs, err := q.Peek(10)
	suite.NoError(err)
	// should return 4 gangs
	suite.Equal(4, len(gangs))

	// first gang should have the highest priority
	suite.Equal(uint32(3), gangs[0].Tasks[0].GetPriority())
}

func (suite *FifoQueueTestSuite) TestRemove() {
	gangs, err := suite.fq.Peek(1)
	suite.NoError(err)
	suite.Equal(len(gangs[0].Tasks), 1)
	err = suite.fq.Remove(gangs[0])
	suite.NoError(err)
	suite.Equal(suite.fq.Len(2), 1, "Length should be 1")

	gangs, err = suite.fq.Peek(1)
	suite.NoError(err)
	err = suite.fq.Remove(gangs[0])
	suite.NoError(err)
	suite.Equal(suite.fq.Len(2), 0, "Length should be 0")

	err = suite.fq.Remove(nil)
	suite.Error(err)
}
