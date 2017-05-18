package queue

import (
	"container/list"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
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

	tlist1 := new(list.List)
	tlist1.PushBack(&enq1)
	suite.fq.Enqueue(tlist1)

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

	tlist2 := new(list.List)
	tlist2.PushBack(&enq2)
	suite.fq.Enqueue(tlist2)

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

	tlist3 := new(list.List)
	tlist3.PushBack(&enq3)
	suite.fq.Enqueue(tlist3)

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

	tlist4 := new(list.List)
	tlist4.PushBack(&enq4)
	suite.fq.Enqueue(tlist4)
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
	tlist, err := suite.fq.Dequeue()
	if err != nil {
		assert.Fail(suite.T(), "Dequeue should not fail")
	}
	if tlist.Len() != 1 {
		assert.Fail(suite.T(), "Dequeue should return single task scheduling unit")
	}
	dqRes := tlist.Front().Value.(*resmgr.Task)
	assert.Equal(suite.T(), dqRes.JobId.Value, "job2", "Should get Job-2")

	tlist, err = suite.fq.Dequeue()
	if err != nil {
		assert.Fail(suite.T(), "Dequeue should not fail")
	}
	if tlist.Len() != 1 {
		assert.Fail(suite.T(), "Dequeue should return single task scheduling unit")
	}
	dqRes = tlist.Front().Value.(*resmgr.Task)
	assert.Equal(suite.T(), dqRes.JobId.Value, "job2", "Should get Job-2")
	assert.Equal(suite.T(), dqRes.Id.GetValue(), "job2-2", "Should get Job-2 and Instance Id 2")

	tlist, err = suite.fq.Dequeue()
	if err != nil {
		assert.Fail(suite.T(), "Dequeue should not fail")
	}
	if tlist.Len() != 1 {
		assert.Fail(suite.T(), "Dequeue should return single task scheduling unit")
	}
	dqRes = tlist.Front().Value.(*resmgr.Task)
	assert.Equal(suite.T(), dqRes.JobId.Value, "job1", "Should get Job-1")
	assert.Equal(suite.T(), dqRes.Id.GetValue(), "job1-2", "Should be instance 2")

	tlist, err = suite.fq.Dequeue()
	if err != nil {
		assert.Fail(suite.T(), "Dequeue should not fail")
	}
	if tlist.Len() != 1 {
		assert.Fail(suite.T(), "Dequeue should return single task scheduling unit")
	}
	dqRes = tlist.Front().Value.(*resmgr.Task)
	assert.Equal(suite.T(), dqRes.JobId.Value, "job1", "Should get Job-1")
	assert.Equal(suite.T(), dqRes.Id.GetValue(), "job1-1", "Should get Job-1 and instance 1")
}
