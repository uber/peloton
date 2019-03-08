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
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/resmgr/queue/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
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
	enq1 := CreateResmgrTask(
		&peloton.JobID{Value: "job1"},
		&peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", "job1", 1)},
		0)
	gang1 := &resmgrsvc.Gang{
		Tasks: []*resmgr.Task{enq1},
	}
	suite.fq.Enqueue(gang1)

	// Task - 2
	enq2 := CreateResmgrTask(
		&peloton.JobID{Value: "job1"},
		&peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", "job1", 2)},
		1)
	gang2 := &resmgrsvc.Gang{
		Tasks: []*resmgr.Task{enq2},
	}
	suite.fq.Enqueue(gang2)

	// Task - 3
	enq3 := CreateResmgrTask(
		&peloton.JobID{Value: "job2"},
		&peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", "job2", 1)},
		2)
	gang3 := &resmgrsvc.Gang{
		Tasks: []*resmgr.Task{enq3},
	}
	suite.fq.Enqueue(gang3)

	// Task - 4
	enq4 := CreateResmgrTask(
		&peloton.JobID{Value: "job2"},
		&peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", "job2", 2)},
		2)
	gang4 := &resmgrsvc.Gang{
		Tasks: []*resmgr.Task{enq4},
	}
	suite.fq.Enqueue(gang4)
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

func (suite *FifoQueueTestSuite) TestEnqueueError() {
	err := suite.fq.Enqueue(nil)
	suite.Error(err)
	suite.EqualError(err, "enqueue of empty list")
}

func (suite *FifoQueueTestSuite) TestDequeueRetryWithNilItem() {
	q, list := suite.createQueueWithMultiLevelList()
	gomock.InOrder(
		list.EXPECT().GetHighestLevel().Return(5),
		list.EXPECT().Pop(gomock.Any()).Return(nil, errors.New("error")),
		list.EXPECT().GetHighestLevel().Return(0),
		list.EXPECT().GetHighestLevel().Return(0),
		list.EXPECT().Pop(gomock.Any()).Return(nil, nil),
	)
	_, err := q.Dequeue()
	suite.EqualError(err, "dequeue failed")

}

func (suite *FifoQueueTestSuite) TestDequeueRetryError() {
	q, list := suite.createQueueWithMultiLevelList()
	gomock.InOrder(
		list.EXPECT().GetHighestLevel().Return(5),
		list.EXPECT().Pop(gomock.Any()).Return(nil, errors.New("error")),
		list.EXPECT().GetHighestLevel().Return(0),
		list.EXPECT().GetHighestLevel().Return(0),
		list.EXPECT().Pop(gomock.Any()).Return(nil, errors.New("error in POP")),
		list.EXPECT().GetHighestLevel().Return(0),
	)
	_, err := q.Dequeue()
	suite.EqualError(err, "error in POP")

}

func (suite *FifoQueueTestSuite) TestPeekItemsError() {
	q, list := suite.createQueueWithMultiLevelList()
	gomock.InOrder(
		list.EXPECT().GetHighestLevel().Return(5),
		list.EXPECT().PeekItems(gomock.Any(), gomock.Any()).Return(nil, ErrorQueueEmpty("no item")),
		list.EXPECT().PeekItems(gomock.Any(), gomock.Any()).Return(nil, errors.New("error")),
	)
	_, err := q.Peek(5)
	suite.EqualError(err, "peek failed err: error")
}

func (suite *FifoQueueTestSuite) TestPeekNoItemsError() {
	q, list := suite.createQueueWithMultiLevelList()
	gomock.InOrder(list.EXPECT().GetHighestLevel().Return(0))
	_, err := q.Peek(0)
	suite.EqualError(err, "peek failed, queue is empty")
}

func (suite *FifoQueueTestSuite) createQueueWithMultiLevelList() (*PriorityQueue, *mocks.MockMultiLevelList) {
	list := mocks.NewMockMultiLevelList(gomock.NewController(suite.T()))
	return &PriorityQueue{
		list: list,
	}, list
}
