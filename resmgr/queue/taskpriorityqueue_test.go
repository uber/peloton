package queue

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"peloton/api/job"
	"peloton/api/task"
	"testing"
)

type PriorityQueueTestSuite struct {
	suite.Suite
	pq *PriorityQueue
}

func (suite *PriorityQueueTestSuite) SetupTest() {
	suite.pq = InitPriorityQueue()
	suite.AddTasks()
}

func (suite *PriorityQueueTestSuite) AddTasks() {
	taskInfo := task.TaskInfo{
		InstanceId: 1,
		JobId: &job.JobID{
			Value: "job1",
		},
	}
	taskItem := NewTaskItem(taskInfo, 0)
	suite.pq.Push(taskItem)
	taskInfo = task.TaskInfo{
		InstanceId: 2,
		JobId: &job.JobID{
			Value: "job1",
		},
	}
	taskItem = NewTaskItem(taskInfo, 0)
	suite.pq.Push(taskItem)
}

func (suite *PriorityQueueTestSuite) TearDownTest() {
	fmt.Println("tearing down")
}

func TestPelotonPriorityQueue(t *testing.T) {
	suite.Run(t, new(PriorityQueueTestSuite))
}

func (suite *PriorityQueueTestSuite) TestLength() {
	assert.Equal(suite.T(), suite.pq.Len(0), 2, "Length should be 2")
}

func (suite *PriorityQueueTestSuite) TestPop() {
	t, _ := suite.pq.Pop(0)
	assert.Equal(suite.T(), t.taskInfo.JobId.Value, "job1", "Job 1 should be out")
	assert.Equal(suite.T(), t.taskInfo.InstanceId, uint32(1), "Job 1 , Instance 1 should be out")
	assert.Equal(suite.T(), suite.pq.Len(0), 1, "Length should be 1")
}

func (suite *PriorityQueueTestSuite) TestRemove() {
	assert.Equal(suite.T(), suite.pq.Len(0), 2, "Length should be 2")
	taskInfo := task.TaskInfo{
		InstanceId: 2,
		JobId: &job.JobID{
			Value: "job1",
		},
	}
	taskItem := NewTaskItem(taskInfo, 0)
	res, _ := suite.pq.Remove(taskItem)
	assert.Equal(suite.T(), res, true, "Should be able to remove item")
	assert.Equal(suite.T(), suite.pq.Len(0), 1, "Length should be 0")
}

func (suite *PriorityQueueTestSuite) TestRemoveTasks() {
	assert.Equal(suite.T(), suite.pq.Len(0), 2, "Length should be 2")
	taskInfo := task.TaskInfo{
		InstanceId: 2,
		JobId: &job.JobID{
			Value: "job1",
		},
	}
	taskItem := NewTaskItem(taskInfo, 0)
	mapTasks := make(map[string]*TaskItem)
	mapTasks[taskItem.taskID] = taskItem

	taskInfo = task.TaskInfo{
		InstanceId: 1,
		JobId: &job.JobID{
			Value: "job1",
		},
	}
	taskItem = NewTaskItem(taskInfo, 0)
	mapTasks[taskItem.taskID] = taskItem

	res, _ := suite.pq.RemoveTasks(mapTasks, 0)

	assert.Equal(suite.T(), res, true, "Should be able to remove item")
	assert.Equal(suite.T(), suite.pq.Len(0), 0, "Length should be 0")
}

func (suite *PriorityQueueTestSuite) TestIsEmpty() {
	suite.pq.Pop(0)
	suite.pq.Pop(0)
	assert.Equal(suite.T(), suite.pq.IsEmpty(0), true, "Should be empty")
}
