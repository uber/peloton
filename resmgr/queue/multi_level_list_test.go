package queue

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"peloton/api/job"
	"peloton/api/task"
	"testing"
)

type MultiLevelListTestSuite struct {
	suite.Suite
	mll      *MultiLevelList
	mapTasks map[string]*TaskItem
}

func (suite *MultiLevelListTestSuite) SetupTest() {
	suite.mll = NewMultiLevelList()
	suite.mapTasks = make(map[string]*TaskItem)
	suite.AddTasks()
}

func (suite *MultiLevelListTestSuite) AddTasks() {
	taskInfo1 := task.TaskInfo{
		InstanceId: 1,
		JobId: &job.JobID{
			Value: "job1",
		},
	}
	taskid1 := fmt.Sprintf("%s-%d", taskInfo1.JobId.Value, taskInfo1.InstanceId)
	taskItem1 := NewTaskItem(&taskInfo1, 0, taskid1)
	suite.mll.Push(0, taskItem1)
	suite.mapTasks["job1-1"] = taskItem1
	assert.Equal(suite.T(), suite.mll.GetHighestLevel(), 0, "Highest Level should be 0")

	taskInfo0 := task.TaskInfo{
		InstanceId: 1,
		JobId: &job.JobID{
			Value: "job2",
		},
	}
	taskid0 := fmt.Sprintf("%s-%d", taskInfo0.JobId.Value, taskInfo0.InstanceId)
	taskItem0 := NewTaskItem(&taskInfo0, 1, taskid0)
	suite.mll.Push(1, taskItem0)
	suite.mapTasks["job2-1"] = taskItem0
	assert.Equal(suite.T(), suite.mll.GetHighestLevel(), 1, "Highest Level should be 1")

	taskInfo2 := task.TaskInfo{
		InstanceId: 2,
		JobId: &job.JobID{
			Value: "job1",
		},
	}
	taskid2 := fmt.Sprintf("%s-%d", taskInfo2.JobId.Value, taskInfo2.InstanceId)
	taskItem2 := NewTaskItem(&taskInfo2, 0, taskid2)
	suite.mll.Push(0, taskItem2)
	suite.mapTasks["job1-2"] = taskItem2
}

func (suite *MultiLevelListTestSuite) TearDownTest() {
	fmt.Println("tearing down")
}

func TestPelotonPriorityMap(t *testing.T) {
	suite.Run(t, new(MultiLevelListTestSuite))
}

func (suite *MultiLevelListTestSuite) TestLength() {
	assert.Equal(suite.T(), suite.mll.Len(0), 2, "Length should be 2")
}

func (suite *MultiLevelListTestSuite) TestPop() {
	t, _ := suite.mll.Pop(0)
	assert.Equal(suite.T(), t.(*TaskItem).TaskInfo.JobId.Value, "job1", "Job 1 should be out")
	assert.Equal(suite.T(), t.(*TaskItem).TaskInfo.InstanceId, uint32(1), "Job 1 , Instance 1 should be out")
	assert.Equal(suite.T(), suite.mll.Len(0), 1, "Length should be 1")
}

func (suite *MultiLevelListTestSuite) TestRemove() {
	assert.Equal(suite.T(), suite.mll.Len(0), 2, "Length should be 2")
	taskItem := suite.mapTasks["job1-2"]
	res := suite.mll.Remove(0, taskItem)

	assert.Equal(suite.T(), res, nil, "Should be able to remove item")
	assert.Equal(suite.T(), suite.mll.Len(0), 1, "Length should be 0")
}

func (suite *MultiLevelListTestSuite) TestRemoveTasks() {
	assert.Equal(suite.T(), suite.mll.Len(0), 2, "Length should be 2")
	assert.Equal(suite.T(), suite.mll.Len(1), 1, "Length should be 1")
	mapValues := make(map[interface{}]bool)
	for _, v := range suite.mapTasks {
		if v.Priority == 0 {
			mapValues[v] = true
		}
	}

	res, _, _ := suite.mll.RemoveItems(mapValues, 0)
	assert.Equal(suite.T(), res, true, "Should be able to remove item")
	assert.Equal(suite.T(), suite.mll.Len(0), 0, "Length should be 0")
}

func (suite *MultiLevelListTestSuite) PriorityMapTestSuite() {
	suite.mll.Pop(0)
	suite.mll.Pop(0)
	assert.Equal(suite.T(), suite.mll.IsEmpty(0), true, "Should be empty")
}
