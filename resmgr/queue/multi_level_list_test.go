package queue

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"peloton/api/peloton"
	"peloton/private/resmgr"
	"testing"
)

type MultiLevelListTestSuite struct {
	suite.Suite
	mll      *MultiLevelList
	mapTasks map[string]*resmgr.Task
}

func (suite *MultiLevelListTestSuite) SetupTest() {
	suite.mll = NewMultiLevelList()
	suite.mapTasks = make(map[string]*resmgr.Task)
	suite.AddTasks()
}

func (suite *MultiLevelListTestSuite) AddTasks() {
	jobID1 := &peloton.JobID{
		Value: "job1",
	}
	taskID1 := &peloton.TaskID{
		Value: fmt.Sprintf("%s-%d", jobID1.Value, 1),
	}
	taskItem1 := &resmgr.Task{
		Name:     "job1-1",
		Priority: 0,
		JobId:    jobID1,
		Id:       taskID1,
	}

	suite.mll.Push(0, taskItem1)
	suite.mapTasks["job1-1"] = taskItem1
	assert.Equal(suite.T(), suite.mll.GetHighestLevel(), 0, "Highest Level should be 0")

	jobID0 := &peloton.JobID{
		Value: "job2",
	}
	taskID0 := &peloton.TaskID{
		Value: fmt.Sprintf("%s-%d", jobID0.Value, 1),
	}
	taskItem0 := &resmgr.Task{
		Name:     "job2-1",
		Priority: 1,
		JobId:    jobID0,
		Id:       taskID0,
	}

	suite.mll.Push(1, taskItem0)
	suite.mapTasks["job2-1"] = taskItem0
	assert.Equal(suite.T(), suite.mll.GetHighestLevel(), 1, "Highest Level should be 1")

	jobID2 := &peloton.JobID{
		Value: "job1",
	}
	taskID2 := &peloton.TaskID{
		Value: fmt.Sprintf("%s-%d", jobID2.Value, 2),
	}
	taskItem2 := &resmgr.Task{
		Name:     "job1-2",
		Priority: 0,
		JobId:    jobID2,
		Id:       taskID2,
	}
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
	assert.Equal(suite.T(), t.(*resmgr.Task).JobId.Value, "job1", "Job 1 should be out")
	assert.Equal(suite.T(), t.(*resmgr.Task).Id.Value, "job1-1", "Job 1 , Instance 1 should be out")
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
