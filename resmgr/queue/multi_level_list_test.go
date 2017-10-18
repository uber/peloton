package queue

import (
	"container/list"
	"fmt"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type MultiLevelListTestSuite struct {
	suite.Suite
	mll      *MultiLevelList
	mapTasks map[string]*resmgr.Task
}

func (suite *MultiLevelListTestSuite) SetupTest() {
	suite.mll = NewMultiLevelList("multi-level-list", -1)
	suite.mapTasks = make(map[string]*resmgr.Task)
	suite.AddTasks()
}

func (suite *MultiLevelListTestSuite) AddTasks() {
	jobID1 := &peloton.JobID{
		Value: "job1",
	}
	taskID1 := util.BuildTaskID(jobID1, 1)
	taskItem1 := &resmgr.Task{
		Name:     taskID1.Value,
		Priority: 0,
		JobId:    jobID1,
		Id:       taskID1,
	}

	err := suite.mll.Push(0, taskItem1)
	suite.Nil(err)
	suite.mapTasks["job1-1"] = taskItem1
	suite.Equal(suite.mll.GetHighestLevel(), 0, "Highest Level should be 0")

	jobID0 := &peloton.JobID{
		Value: "job2",
	}
	taskID0 := util.BuildTaskID(jobID0, 1)
	taskItem0 := &resmgr.Task{
		Name:     taskID0.Value,
		Priority: 1,
		JobId:    jobID0,
		Id:       taskID0,
	}

	err = suite.mll.Push(1, taskItem0)
	suite.Nil(err)
	suite.mapTasks["job2-1"] = taskItem0
	suite.Equal(suite.mll.GetHighestLevel(), 1, "Highest Level should be 1")

	jobID2 := &peloton.JobID{
		Value: "job1",
	}
	taskID2 := util.BuildTaskID(jobID2, 2)
	taskItem2 := &resmgr.Task{
		Name:     taskID2.Value,
		Priority: 0,
		JobId:    jobID2,
		Id:       taskID2,
	}
	err = suite.mll.Push(0, taskItem2)
	suite.Nil(err)
	suite.mapTasks["job1-2"] = taskItem2
}

func (suite *MultiLevelListTestSuite) TearDownTest() {
	fmt.Println("tearing down")
}

func TestPelotonPriorityMap(t *testing.T) {
	suite.Run(t, new(MultiLevelListTestSuite))
}

func (suite *MultiLevelListTestSuite) TestLength() {
	suite.Equal(suite.mll.Len(0), 2, "Length should be 2")
}

func (suite *MultiLevelListTestSuite) TestSize() {
	suite.Equal(3, suite.mll.Size(), "Size should be 3")
}

func (suite *MultiLevelListTestSuite) TestLevels() {
	levels := suite.mll.Levels()
	suite.Equal(2, len(levels))
	suite.Equal(0, levels[0])
	suite.Equal(1, levels[1])
}

func (suite *MultiLevelListTestSuite) TestPop() {
	t, _ := suite.mll.Pop(0)
	suite.Equal(t.(*resmgr.Task).JobId.Value, "job1", "Job 1 should be out")
	suite.Equal(t.(*resmgr.Task).Id.Value, "job1-1", "Job 1 , Instance 1 should be out")
	suite.Equal(suite.mll.Len(0), 1, "Length should be 1")
}

func (suite *MultiLevelListTestSuite) TestRemove() {
	suite.Equal(suite.mll.Len(0), 2, "Length should be 2")
	taskItem := suite.mapTasks["job1-2"]
	res := suite.mll.Remove(0, taskItem)

	suite.Equal(res, nil, "Should be able to remove item")
	suite.Equal(suite.mll.Len(0), 1, "Length should be 0")
}

func (suite *MultiLevelListTestSuite) TestRemoveTasks() {
	suite.Equal(suite.mll.Len(0), 2, "Length should be 2")
	suite.Equal(suite.mll.Len(1), 1, "Length should be 1")
	mapValues := make(map[interface{}]bool)
	for _, v := range suite.mapTasks {
		if v.Priority == 0 {
			mapValues[v] = true
		}
	}

	res, _, _ := suite.mll.RemoveItems(mapValues, 0)
	suite.Equal(res, true, "Should be able to remove item")
	suite.Equal(suite.mll.Len(0), 0, "Length should be 0")
}

func (suite *MultiLevelListTestSuite) PriorityMapTestSuite() {
	suite.mll.Pop(0)
	suite.mll.Pop(0)
	suite.Equal(suite.mll.IsEmpty(0), true, "Should be empty")
}

func TestMultiLevelList_Push(t *testing.T) {
	list := NewMultiLevelList("multi-level-list", 3)
	err := list.Push(0, 0)
	assert.NoError(t, err)
	err = list.Push(0, 1)
	assert.NoError(t, err)
	err = list.Push(0, 2)
	assert.NoError(t, err)

	val, err := list.Pop(0)
	assert.NoError(t, err)
	assert.Equal(t, 0, val.(int))

	val, err = list.Pop(0)
	assert.NoError(t, err)
	assert.Equal(t, 1, val.(int))

	val, err = list.Pop(0)
	assert.NoError(t, err)
	assert.Equal(t, 2, val.(int))
}

func (suite *MultiLevelListTestSuite) TestPushList() {
	newList := list.New()
	jobID := &peloton.JobID{
		Value: "job3",
	}
	taskID := util.BuildTaskID(jobID, 1)
	taskItem := &resmgr.Task{
		Name:     taskID.Value,
		Priority: 2,
		JobId:    jobID,
		Id:       taskID,
	}
	newList.PushBack(taskItem)

	taskID = util.BuildTaskID(jobID, 2)
	taskItem = &resmgr.Task{
		Name:     taskID.Value,
		Priority: 2,
		JobId:    jobID,
		Id:       taskID,
	}
	newList.PushBack(taskItem)
	err := suite.mll.PushList(2, newList)
	suite.Nil(err)
	suite.Equal(2, suite.mll.GetHighestLevel(), "Highest Level should be 2")
	e, err := suite.mll.Pop(2)
	suite.NoError(err)
	retTask := e.(*resmgr.Task)
	suite.Equal("job3-1", retTask.Id.Value)
	suite.Equal(false, suite.mll.IsEmpty(2), "Should Not be empty")
	e, err = suite.mll.Pop(2)
	suite.NoError(err)
	retTask = e.(*resmgr.Task)
	suite.Equal("job3-2", retTask.Id.Value)
	suite.Equal(true, suite.mll.IsEmpty(2), "Should be empty")
}

func TestMultiLevelList_PushList_at_max_capacity(t *testing.T) {
	list := NewMultiLevelList("multi-level-list", 1)
	err := list.Push(0, 1)
	assert.NoError(t, err)
	err = list.Push(0, 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "list size limit reached")
}

func (suite *MultiLevelListTestSuite) TestPeek() {
	e, err := suite.mll.PeekItem(1)
	suite.NoError(err)
	retTask := e.(*resmgr.Task)
	suite.Equal(retTask.Id.Value, "job2-1")
}
