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
	"container/list"
	"fmt"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type MultiLevelListTestSuite struct {
	suite.Suite
	mll      MultiLevelList
	mapTasks map[string]*resmgr.Task
}

func (suite *MultiLevelListTestSuite) SetupTest() {
	suite.mll = NewMultiLevelList("multi-level-list", -1)
	suite.mapTasks = make(map[string]*resmgr.Task)
	suite.AddTasks()
}

func (suite *MultiLevelListTestSuite) AddTasks() {
	taskItem1 := CreateResmgrTask(
		&peloton.JobID{Value: "job1"},
		&peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", "job1", 1)},
		0)

	err := suite.mll.Push(0, taskItem1)
	suite.Nil(err)
	suite.mapTasks["job1-1"] = taskItem1
	suite.Equal(suite.mll.GetHighestLevel(), 0, "Highest Level should be 0")

	taskItem0 := CreateResmgrTask(
		&peloton.JobID{Value: "job2"},
		&peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", "job2", 1)},
		1)

	err = suite.mll.Push(1, taskItem0)
	suite.Nil(err)
	suite.mapTasks["job2-1"] = taskItem0
	suite.Equal(suite.mll.GetHighestLevel(), 1, "Highest Level should be 1")

	taskItem2 := CreateResmgrTask(
		&peloton.JobID{Value: "job1"},
		&peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", "job1", 2)},
		0)
	err = suite.mll.Push(0, taskItem2)
	suite.Nil(err)
	suite.mapTasks["job1-2"] = taskItem2
}

// createResmgrTask returns the resmgr task
func CreateResmgrTask(
	jobID *peloton.JobID,
	taskID *peloton.TaskID,
	priority uint32,
) *resmgr.Task {
	return &resmgr.Task{
		Name:     taskID.Value,
		Priority: priority,
		JobId:    jobID,
		Id:       taskID,
	}
}

func (suite *MultiLevelListTestSuite) TearDownTest() {
	fmt.Println("tearing down")
}

func TestPelotonPriorityMap(t *testing.T) {
	suite.Run(t, new(MultiLevelListTestSuite))
}

func (suite *MultiLevelListTestSuite) TestLength() {
	suite.Equal(suite.mll.Len(0), 2,
		"Length should be 2")
}

func (suite *MultiLevelListTestSuite) TestSize() {
	suite.Equal(3, suite.mll.Size(),
		"Size should be 3")
}

func (suite *MultiLevelListTestSuite) TestLevels() {
	levels := suite.mll.Levels()
	suite.Equal(2, len(levels))
	suite.Equal(0, levels[0])
	suite.Equal(1, levels[1])
}

func (suite *MultiLevelListTestSuite) TestPop() {
	t, err := suite.mll.Pop(0)
	suite.NoError(err)
	suite.Equal(t.(*resmgr.Task).JobId.Value, "job1",
		"Job 1 should be out")
	suite.Equal(t.(*resmgr.Task).Id.Value, "job1-1",
		"Job 1 , Instance 1 should be out")
	suite.Equal(suite.mll.Len(0), 1,
		"Length should be 1")
	t, err = suite.mll.Pop(0)
	suite.NoError(err)
	suite.Equal(t.(*resmgr.Task).JobId.Value, "job1",
		"Job 1 should be out")
	suite.NoError(err)
	t, err = suite.mll.Pop(0)
	suite.Error(err)
	suite.Contains(err.Error(), "No items found")
}

func (suite *MultiLevelListTestSuite) TestRemove() {
	suite.Equal(suite.mll.Len(0), 2,
		"Length should be 2")
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
	taskItem := CreateResmgrTask(
		&peloton.JobID{Value: "job3"},
		&peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", "job3", 1)},
		2)
	newList.PushBack(taskItem)
	taskItem1 := CreateResmgrTask(
		&peloton.JobID{Value: "job3"},
		&peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", "job3", 2)},
		2)
	newList.PushBack(taskItem1)

	err := suite.mll.PushList(2, newList)
	suite.Nil(err)
	suite.Equal(2, suite.mll.GetHighestLevel(),
		"Highest Level should be 2")
	e, err := suite.mll.Pop(2)
	suite.NoError(err)
	retTask := e.(*resmgr.Task)
	suite.Equal("job3-1", retTask.Id.Value)
	suite.Equal(false, suite.mll.IsEmpty(2),
		"Should Not be empty")
	e, err = suite.mll.Pop(2)
	suite.NoError(err)
	retTask = e.(*resmgr.Task)
	suite.Equal("job3-2", retTask.Id.Value)
	suite.Equal(true, suite.mll.IsEmpty(2),
		"Should be empty")
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

func (suite *MultiLevelListTestSuite) TestPeekItems() {
	// level 1 has 1 item
	es, err := suite.mll.PeekItems(1, 100)
	suite.NoError(err)
	suite.Equal(1, len(es))
	retTask := es[0].(*resmgr.Task)
	suite.Equal(retTask.Id.Value, "job2-1")

	// level 0 has 2 items
	es, err = suite.mll.PeekItems(0, 100)
	suite.NoError(err)
	suite.Equal(2, len(es))
	retTask = es[0].(*resmgr.Task)
	suite.Equal(retTask.Id.Value, "job1-1")

	// There is no level 3
	es, err = suite.mll.PeekItems(3, 100)
	suite.Error(err)
	suite.EqualError(err, "No items found in queue for priority 3")
}

func (suite *MultiLevelListTestSuite) TestPeekItemsReachedLimit() {
	// level 0 has 2 items
	es, err := suite.mll.PeekItems(0, 1)
	suite.NoError(err)
	suite.Equal(1, len(es))
	retTask := es[0].(*resmgr.Task)
	suite.Equal(retTask.Id.Value, "job1-1")
}

func (suite *MultiLevelListTestSuite) TestPeekItem() {
	// level 1 has 1 item
	es, err := suite.mll.PeekItem(1)
	suite.NoError(err)

	retTask := es.(*resmgr.Task)
	suite.Equal(retTask.Id.Value, "job2-1")

	// There is no level 3
	es, err = suite.mll.PeekItem(3)
	suite.Error(err)
	suite.Contains(err.Error(), "No items found in queue")
}
