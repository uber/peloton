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
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type QueueTestSuite struct {
	suite.Suite
}

func (suite *QueueTestSuite) SetupTest() {
}

func (suite *QueueTestSuite) TearDownTest() {
}

func TestQueue(t *testing.T) {
	suite.Run(t, new(QueueTestSuite))
}

func (suite *QueueTestSuite) TestEnqueueDequeueIntSuccess() {
	q := NewQueue("test_queue", reflect.TypeOf(int(0)), 100)

	suite.Equal(q.GetName(), "test_queue")
	suite.Equal(q.GetItemType(), reflect.TypeOf(int(0)))

	for i := 0; i < 100; i++ {
		err := q.Enqueue(i)
		suite.NoError(err)
	}

	for i := 0; i < 100; i++ {
		item, err := q.Dequeue(1 * time.Millisecond)
		suite.NoError(err)
		suite.Equal(item.(int), i)
	}
}

func (suite *QueueTestSuite) TestEnqueueDequeueStringSuccess() {
	var str string
	q := NewQueue("test_queue", reflect.TypeOf(str), 100)

	suite.Equal(q.GetName(), "test_queue")
	suite.Equal(q.GetItemType(), reflect.TypeOf(str))

	for i := 0; i < 100; i++ {
		err := q.Enqueue(strconv.Itoa(i))
		suite.NoError(err)
	}

	for i := 0; i < 100; i++ {
		item, err := q.Dequeue(1 * time.Millisecond)
		suite.NoError(err)
		val, err := strconv.Atoi(item.(string))
		suite.Equal(val, i)
	}
}

type TaskInfo struct {
	InstanceID int
	JobID      string
}

func (suite *QueueTestSuite) TestEnqueueDequeueStruct() {
	q := NewQueue("test_queue", reflect.TypeOf(TaskInfo{}), 100)

	for i := 0; i < 100; i++ {
		task := TaskInfo{
			InstanceID: i,
			JobID:      "test-job",
		}
		err := q.Enqueue(task)
		suite.NoError(err)
	}
	for i := 0; i < 100; i++ {
		item, err := q.Dequeue(1 * time.Millisecond)
		suite.NoError(err)
		task := item.(TaskInfo)
		suite.Equal(task.InstanceID, i)
		suite.Equal(task.JobID, "test-job")
	}
}

func (suite *QueueTestSuite) TestEnqueueDequeueStructPointer() {
	q := NewQueue("test_queue", reflect.TypeOf(TaskInfo{}), 100)

	for i := 0; i < 100; i++ {
		task := TaskInfo{
			InstanceID: i,
			JobID:      "test-job",
		}
		err := q.Enqueue(&task)
		suite.NoError(err)
	}
	for i := 0; i < 100; i++ {
		item, err := q.Dequeue(1 * time.Millisecond)
		suite.NoError(err)
		task := item.(*TaskInfo)
		suite.Equal(task.InstanceID, i)
		suite.Equal(task.JobID, "test-job")
	}
}

func (suite *QueueTestSuite) TestEnqueueMaxQueueSize() {
	q := NewQueue("test_queue", reflect.TypeOf(int(0)), 100)

	for i := 0; i < 100; i++ {
		err := q.Enqueue(i)
		suite.NoError(err)
	}

	err := q.Enqueue(100)
	suite.Error(err)
}

func (suite *QueueTestSuite) TestEnqueueInvalidItemType() {
	q := NewQueue("test_queue", reflect.TypeOf(int(0)), 100)

	str := "100"
	err := q.Enqueue(str)
	suite.Error(err)
}

func (suite *QueueTestSuite) TestDequeueTimedout() {
	q := NewQueue("test_queue", reflect.TypeOf(int(0)), 100)

	item, err := q.Dequeue(100 * time.Millisecond)
	suite.Error(err)
	_, timedOut := err.(DequeueTimeOutError)
	suite.True(timedOut)
	suite.Equal(item, nil)
}
