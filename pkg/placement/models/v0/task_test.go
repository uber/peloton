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

package models_v0

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
)

func setupTaskVariables() (time.Time, *resmgrsvc.Gang, *resmgr.Task, *TaskV0) {
	resmgrTask := &resmgr.Task{
		Name: "task",
	}
	resmgrGang := &resmgrsvc.Gang{
		Tasks: []*resmgr.Task{
			resmgrTask,
		},
	}
	now, _ := time.Parse("RFC3339", "2006-01-02T15:04:05Z07:00")
	task := NewTask(resmgrGang, resmgrTask, now.Add(5*time.Second), now, 3)

	return now, resmgrGang, resmgrTask, task
}

func TestTask_Gang(t *testing.T) {
	_, resmgrGang, _, task := setupTaskVariables()
	assert.Equal(t, resmgrGang, task.GetGang())

	task.SetGang(&resmgrsvc.Gang{
		Tasks: append(resmgrGang.GetTasks(), &resmgr.Task{
			Name: "task2",
		}),
	})
	assert.Equal(t, 2, len(task.GetGang().GetTasks()))
}

func TestTask_Task(t *testing.T) {
	_, _, resmgrTask, task := setupTaskVariables()
	assert.Equal(t, resmgrTask, task.GetTask())

	resmgrTask = &resmgr.Task{
		Name: "task1",
	}
	task.SetTask(resmgrTask)
	assert.Equal(t, "task1", task.GetTask().GetName())
}

func TestTask_DataAndSetData(t *testing.T) {
	_, _, _, task := setupTaskVariables()
	assert.Nil(t, task.Data())
	task.SetData(42)
	assert.NotNil(t, task.Data())
}

func TestTask_PastMaxRounds(t *testing.T) {
	_, _, _, task := setupTaskVariables()
	assert.False(t, task.PastMaxRounds())
	task.IncRounds()
	assert.False(t, task.PastMaxRounds())
	task.IncRounds()
	assert.False(t, task.PastMaxRounds())
	task.IncRounds()
	assert.True(t, task.PastMaxRounds())
	assert.Equal(t, task.GetRounds(), 3)
	task.SetRounds(5)
	assert.Equal(t, task.GetRounds(), 5)
}

func TestTask_PastDeadline(t *testing.T) {
	now, _, _, task := setupTaskVariables()
	assert.False(t, task.PastDeadline(now))
	assert.False(t, task.PastDeadline(now.Add(2*time.Second)))
	assert.False(t, task.PastDeadline(now.Add(4*time.Second)))
	assert.True(t, task.PastDeadline(now.Add(6*time.Second)))
	task.SetDeadline(now.Add(1 * time.Second))
	assert.Equal(t, 1, task.GetDeadline().Second())
}
