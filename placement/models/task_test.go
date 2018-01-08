package models

import (
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"github.com/stretchr/testify/assert"
)

func setupTaskVariables() (time.Time, *resmgrsvc.Gang, *resmgr.Task, *Task) {
	resmgrTask := &resmgr.Task{
		Name: "task",
	}
	resmgrGang := &resmgrsvc.Gang{
		Tasks: []*resmgr.Task{
			resmgrTask,
		},
	}
	now := time.Now()
	task := NewTask(resmgrGang, resmgrTask, now.Add(5*time.Second), 3)
	return now, resmgrGang, resmgrTask, task
}

func TestTask_Gang(t *testing.T) {
	_, resmgrGang, _, task := setupTaskVariables()
	assert.Equal(t, resmgrGang, task.GetGang())
}

func TestTask_Task(t *testing.T) {
	_, _, resmgrTask, task := setupTaskVariables()
	assert.Equal(t, resmgrTask, task.GetTask())
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
}

func TestTask_PastDeadline(t *testing.T) {
	now, _, _, task := setupTaskVariables()
	assert.False(t, task.PastDeadline(now))
	assert.False(t, task.PastDeadline(now.Add(2*time.Second)))
	assert.False(t, task.PastDeadline(now.Add(4*time.Second)))
	assert.True(t, task.PastDeadline(now.Add(6*time.Second)))
}
