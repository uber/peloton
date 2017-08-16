package goalstate

import (
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"github.com/stretchr/testify/assert"
)

func TestSuggestAction(t *testing.T) {
	assert.Equal(t, _noAction, suggestAction(
		State{task.TaskState_RUNNING, 0},
		State{task.TaskState_RUNNING, 0},
	))

	assert.Equal(t, _stopAction, suggestAction(
		State{task.TaskState_RUNNING, 0},
		State{task.TaskState_RUNNING, 1},
	))

	assert.Equal(t, _stopAction, suggestAction(
		State{task.TaskState_RUNNING, 0},
		State{task.TaskState_KILLED, 0},
	))

	assert.Equal(t, _useGoalVersionAction, suggestAction(
		State{task.TaskState_KILLED, 0},
		State{task.TaskState_RUNNING, 1},
	))

	assert.Equal(t, _noAction, suggestAction(
		State{task.TaskState_KILLED, UnknownVersion},
		State{task.TaskState_RUNNING, 0},
	))

	assert.Equal(t, _noAction, suggestAction(
		State{task.TaskState_KILLED, 0},
		State{task.TaskState_RUNNING, UnknownVersion},
	))
}

func TestJobRun(t *testing.T) {
	j := &trackedJob{
		e:     &engine{},
		queue: newTimerQueue(),
	}

	stopChan := make(chan struct{})

	var done sync.WaitGroup
	done.Add(1)
	go func() {
		j.run(stopChan)
		done.Done()
	}()

	tt := &trackedTask{queueIndex: -1}

	j.scheduleTask(tt, time.Now())

	for {
		j.Lock()
		l := j.queue.pq.Len()
		j.Unlock()

		if l == 0 {
			break
		}

		time.Sleep(1 * time.Millisecond)
	}

	close(stopChan)

	done.Wait()
}

func TestJobRunTask(t *testing.T) {
	j := &trackedJob{
		e:     &engine{},
		queue: newTimerQueue(),
	}
	j.e.cfg.normalize()

	tt := newTrackedTask(j, 1)
	tt.updateRuntime(&task.RuntimeInfo{
		State: task.TaskState_RUNNING,
	})

	before := time.Now()

	j.runTaskAction(_useGoalVersionAction, tt)
	assert.Equal(t, _useGoalVersionAction, tt.lastAction)
	assert.Equal(t, task.TaskState_RUNNING, tt.lastState.State)
	assert.True(t, tt.lastActionTime.After(before))
	assert.True(t, tt.deadline().After(time.Now()))
	assert.Equal(t, tt, j.queue.popIfReady())

	j.runTaskAction(_noAction, tt)
	assert.Equal(t, _noAction, tt.lastAction)
	assert.Equal(t, task.TaskState_RUNNING, tt.lastState.State)
	assert.True(t, tt.lastActionTime.After(before))
	assert.Equal(t, time.Time{}, tt.deadline())
	assert.Nil(t, j.queue.popIfReady())
}
