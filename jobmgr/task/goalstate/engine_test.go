package goalstate

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
)

func TestEngineOnEvents(t *testing.T) {
	e := &engine{
		tracker: newTracker(),
		queue:   newTimerQueue(),
	}

	jmt, err := e.tracker.addTask(&task.TaskInfo{
		JobId: &peloton.JobID{
			Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c",
		},
		Runtime:    &task.RuntimeInfo{},
		InstanceId: 1,
	})
	assert.NotNil(t, jmt)
	assert.NoError(t, err)

	e.OnEvents([]*pb_eventstream.Event{{
		MesosTaskStatus: &mesos_v1.TaskStatus{
			TaskId: &mesos_v1.TaskID{
				Value: &[]string{"3c8a3c3e-71e3-49c5-9aed-2929823f595c-1-3c8a3c3e-71e3-49c5-9aed-2929823f5957"}[0],
			},
		},
		Offset: 5,
	}})

	assert.Equal(t, uint64(5), e.progress.Load())
	assert.Equal(t, jmt, e.queue.popIfReady())
}

func TestEngineUpdateTaskGoalState(t *testing.T) {
	e := &engine{
		tracker: newTracker(),
		queue:   newTimerQueue(),
	}

	jmt, err := e.tracker.addTask(&task.TaskInfo{
		JobId: &peloton.JobID{
			Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c",
		},
		Runtime:    &task.RuntimeInfo{},
		InstanceId: 1,
	})
	assert.NotNil(t, jmt)
	assert.NoError(t, err)

	assert.NoError(t, e.UpdateTaskGoalState(context.Background(), &task.TaskInfo{
		JobId: &peloton.JobID{
			Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c",
		},
		InstanceId: 1,
		Runtime: &task.RuntimeInfo{
			GoalState:            task.TaskState_PREEMPTING,
			DesiredConfigVersion: 42,
			ConfigVersion:        42,
		},
	}))

	assert.Equal(t, jmt, e.queue.popIfReady())
}

func TestEngineSyncFromDB(t *testing.T) {
	ctrl := gomock.NewController(t)

	jobstoreMock := store_mocks.NewMockJobStore(ctrl)
	taskstoreMock := store_mocks.NewMockTaskStore(ctrl)

	e := &engine{
		tracker:   newTracker(),
		jobStore:  jobstoreMock,
		taskStore: taskstoreMock,
		queue:     newTimerQueue(),
	}

	jobstoreMock.EXPECT().GetAllJobs(gomock.Any()).Return(map[string]*job.RuntimeInfo{
		"3c8a3c3e-71e3-49c5-9aed-2929823f595c": nil,
	}, nil)

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	taskstoreMock.EXPECT().GetTasksForJob(gomock.Any(), jobID).
		Return(map[uint32]*task.TaskInfo{
			1: {
				JobId:      jobID,
				InstanceId: 1,
				Runtime: &task.RuntimeInfo{
					GoalState:            task.TaskState_RUNNING,
					DesiredConfigVersion: 42,
					ConfigVersion:        42,
				},
			},
		}, nil)

	e.syncFromDB(context.Background())

	jm := e.tracker.getTask(&peloton.TaskID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c-1"})
	assert.NotNil(t, jm)

	assert.Equal(t, jm, e.queue.popIfReady())
}

func TestEngineRunTask(t *testing.T) {
	e := &engine{
		queue: newTimerQueue(),
	}
	e.cfg.normalize()

	jmt, err := newJMTask(&task.TaskInfo{
		JobId: &peloton.JobID{
			Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c",
		},
		Runtime: &task.RuntimeInfo{
			State: task.TaskState_RUNNING,
		},
		InstanceId: 1,
	})
	assert.NoError(t, err)

	before := time.Now()

	e.runTask(_useGoalVersionAction, jmt)
	assert.Equal(t, _useGoalVersionAction, jmt.lastAction)
	assert.Equal(t, task.TaskState_RUNNING, jmt.lastState.State)
	assert.True(t, jmt.lastActionTime.After(before))
	assert.True(t, jmt.timeout().After(time.Now()))
	assert.Equal(t, jmt, e.queue.popIfReady())

	e.runTask(_noAction, jmt)
	assert.Equal(t, _noAction, jmt.lastAction)
	assert.Equal(t, task.TaskState_RUNNING, jmt.lastState.State)
	assert.True(t, jmt.lastActionTime.After(before))
	assert.Equal(t, time.Time{}, jmt.timeout())
	assert.Nil(t, e.queue.popIfReady())
}

func TestEngineRun(t *testing.T) {
	e := &engine{
		queue:    newTimerQueue(),
		stopChan: make(chan struct{}),
	}

	var done sync.WaitGroup
	done.Add(1)
	go func() {
		e.run()
		done.Done()
	}()

	jmt := &jmTask{queueIndex: -1}

	e.scheduleTask(jmt, time.Now())

	for {
		e.Lock()
		l := e.queue.pq.Len()
		e.Unlock()

		if l == 0 {
			break
		}
	}

	close(e.stopChan)

	done.Wait()
}
