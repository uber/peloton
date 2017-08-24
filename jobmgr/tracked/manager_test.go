package tracked

import (
	"context"
	"sync"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
)

func TestManagerAddAndGet(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	m := &manager{
		jobs: map[string]*job{},
	}

	assert.Nil(t, m.GetJob(jobID))

	j := m.AddJob(jobID)
	assert.NotNil(t, j)

	assert.Equal(t, j, m.GetJob(jobID))
	assert.Equal(t, j, m.AddJob(jobID))
}

func TestManagerScheduleAndDequeueTasks(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	m := &manager{
		jobs:             map[string]*job{},
		taskQueue:        newDeadlineQueue(),
		taskQueueChanged: make(chan struct{}, 1),
	}

	j := m.AddJob(jobID)

	c := 100
	var wg sync.WaitGroup
	wg.Add(c)

	for i := 0; i < c; i++ {
		go func() {
			tt := m.WaitForScheduledTask(nil)
			assert.NotNil(t, tt)
			wg.Done()
		}()
	}

	go func() {
		for i := 0; i < c; i++ {
			j.SetTask(uint32(i), nil)
			m.ScheduleTask(j.GetTask(uint32(i)), time.Now())
		}
	}()

	wg.Wait()
}

func TestManagerOnEvents(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskStoreMock := store_mocks.NewMockTaskStore(ctrl)

	m := &manager{
		jobs:             map[string]*job{},
		taskQueue:        newDeadlineQueue(),
		taskQueueChanged: make(chan struct{}, 1),
		taskStore:        taskStoreMock,
	}

	taskStoreMock.EXPECT().GetTaskByID(context.Background(), "3c8a3c3e-71e3-49c5-9aed-2929823f595c-1").
		Return(&pb_task.TaskInfo{
			Runtime: &pb_task.RuntimeInfo{
				State: pb_task.TaskState_RUNNING,
			},
		}, nil)

	m.OnEvents([]*pb_eventstream.Event{{
		Type: pb_eventstream.Event_MESOS_TASK_STATUS,
		MesosTaskStatus: &mesos_v1.TaskStatus{
			TaskId: &mesos_v1.TaskID{
				Value: &[]string{"3c8a3c3e-71e3-49c5-9aed-2929823f595c-1-3c8a3c3e-71e3-49c5-9aed-2929823f5957"}[0],
			},
			State: &[]mesos_v1.TaskState{mesos_v1.TaskState_TASK_RUNNING}[0],
		},
		Offset: 5,
	}})

	assert.Equal(t, uint64(5), m.progress.Load())
}
