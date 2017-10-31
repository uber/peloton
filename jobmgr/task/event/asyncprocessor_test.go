package event

import (
	"context"
	"sync"
	"testing"
	"time"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	mocks2 "code.uber.internal/infra/peloton/jobmgr/task/event/mocks"
	"code.uber.internal/infra/peloton/jobmgr/tracked/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

var uuidStr = uuid.NewUUID().String()

func TestAddEvent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	wg := sync.WaitGroup{}

	mockStatusProcessor := mocks2.NewMockStatusProcessor(ctrl)
	bep := newBucketEventProcessor(mockStatusProcessor, 1, 10)

	mevt := &pb_eventstream.Event{
		Type: pb_eventstream.Event_MESOS_TASK_STATUS,
		MesosTaskStatus: &mesos.TaskStatus{
			TaskId: util.BuildMesosTaskID(util.BuildTaskID(&peloton.JobID{
				Value: "my-job",
			}, 0), uuidStr),
		},
	}
	mockStatusProcessor.EXPECT().ProcessStatusUpdate(gomock.Any(), mevt).Return(nil)
	mockStatusProcessor.EXPECT().ProcessListeners(mevt).Do(func(interface{}) {
		wg.Done()
	})

	tevt := &pb_eventstream.Event{
		Type: pb_eventstream.Event_PELOTON_TASK_EVENT,
		PelotonTaskEvent: &task.TaskEvent{
			TaskId: util.BuildTaskID(&peloton.JobID{
				Value: "my-job",
			}, 0),
		},
	}
	mockStatusProcessor.EXPECT().ProcessStatusUpdate(gomock.Any(), tevt).Return(nil)
	mockStatusProcessor.EXPECT().ProcessListeners(tevt).Do(func(interface{}) {
		wg.Done()
	})

	wg.Add(2)
	bep.addEvent(mevt)
	bep.addEvent(tevt)

	// Yield to let the StatusProcessor get the events.
	wg.Wait()
}

type eventListener struct {
	sync.RWMutex
	sync.WaitGroup
	progress uint64
}

func (*eventListener) Start() {}

func (*eventListener) Stop() {}

func (el *eventListener) OnEvents(events []*pb_eventstream.Event) {
	el.Lock()
	defer el.Unlock()

	for _, e := range events {
		if el.progress < e.Offset {
			el.progress = e.Offset
		}
		el.Done()
	}
}

func (el *eventListener) OnEvent(event *pb_eventstream.Event) {}

func (el *eventListener) GetEventProgress() uint64 {
	el.RLock()
	defer el.RUnlock()

	return el.progress
}

func TestBucketEventProcessor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTrackedManager := mocks.NewMockManager(ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)

	el := &eventListener{}

	handler := &statusUpdate{
		trackedManager: mockTrackedManager,
		taskStore:      mockTaskStore,
		metrics:        NewMetrics(tally.NoopScope),
		listeners:      []Listener{el},
		now:            time.Now,
	}
	var offset uint64
	applier := newBucketEventProcessor(handler, 15, 100)

	jobID := &peloton.JobID{Value: "Test"}
	n := uint32(243)

	for i := uint32(0); i < n; i++ {
		mesosTaskID := util.BuildMesosTaskID(util.BuildTaskID(jobID, i), uuidStr)
		mockTrackedManager.EXPECT().GetTaskRuntime(context.Background(), jobID, i).Return(&task.RuntimeInfo{
			MesosTaskId: mesosTaskID,
		}, nil)
		mockTaskStore.EXPECT().GetTaskConfig(context.Background(), jobID, i, uint64(0)).Return(&task.TaskConfig{}, nil)
		mockTrackedManager.EXPECT().UpdateTaskRuntime(context.Background(), jobID, i, gomock.Any()).Return(nil)
	}
	for i := uint32(0); i < n; i++ {
		mesosTaskID := util.BuildMesosTaskID(util.BuildTaskID(jobID, i), uuidStr)
		state := mesos.TaskState_TASK_STARTING
		status := &mesos.TaskStatus{
			TaskId: mesosTaskID,
			State:  &state,
		}

		offset++
		el.Add(1)
		applier.addEvent(&pb_eventstream.Event{
			Offset:          offset,
			MesosTaskStatus: status,
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		})
	}

	el.Wait()

	for i := uint32(0); i < n; i++ {
		mesosTaskID := util.BuildMesosTaskID(util.BuildTaskID(jobID, i), uuidStr)
		mockTrackedManager.EXPECT().GetTaskRuntime(context.Background(), jobID, i).Return(&task.RuntimeInfo{
			MesosTaskId: mesosTaskID,
		}, nil)
		mockTaskStore.EXPECT().GetTaskConfig(context.Background(), jobID, i, uint64(0)).Return(&task.TaskConfig{}, nil)
		mockTrackedManager.EXPECT().UpdateTaskRuntime(context.Background(), jobID, i, gomock.Any()).Return(nil)
	}
	for i := uint32(0); i < n; i++ {
		mesosTaskID := util.BuildMesosTaskID(util.BuildTaskID(jobID, i), uuidStr)
		state := mesos.TaskState_TASK_RUNNING
		status := &mesos.TaskStatus{
			TaskId: mesosTaskID,
			State:  &state,
		}

		offset++
		el.Add(1)
		applier.addEvent(&pb_eventstream.Event{
			Offset:          offset,
			MesosTaskStatus: status,
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		})
	}

	el.Wait()

	for i := uint32(0); i < n; i++ {
		mesosTaskID := util.BuildMesosTaskID(util.BuildTaskID(jobID, i), uuidStr)
		mockTrackedManager.EXPECT().GetTaskRuntime(context.Background(), jobID, i).Return(&task.RuntimeInfo{
			MesosTaskId: mesosTaskID,
		}, nil)
		mockTaskStore.EXPECT().GetTaskConfig(context.Background(), jobID, i, uint64(0)).Return(&task.TaskConfig{}, nil)
		mockTrackedManager.EXPECT().UpdateTaskRuntime(context.Background(), jobID, i, gomock.Any()).Return(nil)
	}
	for i := uint32(0); i < n; i++ {
		mesosTaskID := util.BuildMesosTaskID(util.BuildTaskID(jobID, i), uuidStr)
		state := mesos.TaskState_TASK_FINISHED
		status := &mesos.TaskStatus{
			TaskId: mesosTaskID,
			State:  &state,
		}

		offset++
		el.Add(1)
		applier.addEvent(&pb_eventstream.Event{
			Offset:          offset,
			MesosTaskStatus: status,
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		})
	}

	el.Wait()

	for _, bucket := range applier.eventBuckets {
		assert.True(t, bucket.getProcessedCount() > 0)
	}
	applier.shutdown()
}
