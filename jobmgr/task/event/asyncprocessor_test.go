package event

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	mocks2 "code.uber.internal/infra/peloton/jobmgr/task/event/mocks"
	"code.uber.internal/infra/peloton/jobmgr/tracked/mocks"
	"code.uber.internal/infra/peloton/util"
)

var uuidStr = uuid.NewUUID().String()

func TestAddEvent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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
	mockStatusProcessor.EXPECT().ProcessListeners(mevt)

	tevt := &pb_eventstream.Event{
		Type: pb_eventstream.Event_PELOTON_TASK_EVENT,
		PelotonTaskEvent: &task.TaskEvent{
			TaskId: util.BuildTaskID(&peloton.JobID{
				Value: "my-job",
			}, 0),
		},
	}
	mockStatusProcessor.EXPECT().ProcessStatusUpdate(gomock.Any(), tevt).Return(nil)
	mockStatusProcessor.EXPECT().ProcessListeners(tevt)

	bep.addEvent(mevt)
	bep.addEvent(tevt)

	// Yield to let the StatusProcessor get the events
	time.Sleep(200 * time.Millisecond)
}

func TestBucketEventProcessor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTrackedManager := mocks.NewMockManager(ctrl)

	handler := &statusUpdate{
		trackedManager: mockTrackedManager,
		metrics:        NewMetrics(tally.NoopScope),
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
		applier.addEvent(&pb_eventstream.Event{
			Offset:          offset,
			MesosTaskStatus: status,
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		})
	}

	time.Sleep(200 * time.Millisecond)

	for i := uint32(0); i < n; i++ {
		mesosTaskID := util.BuildMesosTaskID(util.BuildTaskID(jobID, i), uuidStr)
		mockTrackedManager.EXPECT().GetTaskRuntime(context.Background(), jobID, i).Return(&task.RuntimeInfo{
			MesosTaskId: mesosTaskID,
		}, nil)
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
		applier.addEvent(&pb_eventstream.Event{
			Offset:          offset,
			MesosTaskStatus: status,
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		})
	}

	time.Sleep(200 * time.Millisecond)

	for i := uint32(0); i < n; i++ {
		mesosTaskID := util.BuildMesosTaskID(util.BuildTaskID(jobID, i), uuidStr)
		mockTrackedManager.EXPECT().GetTaskRuntime(context.Background(), jobID, i).Return(&task.RuntimeInfo{
			MesosTaskId: mesosTaskID,
		}, nil)
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
		applier.addEvent(&pb_eventstream.Event{
			Offset:          offset,
			MesosTaskStatus: status,
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		})
	}

	time.Sleep(200 * time.Millisecond)

	for _, bucket := range applier.eventBuckets {
		assert.True(t, bucket.getProcessedCount() > 0)
	}
	applier.shutdown()

}
