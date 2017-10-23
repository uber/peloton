package event

import (
	"context"
	"fmt"
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
	"code.uber.internal/infra/peloton/jobmgr/tracked/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
)

var uuidStr = uuid.NewUUID().String()

func TestBucketEventProcessor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTrackedManager := mocks.NewMockManager(ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(ctrl)

	handler := &statusUpdate{
		taskStore:      mockTaskStore,
		trackedManager: mockTrackedManager,
		metrics:        NewMetrics(tally.NoopScope),
	}
	var offset uint64
	applier := newBucketEventProcessor(handler, 15, 100)

	jobID := "Test"
	n := 243

	for i := 0; i < n; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, i, uuidStr)
		mockTaskStore.EXPECT().GetTaskByID(context.Background(), fmt.Sprintf("%s-%d", jobID, i)).Return(&task.TaskInfo{
			Runtime: &task.RuntimeInfo{
				MesosTaskId: &mesos.TaskID{
					Value: &mesosTaskID,
				},
			},
			InstanceId: uint32(i),
			JobId:      &peloton.JobID{Value: jobID},
		}, nil)
		mockTrackedManager.EXPECT().UpdateTask(context.Background(), &peloton.JobID{Value: jobID}, uint32(i), gomock.Any()).Return(nil)
	}
	for i := 0; i < n; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, i, uuidStr)
		state := mesos.TaskState_TASK_STARTING
		status := &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			State: &state,
		}

		offset++
		applier.addEvent(&pb_eventstream.Event{
			Offset:          offset,
			MesosTaskStatus: status,
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		})
	}

	time.Sleep(200 * time.Millisecond)

	for i := 0; i < n; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, i, uuidStr)
		mockTaskStore.EXPECT().GetTaskByID(context.Background(), fmt.Sprintf("%s-%d", jobID, i)).Return(&task.TaskInfo{
			Runtime: &task.RuntimeInfo{
				MesosTaskId: &mesos.TaskID{
					Value: &mesosTaskID,
				},
			},
			InstanceId: uint32(i),
			JobId:      &peloton.JobID{Value: jobID},
		}, nil)
		mockTrackedManager.EXPECT().UpdateTask(context.Background(), &peloton.JobID{Value: jobID}, uint32(i), gomock.Any()).Return(nil)
	}
	for i := 0; i < n; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, i, uuidStr)
		state := mesos.TaskState_TASK_RUNNING
		status := &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			State: &state,
		}

		offset++
		applier.addEvent(&pb_eventstream.Event{
			Offset:          offset,
			MesosTaskStatus: status,
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		})
	}

	time.Sleep(200 * time.Millisecond)

	for i := 0; i < n; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, i, uuidStr)
		mockTaskStore.EXPECT().GetTaskByID(context.Background(), fmt.Sprintf("%s-%d", jobID, i)).Return(&task.TaskInfo{
			Runtime: &task.RuntimeInfo{
				MesosTaskId: &mesos.TaskID{
					Value: &mesosTaskID,
				},
			},
			InstanceId: uint32(i),
			JobId:      &peloton.JobID{Value: jobID},
		}, nil)
		mockTrackedManager.EXPECT().UpdateTask(context.Background(), &peloton.JobID{Value: jobID}, uint32(i), gomock.Any()).Return(nil)
	}
	for i := 0; i < n; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, i, uuidStr)
		state := mesos.TaskState_TASK_FINISHED
		status := &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			State: &state,
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
