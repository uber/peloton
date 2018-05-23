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
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pbeventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"

	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	goalstatemocks "code.uber.internal/infra/peloton/jobmgr/goalstate/mocks"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"
)

var uuidStr = uuid.NewUUID().String()

func TestBucketEventProcessor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	goalStateDriver := goalstatemocks.NewMockDriver(ctrl)
	mockTaskStore := storemocks.NewMockTaskStore(ctrl)

	handler := &statusUpdate{
		taskStore:       mockTaskStore,
		jobFactory:      jobFactory,
		goalStateDriver: goalStateDriver,
		metrics:         NewMetrics(tally.NoopScope),
	}
	var offset uint64
	applier := newBucketEventProcessor(handler, 15, 100)

	jobID := &peloton.JobID{Value: "Test"}
	n := uint32(243)

	for i := uint32(0); i < n; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), i, uuidStr)
		pelotonTaskID := fmt.Sprintf("%s-%d", jobID.GetValue(), i)
		taskInfo := &task.TaskInfo{
			Runtime: &task.RuntimeInfo{
				MesosTaskId: &mesos.TaskID{Value: &mesosTaskID},
			},
			InstanceId: i,
			JobId:      jobID,
		}
		mockTaskStore.EXPECT().GetTaskByID(context.Background(), pelotonTaskID).Return(taskInfo, nil).Times(3)
		jobFactory.EXPECT().AddJob(jobID).Return(cachedJob).Times(3)
		cachedJob.EXPECT().SetTaskUpdateTime(gomock.Any()).Return().Times(3)
		cachedJob.EXPECT().UpdateTasks(context.Background(), gomock.Any(), cached.UpdateCacheAndDB).Return(nil).Times(3)
		goalStateDriver.EXPECT().EnqueueTask(jobID, i, gomock.Any()).Return().Times(3)
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH).Times(3)
		goalStateDriver.EXPECT().
			JobRuntimeDuration(job.JobType_BATCH).
			Return(1 * time.Second).Times(3)
		goalStateDriver.EXPECT().EnqueueJob(jobID, gomock.Any()).Return().Times(3)
	}
	for i := uint32(0); i < n; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), i, uuidStr)
		state := mesos.TaskState_TASK_STARTING
		status := &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			State: &state,
		}

		offset++
		applier.addEvent(&pbeventstream.Event{
			Offset:          offset,
			MesosTaskStatus: status,
			Type:            pbeventstream.Event_MESOS_TASK_STATUS,
		})
	}

	for i := uint32(0); i < n; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), i, uuidStr)
		state := mesos.TaskState_TASK_RUNNING
		status := &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			State: &state,
		}

		offset++
		applier.addEvent(&pbeventstream.Event{
			Offset:          offset,
			MesosTaskStatus: status,
			Type:            pbeventstream.Event_MESOS_TASK_STATUS,
		})
	}

	for i := uint32(0); i < n; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), i, uuidStr)
		state := mesos.TaskState_TASK_FINISHED
		status := &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			State: &state,
		}

		offset++
		applier.addEvent(&pbeventstream.Event{
			Offset:          offset,
			MesosTaskStatus: status,
			Type:            pbeventstream.Event_MESOS_TASK_STATUS,
		})
	}

	applier.drainAndShutdown()

	for i, bucket := range applier.eventBuckets {
		assert.True(t, bucket.getProcessedCount() > 0, fmt.Sprintf("bucket %d did not process any event", i))
	}
}
