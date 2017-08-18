package goalstate

import (
	"context"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	"code.uber.internal/infra/peloton/jobmgr/tracked/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
)

func TestEngineOnEvents(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tmMock := mocks.NewMockManager(ctrl)
	jobMock := mocks.NewMockJob(ctrl)

	e := &engine{
		trackedManager: tmMock,
	}

	jobID := &peloton.JobID{
		Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c",
	}

	tmMock.EXPECT().GetJob(jobID).Return(jobMock)
	jobMock.EXPECT().UpdateTaskState(uint32(1), pb_task.TaskState_RUNNING)

	e.OnEvents([]*pb_eventstream.Event{{
		Type: pb_eventstream.Event_MESOS_TASK_STATUS,
		MesosTaskStatus: &mesos_v1.TaskStatus{
			TaskId: &mesos_v1.TaskID{
				Value: &[]string{"3c8a3c3e-71e3-49c5-9aed-2929823f595c-1-3c8a3c3e-71e3-49c5-9aed-2929823f5957"}[0],
			},
			State: &[]mesos_v1.TaskState{mesos_v1.TaskState_TASK_RUNNING}[0],
		},
		Offset: 5,
	}})
}

func TestEngineSyncFromDB(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobstoreMock := store_mocks.NewMockJobStore(ctrl)
	taskstoreMock := store_mocks.NewMockTaskStore(ctrl)
	tmMock := mocks.NewMockManager(ctrl)
	jobMock := mocks.NewMockJob(ctrl)

	e := &engine{
		trackedManager: tmMock,
		jobStore:       jobstoreMock,
		taskStore:      taskstoreMock,
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

	jobMock.EXPECT().UpdateTask(uint32(1), &task.RuntimeInfo{
		GoalState:            task.TaskState_RUNNING,
		DesiredConfigVersion: 42,
		ConfigVersion:        42,
	})
	tmMock.EXPECT().AddJob(jobID).Return(jobMock)

	e.syncFromDB(context.Background())
}

func TestEngineStartStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobstoreMock := store_mocks.NewMockJobStore(ctrl)
	tmMock := mocks.NewMockManager(ctrl)

	e := &engine{
		trackedManager: tmMock,
		jobStore:       jobstoreMock,
	}

	var wg sync.WaitGroup
	wg.Add(2)

	jobstoreMock.EXPECT().GetAllJobs(gomock.Any()).Return(nil, nil).Do(func(_ interface{}) error {
		wg.Done()
		return nil
	})

	tmMock.EXPECT().WaitForScheduledTask(gomock.Any()).Do(func(stopChan <-chan struct{}) {
		<-stopChan
		wg.Done()
	}).Return(nil)

	e.Start()

	e.Stop()

	wg.Wait()
}
