package goalstate

import (
	"context"
	"testing"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/task"

	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestTaskInitialize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := store_mocks.NewMockJobStore(ctrl)
	taskStore := store_mocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)

	goalStateDriver := &driver{
		jobEngine:  jobGoalStateEngine,
		taskEngine: taskGoalStateEngine,
		jobStore:   jobStore,
		taskStore:  taskStore,
		jobFactory: jobFactory,
		mtx:        NewMetrics(tally.NoopScope),
		cfg:        &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
		driver:     goalStateDriver,
	}

	oldMesosTaskID := uuid.New()
	taskInfo := &pbtask.TaskInfo{
		InstanceId: 1,
		JobId:      &peloton.JobID{Value: uuid.New()},
		Runtime: &pbtask.RuntimeInfo{
			State: pbtask.TaskState_KILLED,
			MesosTaskId: &mesos_v1.TaskID{
				Value: &oldMesosTaskID,
			},
		},
	}

	jobConfig := &pb_job.JobConfig{
		Type: pb_job.JobType_BATCH,
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	taskStore.EXPECT().GetTaskByID(gomock.Any(), gomock.Any()).Return(taskInfo, nil)

	jobStore.EXPECT().GetJobConfig(gomock.Any(), gomock.Any()).Return(jobConfig, nil)

	cachedJob.EXPECT().UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).Do(
		func(_ context.Context, runtimes map[uint32]*pbtask.RuntimeInfo, req cached.UpdateRequest) {
			for _, updatedRuntimeInfo := range runtimes {
				taskInfo.Runtime = updatedRuntimeInfo
			}
		}).Return(nil)

	cachedJob.EXPECT().
		GetJobType().Return(pb_job.JobType_BATCH)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskInitialize(context.Background(), taskEnt)
	assert.NoError(t, err)
	assert.NotEqual(t, oldMesosTaskID, taskInfo.Runtime.MesosTaskId)
	assert.Equal(t, pbtask.TaskState_INITIALIZED, taskInfo.Runtime.State)
	assert.Equal(t, pbtask.TaskState_SUCCEEDED, taskInfo.Runtime.GoalState)
}
