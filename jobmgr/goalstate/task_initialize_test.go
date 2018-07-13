package goalstate

import (
	"context"
	"testing"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

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
	cachedTask := cachedmocks.NewMockTask(ctrl)
	cachedConfig := cachedmocks.NewMockJobConfigCache(ctrl)

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

	newConfigVersion := uint64(2)

	oldMesosTaskID := uuid.New()
	runtime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_KILLED,
		MesosTaskId: &mesos_v1.TaskID{
			Value: &oldMesosTaskID,
		},
		ConfigVersion:        newConfigVersion - 1,
		DesiredConfigVersion: newConfigVersion,
	}
	newRuntime := runtime

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	cachedJob.EXPECT().GetConfig(gomock.Any()).Return(cachedConfig, nil)

	cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any()).Do(
		func(_ context.Context, runtimeDiffs map[uint32]cached.RuntimeDiff) {
			for _, runtimeDiff := range runtimeDiffs {
				assert.Equal(
					t,
					pbtask.TaskState_INITIALIZED,
					runtimeDiff[cached.StateField],
				)
				assert.Equal(
					t,
					pbtask.TaskState_SUCCEEDED,
					runtimeDiff[cached.GoalStateField],
				)
				assert.Equal(
					t,
					newConfigVersion,
					runtimeDiff[cached.ConfigVersionField],
				)
			}
		}).Return(nil)

	cachedConfig.EXPECT().
		GetType().Return(pb_job.JobType_BATCH)

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
	assert.NotEqual(t, oldMesosTaskID, newRuntime.MesosTaskId)

}
