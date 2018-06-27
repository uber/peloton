package goalstate

import (
	"context"
	"fmt"
	"testing"

	mesosv1 "code.uber.internal/infra/peloton/.gen/mesos/v1"
	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestTaskFailNoRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskStore := storemocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)

	goalStateDriver := &driver{
		jobEngine:  jobGoalStateEngine,
		taskEngine: taskGoalStateEngine,
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

	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), instanceID, uuid.NewUUID().String())

	runtime := &pbtask.RuntimeInfo{
		MesosTaskId:   &mesosv1.TaskID{Value: &mesosTaskID},
		State:         pbtask.TaskState_FAILED,
		GoalState:     pbtask.TaskState_SUCCEEDED,
		ConfigVersion: 1,
		Message:       "testFailure",
		Reason:        mesosv1.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED.String(),
	}

	taskConfig := pbtask.TaskConfig{
		RestartPolicy: &pbtask.RestartPolicy{
			MaxFailures: 0,
		},
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	taskStore.EXPECT().
		GetTaskConfig(gomock.Any(), jobID, instanceID, gomock.Any()).Return(&taskConfig, nil)

	err := TaskFailRetry(context.Background(), taskEnt)
	assert.NoError(t, err)
}

func TestTaskFailRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskStore := storemocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)

	goalStateDriver := &driver{
		jobEngine:  jobGoalStateEngine,
		taskEngine: taskGoalStateEngine,
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

	mesosTaskID := fmt.Sprintf("%s-%d-%d", jobID.GetValue(), instanceID, 1)

	runtime := &pbtask.RuntimeInfo{
		MesosTaskId:   &mesosv1.TaskID{Value: &mesosTaskID},
		State:         pbtask.TaskState_FAILED,
		GoalState:     pbtask.TaskState_SUCCEEDED,
		ConfigVersion: 1,
		Message:       "testFailure",
		Reason:        mesosv1.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED.String(),
	}

	taskConfig := pbtask.TaskConfig{
		RestartPolicy: &pbtask.RestartPolicy{
			MaxFailures: 3,
		},
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	taskStore.EXPECT().
		GetTaskConfig(gomock.Any(), jobID, instanceID, gomock.Any()).Return(&taskConfig, nil)

	cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, runtimesDiffs map[uint32]map[string]interface{}) {
			runtimeDiff := runtimesDiffs[instanceID]
			assert.True(t, runtimeDiff[cached.MesosTaskIDField].(*mesosv1.TaskID).GetValue() != mesosTaskID)
			assert.True(t, runtimeDiff[cached.PrevMesosTaskIDField].(*mesosv1.TaskID).GetValue() == mesosTaskID)
			assert.True(t, runtimeDiff[cached.StateField].(pbtask.TaskState) == pbtask.TaskState_INITIALIZED)
		}).
		Return(nil)

	cachedJob.EXPECT().
		GetJobType().Return(pbjob.JobType_BATCH)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskFailRetry(context.Background(), taskEnt)
	assert.NoError(t, err)
}

func TestTaskFailSystemFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskStore := storemocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)

	goalStateDriver := &driver{
		jobEngine:  jobGoalStateEngine,
		taskEngine: taskGoalStateEngine,
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

	mesosTaskID := fmt.Sprintf("%s-%d-%d", jobID.GetValue(), instanceID, 1)

	runtime := &pbtask.RuntimeInfo{
		MesosTaskId:   &mesosv1.TaskID{Value: &mesosTaskID},
		State:         pbtask.TaskState_FAILED,
		GoalState:     pbtask.TaskState_SUCCEEDED,
		ConfigVersion: 1,
		Message:       "testFailure",
		Reason:        mesosv1.TaskStatus_REASON_CONTAINER_LAUNCH_FAILED.String(),
	}

	taskConfig := pbtask.TaskConfig{
		RestartPolicy: &pbtask.RestartPolicy{
			MaxFailures: 0,
		},
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	taskStore.EXPECT().
		GetTaskConfig(gomock.Any(), jobID, instanceID, gomock.Any()).Return(&taskConfig, nil)

	cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, runtimesDiffs map[uint32]map[string]interface{}) {
			runtimeDiff := runtimesDiffs[instanceID]
			assert.True(t, runtimeDiff[cached.MesosTaskIDField].(*mesosv1.TaskID).GetValue() != mesosTaskID)
			assert.True(t, runtimeDiff[cached.PrevMesosTaskIDField].(*mesosv1.TaskID).GetValue() == mesosTaskID)
			assert.True(t, runtimeDiff[cached.StateField].(pbtask.TaskState) == pbtask.TaskState_INITIALIZED)
		}).
		Return(nil)

	cachedJob.EXPECT().
		GetJobType().Return(pbjob.JobType_BATCH)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskFailRetry(context.Background(), taskEnt)
	assert.NoError(t, err)
}

func TestTaskFailDBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	taskStore := storemocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)

	goalStateDriver := &driver{
		jobEngine:  jobGoalStateEngine,
		taskEngine: taskGoalStateEngine,
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

	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID.GetValue(), instanceID, uuid.NewUUID().String())

	runtime := &pbtask.RuntimeInfo{
		MesosTaskId:   &mesosv1.TaskID{Value: &mesosTaskID},
		State:         pbtask.TaskState_FAILED,
		GoalState:     pbtask.TaskState_SUCCEEDED,
		ConfigVersion: 1,
		Message:       "testFailure",
		Reason:        mesosv1.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED.String(),
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRunTime(gomock.Any()).Return(runtime, nil)

	taskStore.EXPECT().
		GetTaskConfig(gomock.Any(), jobID, instanceID, gomock.Any()).Return(nil, fmt.Errorf("fake db error"))

	err := TaskFailRetry(context.Background(), taskEnt)
	assert.Error(t, err)
}
