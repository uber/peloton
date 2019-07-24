// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package goalstate

import (
	"context"
	"fmt"
	"testing"
	"time"

	mesos_v1 "github.com/uber/peloton/.gen/mesos/v1"
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	resmocks "github.com/uber/peloton/.gen/peloton/private/resmgrsvc/mocks"

	"github.com/uber/peloton/pkg/common/goalstate"
	goalstatemocks "github.com/uber/peloton/pkg/common/goalstate/mocks"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	lmmocks "github.com/uber/peloton/pkg/jobmgr/task/lifecyclemgr/mocks"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"

	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestTaskStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)
	lmMock := lmmocks.NewMockManager(ctrl)

	goalStateDriver := &driver{
		jobEngine:  jobGoalStateEngine,
		taskEngine: taskGoalStateEngine,
		jobFactory: jobFactory,
		lm:         lmMock,
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

	taskID := &mesos_v1.TaskID{
		Value: &[]string{"3c8a3c3e-71e3-49c5-9aed-2929823f595c-1-3c8a3c3e-71e3-49c5-9aed-2929823f5957"}[0],
	}

	runtime := &pbtask.RuntimeInfo{
		State:       pbtask.TaskState_RUNNING,
		MesosTaskId: taskID,
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask).Times(2)

	cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(runtime, nil)

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	expectedRuntimeDiff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.StateField:   pbtask.TaskState_KILLING,
		jobmgrcommon.MessageField: "Killing the task",
		jobmgrcommon.ReasonField:  "",
	}
	cachedJob.EXPECT().
		PatchTasks(gomock.Any(), map[uint32]jobmgrcommon.RuntimeDiff{
			instanceID: expectedRuntimeDiff,
		}, false,
		)

	lmMock.EXPECT().Kill(
		gomock.Any(),
		taskID.GetValue(),
		"",
		nil,
	).Return(nil)

	cachedJob.EXPECT().
		GetJobType().Return(pbjob.JobType_BATCH)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(entity goalstate.Entity, deadline time.Time) {
			// The test should not take more than one min
			assert.True(t, deadline.Sub(time.Now()).Round(time.Minute) ==
				_defaultShutdownExecutorTimeout)
		}).
		Return()

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskStop(context.Background(), taskEnt)
	assert.NoError(t, err)

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(nil, fmt.Errorf("fake error"))

	err = TaskStop(context.Background(), taskEnt)
	assert.EqualError(t, err, "fake error")
}

func TestTaskStopForInPlaceUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)
	lmMock := lmmocks.NewMockManager(ctrl)

	goalStateDriver := &driver{
		jobEngine:  jobGoalStateEngine,
		taskEngine: taskGoalStateEngine,
		jobFactory: jobFactory,
		lm:         lmMock,
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

	taskID := &mesos_v1.TaskID{
		Value: &[]string{"3c8a3c3e-71e3-49c5-9aed-2929823f595c-1-3c8a3c3e-71e3-49c5-9aed-2929823f5957"}[0],
	}

	runtime := &pbtask.RuntimeInfo{
		State:       pbtask.TaskState_RUNNING,
		MesosTaskId: taskID,
		DesiredHost: "host1",
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask).Times(2)

	cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(runtime, nil)

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	expectedRuntimeDiff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.StateField:   pbtask.TaskState_KILLING,
		jobmgrcommon.MessageField: "Killing the task",
		jobmgrcommon.ReasonField:  "",
	}
	cachedJob.EXPECT().
		PatchTasks(gomock.Any(), map[uint32]jobmgrcommon.RuntimeDiff{
			instanceID: expectedRuntimeDiff,
		}, false,
		)

	lmMock.EXPECT().Kill(
		gomock.Any(),
		taskID.GetValue(),
		"host1",
		nil,
	).Return(nil)

	cachedJob.EXPECT().
		GetJobType().Return(pbjob.JobType_BATCH)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(entity goalstate.Entity, deadline time.Time) {
			// The test should not take more than one min
			assert.True(t, deadline.Sub(time.Now()).Round(time.Minute) ==
				_defaultShutdownExecutorTimeout)
		}).
		Return()

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskStop(context.Background(), taskEnt)
	assert.NoError(t, err)
}

func TestTaskStopIfInitializedCallsKillOnResmgr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskStore := storemocks.NewMockTaskStore(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)
	mockResmgr := resmocks.NewMockResourceManagerServiceYARPCClient(ctrl)

	goalStateDriver := &driver{
		jobEngine:    jobGoalStateEngine,
		taskEngine:   taskGoalStateEngine,
		taskStore:    taskStore,
		jobFactory:   jobFactory,
		resmgrClient: mockResmgr,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	instanceID := uint32(7)
	taskID := &peloton.TaskID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c-7"}

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
		driver:     goalStateDriver,
	}

	runtime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_INITIALIZED,
	}

	var killResponseErr []*resmgrsvc.KillTasksResponse_Error
	killResponseErr = append(killResponseErr,
		&resmgrsvc.KillTasksResponse_Error{
			NotFound: &resmgrsvc.TasksNotFound{
				Message: "Tasks Not Found",
				Task:    taskID,
			},
		})
	res := &resmgrsvc.KillTasksResponse{
		Error: killResponseErr,
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(runtime, nil)

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	mockResmgr.EXPECT().KillTasks(context.Background(), &resmgrsvc.KillTasksRequest{
		Tasks: []*peloton.TaskID{
			{
				Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c-7",
			},
		},
	}).Return(res, nil)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(runtime, nil)

	cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, nil)

	cachedJob.EXPECT().
		GetJobType().Return(pbjob.JobType_BATCH)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskStop(context.Background(), taskEnt)
	assert.NoError(t, err)
}

func TestTaskStopIfPendingCallsKillOnResmgr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskStore := storemocks.NewMockTaskStore(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)
	mockResmgr := resmocks.NewMockResourceManagerServiceYARPCClient(ctrl)

	goalStateDriver := &driver{
		jobEngine:    jobGoalStateEngine,
		taskEngine:   taskGoalStateEngine,
		taskStore:    taskStore,
		jobFactory:   jobFactory,
		resmgrClient: mockResmgr,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	instanceID := uint32(7)
	taskID := &peloton.TaskID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c-7"}

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
		driver:     goalStateDriver,
	}

	runtime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_PENDING,
	}

	var killResponseErr []*resmgrsvc.KillTasksResponse_Error
	killResponseErr = append(killResponseErr,
		&resmgrsvc.KillTasksResponse_Error{
			NotFound: &resmgrsvc.TasksNotFound{
				Message: "Tasks Not Found",
				Task:    taskID,
			},
		})
	res := &resmgrsvc.KillTasksResponse{
		Error: killResponseErr,
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(runtime, nil)

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	mockResmgr.EXPECT().KillTasks(gomock.Any(), &resmgrsvc.KillTasksRequest{
		Tasks: []*peloton.TaskID{
			{
				Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c-7",
			},
		},
	}).Return(res, nil)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(runtime, nil)

	cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any(), false).
		Return(nil, nil, nil)

	cachedJob.EXPECT().
		GetJobType().Return(pbjob.JobType_BATCH)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskStop(context.Background(), taskEnt)
	assert.NoError(t, err)
}
