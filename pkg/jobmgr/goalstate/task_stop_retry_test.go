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
	"testing"
	"time"

	"github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"

	goalstatemocks "github.com/uber/peloton/pkg/common/goalstate/mocks"
	"github.com/uber/peloton/pkg/common/util"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	lmmocks "github.com/uber/peloton/pkg/jobmgr/task/lifecyclemgr/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
	"golang.org/x/time/rate"
)

func TestTaskStopShutdownExecutor(t *testing.T) {
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

	agentID := &mesos_v1.AgentID{
		Value: util.PtrPrintf("host-agent-0"),
	}

	runtime := &pbtask.RuntimeInfo{
		State:       pbtask.TaskState_RUNNING,
		MesosTaskId: taskID,
		AgentID:     agentID,
		Revision: &peloton.ChangeLog{
			UpdatedAt: uint64(time.Now().Add(-_defaultShutdownExecutorTimeout).UnixNano()),
		},
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(runtime, nil)

	lmMock.EXPECT().
		ShutdownExecutor(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			nil).
		Do(func(
			ctx context.Context,
			tid, agent string,
			_ *rate.Limiter,
		) {
			assert.Equal(t, tid, taskID.GetValue())
			assert.Equal(t, agent, agentID.GetValue())
		}).
		Return(nil)

	err := TaskExecutorShutdown(context.Background(), taskEnt)
	assert.NoError(t, err)
}

func TestTaskStopNoTimeout(t *testing.T) {
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

	agentID := &mesos_v1.AgentID{
		Value: util.PtrPrintf("host-agent-0"),
	}

	runtime := &pbtask.RuntimeInfo{
		State:       pbtask.TaskState_RUNNING,
		MesosTaskId: taskID,
		AgentID:     agentID,
		Revision: &peloton.ChangeLog{
			UpdatedAt: uint64(time.Now().UnixNano()),
		},
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRuntime(gomock.Any()).Return(runtime, nil)

	cachedTask.EXPECT().
		JobID().Return(jobID)

	cachedTask.EXPECT().
		ID().Return(instanceID)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskExecutorShutdown(context.Background(), taskEnt)
	assert.NoError(t, err)
}
