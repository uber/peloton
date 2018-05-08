package goalstate

import (
	"context"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	hostmocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"

	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	"code.uber.internal/infra/peloton/util"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

func TestTaskStopShutdownExecutor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)
	hostMock := hostmocks.NewMockInternalHostServiceYARPCClient(ctrl)

	goalStateDriver := &driver{
		jobEngine:     jobGoalStateEngine,
		taskEngine:    taskGoalStateEngine,
		jobFactory:    jobFactory,
		hostmgrClient: hostMock,
		mtx:           NewMetrics(tally.NoopScope),
		cfg:           &Config{},
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
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRunTime().Return(runtime)

	cachedTask.EXPECT().
		GetLastRuntimeUpdateTime().Return(time.Now().Add(-_defaultShutdownExecutorTimeout))

	hostMock.EXPECT().
		ShutdownExecutors(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, req *hostsvc.ShutdownExecutorsRequest, opt ...yarpc.CallOption) {
			assert.Equal(t, req.GetExecutors()[0].GetExecutorId().GetValue(), taskID.GetValue())
			assert.Equal(t, req.GetExecutors()[0].GetAgentId().GetValue(), agentID.GetValue())
		}).
		Return(&hostsvc.ShutdownExecutorsResponse{}, nil)

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
	hostMock := hostmocks.NewMockInternalHostServiceYARPCClient(ctrl)

	goalStateDriver := &driver{
		jobEngine:     jobGoalStateEngine,
		taskEngine:    taskGoalStateEngine,
		jobFactory:    jobFactory,
		hostmgrClient: hostMock,
		mtx:           NewMetrics(tally.NoopScope),
		cfg:           &Config{},
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
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob).Times(2)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GetRunTime().Return(runtime)

	cachedTask.EXPECT().
		GetLastRuntimeUpdateTime().Return(time.Now())

	cachedTask.EXPECT().
		JobID().Return(jobID)

	cachedTask.EXPECT().
		ID().Return(instanceID)

	taskGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	cachedJob.EXPECT().
		GetJobType().Return(pbjob.JobType_BATCH)

	err := TaskExecutorShutdown(context.Background(), taskEnt)
	assert.NoError(t, err)
}
