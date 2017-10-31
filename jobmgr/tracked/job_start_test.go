package tracked

import (
	"context"
	"testing"

	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	host_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"
	storage_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestJobStartBeforeSchedulingUnitReschedules(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHost := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	mockJobStore := storage_mocks.NewMockJobStore(ctrl)
	mockTaskStore := storage_mocks.NewMockTaskStore(ctrl)
	mockResmgrClient := mocks.NewMockResourceManagerServiceYARPCClient(ctrl)

	jobID := &peloton.JobID{
		Value: "my-job-id",
	}
	jobConfig := &pb_job.JobConfig{
		RespoolID: &peloton.ResourcePoolID{
			Value: "my-respool-id",
		},
		Sla: &pb_job.SlaConfig{
			MinimumRunningInstances: 2,
		},
	}
	mockJobStore.EXPECT().GetJobConfig(context.Background(), jobID, uint64(0)).AnyTimes().Return(jobConfig, nil)
	mockJobStore.EXPECT().GetJobRuntime(context.Background(), jobID).AnyTimes().Return(&pb_job.RuntimeInfo{
		State: pb_job.JobState_PENDING,
	}, nil)

	m := &manager{
		jobs:          map[string]*job{},
		hostmgrClient: mockHost,
		jobStore:      mockJobStore,
		taskStore:     mockTaskStore,
		resmgrClient:  mockResmgrClient,
		mtx:           newMetrics(tally.NoopScope),
	}
	j := m.addJob(jobID, &jobState{
		state: pb_job.JobState_PENDING,
	})
	assert.Equal(t, nil, j.m.RunTaskAction(context.Background(), jobID, uint32(0), StartAction))
	assert.Equal(t, nil, j.m.RunTaskAction(context.Background(), jobID, uint32(3), StartAction))
}

func TestJobStartAfterSchedulingUnitStarts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHost := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	mockJobStore := storage_mocks.NewMockJobStore(ctrl)
	mockTaskStore := storage_mocks.NewMockTaskStore(ctrl)
	mockResmgrClient := mocks.NewMockResourceManagerServiceYARPCClient(ctrl)

	jobID := &peloton.JobID{
		Value: "my-job-id",
	}
	jobConfig := &pb_job.JobConfig{
		RespoolID: &peloton.ResourcePoolID{
			Value: "my-respool-id",
		},
		Sla: &pb_job.SlaConfig{
			MinimumRunningInstances: 2,
		},
	}
	mockJobStore.EXPECT().GetJobConfig(context.Background(), jobID, uint64(0)).AnyTimes().Return(jobConfig, nil)
	mockJobStore.EXPECT().GetJobRuntime(context.Background(), jobID).Return(&pb_job.RuntimeInfo{
		State: pb_job.JobState_RUNNING,
	}, nil)

	m := &manager{
		jobs:          map[string]*job{},
		hostmgrClient: mockHost,
		jobStore:      mockJobStore,
		taskStore:     mockTaskStore,
		resmgrClient:  mockResmgrClient,
		mtx:           newMetrics(tally.NoopScope),
	}
	j := m.addJob(jobID, &jobState{
		configVersion: 0,
	})

	j.tasks[0] = &task{
		id:      0,
		job:     j,
		runtime: &pb_task.RuntimeInfo{},
	}

	taskInfo := &pb_task.TaskInfo{
		JobId:      jobID,
		InstanceId: 0,
		Config:     &pb_task.TaskConfig{},
		Runtime:    j.tasks[0].runtime,
	}

	mockTaskStore.EXPECT().
		GetTaskConfig(context.Background(), j.id, taskInfo.InstanceId, j.state.configVersion).Return(taskInfo.Config, nil)

	request := &resmgrsvc.EnqueueGangsRequest{
		Gangs:   util.ConvertToResMgrGangs([]*pb_task.TaskInfo{taskInfo}, jobConfig.Sla),
		ResPool: jobConfig.RespoolID,
	}
	mockResmgrClient.EXPECT().EnqueueGangs(gomock.Any(), request).Return(nil, nil)

	assert.Equal(t, nil, j.m.RunTaskAction(context.Background(), j.id, uint32(0), StartAction))
}

func TestJobStartSchedulingUnitEnqueuesGang(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHost := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	mockJobStore := storage_mocks.NewMockJobStore(ctrl)
	mockTaskStore := storage_mocks.NewMockTaskStore(ctrl)
	mockResmgrClient := mocks.NewMockResourceManagerServiceYARPCClient(ctrl)

	jobID := &peloton.JobID{
		Value: "my-job-id",
	}
	jobConfig := &pb_job.JobConfig{
		RespoolID: &peloton.ResourcePoolID{
			Value: "my-respool-id",
		},
		Sla: &pb_job.SlaConfig{
			MinimumRunningInstances: 2,
		},
	}
	mockJobStore.EXPECT().GetJobConfig(context.Background(), jobID, uint64(0)).AnyTimes().Return(jobConfig, nil)
	mockJobStore.EXPECT().GetJobRuntime(context.Background(), jobID).Return(&pb_job.RuntimeInfo{
		State: pb_job.JobState_PENDING,
	}, nil)

	m := &manager{
		jobs:          map[string]*job{},
		hostmgrClient: mockHost,
		jobStore:      mockJobStore,
		taskStore:     mockTaskStore,
		resmgrClient:  mockResmgrClient,
		mtx:           newMetrics(tally.NoopScope),
	}
	j := m.addJob(jobID, &jobState{})

	j.tasks[0] = &task{
		id:         0,
		job:        j,
		runtime:    &pb_task.RuntimeInfo{},
		lastAction: StartAction,
	}
	j.tasks[1] = &task{
		id:         1,
		job:        j,
		runtime:    &pb_task.RuntimeInfo{},
		lastAction: StartAction,
	}

	taskInfo := &pb_task.TaskInfo{
		JobId:      jobID,
		InstanceId: 0,
		Config:     &pb_task.TaskConfig{},
		Runtime:    j.tasks[0].runtime,
	}

	mockTaskStore.EXPECT().
		GetTasksForJobByRange(context.Background(), jobID, &pb_task.InstanceRange{
			From: 0,
			To:   2,
		}).Return(map[uint32]*pb_task.TaskInfo{
		0: taskInfo,
		1: taskInfo,
	}, nil)

	request := &resmgrsvc.EnqueueGangsRequest{
		Gangs:   util.ConvertToResMgrGangs([]*pb_task.TaskInfo{taskInfo, taskInfo}, jobConfig.Sla),
		ResPool: jobConfig.RespoolID,
	}
	mockResmgrClient.EXPECT().EnqueueGangs(gomock.Any(), request).Return(nil, nil)

	assert.Equal(t, nil, j.m.RunTaskAction(context.Background(), j.id, uint32(0), StartAction))
}
