package tracked

import (
	"context"
	"testing"

	"fmt"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	job2 "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"
	host_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"
	mocks2 "code.uber.internal/infra/peloton/jobmgr/task/launcher/mocks"
	"code.uber.internal/infra/peloton/storage"
	storage_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestTaskStartStateless(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockHost := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	mockJobStore := storage_mocks.NewMockJobStore(ctrl)
	mockTaskStore := storage_mocks.NewMockTaskStore(ctrl)
	mockResmgrClient := mocks.NewMockResourceManagerServiceYARPCClient(ctrl)

	tt := &task{
		id: 12345,
		job: &job{
			id: &peloton.JobID{
				Value: "my-job-id",
			},
			m: &manager{
				hostmgrClient: mockHost,
				jobStore:      mockJobStore,
				taskStore:     mockTaskStore,
				resmgrClient:  mockResmgrClient,
				mtx:           NewMetrics(tally.NoopScope),
			},
		},
		runtime: &pb_task.RuntimeInfo{},
	}

	jobConfig := &job2.JobConfig{
		RespoolID: &peloton.ResourcePoolID{
			Value: "my-respool-id",
		},
	}
	taskInfo := &pb_task.TaskInfo{
		InstanceId: tt.id,
		Config: &pb_task.TaskConfig{
			Volume: &pb_task.PersistentVolumeConfig{},
		},
		Runtime: tt.runtime,
	}

	mockJobStore.EXPECT().
		GetJobConfig(gomock.Any(), tt.job.id).Return(jobConfig, nil)
	mockTaskStore.EXPECT().
		GetTaskByID(gomock.Any(), fmt.Sprintf("%s-%d", tt.job.id.Value, tt.id)).Return(taskInfo, nil)

	request := &resmgrsvc.EnqueueGangsRequest{
		Gangs:   util.ConvertToResMgrGangs([]*pb_task.TaskInfo{taskInfo}, jobConfig),
		ResPool: jobConfig.RespoolID,
	}
	mockResmgrClient.EXPECT().EnqueueGangs(gomock.Any(), request).Return(nil, nil)

	reschedule, err := tt.RunAction(context.Background(), StartAction)
	assert.True(t, reschedule)
	assert.NoError(t, err)
}

func TestTaskStartStatefullWithVolume(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockHost := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	mockJobStore := storage_mocks.NewMockJobStore(ctrl)
	mockTaskStore := storage_mocks.NewMockTaskStore(ctrl)
	mockVolumeStore := storage_mocks.NewMockPersistentVolumeStore(ctrl)
	mockResmgrClient := mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	mockTaskLauncher := mocks2.NewMockLauncher(ctrl)

	tt := &task{
		id: 12345,
		job: &job{
			id: &peloton.JobID{
				Value: "my-job-id",
			},
			m: &manager{
				hostmgrClient: mockHost,
				jobStore:      mockJobStore,
				taskStore:     mockTaskStore,
				volumeStore:   mockVolumeStore,
				resmgrClient:  mockResmgrClient,
				taskLauncher:  mockTaskLauncher,
				mtx:           NewMetrics(tally.NoopScope),
			},
		},
		runtime: &pb_task.RuntimeInfo{
			MesosTaskId: &mesos_v1.TaskID{
				Value: &[]string{"3c8a3c3e-71e3-49c5-9aed-2929823f595c-1-3c8a3c3e-71e3-49c5-9aed-2929823f5957"}[0],
			},
			VolumeID: &peloton.VolumeID{
				Value: "my-volume-id",
			},
		},
	}

	jobConfig := &job2.JobConfig{
		RespoolID: &peloton.ResourcePoolID{
			Value: "my-respool-id",
		},
	}
	taskInfo := &pb_task.TaskInfo{
		InstanceId: tt.id,
		Config: &pb_task.TaskConfig{
			Volume: &pb_task.PersistentVolumeConfig{},
		},
		Runtime: tt.runtime,
	}

	mockJobStore.EXPECT().
		GetJobConfig(gomock.Any(), tt.job.id).Return(jobConfig, nil)
	mockTaskStore.EXPECT().
		GetTaskByID(gomock.Any(), fmt.Sprintf("%s-%d", tt.job.id.Value, tt.id)).Return(taskInfo, nil)
	mockVolumeStore.EXPECT().
		GetPersistentVolume(gomock.Any(), tt.runtime.VolumeID).Return(&volume.PersistentVolumeInfo{
		State: volume.VolumeState_CREATED,
	}, nil)
	mockTaskLauncher.EXPECT().
		LaunchTaskWithReservedResource(gomock.Any(), taskInfo).Return(nil)

	reschedule, err := tt.RunAction(context.Background(), StartAction)
	assert.True(t, reschedule)
	assert.NoError(t, err)
}

func TestTaskStartStatefullWithoutVolume(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockHost := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	mockJobStore := storage_mocks.NewMockJobStore(ctrl)
	mockTaskStore := storage_mocks.NewMockTaskStore(ctrl)
	mockVolumeStore := storage_mocks.NewMockPersistentVolumeStore(ctrl)
	mockResmgrClient := mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	mockTaskLauncher := mocks2.NewMockLauncher(ctrl)

	tt := &task{
		id: 12345,
		job: &job{
			id: &peloton.JobID{
				Value: "my-job-id",
			},
			m: &manager{
				hostmgrClient: mockHost,
				jobStore:      mockJobStore,
				taskStore:     mockTaskStore,
				volumeStore:   mockVolumeStore,
				resmgrClient:  mockResmgrClient,
				taskLauncher:  mockTaskLauncher,
				mtx:           NewMetrics(tally.NoopScope),
			},
		},
		runtime: &pb_task.RuntimeInfo{
			MesosTaskId: &mesos_v1.TaskID{
				Value: &[]string{"3c8a3c3e-71e3-49c5-9aed-2929823f595c-1-3c8a3c3e-71e3-49c5-9aed-2929823f5957"}[0],
			},
			VolumeID: &peloton.VolumeID{
				Value: "my-volume-id",
			},
		},
	}

	jobConfig := &job2.JobConfig{
		RespoolID: &peloton.ResourcePoolID{
			Value: "my-respool-id",
		},
	}
	taskInfo := &pb_task.TaskInfo{
		InstanceId: tt.id,
		Config: &pb_task.TaskConfig{
			Volume: &pb_task.PersistentVolumeConfig{},
		},
		Runtime: tt.runtime,
	}

	mockJobStore.EXPECT().
		GetJobConfig(gomock.Any(), tt.job.id).Return(jobConfig, nil)
	mockTaskStore.EXPECT().
		GetTaskByID(gomock.Any(), fmt.Sprintf("%s-%d", tt.job.id.Value, tt.id)).Return(taskInfo, nil)
	mockVolumeStore.EXPECT().
		GetPersistentVolume(gomock.Any(), tt.runtime.VolumeID).Return(nil, &storage.VolumeNotFoundError{})

	request := &resmgrsvc.EnqueueGangsRequest{
		Gangs:   util.ConvertToResMgrGangs([]*pb_task.TaskInfo{taskInfo}, jobConfig),
		ResPool: jobConfig.RespoolID,
	}
	mockResmgrClient.EXPECT().EnqueueGangs(gomock.Any(), request).Return(nil, nil)

	reschedule, err := tt.RunAction(context.Background(), StartAction)
	assert.True(t, reschedule)
	assert.NoError(t, err)
}
