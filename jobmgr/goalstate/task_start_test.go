package goalstate

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
	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"

	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	mocks2 "code.uber.internal/infra/peloton/jobmgr/task/launcher/mocks"
	"code.uber.internal/infra/peloton/storage"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestTaskStartStateless(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := store_mocks.NewMockJobStore(ctrl)
	taskStore := store_mocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	resmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)

	goalStateDriver := &driver{
		jobEngine:    jobGoalStateEngine,
		taskEngine:   taskGoalStateEngine,
		jobStore:     jobStore,
		taskStore:    taskStore,
		jobFactory:   jobFactory,
		resmgrClient: resmgrClient,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
		driver:     goalStateDriver,
	}

	jobConfig := &job2.JobConfig{
		RespoolID: &peloton.ResourcePoolID{
			Value: "my-respool-id",
		},
	}
	taskInfo := &pb_task.TaskInfo{
		InstanceId: instanceID,
		Config: &pb_task.TaskConfig{
			Volume: &pb_task.PersistentVolumeConfig{},
		},
		Runtime: &pb_task.RuntimeInfo{},
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), jobID).Return(jobConfig, nil)

	taskStore.EXPECT().
		GetTaskByID(gomock.Any(), fmt.Sprintf("%s-%d", jobID.GetValue(), instanceID)).Return(taskInfo, nil)

	request := &resmgrsvc.EnqueueGangsRequest{
		Gangs:   util.ConvertToResMgrGangs([]*pb_task.TaskInfo{taskInfo}, jobConfig),
		ResPool: jobConfig.RespoolID,
	}

	resmgrClient.EXPECT().EnqueueGangs(gomock.Any(), request).Return(nil, nil)

	cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).Return(nil)

	err := TaskStart(context.Background(), taskEnt)
	assert.NoError(t, err)
}

func TestTaskStartStatefullWithVolume(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := store_mocks.NewMockJobStore(ctrl)
	taskStore := store_mocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	mockHost := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	mockTaskLauncher := mocks2.NewMockLauncher(ctrl)
	mockVolumeStore := store_mocks.NewMockPersistentVolumeStore(ctrl)

	goalStateDriver := &driver{
		jobEngine:     jobGoalStateEngine,
		taskEngine:    taskGoalStateEngine,
		jobStore:      jobStore,
		taskStore:     taskStore,
		volumeStore:   mockVolumeStore,
		taskLauncher:  mockTaskLauncher,
		hostmgrClient: mockHost,
		jobFactory:    jobFactory,
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

	runtime := &pb_task.RuntimeInfo{
		MesosTaskId: &mesos_v1.TaskID{
			Value: &[]string{"3c8a3c3e-71e3-49c5-9aed-2929823f595c-1-3c8a3c3e-71e3-49c5-9aed-2929823f5957"}[0],
		},
		VolumeID: &peloton.VolumeID{
			Value: "my-volume-id",
		},
	}

	jobConfig := &job2.JobConfig{
		RespoolID: &peloton.ResourcePoolID{
			Value: "my-respool-id",
		},
	}

	taskInfo := &pb_task.TaskInfo{
		InstanceId: instanceID,
		Config: &pb_task.TaskConfig{
			Volume: &pb_task.PersistentVolumeConfig{},
		},
		Runtime: runtime,
	}

	taskID := fmt.Sprintf("%s-%d", jobID, instanceID)
	taskInfos := make(map[string]*pb_task.TaskInfo)
	taskInfos[taskID] = taskInfo

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), jobID).Return(jobConfig, nil)

	taskStore.EXPECT().
		GetTaskByID(gomock.Any(), fmt.Sprintf("%s-%d", jobID.GetValue(), instanceID)).Return(taskInfo, nil)

	mockVolumeStore.EXPECT().
		GetPersistentVolume(gomock.Any(), runtime.VolumeID).Return(&volume.PersistentVolumeInfo{
		State: volume.VolumeState_CREATED,
	}, nil)

	mockTaskLauncher.EXPECT().
		GetLaunchableTasks(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).Return(taskInfos, nil)

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).Return(nil)

	mockTaskLauncher.EXPECT().
		LaunchStatefulTasks(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), false).Return(nil)

	err := TaskStart(context.Background(), taskEnt)
	assert.NoError(t, err)
}

func TestTaskStartStatefullWithVolumeDBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := store_mocks.NewMockJobStore(ctrl)
	taskStore := store_mocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	mockHost := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	mockTaskLauncher := mocks2.NewMockLauncher(ctrl)
	mockVolumeStore := store_mocks.NewMockPersistentVolumeStore(ctrl)

	goalStateDriver := &driver{
		jobEngine:     jobGoalStateEngine,
		taskEngine:    taskGoalStateEngine,
		jobStore:      jobStore,
		taskStore:     taskStore,
		volumeStore:   mockVolumeStore,
		taskLauncher:  mockTaskLauncher,
		hostmgrClient: mockHost,
		jobFactory:    jobFactory,
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

	runtime := &pb_task.RuntimeInfo{
		MesosTaskId: &mesos_v1.TaskID{
			Value: &[]string{"3c8a3c3e-71e3-49c5-9aed-2929823f595c-1-3c8a3c3e-71e3-49c5-9aed-2929823f5957"}[0],
		},
		VolumeID: &peloton.VolumeID{
			Value: "my-volume-id",
		},
	}

	jobConfig := &job2.JobConfig{
		RespoolID: &peloton.ResourcePoolID{
			Value: "my-respool-id",
		},
	}

	taskInfo := &pb_task.TaskInfo{
		InstanceId: instanceID,
		Config: &pb_task.TaskConfig{
			Volume: &pb_task.PersistentVolumeConfig{},
		},
		Runtime: runtime,
	}

	taskID := fmt.Sprintf("%s-%d", jobID, instanceID)
	taskInfos := make(map[string]*pb_task.TaskInfo)
	taskInfos[taskID] = taskInfo

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), jobID).Return(jobConfig, nil)

	taskStore.EXPECT().
		GetTaskByID(gomock.Any(), fmt.Sprintf("%s-%d", jobID.GetValue(), instanceID)).Return(taskInfo, nil)

	mockVolumeStore.EXPECT().
		GetPersistentVolume(gomock.Any(), runtime.VolumeID).Return(&volume.PersistentVolumeInfo{
		State: volume.VolumeState_CREATED,
	}, nil)

	mockTaskLauncher.EXPECT().
		GetLaunchableTasks(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).Return(taskInfos, nil)

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).Return(fmt.Errorf("fake db write error"))

	err := TaskStart(context.Background(), taskEnt)
	assert.Error(t, err)
}

func TestTaskStartStatefullWithoutVolume(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := store_mocks.NewMockJobStore(ctrl)
	taskStore := store_mocks.NewMockTaskStore(ctrl)
	jobGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	taskGoalStateEngine := goalstatemocks.NewMockEngine(ctrl)
	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	mockHost := host_mocks.NewMockInternalHostServiceYARPCClient(ctrl)
	mockResmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	mockTaskLauncher := mocks2.NewMockLauncher(ctrl)
	mockVolumeStore := store_mocks.NewMockPersistentVolumeStore(ctrl)

	goalStateDriver := &driver{
		jobEngine:     jobGoalStateEngine,
		taskEngine:    taskGoalStateEngine,
		jobStore:      jobStore,
		taskStore:     taskStore,
		volumeStore:   mockVolumeStore,
		taskLauncher:  mockTaskLauncher,
		hostmgrClient: mockHost,
		jobFactory:    jobFactory,
		resmgrClient:  mockResmgrClient,
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

	runtime := &pb_task.RuntimeInfo{
		MesosTaskId: &mesos_v1.TaskID{
			Value: &[]string{"3c8a3c3e-71e3-49c5-9aed-2929823f595c-1-3c8a3c3e-71e3-49c5-9aed-2929823f5957"}[0],
		},
		VolumeID: &peloton.VolumeID{
			Value: "my-volume-id",
		},
	}

	jobConfig := &job2.JobConfig{
		RespoolID: &peloton.ResourcePoolID{
			Value: "my-respool-id",
		},
	}

	taskInfo := &pb_task.TaskInfo{
		InstanceId: instanceID,
		Config: &pb_task.TaskConfig{
			Volume: &pb_task.PersistentVolumeConfig{},
		},
		Runtime: runtime,
	}

	jobFactory.EXPECT().
		GetJob(jobID).Return(cachedJob)

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), jobID).Return(jobConfig, nil)

	taskStore.EXPECT().
		GetTaskByID(gomock.Any(), fmt.Sprintf("%s-%d", jobID.GetValue(), instanceID)).Return(taskInfo, nil)

	mockVolumeStore.EXPECT().
		GetPersistentVolume(gomock.Any(), runtime.VolumeID).Return(nil, &storage.VolumeNotFoundError{})

	request := &resmgrsvc.EnqueueGangsRequest{
		Gangs:   util.ConvertToResMgrGangs([]*pb_task.TaskInfo{taskInfo}, jobConfig),
		ResPool: jobConfig.RespoolID,
	}
	mockResmgrClient.EXPECT().EnqueueGangs(gomock.Any(), request).Return(nil, nil)

	cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).Return(nil)

	err := TaskStart(context.Background(), taskEnt)
	assert.NoError(t, err)
}
