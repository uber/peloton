package goalstate

import (
	"context"
	"testing"

	"fmt"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	job2 "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/volume"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	resmocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"

	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	mocks2 "code.uber.internal/infra/peloton/jobmgr/task/launcher/mocks"
	"code.uber.internal/infra/peloton/storage"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type TaskStartTestSuite struct {
	suite.Suite

	ctrl                *gomock.Controller
	jobStore            *storemocks.MockJobStore
	taskStore           *storemocks.MockTaskStore
	jobGoalStateEngine  *goalstatemocks.MockEngine
	taskGoalStateEngine *goalstatemocks.MockEngine
	jobFactory          *cachedmocks.MockJobFactory
	cachedJob           *cachedmocks.MockJob
	cachedConfig        *cachedmocks.MockJobConfig
	cachedTask          *cachedmocks.MockTask
	mockTaskLauncher    *mocks2.MockLauncher
	mockVolumeStore     *storemocks.MockPersistentVolumeStore
	goalStateDriver     *driver
	resmgrClient        *resmocks.MockResourceManagerServiceYARPCClient
	jobID               *peloton.JobID
	instanceID          uint32
	taskEnt             *taskEntity
}

func TestTaskStart(t *testing.T) {
	suite.Run(t, new(TaskStartTestSuite))
}

func (suite *TaskStartTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)

	suite.resmgrClient = resmocks.NewMockResourceManagerServiceYARPCClient(suite.ctrl)
	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.taskGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedConfig = cachedmocks.NewMockJobConfig(suite.ctrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.ctrl)
	suite.mockTaskLauncher = mocks2.NewMockLauncher(suite.ctrl)
	suite.mockVolumeStore = storemocks.NewMockPersistentVolumeStore(suite.ctrl)

	suite.goalStateDriver = &driver{
		jobEngine:    suite.jobGoalStateEngine,
		taskEngine:   suite.taskGoalStateEngine,
		jobStore:     suite.jobStore,
		taskStore:    suite.taskStore,
		jobFactory:   suite.jobFactory,
		resmgrClient: suite.resmgrClient,
		volumeStore:  suite.mockVolumeStore,
		taskLauncher: suite.mockTaskLauncher,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
	}
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.instanceID = 0
	suite.taskEnt = &taskEntity{
		jobID:      suite.jobID,
		instanceID: suite.instanceID,
		driver:     suite.goalStateDriver,
	}

	suite.goalStateDriver.cfg.normalize()
}

func (suite *TaskStartTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *TaskStartTestSuite) TestTaskStartStateless() {
	jobConfig := &job2.JobConfig{
		RespoolID: &peloton.ResourcePoolID{
			Value: "my-respool-id",
		},
	}
	taskInfo := &pbtask.TaskInfo{
		InstanceId: suite.instanceID,
		Config: &pbtask.TaskConfig{
			Volume: &pbtask.PersistentVolumeConfig{},
		},
		Runtime: &pbtask.RuntimeInfo{},
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(suite.cachedConfig, nil)

	suite.cachedConfig.EXPECT().
		GetSLA().
		Return(&job2.SlaConfig{}).
		AnyTimes()

	suite.cachedConfig.EXPECT().
		GetRespoolID().
		Return(jobConfig.RespoolID)

	suite.taskStore.EXPECT().
		GetTaskByID(gomock.Any(), fmt.Sprintf("%s-%d", suite.jobID.GetValue(), suite.instanceID)).
		Return(taskInfo, nil)

	request := &resmgrsvc.EnqueueGangsRequest{
		Gangs:   util.ConvertToResMgrGangs([]*pbtask.TaskInfo{taskInfo}, jobConfig.GetSla()),
		ResPool: jobConfig.RespoolID,
	}

	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), request).
		Return(nil, nil)

	suite.cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Return(nil)

	err := TaskStart(context.Background(), suite.taskEnt)
	suite.NoError(err)
}

func (suite *TaskStartTestSuite) TestTaskStartWithSlaMaxRunningInstances() {
	jobConfig := &job2.JobConfig{
		InstanceCount: 2,
		Sla: &job2.SlaConfig{
			MaximumRunningInstances: 1,
		},
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(suite.cachedConfig, nil)

	suite.cachedConfig.EXPECT().
		GetSLA().
		Return(jobConfig.Sla)

	suite.cachedConfig.EXPECT().
		GetType().
		Return(job2.JobType_BATCH)

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := TaskStart(context.Background(), suite.taskEnt)
	suite.NoError(err)
}

func (suite *TaskStartTestSuite) TestTaskStartStatefulWithVolume() {
	runtime := &pbtask.RuntimeInfo{
		MesosTaskId: &mesos_v1.TaskID{
			Value: &[]string{"3c8a3c3e-71e3-49c5-9aed-2929823f595c-1-3c8a3c3e-71e3-49c5-9aed-2929823f5957"}[0],
		},
		VolumeID: &peloton.VolumeID{
			Value: "my-volume-id",
		},
	}

	taskInfo := &pbtask.TaskInfo{
		InstanceId: suite.instanceID,
		Config: &pbtask.TaskConfig{
			Volume: &pbtask.PersistentVolumeConfig{},
		},
		Runtime: runtime,
	}

	taskID := fmt.Sprintf("%s-%d", suite.jobID.GetValue(), suite.instanceID)
	taskInfos := make(map[string]*pbtask.TaskInfo)
	taskInfos[taskID] = taskInfo

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(suite.cachedConfig, nil)

	suite.cachedConfig.EXPECT().
		GetSLA().
		Return(&job2.SlaConfig{})

	suite.taskStore.EXPECT().
		GetTaskByID(gomock.Any(), fmt.Sprintf("%s-%d", suite.jobID.GetValue(), suite.instanceID)).
		Return(taskInfo, nil)

	suite.mockVolumeStore.EXPECT().
		GetPersistentVolume(gomock.Any(), runtime.VolumeID).Return(&volume.PersistentVolumeInfo{
		State: volume.VolumeState_CREATED,
	}, nil)

	suite.mockTaskLauncher.EXPECT().
		GetLaunchableTasks(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).Return(taskInfos, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).
		Return(suite.cachedTask)

	suite.cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Return(nil)

	suite.cachedTask.EXPECT().
		GetRunTime(gomock.Any()).
		Return(runtime, nil)

	suite.mockTaskLauncher.EXPECT().
		CreateLaunchableTasks(gomock.Any(), gomock.Any()).
		Return(nil, nil)

	suite.mockTaskLauncher.EXPECT().
		LaunchStatefulTasks(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), false).
		Return(nil)

	err := TaskStart(context.Background(), suite.taskEnt)
	suite.NoError(err)
}

func (suite *TaskStartTestSuite) TestTaskStartStatefulWithVolumeDBError() {
	runtime := &pbtask.RuntimeInfo{
		MesosTaskId: &mesos_v1.TaskID{
			Value: &[]string{"3c8a3c3e-71e3-49c5-9aed-2929823f595c-1-3c8a3c3e-71e3-49c5-9aed-2929823f5957"}[0],
		},
		VolumeID: &peloton.VolumeID{
			Value: "my-volume-id",
		},
	}

	taskInfo := &pbtask.TaskInfo{
		InstanceId: suite.instanceID,
		Config: &pbtask.TaskConfig{
			Volume: &pbtask.PersistentVolumeConfig{},
		},
		Runtime: runtime,
	}

	taskID := fmt.Sprintf("%s-%d", suite.jobID.GetValue(), suite.instanceID)
	taskInfos := make(map[string]*pbtask.TaskInfo)
	taskInfos[taskID] = taskInfo

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(suite.cachedConfig, nil)

	suite.cachedConfig.EXPECT().
		GetSLA().
		Return(&job2.SlaConfig{})

	suite.taskStore.EXPECT().
		GetTaskByID(gomock.Any(), fmt.Sprintf("%s-%d", suite.jobID.GetValue(), suite.instanceID)).
		Return(taskInfo, nil)

	suite.mockVolumeStore.EXPECT().
		GetPersistentVolume(gomock.Any(), runtime.VolumeID).
		Return(&volume.PersistentVolumeInfo{
			State: volume.VolumeState_CREATED,
		}, nil)

	suite.mockTaskLauncher.EXPECT().
		GetLaunchableTasks(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).Return(taskInfos, nil)

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(suite.instanceID).
		Return(suite.cachedTask)

	suite.cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Return(fmt.Errorf("fake db write error"))

	err := TaskStart(context.Background(), suite.taskEnt)
	suite.Error(err)
}

func (suite *TaskStartTestSuite) TestTaskStartStatefulWithoutVolume() {

	runtime := &pbtask.RuntimeInfo{
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

	taskInfo := &pbtask.TaskInfo{
		InstanceId: suite.instanceID,
		Config: &pbtask.TaskConfig{
			Volume: &pbtask.PersistentVolumeConfig{},
		},
		Runtime: runtime,
	}

	suite.jobFactory.EXPECT().
		GetJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(suite.cachedConfig, nil)

	suite.cachedConfig.EXPECT().
		GetSLA().
		Return(&job2.SlaConfig{}).
		AnyTimes()

	suite.cachedConfig.EXPECT().
		GetRespoolID().
		Return(jobConfig.RespoolID)

	suite.taskStore.EXPECT().
		GetTaskByID(gomock.Any(), fmt.Sprintf("%s-%d", suite.jobID.GetValue(), suite.instanceID)).
		Return(taskInfo, nil)

	suite.mockVolumeStore.EXPECT().
		GetPersistentVolume(gomock.Any(), runtime.VolumeID).
		Return(nil, &storage.VolumeNotFoundError{})

	request := &resmgrsvc.EnqueueGangsRequest{
		Gangs:   util.ConvertToResMgrGangs([]*pbtask.TaskInfo{taskInfo}, jobConfig.GetSla()),
		ResPool: jobConfig.RespoolID,
	}
	suite.resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), request).
		Return(nil, nil)

	suite.cachedJob.EXPECT().
		UpdateTasks(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Return(nil)

	err := TaskStart(context.Background(), suite.taskEnt)
	suite.NoError(err)
}
