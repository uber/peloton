package goalstate

import (
	"context"
	"testing"

	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"

	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	"code.uber.internal/infra/peloton/storage/cassandra"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

var csStore *cassandra.Store

func init() {
	conf := cassandra.MigrateForTest()
	var err error
	csStore, err = cassandra.NewStore(conf, tally.NoopScope)
	if err != nil {
		log.Fatal(err)
	}
}

func TestJobCreateTasks(t *testing.T) {
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
		resmgrClient: resmgrClient,
		jobStore:     jobStore,
		taskStore:    taskStore,
		jobFactory:   jobFactory,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	jobEnt := &jobEntity{
		id:     jobID,
		driver: goalStateDriver,
	}

	instanceCount := uint32(4)

	jobConfig := pb_job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: instanceCount,
		Type:          pb_job.JobType_BATCH,
	}

	emptyTaskInfo := make(map[uint32]*pb_task.TaskInfo)

	taskStore.EXPECT().
		GetTasksForJob(gomock.Any(), jobID).
		Return(emptyTaskInfo, nil)

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), jobID).
		Return(&jobConfig, nil)

	taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), jobID, gomock.Any()).
		Return(nil)

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		CreateTasks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(_ context.Context, jobInfo *pb_job.JobInfo, _ cached.UpdateRequest) {
			assert.Equal(t, jobInfo.Runtime.State, pb_job.JobState_PENDING)
		}).
		Return(nil)

	resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	jobFactory.EXPECT().
		GetJob(jobID).
		Return(cachedJob).
		Times(2)

	cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Return(nil)

	err := JobCreateTasks(context.Background(), jobEnt)
	assert.NoError(t, err)
}

// These are integration tests within job manager.
//TODO find a place to put them.
/*func TestJobCreateTasksWithStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var jobID = &peloton.JobID{Value: uuid.New()}
	var sla = pb_job.SlaConfig{
		Priority:    22,
		Preemptible: false,
	}
	var taskConfig = pb_task.TaskConfig{
		Resource: &pb_task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
		},
	}
	var jobConfig = pb_job.JobConfig{
		Name:          "TestValidatorWithStore",
		OwningTeam:    "team6",
		LdapGroups:    []string{"money", "team6", "gsg9"},
		SLA:           &sla,
		DefaultConfig: &taskConfig,
		InstanceCount: 10,
	}

	err := csStore.CreateJob(context.Background(), jobID, &jobConfig, "gsg9")

	resmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)

	j := &job{
		id: jobID,
		m: &manager{
			mtx:          NewMetrics(tally.NoopScope),
			resmgrClient: resmgrClient,
			jobStore:     csStore,
			taskStore:    csStore,
			jobs:         map[string]*job{},
		},
		tasks: map[uint32]*task{},
	}

	resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	reschedule, err := j.RunAction(context.Background(), JobCreateTasks)
	assert.False(t, reschedule)
	assert.NoError(t, err)
	assert.Equal(t, jobConfig.InstanceCount, uint32(len(j.tasks)))

	jobRuntime, err := csStore.GetJobRuntime(context.Background(), jobID)
	assert.Nil(t, err)
	assert.Equal(t, pb_job.JobState_PENDING, jobRuntime.GetState())

	for i := uint32(0); i < jobConfig.InstanceCount; i++ {
		taskInfo, err := csStore.GetTaskForJob(context.Background(), jobID, i)
		assert.Nil(t, err)
		assert.Equal(t, pb_task.TaskState_PENDING, taskInfo[i].Runtime.GetState())
	}
}*/

func TestJobRecover(t *testing.T) {
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
		resmgrClient: resmgrClient,
		jobStore:     jobStore,
		taskStore:    taskStore,
		jobFactory:   jobFactory,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	jobEnt := &jobEntity{
		id:     jobID,
		driver: goalStateDriver,
	}

	instanceCount := uint32(4)

	jobConfig := pb_job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: instanceCount,
	}

	taskInfos := make(map[uint32]*pb_task.TaskInfo)
	taskInfos[0] = &pb_task.TaskInfo{
		Runtime: &pb_task.RuntimeInfo{
			State:     pb_task.TaskState_RUNNING,
			GoalState: pb_task.TaskState_SUCCEEDED,
		},
		InstanceId: 0,
		JobId:      jobID,
	}
	taskInfos[1] = &pb_task.TaskInfo{
		Runtime: &pb_task.RuntimeInfo{
			State:     pb_task.TaskState_INITIALIZED,
			GoalState: pb_task.TaskState_SUCCEEDED,
		},
		InstanceId: 1,
		JobId:      jobID,
	}

	taskStore.EXPECT().
		GetTasksForJob(gomock.Any(), jobID).
		Return(taskInfos, nil)

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), jobID).
		Return(&jobConfig, nil)

	taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), jobID, gomock.Any()).
		Return(nil)

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(_ context.Context, jobInfo *pb_job.JobInfo, _ cached.UpdateRequest) {
			assert.Equal(t, jobInfo.Runtime.State, pb_job.JobState_PENDING)
		}).
		Return(nil)

	cachedJob.EXPECT().
		GetTask(uint32(1)).Return(nil)

	cachedJob.EXPECT().
		ReplaceTasks(map[uint32]*pb_task.RuntimeInfo{1: taskInfos[1].GetRuntime()}, false).
		Return(nil)

	cachedJob.EXPECT().
		CreateTasks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	cachedJob.EXPECT().
		CreateTasks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	jobFactory.EXPECT().
		GetJob(jobID).
		Return(cachedJob).
		Times(2)

	cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Return(nil)

	err := JobCreateTasks(context.Background(), jobEnt)
	assert.NoError(t, err)
}

// These are integration tests within job manager.
//TODO find a place to put them.
/*func TestJobRecoverWithStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var jobID = &peloton.JobID{Value: uuid.New()}
	var sla = pb_job.SlaConfig{
		Priority:    22,
		Preemptible: false,
	}
	var taskConfig = pb_task.TaskConfig{
		Resource: &pb_task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
		},
	}
	var jobConfig = pb_job.JobConfig{
		Name:          "TestValidatorWithStore",
		OwningTeam:    "team6",
		LdapGroups:    []string{"money", "team6", "gsg9"},
		SLA:           &sla,
		DefaultConfig: &taskConfig,
		InstanceCount: 10,
	}
	ctx := context.Background()

	err := csStore.CreateJob(ctx, jobID, &jobConfig, "gsg9")
	resmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)
	j := &job{
		id: jobID,
		m: &manager{
			mtx:          NewMetrics(tally.NoopScope),
			resmgrClient: resmgrClient,
			jobStore:     csStore,
			taskStore:    csStore,
			jobs:         map[string]*job{},
		},
		tasks: map[uint32]*task{},
	}

	runtimes := make(map[uint32]*pb_task.RuntimeInfo)
	for i := uint32(0); i < uint32(3); i++ {
		runtime := jobmgr_task.CreateInitializingTask(jobID, i, &jobConfig)
		err := csStore.CreateTaskRuntime(ctx, jobID, i, runtime, jobConfig.OwningTeam)
		assert.NoError(t, err)
		runtimes[i] = runtime
	}

	for i := uint32(7); i < uint32(9); i++ {
		runtime := jobmgr_task.CreateInitializingTask(jobID, i, &jobConfig)
		err := csStore.CreateTaskRuntime(ctx, jobID, i, runtime, jobConfig.OwningTeam)
		assert.NoError(t, err)
		runtimes[i] = runtime
	}

	j.m.SetTasks(j.id, runtimes, UpdateAndSchedule)

	resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	reschedule, err := j.RunAction(context.Background(), JobCreateTasks)
	assert.False(t, reschedule)
	assert.NoError(t, err)

	jobRuntime, err := csStore.GetJobRuntime(context.Background(), jobID)
	assert.Nil(t, err)
	assert.Equal(t, pb_job.JobState_PENDING, jobRuntime.GetState())

	for i := uint32(0); i < jobConfig.InstanceCount; i++ {
		taskInfo, err := csStore.GetTaskForJob(context.Background(), jobID, i)
		assert.Nil(t, err)
		assert.Equal(t, pb_task.TaskState_PENDING, taskInfo[i].GetRuntime().GetState())
	}
}*/

func TestJobMaxRunningInstances(t *testing.T) {
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
		resmgrClient: resmgrClient,
		jobStore:     jobStore,
		taskStore:    taskStore,
		jobFactory:   jobFactory,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	jobEnt := &jobEntity{
		id:     jobID,
		driver: goalStateDriver,
	}

	instanceCount := uint32(4)

	jobConfig := pb_job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: instanceCount,
		Type:          pb_job.JobType_BATCH,
		SLA: &pb_job.SlaConfig{
			MaximumRunningInstances: 1,
		},
	}

	emptyTaskInfo := make(map[uint32]*pb_task.TaskInfo)

	taskStore.EXPECT().
		GetTasksForJob(gomock.Any(), jobID).
		Return(emptyTaskInfo, nil)

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), jobID).
		Return(&jobConfig, nil)

	taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), jobID, gomock.Any()).
		Return(nil)

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		CreateTasks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	jobFactory.EXPECT().
		GetJob(jobID).
		Return(cachedJob).
		Times(2)

	cachedJob.EXPECT().
		PatchTasks(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, runtimeDiffs map[uint32]map[string]interface{}) {
			assert.Equal(t, uint32(len(runtimeDiffs)), jobConfig.SLA.MaximumRunningInstances)
			for _, runtimeDiff := range runtimeDiffs {
				assert.Equal(t, runtimeDiff[cached.StateField], pb_task.TaskState_PENDING)
			}
		}).
		Return(nil)

	cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(_ context.Context, jobInfo *pb_job.JobInfo, _ cached.UpdateRequest) {
			assert.Equal(t, jobInfo.Runtime.State, pb_job.JobState_PENDING)
		}).
		Return(nil)

	err := JobCreateTasks(context.Background(), jobEnt)
	assert.NoError(t, err)
}

func TestJobRecoverMaxRunningInstances(t *testing.T) {
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
		resmgrClient: resmgrClient,
		jobStore:     jobStore,
		taskStore:    taskStore,
		jobFactory:   jobFactory,
		mtx:          NewMetrics(tally.NoopScope),
		cfg:          &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	jobEnt := &jobEntity{
		id:     jobID,
		driver: goalStateDriver,
	}

	instanceCount := uint32(4)

	jobConfig := pb_job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: instanceCount,
		SLA: &pb_job.SlaConfig{
			MaximumRunningInstances: 1,
		},
	}

	taskInfos := make(map[uint32]*pb_task.TaskInfo)
	taskInfos[0] = &pb_task.TaskInfo{
		Runtime: &pb_task.RuntimeInfo{
			State:     pb_task.TaskState_RUNNING,
			GoalState: pb_task.TaskState_SUCCEEDED,
		},
		InstanceId: 0,
		JobId:      jobID,
	}
	taskInfos[1] = &pb_task.TaskInfo{
		Runtime: &pb_task.RuntimeInfo{
			State:     pb_task.TaskState_INITIALIZED,
			GoalState: pb_task.TaskState_SUCCEEDED,
		},
		InstanceId: 1,
		JobId:      jobID,
	}

	taskStore.EXPECT().
		GetTasksForJob(gomock.Any(), jobID).
		Return(taskInfos, nil)

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), jobID).
		Return(&jobConfig, nil)

	taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), jobID, gomock.Any()).
		Return(nil)

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	jobFactory.EXPECT().
		GetJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(uint32(1)).Return(nil)

	cachedJob.EXPECT().
		ReplaceTasks(map[uint32]*pb_task.RuntimeInfo{1: taskInfos[1].GetRuntime()}, false).
		Return(nil)

	cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(_ context.Context, jobInfo *pb_job.JobInfo, _ cached.UpdateRequest) {
			assert.Equal(t, jobInfo.Runtime.State, pb_job.JobState_PENDING)
		}).
		Return(nil)

	cachedJob.EXPECT().
		GetJobType().Return(pb_job.JobType_BATCH)

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	cachedJob.EXPECT().
		CreateTasks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	cachedJob.EXPECT().
		GetJobType().Return(pb_job.JobType_BATCH)

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	cachedJob.EXPECT().
		CreateTasks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	cachedJob.EXPECT().
		GetJobType().Return(pb_job.JobType_BATCH)

	jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Return()

	err := JobCreateTasks(context.Background(), jobEnt)
	assert.NoError(t, err)
}
