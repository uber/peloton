package tracked

import (
	"context"
	"testing"

	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	res_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc/mocks"

	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"
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

func TestJob(t *testing.T) {
	j := &job{
		id: &peloton.JobID{Value: uuid.NewRandom().String()},
		m: &manager{
			mtx: NewMetrics(tally.NoopScope),
		},
	}

	reschedule, err := j.RunAction(context.Background(), JobNoAction)
	assert.False(t, reschedule)
	assert.NoError(t, err)

	jobRuntime := &pb_job.RuntimeInfo{
		State:     pb_job.JobState_RUNNING,
		GoalState: pb_job.JobState_SUCCEEDED,
	}
	jobConfig := &pb_job.JobConfig{
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		InstanceCount: 1,
	}
	jobInfo := &pb_job.JobInfo{
		Runtime: jobRuntime,
		Config:  jobConfig,
	}
	j.updateRuntime(jobInfo)

	runtime, err := j.GetJobRuntime(context.Background())
	assert.Equal(t, jobRuntime, runtime)
	assert.NoError(t, err)

	config, err := j.getConfig()
	assert.Equal(t, jobConfig.RespoolID, config.respoolID)
	assert.Equal(t, jobConfig.InstanceCount, config.instanceCount)
	assert.NoError(t, err)

	j.ClearJobRuntime()
	assert.Nil(t, j.runtime)
}

func TestJobCreateTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := store_mocks.NewMockJobStore(ctrl)
	taskStore := store_mocks.NewMockTaskStore(ctrl)
	resmgrClient := res_mocks.NewMockResourceManagerServiceYARPCClient(ctrl)

	j := &job{
		id: &peloton.JobID{Value: uuid.NewRandom().String()},
		m: &manager{
			mtx:          NewMetrics(tally.NoopScope),
			resmgrClient: resmgrClient,
			jobStore:     jobStore,
			taskStore:    taskStore,
		},
		tasks: map[uint32]*task{},
	}

	instanceCount := uint32(4)

	jobConfig := pb_job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: instanceCount,
		Type:          pb_job.JobType_BATCH,
	}

	jobRuntime := pb_job.RuntimeInfo{
		State:     pb_job.JobState_INITIALIZED,
		GoalState: pb_job.JobState_SUCCEEDED,
	}

	newJobRuntime := pb_job.RuntimeInfo{
		State:     pb_job.JobState_PENDING,
		GoalState: pb_job.JobState_SUCCEEDED,
	}

	emptyTaskInfo := make(map[uint32]*pb_task.TaskInfo)

	taskStore.EXPECT().
		GetTasksForJob(gomock.Any(), j.id).
		Return(emptyTaskInfo, nil)

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), j.id).
		Return(&jobConfig, nil)

	taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), j.id, gomock.Any()).
		Return(nil)

	taskStore.EXPECT().
		CreateTaskRuntimes(gomock.Any(), j.id, gomock.Any(), gomock.Any()).
		Return(nil)

	resmgrClient.EXPECT().
		EnqueueGangs(gomock.Any(), gomock.Any()).
		Return(&resmgrsvc.EnqueueGangsResponse{}, nil)

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), j.id).
		Return(&jobRuntime, nil)

	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), j.id, &newJobRuntime).
		Return(nil)

	reschedule, err := j.RunAction(context.Background(), JobCreateTasks)
	assert.False(t, reschedule)
	assert.NoError(t, err)
	assert.Equal(t, instanceCount, uint32(len(j.tasks)))
}

func TestJobCreateTasksWithStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var jobID = &peloton.JobID{Value: uuid.New()}
	var sla = pb_job.SlaConfig{
		Priority:                22,
		MaximumRunningInstances: 3,
		Preemptible:             false,
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
		Sla:           &sla,
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
		assert.Equal(t, pb_task.TaskState_INITIALIZED, taskInfo[i].Runtime.GetState())
	}
}

func TestJobRecover(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStore := store_mocks.NewMockJobStore(ctrl)
	taskStore := store_mocks.NewMockTaskStore(ctrl)

	j := &job{
		id: &peloton.JobID{Value: uuid.NewRandom().String()},
		m: &manager{
			mtx:       NewMetrics(tally.NoopScope),
			jobStore:  jobStore,
			taskStore: taskStore,
		},
		tasks: map[uint32]*task{},
	}

	instanceCount := uint32(4)

	jobConfig := pb_job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"team1", "team2", "team3"},
		InstanceCount: instanceCount,
	}

	jobRuntime := pb_job.RuntimeInfo{
		State:     pb_job.JobState_INITIALIZED,
		GoalState: pb_job.JobState_SUCCEEDED,
	}

	newJobRuntime := pb_job.RuntimeInfo{
		State:     pb_job.JobState_PENDING,
		GoalState: pb_job.JobState_SUCCEEDED,
	}

	taskInfos := make(map[uint32]*pb_task.TaskInfo)
	taskInfos[0] = &pb_task.TaskInfo{
		Runtime: &pb_task.RuntimeInfo{
			State:     pb_task.TaskState_RUNNING,
			GoalState: pb_task.TaskState_SUCCEEDED,
		},
		InstanceId: 0,
		JobId:      j.id,
	}
	taskInfos[1] = &pb_task.TaskInfo{
		Runtime: &pb_task.RuntimeInfo{
			State:     pb_task.TaskState_INITIALIZED,
			GoalState: pb_task.TaskState_SUCCEEDED,
		},
		InstanceId: 1,
		JobId:      j.id,
	}

	taskStore.EXPECT().
		GetTasksForJob(gomock.Any(), j.id).
		Return(taskInfos, nil)

	jobStore.EXPECT().
		GetJobConfig(gomock.Any(), j.id).
		Return(&jobConfig, nil)

	taskStore.EXPECT().
		CreateTaskConfigs(gomock.Any(), j.id, gomock.Any()).
		Return(nil)

	taskStore.EXPECT().
		CreateTaskRuntime(gomock.Any(), j.id, uint32(2), gomock.Any(), gomock.Any()).
		Return(nil)

	taskStore.EXPECT().
		CreateTaskRuntime(gomock.Any(), j.id, uint32(3), gomock.Any(), gomock.Any()).
		Return(nil)

	jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), j.id).
		Return(&jobRuntime, nil)

	jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), j.id, &newJobRuntime).
		Return(nil)

	reschedule, err := j.RunAction(context.Background(), JobCreateTasks)
	assert.False(t, reschedule)
	assert.NoError(t, err)
}

func TestJobRecoverWithStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var jobID = &peloton.JobID{Value: uuid.New()}
	var sla = pb_job.SlaConfig{
		Priority:                22,
		MaximumRunningInstances: 3,
		Preemptible:             false,
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
		Sla:           &sla,
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
		},
		tasks: map[uint32]*task{},
	}

	for i := uint32(0); i < uint32(3); i++ {
		runtime := jobmgr_task.CreateInitializingTask(jobID, i, &jobConfig)
		err := csStore.CreateTaskRuntime(ctx, jobID, i, runtime, jobConfig.OwningTeam)
		assert.NoError(t, err)
		j.m.SetTask(j.id, i, runtime)
	}

	for i := uint32(7); i < uint32(9); i++ {
		runtime := jobmgr_task.CreateInitializingTask(jobID, i, &jobConfig)
		err := csStore.CreateTaskRuntime(ctx, jobID, i, runtime, jobConfig.OwningTeam)
		assert.NoError(t, err)
		j.m.SetTask(j.id, i, runtime)
	}

	reschedule, err := j.RunAction(context.Background(), JobCreateTasks)
	assert.False(t, reschedule)
	assert.NoError(t, err)

	jobRuntime, err := csStore.GetJobRuntime(context.Background(), jobID)
	assert.Nil(t, err)
	assert.Equal(t, pb_job.JobState_PENDING, jobRuntime.GetState())

	for i := uint32(0); i < jobConfig.InstanceCount; i++ {
		taskInfo, err := csStore.GetTaskForJob(context.Background(), jobID, i)
		assert.Nil(t, err)
		assert.Equal(t, pb_task.TaskState_INITIALIZED, taskInfo[i].GetRuntime().GetState())
	}
}
