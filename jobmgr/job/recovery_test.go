package job

import (
	"context"
	"fmt"
	"testing"
	"time"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/jobmgr/tracked/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/storage/cassandra"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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

func TestValidatorWithStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTrackedManager := mocks.NewMockManager(ctrl)

	var jobID = &peloton.JobID{Value: uuid.New()}
	var sla = job.SlaConfig{
		Priority:                22,
		MaximumRunningInstances: 3,
		Preemptible:             false,
	}
	var taskConfig = task.TaskConfig{
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
		},
	}
	var jobConfig = job.JobConfig{
		Name:          "TestValidatorWithStore",
		OwningTeam:    "team6",
		LdapGroups:    []string{"money", "team6", "gsg9"},
		Sla:           &sla,
		DefaultConfig: &taskConfig,
		InstanceCount: 10,
	}
	err := csStore.CreateJob(context.Background(), jobID, &jobConfig, "gsg9")
	assert.Nil(t, err)

	// Create a job with instanceCount 10 but only 5 tasks
	// validate that after recovery, all tasks are created and
	// the job state is update to pending.
	jobRuntime, err := csStore.GetJobRuntime(context.Background(), jobID)
	assert.Nil(t, err)
	jobRuntime.GoalState = job.JobState_SUCCEEDED
	jobRuntime.CreationTime = (time.Now().Add(-10 * time.Hour)).Format(time.RFC3339Nano)

	for i := uint32(0); i < uint32(3); i++ {
		mockTrackedManager.EXPECT().SetTask(jobID, i, gomock.Any()).AnyTimes()
		err := createTaskForJob(
			context.Background(),
			mockTrackedManager,
			csStore,
			jobID,
			i,
			&jobConfig)
		assert.Nil(t, err)
	}

	for i := uint32(7); i < uint32(9); i++ {
		mockTrackedManager.EXPECT().SetTask(jobID, i, gomock.Any()).AnyTimes()
		err := createTaskForJob(
			context.Background(),
			mockTrackedManager,
			csStore,
			jobID,
			i,
			&jobConfig)
		assert.Nil(t, err)
	}
	err = csStore.UpdateJobRuntime(context.Background(), jobID, jobRuntime)
	assert.Nil(t, err)

	validator := NewJobRecovery(mockTrackedManager, csStore, csStore, tally.NoopScope)

	for i := uint32(3); i < uint32(7); i++ {
		mockTrackedManager.EXPECT().SetTask(jobID, i, gomock.Any())
	}
	mockTrackedManager.EXPECT().SetTask(jobID, uint32(9), gomock.Any())
	validator.recoverJob(context.Background(), jobID, false)

	jobRuntime, err = csStore.GetJobRuntime(context.Background(), jobID)
	assert.Nil(t, err)
	assert.Equal(t, job.JobState_PENDING, jobRuntime.State)
	for i := uint32(0); i < jobConfig.InstanceCount; i++ {
		_, err := csStore.GetTaskForJob(context.Background(), jobID, i)
		assert.Nil(t, err)
	}
}

func TestValidator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTrackedManager := mocks.NewMockManager(ctrl)

	jobID := &peloton.JobID{
		Value: "job0",
	}
	jobConfig := job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"money", "team6", "qa"},
		InstanceCount: uint32(3),
		Sla:           &job.SlaConfig{},
	}
	var jobRuntime = job.RuntimeInfo{
		State:        job.JobState_INITIALIZED,
		CreationTime: (time.Now().Add(-10 * time.Hour)).Format(time.RFC3339Nano),
	}
	var tasks = map[uint32]*task.TaskInfo{
		0: createTaskInfo(jobID, uint32(0), task.TaskState_INITIALIZED),
		2: createTaskInfo(jobID, uint32(2), task.TaskState_INITIALIZED),
	}
	var mockJobStore = store_mocks.NewMockJobStore(ctrl)
	var mockTaskStore = store_mocks.NewMockTaskStore(ctrl)

	mockJobStore.EXPECT().
		GetJobsByStates(context.Background(), []job.JobState{job.JobState_INITIALIZED}).
		Return([]peloton.JobID{*jobID}, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		GetJobConfig(context.Background(), gomock.Any()).
		Return(&jobConfig, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		GetJobRuntime(context.Background(), gomock.Any()).
		Return(&jobRuntime, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		UpdateJobRuntime(context.Background(), jobID, gomock.Any()).
		Return(nil).
		AnyTimes()

	mockTaskStore.EXPECT().
		CreateTaskRuntime(context.Background(), gomock.Any(), uint32(1), gomock.Any(), gomock.Any()).
		Return(nil)
	mockTrackedManager.EXPECT().SetTask(jobID, gomock.Any(), gomock.Any()).AnyTimes()
	mockTaskStore.EXPECT().
		GetTasksForJobByRange(context.Background(), jobID, &task.InstanceRange{From: 0, To: 3}, storage.ConfigurationNotNeeded).
		Return(tasks, nil)

	validator := NewJobRecovery(mockTrackedManager, mockJobStore, mockTaskStore, tally.NoopScope)
	validator.recoverJobs(context.Background())

	// jobRuntime create time is long ago, thus after validateJobs(),
	// job runtime state should be pending
	assert.Equal(t, job.JobState_PENDING, jobRuntime.State)

	// Calling recoverJobs a second time, before recoveryInterval, should skip
	// the recovery
	jobRuntime.State = job.JobState_INITIALIZED
	validator.recoverJobs(context.Background())

	assert.Equal(t, job.JobState_INITIALIZED, jobRuntime.State)

	// jobRuntime create time is recent, thus validateJobs() should skip the
	// validation
	jobRuntime.CreationTime = time.Now().Format(time.RFC3339Nano)
	validator.lastRecoveryTime = time.Now().Add(-recoveryInterval - 1)
	validator.recoverJobs(context.Background())

	assert.Equal(t, job.JobState_INITIALIZED, jobRuntime.State)
}

func TestValidatorFailures(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTrackedManager := mocks.NewMockManager(ctrl)

	jobID := &peloton.JobID{
		Value: "job0",
	}
	jobConfig := job.JobConfig{
		OwningTeam:    "team6",
		LdapGroups:    []string{"money", "team6", "qa"},
		InstanceCount: uint32(3),
		Sla:           &job.SlaConfig{},
	}
	var jobRuntime = job.RuntimeInfo{
		State:        job.JobState_INITIALIZED,
		CreationTime: (time.Now().Add(-10 * time.Hour)).Format(time.RFC3339Nano),
	}
	var tasks = map[uint32]*task.TaskInfo{
		0: createTaskInfo(jobID, uint32(0), task.TaskState_INITIALIZED),
		2: createTaskInfo(jobID, uint32(2), task.TaskState_INITIALIZED),
	}
	var mockJobStore = store_mocks.NewMockJobStore(ctrl)
	var mockTaskStore = store_mocks.NewMockTaskStore(ctrl)

	mockJobStore.EXPECT().
		GetJobsByStates(context.Background(), []job.JobState{job.JobState_INITIALIZED}).
		Return([]peloton.JobID{*jobID}, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		GetJobConfig(context.Background(), gomock.Any()).
		Return(&jobConfig, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		GetJobRuntime(context.Background(), gomock.Any()).
		Return(&jobRuntime, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		UpdateJobRuntime(context.Background(), jobID, gomock.Any()).
		Return(errors.New("Mock error")).
		MinTimes(1).
		MaxTimes(1)
	mockJobStore.EXPECT().
		UpdateJobRuntime(context.Background(), jobID, gomock.Any()).
		Return(nil).
		AnyTimes()

	mockTaskStore.EXPECT().
		CreateTaskRuntime(context.Background(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	mockTrackedManager.EXPECT().SetTask(jobID, gomock.Any(), gomock.Any()).AnyTimes()
	mockTaskStore.EXPECT().
		GetTasksForJobByRange(context.Background(), jobID, &task.InstanceRange{From: 0, To: 3}, storage.ConfigurationNotNeeded).
		Return(tasks, nil).
		AnyTimes()

	validator := NewJobRecovery(mockTrackedManager, mockJobStore, mockTaskStore, tally.NoopScope)
	validator.recoverJobs(context.Background())

	assert.Equal(t, job.JobState_PENDING, jobRuntime.State)

	// Second call would fail as the jobstore updateJobRuntime would fail once,
	// nothing changed to the job but tasks sent
	validator = NewJobRecovery(mockTrackedManager, mockJobStore, mockTaskStore, tally.NoopScope)
	validator.recoverJobs(context.Background())
	assert.Equal(t, job.JobState_PENDING, jobRuntime.State)

	// jobRuntime create time is long ago, thus after validateJobs(),
	// job runtime state should be pending and task 1 and task 2 are sent by resmgr client
	validator = NewJobRecovery(mockTrackedManager, mockJobStore, mockTaskStore, tally.NoopScope)
	validator.recoverJobs(context.Background())
	assert.Equal(t, job.JobState_PENDING, jobRuntime.State)
}

func createTaskInfo(jobID *peloton.JobID, i uint32, state task.TaskState) *task.TaskInfo {
	var tID = fmt.Sprintf("%s-%d-%s", jobID.Value, i, uuid.NewUUID().String())
	var taskInfo = task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			MesosTaskId: &mesos.TaskID{Value: &tID},
			State:       state,
		},
		Config: &task.TaskConfig{
			Name: tID,
		},
		InstanceId: uint32(i),
		JobId:      jobID,
	}
	return &taskInfo
}
