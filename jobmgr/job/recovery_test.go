package job

import (
	"context"
	"fmt"
	"testing"
	"time"

	mesos "mesos/v1"
	"peloton/api/job"
	"peloton/api/peloton"
	"peloton/api/task"
	"peloton/private/resmgrsvc"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/yarpc"

	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/storage/cassandra"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"
	yarpc_mocks "code.uber.internal/infra/peloton/vendor_mocks/go.uber.org/yarpc/encoding/json/mocks"
	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
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
	var mockClient = yarpc_mocks.NewMockClient(ctrl)

	var sentTasks = make(map[int]bool)
	mockClient.EXPECT().Call(
		gomock.Any(),
		gomock.Eq(yarpc.NewReqMeta().Procedure("ResourceManagerService.EnqueueTasks")),
		gomock.Any(),
		gomock.Any()).
		Do(func(_ context.Context, _ yarpc.CallReqMeta, reqBody interface{}, _ interface{}) {
			req := reqBody.(*resmgrsvc.EnqueueTasksRequest)
			for _, task := range req.Tasks {
				_, instance, err := util.ParseTaskID(task.Id.Value)
				assert.Nil(t, err)
				sentTasks[instance] = true
			}
		}).
		Return(nil, nil).AnyTimes()

	var jobID = &peloton.JobID{Value: "TestValidatorWithStore"}
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
	err := csStore.CreateJob(jobID, &jobConfig, "gsg9")
	assert.Nil(t, err)

	// Create a job with instanceCount 10 but only 5 tasks
	// validate that after recovery, all tasks are created and
	// the job state is update to pending.
	jobRuntime, err := csStore.GetJobRuntime(jobID)
	assert.Nil(t, err)
	jobRuntime.CreationTime = (time.Now().Add(-10 * time.Hour)).String()

	for i := uint32(0); i < uint32(3); i++ {
		_, err := createTaskForJob(
			csStore,
			jobID,
			i,
			&jobConfig)
		assert.Nil(t, err)
	}

	for i := uint32(7); i < uint32(9); i++ {
		_, err := createTaskForJob(
			csStore,
			jobID,
			i,
			&jobConfig)
		assert.Nil(t, err)
	}
	err = csStore.UpdateJobRuntime(jobID, jobRuntime)
	assert.Nil(t, err)

	validator := NewJobRecovery(csStore, csStore, mockClient, tally.NoopScope)
	validator.recoverJob(jobID)

	jobRuntime, err = csStore.GetJobRuntime(jobID)
	assert.Nil(t, err)
	assert.Equal(t, job.JobState_PENDING, jobRuntime.State)
	for i := uint32(0); i < jobConfig.InstanceCount; i++ {
		_, err := csStore.GetTaskForJob(jobID, i)
		assert.Nil(t, err)
		assert.True(t, sentTasks[int(i)])
	}
}

func TestValidator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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
		CreationTime: (time.Now().Add(-10 * time.Hour)).String(),
	}
	var tasks = []*task.TaskInfo{
		createTaskInfo(jobID, uint32(1), task.TaskState_INITIALIZED),
		createTaskInfo(jobID, uint32(2), task.TaskState_INITIALIZED),
	}
	var createdTasks []*task.TaskInfo
	var sentTasks = make(map[int]bool)
	var mockJobStore = store_mocks.NewMockJobStore(ctrl)
	var mockTaskStore = store_mocks.NewMockTaskStore(ctrl)
	var mockClient = yarpc_mocks.NewMockClient(ctrl)

	mockJobStore.EXPECT().
		GetJobsByState(job.JobState_INITIALIZED).
		Return([]peloton.JobID{*jobID}, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		GetJobConfig(gomock.Any()).
		Return(&jobConfig, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		GetJobRuntime(gomock.Any()).
		Return(&jobRuntime, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		UpdateJobRuntime(jobID, gomock.Any()).
		Return(nil).
		AnyTimes()

	mockTaskStore.EXPECT().
		CreateTask(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(id *peloton.JobID, instanceID uint32, taskInfo *task.TaskInfo, _ string) {
			createdTasks = append(createdTasks, taskInfo)
		}).Return(nil)
	mockTaskStore.EXPECT().
		GetTaskForJob(gomock.Any(), uint32(1)).
		Return(nil, &storage.TaskNotFoundError{TaskID: ""})
	mockTaskStore.EXPECT().
		GetTaskForJob(gomock.Any(), uint32(0)).
		Return(map[uint32]*task.TaskInfo{
			uint32(0): createTaskInfo(jobID, uint32(0), task.TaskState_RUNNING),
		}, nil)
	mockTaskStore.EXPECT().
		GetTaskForJob(gomock.Any(), uint32(2)).
		Return(map[uint32]*task.TaskInfo{
			uint32(2): tasks[1],
		}, nil)
	mockClient.EXPECT().Call(
		gomock.Any(),
		gomock.Eq(yarpc.NewReqMeta().Procedure("ResourceManagerService.EnqueueTasks")),
		gomock.Any(),
		gomock.Any()).
		Do(func(_ context.Context, _ yarpc.CallReqMeta, reqBody interface{}, _ interface{}) {
			req := reqBody.(*resmgrsvc.EnqueueTasksRequest)
			for _, task := range req.Tasks {
				_, instance, err := util.ParseTaskID(task.Id.Value)
				assert.Nil(t, err)
				sentTasks[instance] = true
			}
		}).
		Return(nil, nil)

	validator := NewJobRecovery(mockJobStore, mockTaskStore, mockClient, tally.NoopScope)
	validator.recoverJobs()

	// jobRuntime create time is long ago, thus after validateJobs(),
	// job runtime state should be pending and task 1 and task 2 are sent by resmgr client
	assert.Equal(t, job.JobState_PENDING, jobRuntime.State)
	assert.True(t, sentTasks[1])
	assert.True(t, sentTasks[2])

	// jobRuntime create time is recent, thus validateJobs() should skip the validation
	jobRuntime.State = job.JobState_INITIALIZED
	jobRuntime.CreationTime = time.Now().String()
	sentTasks = make(map[int]bool)

	validator.recoverJobs()

	assert.Equal(t, job.JobState_INITIALIZED, jobRuntime.State)
	assert.False(t, sentTasks[1])
	assert.False(t, sentTasks[2])
}

func TestValidatorFailures(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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
		CreationTime: (time.Now().Add(-10 * time.Hour)).String(),
	}
	var tasks = []*task.TaskInfo{
		createTaskInfo(jobID, uint32(1), task.TaskState_INITIALIZED),
		createTaskInfo(jobID, uint32(2), task.TaskState_INITIALIZED),
	}
	var createdTasks []*task.TaskInfo
	var sentTasks = make(map[int]bool)
	var mockJobStore = store_mocks.NewMockJobStore(ctrl)
	var mockTaskStore = store_mocks.NewMockTaskStore(ctrl)
	var mockClient = yarpc_mocks.NewMockClient(ctrl)

	mockJobStore.EXPECT().
		GetJobsByState(job.JobState_INITIALIZED).
		Return([]peloton.JobID{*jobID}, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		GetJobConfig(gomock.Any()).
		Return(&jobConfig, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		GetJobRuntime(gomock.Any()).
		Return(&jobRuntime, nil).
		AnyTimes()
	mockJobStore.EXPECT().
		UpdateJobRuntime(jobID, gomock.Any()).
		Return(errors.New("Mock error")).
		MinTimes(1).
		MaxTimes(1)
	mockJobStore.EXPECT().
		UpdateJobRuntime(jobID, gomock.Any()).
		Return(nil).
		AnyTimes()

	mockTaskStore.EXPECT().
		CreateTask(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(id *peloton.JobID, instanceID uint32, taskInfo *task.TaskInfo, _ string) {
			createdTasks = append(createdTasks, taskInfo)
		}).Return(nil).
		AnyTimes()
	mockTaskStore.EXPECT().
		GetTaskForJob(gomock.Any(), uint32(1)).
		Return(nil, &storage.TaskNotFoundError{TaskID: ""}).
		AnyTimes()
	mockTaskStore.EXPECT().
		GetTaskForJob(gomock.Any(), uint32(0)).
		Return(map[uint32]*task.TaskInfo{
			uint32(0): createTaskInfo(jobID, uint32(0), task.TaskState_RUNNING),
		}, nil).
		AnyTimes()
	mockTaskStore.EXPECT().
		GetTaskForJob(gomock.Any(), uint32(2)).
		Return(map[uint32]*task.TaskInfo{
			uint32(2): tasks[1],
		}, nil).
		AnyTimes()
	mockClient.EXPECT().Call(
		gomock.Any(),
		gomock.Eq(yarpc.NewReqMeta().Procedure("ResourceManagerService.EnqueueTasks")),
		gomock.Any(),
		gomock.Any()).
		Return(nil, errors.New("Mock client error")).
		MinTimes(1).
		MaxTimes(1)
	mockClient.EXPECT().Call(
		gomock.Any(),
		gomock.Eq(yarpc.NewReqMeta().Procedure("ResourceManagerService.EnqueueTasks")),
		gomock.Any(),
		gomock.Any()).
		Do(func(_ context.Context, _ yarpc.CallReqMeta, reqBody interface{}, _ interface{}) {
			req := reqBody.(*resmgrsvc.EnqueueTasksRequest)
			for _, task := range req.Tasks {
				_, instance, err := util.ParseTaskID(task.Id.Value)
				assert.Nil(t, err)
				sentTasks[instance] = true
			}
		}).
		Return(nil, nil).
		AnyTimes()

	validator := NewJobRecovery(mockJobStore, mockTaskStore, mockClient, tally.NoopScope)
	validator.recoverJobs()

	// First call would fail as the client would fail once, nothing changed to the job
	assert.Equal(t, job.JobState_INITIALIZED, jobRuntime.State)
	assert.False(t, sentTasks[1])
	assert.False(t, sentTasks[2])

	// Second call would fail as the jobstore updateJobRuntime would fail once,
	// nothing changed to the job but tasks sent
	validator = NewJobRecovery(mockJobStore, mockTaskStore, mockClient, tally.NoopScope)
	validator.recoverJobs()
	assert.Equal(t, job.JobState_PENDING, jobRuntime.State)
	assert.True(t, sentTasks[1])
	assert.True(t, sentTasks[2])

	// jobRuntime create time is long ago, thus after validateJobs(),
	// job runtime state should be pending and task 1 and task 2 are sent by resmgr client
	validator = NewJobRecovery(mockJobStore, mockTaskStore, mockClient, tally.NoopScope)
	validator.recoverJobs()
	assert.Equal(t, job.JobState_PENDING, jobRuntime.State)
	assert.True(t, sentTasks[1])
	assert.True(t, sentTasks[2])
}

func createTaskInfo(jobID *peloton.JobID, i uint32, state task.TaskState) *task.TaskInfo {
	var tID = fmt.Sprintf("%s-%d-%s", jobID.Value, i, uuid.NewUUID().String())
	var taskInfo = task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			TaskId: &mesos.TaskID{Value: &tID},
			State:  state,
		},
		Config: &task.TaskConfig{
			Name: tID,
		},
		InstanceId: uint32(i),
		JobId:      jobID,
	}
	return &taskInfo
}
