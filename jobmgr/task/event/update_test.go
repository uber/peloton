package event

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/volume"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	host_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"

	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	goalstatemocks "code.uber.internal/infra/peloton/jobmgr/goalstate/mocks"
	event_mocks "code.uber.internal/infra/peloton/jobmgr/task/event/mocks"
	"code.uber.internal/infra/peloton/storage"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
)

const (
	_waitTime          = 1 * time.Second
	_instanceID uint32 = 0
)

var (
	_jobID         = uuid.NewUUID().String()
	_uuidStr       = uuid.NewUUID().String()
	_mesosTaskID   = fmt.Sprintf("%s-%d-%s", _jobID, _instanceID, _uuidStr)
	_mesosReason   = mesos.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED
	_pelotonTaskID = fmt.Sprintf("%s-%d", _jobID, _instanceID)
	_sla           = &job.SlaConfig{
		Preemptible: false,
	}
	_jobConfig = &job.JobConfig{
		Name:          _jobID,
		Sla:           _sla,
		InstanceCount: 1,
	}
	_pelotonJobID = &peloton.JobID{
		Value: _jobID,
	}
	_failureMsg  = "testFailure"
	_currentTime = "2017-01-02T15:04:05.456789016Z"
)

var nowMock = func() time.Time {
	now, _ := time.Parse(time.RFC3339Nano, _currentTime)
	return now
}

type TaskUpdaterTestSuite struct {
	suite.Suite

	updater           *statusUpdate
	ctrl              *gomock.Controller
	testScope         tally.TestScope
	mockJobStore      *store_mocks.MockJobStore
	mockTaskStore     *store_mocks.MockTaskStore
	mockVolumeStore   *store_mocks.MockPersistentVolumeStore
	jobFactory        *cachedmocks.MockJobFactory
	goalStateDriver   *goalstatemocks.MockDriver
	mockListener1     *event_mocks.MockListener
	mockListener2     *event_mocks.MockListener
	mockHostMgrClient *host_mocks.MockInternalHostServiceYARPCClient
}

func (suite *TaskUpdaterTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.testScope = tally.NewTestScope("", map[string]string{})
	suite.mockJobStore = store_mocks.NewMockJobStore(suite.ctrl)
	suite.mockTaskStore = store_mocks.NewMockTaskStore(suite.ctrl)
	suite.mockVolumeStore = store_mocks.NewMockPersistentVolumeStore(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.goalStateDriver = goalstatemocks.NewMockDriver(suite.ctrl)
	suite.mockListener1 = event_mocks.NewMockListener(suite.ctrl)
	suite.mockListener2 = event_mocks.NewMockListener(suite.ctrl)
	suite.mockHostMgrClient = host_mocks.NewMockInternalHostServiceYARPCClient(suite.ctrl)

	suite.updater = &statusUpdate{
		jobStore:        suite.mockJobStore,
		taskStore:       suite.mockTaskStore,
		volumeStore:     suite.mockVolumeStore,
		listeners:       []Listener{suite.mockListener1, suite.mockListener2},
		jobFactory:      suite.jobFactory,
		goalStateDriver: suite.goalStateDriver,
		rootCtx:         context.Background(),
		metrics:         NewMetrics(suite.testScope.SubScope("status_updater")),
		hostmgrClient:   suite.mockHostMgrClient,
	}
}

func (suite *TaskUpdaterTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func TestPelotonTaskUpdater(t *testing.T) {
	suite.Run(t, new(TaskUpdaterTestSuite))
}

func createTestTaskUpdateEvent(state mesos.TaskState) *pb_eventstream.Event {
	taskStatus := &mesos.TaskStatus{
		TaskId: &mesos.TaskID{
			Value: &_mesosTaskID,
		},
		State:   &state,
		Reason:  &_mesosReason,
		Message: &_failureMsg,
	}
	event := &pb_eventstream.Event{
		MesosTaskStatus: taskStatus,
		Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
	}
	return event
}

func createTestTaskInfo(state task.TaskState) *task.TaskInfo {
	taskInfo := &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			MesosTaskId: &mesos.TaskID{Value: &_mesosTaskID},
			State:       state,
			GoalState:   task.TaskState_SUCCEEDED,
		},
		Config: &task.TaskConfig{
			Name: _jobID,
			RestartPolicy: &task.RestartPolicy{
				MaxFailures: 3,
			},
		},
		InstanceId: uint32(_instanceID),
		JobId:      _pelotonJobID,
	}
	return taskInfo
}

// Test happy case of processing status update.
func (suite *TaskUpdaterTestSuite) TestProcessStatusUpdate() {
	defer suite.ctrl.Finish()

	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_RUNNING)
	timeNow := float64(time.Now().UnixNano())
	event.MesosTaskStatus.Timestamp = &timeNow
	taskInfo := createTestTaskInfo(task.TaskState_INITIALIZED)
	updatedRuntime := &task.RuntimeInfo{
		State:     task.TaskState_RUNNING,
		StartTime: _currentTime,
		Message:   "testFailure",
	}
	runtimes := make(map[uint32]*task.RuntimeInfo)
	runtimes[_instanceID] = updatedRuntime

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
		suite.jobFactory.EXPECT().AddJob(_pelotonJobID).Return(cachedJob),
		cachedJob.EXPECT().SetTaskUpdateTime(event.MesosTaskStatus.Timestamp).Return(),
		cachedJob.EXPECT().UpdateTasks(context.Background(), runtimes, cached.UpdateCacheAndDB).Return(nil),
		suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return(),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(job.JobType_BATCH).
			Return(1*time.Second),
		suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return(),
	)

	now = nowMock
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))

	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["status_updater.tasks_running_total+"].Value())
}

func (suite *TaskUpdaterTestSuite) TestProcessStatusUpdateSkipSameState() {
	defer suite.ctrl.Finish()

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_RUNNING)
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
	)

	now = nowMock
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["status_updater.tasks_running_total+"].Value())
}

// Test processing task failure status update w/ retry.
func (suite *TaskUpdaterTestSuite) TestProcessTaskFailedStatusUpdate() {
	defer suite.ctrl.Finish()

	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_FAILED)
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)

	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.jobFactory.EXPECT().
		AddJob(_pelotonJobID).Return(cachedJob)
	cachedJob.EXPECT().
		SetTaskUpdateTime(gomock.Any()).Return()
	cachedJob.EXPECT().
		UpdateTasks(context.Background(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(ctx context.Context, runtimes map[uint32]*task.RuntimeInfo, req cached.UpdateRequest) {
			runtime := runtimes[_instanceID]
			suite.Equal(
				runtime.State,
				task.TaskState_FAILED,
			)
			suite.Equal(
				runtime.Reason,
				_mesosReason.String(),
			)
			suite.Equal(
				runtime.Message,
				_failureMsg,
			)
			suite.Equal(
				runtime.FailureCount,
				uint32(0),
			)
		}).
		Return(nil)
	suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return()
	cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH)
	suite.goalStateDriver.EXPECT().
		JobRuntimeDuration(job.JobType_BATCH).
		Return(1 * time.Second)
	suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return()

	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["status_updater.tasks_failed_total+"].Value())
	time.Sleep(_waitTime)
}

// Test processing task LOST status update w/ retry.
func (suite *TaskUpdaterTestSuite) TestProcessTaskLostStatusUpdateWithRetry() {
	defer suite.ctrl.Finish()

	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_LOST)
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)

	rescheduleMsg := "Rescheduled due to task LOST: testFailure"
	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.jobFactory.EXPECT().
		AddJob(_pelotonJobID).Return(cachedJob)
	cachedJob.EXPECT().
		SetTaskUpdateTime(gomock.Any()).Return()
	cachedJob.EXPECT().
		UpdateTasks(context.Background(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(ctx context.Context, runtimes map[uint32]*task.RuntimeInfo, req cached.UpdateRequest) {
			runtime := runtimes[_instanceID]
			suite.Equal(
				runtime.State,
				task.TaskState_INITIALIZED,
			)
			suite.Equal(
				runtime.Message,
				rescheduleMsg,
			)
		}).
		Return(nil)
	suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return()
	cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH)
	suite.goalStateDriver.EXPECT().
		JobRuntimeDuration(job.JobType_BATCH).
		Return(1 * time.Second)
	suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return()

	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	time.Sleep(_waitTime)
}

// Test processing task LOST status update w/o retry due to current state already in terminal state.
func (suite *TaskUpdaterTestSuite) TestProcessTaskLostStatusUpdateWithoutRetry() {
	defer suite.ctrl.Finish()

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_LOST)
	taskInfo := createTestTaskInfo(task.TaskState_FAILED)

	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	time.Sleep(_waitTime)
}

// Test processing task FAILED status update due to launch of duplicate task IDs.
func (suite *TaskUpdaterTestSuite) TestProcessTaskFailedDuplicateTask() {
	defer suite.ctrl.Finish()

	failureReason := mesos.TaskStatus_REASON_TASK_INVALID
	failureMsg := "Task has duplicate ID"
	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_FAILED)
	event.MesosTaskStatus.Reason = &failureReason
	event.MesosTaskStatus.Message = &failureMsg
	taskInfo := createTestTaskInfo(task.TaskState_LAUNCHED)

	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	time.Sleep(_waitTime)
}

// Test processing task LOST status update w/o retry for stateful task.
func (suite *TaskUpdaterTestSuite) TestProcessTaskLostStatusUpdateNoRetryForStatefulTask() {
	defer suite.ctrl.Finish()

	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_LOST)
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)
	taskInfo.GetConfig().Volume = &task.PersistentVolumeConfig{}
	taskInfo.GetRuntime().VolumeID = &peloton.VolumeID{
		Value: "testVolumeID",
	}

	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.jobFactory.EXPECT().
		AddJob(_pelotonJobID).Return(cachedJob)
	cachedJob.EXPECT().
		SetTaskUpdateTime(gomock.Any()).Return()
	cachedJob.EXPECT().
		UpdateTasks(context.Background(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(ctx context.Context, runtimes map[uint32]*task.RuntimeInfo, req cached.UpdateRequest) {
			runtime := runtimes[_instanceID]
			suite.Equal(
				runtime.State,
				task.TaskState_LOST,
			)
			suite.Equal(
				runtime.Message,
				_failureMsg,
			)
		}).
		Return(nil)
	suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return()
	cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH)
	suite.goalStateDriver.EXPECT().
		JobRuntimeDuration(job.JobType_BATCH).
		Return(1 * time.Second)
	suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return()

	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	time.Sleep(_waitTime)
}

// Test processing task LOST status update due to reconciliation w/o retry due to goal state being killed.
func (suite *TaskUpdaterTestSuite) TestProcessStoppedTaskLostStatusUpdate() {
	defer suite.ctrl.Finish()

	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	failureReason := mesos.TaskStatus_REASON_RECONCILIATION
	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_LOST)
	event.MesosTaskStatus.Reason = &failureReason
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)
	taskInfo.Runtime.GoalState = task.TaskState_KILLED

	updatedRuntime := &task.RuntimeInfo{
		State:          task.TaskState_KILLED,
		Reason:         failureReason.String(),
		Message:        "Stopped task LOST event: " + _failureMsg,
		CompletionTime: _currentTime,
	}
	runtimes := make(map[uint32]*task.RuntimeInfo)
	runtimes[_instanceID] = updatedRuntime

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
		suite.jobFactory.EXPECT().
			AddJob(_pelotonJobID).Return(cachedJob),
		cachedJob.EXPECT().
			SetTaskUpdateTime(gomock.Any()).Return(),
		cachedJob.EXPECT().
			UpdateTasks(context.Background(), runtimes, cached.UpdateCacheAndDB).
			Return(nil),
		suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return(),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(job.JobType_BATCH).
			Return(1*time.Second),
		suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return(),
	)

	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	time.Sleep(_waitTime)
}

// Test processing orphan RUNNING task status update.
func (suite *TaskUpdaterTestSuite) TestProcessOrphanTaskRunningStatusUpdate() {
	defer suite.ctrl.Finish()

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_RUNNING)
	taskInfo := createTestTaskInfo(task.TaskState_FAILED)
	// generates new mesos task id that is different with the one in the
	// task status update.
	dbMesosTaskID := fmt.Sprintf("%s-%d-%s", _jobID, _instanceID, uuid.NewUUID().String())
	taskInfo.GetRuntime().MesosTaskId = &mesos.TaskID{Value: &dbMesosTaskID}
	orphanTaskID := &mesos.TaskID{
		Value: &[]string{_mesosTaskID}[0],
	}

	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.mockHostMgrClient.EXPECT().KillTasks(gomock.Any(), &hostsvc.KillTasksRequest{
		TaskIds: []*mesos.TaskID{orphanTaskID},
	}).
		Do(func(ctx context.Context, req *hostsvc.KillTasksRequest) {
			_, ok := ctx.Deadline()
			suite.True(ok)
		}).
		Return(nil, nil)
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
}

// Test processing orphan task LOST status update.
func (suite *TaskUpdaterTestSuite) TestProcessOrphanTaskLostStatusUpdate() {
	defer suite.ctrl.Finish()

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_LOST)
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)
	// generates new mesos task id that is different with the one in the
	// task status update.
	dbMesosTaskID := fmt.Sprintf("%s-%d-%s", _jobID, _instanceID, uuid.NewUUID().String())
	taskInfo.GetRuntime().MesosTaskId = &mesos.TaskID{Value: &dbMesosTaskID}

	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
}

// Test processing status update for a task missing from DB.
func (suite *TaskUpdaterTestSuite) TestProcessMissingTaskStatusUpdate() {
	defer suite.ctrl.Finish()

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_LOST)
	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(nil, &storage.TaskNotFoundError{TaskID: _pelotonTaskID})
	suite.Error(suite.updater.ProcessStatusUpdate(context.Background(), event))
}

// Test processing task KILL by PREEMPTION
func (suite *TaskUpdaterTestSuite) TestProcessPreemptedTaskStatusUpdate() {
	defer suite.ctrl.Finish()

	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	tt := []struct {
		killOnPreempt  bool
		preemptMessage string
	}{
		{
			killOnPreempt:  false,
			preemptMessage: "Task will be rescheduled",
		},
		{
			killOnPreempt:  true,
			preemptMessage: "Task will not be rescheduled",
		},
	}

	for _, t := range tt {
		event := createTestTaskUpdateEvent(mesos.TaskState_TASK_KILLED)
		taskInfo := createTestTaskInfo(task.TaskState_RUNNING)
		taskInfo.Runtime.GoalState = task.TaskState_PREEMPTING

		taskInfo.Config.PreemptionPolicy = &task.PreemptionPolicy{
			KillOnPreempt: t.killOnPreempt,
		}
		preemptMessage := t.preemptMessage

		preemptReason := "Task preempted"
		oldFailureCount := taskInfo.Runtime.FailureCount
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil)
		suite.jobFactory.EXPECT().
			AddJob(_pelotonJobID).Return(cachedJob)
		cachedJob.EXPECT().
			SetTaskUpdateTime(gomock.Any()).Return()
		cachedJob.EXPECT().
			UpdateTasks(context.Background(), gomock.Any(), cached.UpdateCacheAndDB).
			Do(func(ctx context.Context, runtimes map[uint32]*task.RuntimeInfo, req cached.UpdateRequest) {
				updateTask := runtimes[_instanceID]
				suite.Equal(
					preemptMessage,
					updateTask.Message,
				)
				suite.Equal(
					preemptReason,
					updateTask.Reason,
				)
				suite.Equal(
					oldFailureCount,
					updateTask.FailureCount,
				)
			}).
			Return(nil)
		suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return()
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH)
		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(job.JobType_BATCH).
			Return(1 * time.Second)
		suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return()
		suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	}
}

func (suite *TaskUpdaterTestSuite) TestProcessStatusUpdateVolumeUponRunning() {
	defer suite.ctrl.Finish()

	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_RUNNING)
	taskInfo := createTestTaskInfo(task.TaskState_LAUNCHED)
	taskInfo.GetConfig().Volume = &task.PersistentVolumeConfig{}
	testVolumeID := &peloton.VolumeID{
		Value: "testVolume",
	}
	taskInfo.GetRuntime().VolumeID = testVolumeID
	updatedRuntime := &task.RuntimeInfo{
		State:     task.TaskState_RUNNING,
		StartTime: _currentTime,
		Message:   "testFailure",
	}
	runtimes := make(map[uint32]*task.RuntimeInfo)
	runtimes[_instanceID] = updatedRuntime

	volumeInfo := &volume.PersistentVolumeInfo{
		State: volume.VolumeState_INITIALIZED,
	}

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
		suite.mockVolumeStore.EXPECT().
			GetPersistentVolume(context.Background(), testVolumeID).
			Return(volumeInfo, nil),
		suite.mockVolumeStore.EXPECT().
			UpdatePersistentVolume(context.Background(), volumeInfo).
			Return(nil),
		suite.jobFactory.EXPECT().
			AddJob(_pelotonJobID).Return(cachedJob),
		cachedJob.EXPECT().
			SetTaskUpdateTime(gomock.Any()).Return(),
		cachedJob.EXPECT().
			UpdateTasks(context.Background(), runtimes, cached.UpdateCacheAndDB).
			Return(nil),
		suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return(),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(job.JobType_BATCH).
			Return(1*time.Second),
		suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return(),
	)

	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["status_updater.tasks_running_total+"].Value())
}

func (suite *TaskUpdaterTestSuite) TestProcessStatusUpdateSkipVolumeUponRunningIfAlreadyCreated() {
	defer suite.ctrl.Finish()

	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_RUNNING)
	taskInfo := createTestTaskInfo(task.TaskState_LAUNCHED)
	taskInfo.GetConfig().Volume = &task.PersistentVolumeConfig{}
	testVolumeID := &peloton.VolumeID{
		Value: "testVolume",
	}
	taskInfo.GetRuntime().VolumeID = testVolumeID
	updatedRuntime := &task.RuntimeInfo{
		State:     task.TaskState_RUNNING,
		StartTime: _currentTime,
		Message:   "testFailure",
	}
	runtimes := make(map[uint32]*task.RuntimeInfo)
	runtimes[_instanceID] = updatedRuntime

	volumeInfo := &volume.PersistentVolumeInfo{
		State: volume.VolumeState_CREATED,
	}

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
		suite.mockVolumeStore.EXPECT().
			GetPersistentVolume(context.Background(), testVolumeID).
			Return(volumeInfo, nil),
		suite.jobFactory.EXPECT().
			AddJob(_pelotonJobID).Return(cachedJob),
		cachedJob.EXPECT().
			SetTaskUpdateTime(gomock.Any()).Return(),
		cachedJob.EXPECT().
			UpdateTasks(context.Background(), runtimes, cached.UpdateCacheAndDB).
			Return(nil),
		suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return(),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(job.JobType_BATCH).
			Return(1*time.Second),
		suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return(),
	)

	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["status_updater.tasks_running_total+"].Value())
}

func (suite *TaskUpdaterTestSuite) TestProcessFailedTaskRunningStatusUpdate() {
	defer suite.ctrl.Finish()

	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_RUNNING)
	taskInfo := createTestTaskInfo(task.TaskState_FAILED)
	taskInfo.GetRuntime().CompletionTime = _currentTime

	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.jobFactory.EXPECT().AddJob(_pelotonJobID).Return(cachedJob)
	cachedJob.EXPECT().
		SetTaskUpdateTime(gomock.Any()).Return()
	cachedJob.EXPECT().
		UpdateTasks(context.Background(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(ctx context.Context, runtimes map[uint32]*task.RuntimeInfo, req cached.UpdateRequest) {
			runtime := runtimes[_instanceID]
			suite.Equal(
				runtime.State,
				task.TaskState_RUNNING,
			)
			suite.Equal(
				"",
				runtime.CompletionTime,
			)
		}).
		Return(nil)
	suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return()
	cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH)
	suite.goalStateDriver.EXPECT().
		JobRuntimeDuration(job.JobType_BATCH).
		Return(1 * time.Second)
	suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return()
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
}

func (suite *TaskUpdaterTestSuite) TestUpdaterProcessListeners() {
	defer suite.ctrl.Finish()

	suite.mockListener1.EXPECT().OnEvents([]*pb_eventstream.Event{nil})
	suite.mockListener2.EXPECT().OnEvents([]*pb_eventstream.Event{nil})

	suite.updater.ProcessListeners(nil)
}

func (suite *TaskUpdaterTestSuite) TestUpdaterStart() {
	defer suite.ctrl.Finish()

	suite.mockListener1.EXPECT().Start()
	suite.mockListener2.EXPECT().Start()

	suite.updater.Start()
}
