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
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/http"
	"go.uber.org/yarpc/yarpcerrors"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/volume"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	host_mocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"

	"code.uber.internal/infra/peloton/common"
	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"

	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	goalstatemocks "code.uber.internal/infra/peloton/jobmgr/goalstate/mocks"
	jobmgrtask "code.uber.internal/infra/peloton/jobmgr/task"
	event_mocks "code.uber.internal/infra/peloton/jobmgr/task/event/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"
)

const (
	_waitTime          = 1 * time.Second
	_instanceID uint32 = 0
)

var (
	_jobID             = uuid.NewUUID().String()
	_uuidStr           = uuid.NewUUID().String()
	_mesosTaskID       = fmt.Sprintf("%s-%d-%s", _jobID, _instanceID, _uuidStr)
	_mesosReason       = mesos.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED
	_healthCheckReason = mesos.TaskStatus_REASON_TASK_HEALTH_CHECK_STATUS_UPDATED
	_pelotonTaskID     = fmt.Sprintf("%s-%d", _jobID, _instanceID)
	_pelotonJobID      = &peloton.JobID{
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
	suite.updater.applier = newBucketEventProcessor(suite.updater, 10, 10)
}

func (suite *TaskUpdaterTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func TestPelotonTaskUpdater(t *testing.T) {
	suite.Run(t, new(TaskUpdaterTestSuite))
}

func (suite *TaskUpdaterTestSuite) TestNewTaskStatusUpdate() {
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: common.PelotonJobManager,
		Outbounds: yarpc.Outbounds{
			common.PelotonResourceManager: transport.Outbounds{
				Unary: http.NewTransport().NewSingleOutbound(""),
			},
			common.PelotonHostManager: transport.Outbounds{
				Unary: http.NewTransport().NewSingleOutbound(""),
			},
		},
	})
	statusUpdater := NewTaskStatusUpdate(
		dispatcher,
		suite.mockJobStore,
		suite.mockTaskStore,
		suite.mockVolumeStore,
		suite.jobFactory,
		suite.goalStateDriver,
		[]Listener{},
		tally.NoopScope,
	)
	suite.NotNil(statusUpdater)
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

func createTestTaskUpdateHealthCheckEvent(healthy bool) *pb_eventstream.Event {
	state := mesos.TaskState_TASK_RUNNING
	taskStatus := &mesos.TaskStatus{
		TaskId: &mesos.TaskID{
			Value: &_mesosTaskID,
		},
		State:   &state,
		Healthy: &healthy,
		Reason:  &_healthCheckReason,
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
			MesosTaskId:   &mesos.TaskID{Value: &_mesosTaskID},
			State:         state,
			GoalState:     task.TaskState_SUCCEEDED,
			ResourceUsage: jobmgrtask.CreateEmptyResourceUsageMap(),
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

func createTestTaskInfoWithHealth(state task.TaskState) *task.TaskInfo {
	taskInfo := &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			MesosTaskId:   &mesos.TaskID{Value: &_mesosTaskID},
			State:         state,
			GoalState:     task.TaskState_RUNNING,
			ResourceUsage: jobmgrtask.CreateEmptyResourceUsageMap(),
		},
		Config: &task.TaskConfig{
			Name: _jobID,
			RestartPolicy: &task.RestartPolicy{
				MaxFailures: 3,
			},
			HealthCheck: &task.HealthCheckConfig{
				InitialIntervalSecs:    10,
				IntervalSecs:           10,
				MaxConsecutiveFailures: 5,
				TimeoutSecs:            5,
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
	runtimeDiff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.MessageField:        "testFailure",
		jobmgrcommon.CompletionTimeField: "",
		jobmgrcommon.StateField:          task.TaskState_RUNNING,
		jobmgrcommon.StartTimeField:      _currentTime,
		jobmgrcommon.ReasonField:         "",
	}
	runtimeDiffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	runtimeDiffs[_instanceID] = runtimeDiff

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
		suite.jobFactory.EXPECT().AddJob(_pelotonJobID).Return(cachedJob),
		cachedJob.EXPECT().SetTaskUpdateTime(event.MesosTaskStatus.Timestamp).Return(),
		cachedJob.EXPECT().PatchTasks(context.Background(), runtimeDiffs).Return(nil),
		suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return(),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(job.JobType_BATCH).
			Return(1*time.Second),
		suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return(),
		cachedJob.EXPECT().UpdateResourceUsage(gomock.Any()).Return(),
	)

	now = nowMock
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))

	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["status_updater.tasks_running_total+"].Value())
}

// Test processing Health check event
func (suite *TaskUpdaterTestSuite) TestProcessStatusUpdateHealthy() {
	defer suite.ctrl.Finish()

	tt := []struct {
		health      bool
		healthState task.HealthState
	}{
		{
			health:      true,
			healthState: task.HealthState_HEALTHY,
		},
		{
			health:      false,
			healthState: task.HealthState_UNHEALTHY,
		},
	}

	for _, t := range tt {
		cachedJob := cachedmocks.NewMockJob(suite.ctrl)
		event := createTestTaskUpdateHealthCheckEvent(t.health)
		timeNow := float64(time.Now().UnixNano())
		event.MesosTaskStatus.Timestamp = &timeNow
		taskInfo := createTestTaskInfoWithHealth(task.TaskState_INITIALIZED)

		gomock.InOrder(
			suite.mockTaskStore.EXPECT().
				GetTaskByID(context.Background(), _pelotonTaskID).
				Return(taskInfo, nil),
			suite.jobFactory.EXPECT().AddJob(_pelotonJobID).Return(cachedJob),
			cachedJob.EXPECT().SetTaskUpdateTime(event.MesosTaskStatus.Timestamp).Return(),
			cachedJob.EXPECT().PatchTasks(gomock.Any(), gomock.Any()).Do(
				func(_ context.Context, runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff) {
					for _, runtimeDiff := range runtimeDiffs {
						suite.Equal(t.healthState, runtimeDiff[jobmgrcommon.HealthyField])
					}
				}).Return(nil),

			suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return(),
			cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
			suite.goalStateDriver.EXPECT().
				JobRuntimeDuration(job.JobType_BATCH).
				Return(1*time.Second),
			suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return(),
			cachedJob.EXPECT().UpdateResourceUsage(gomock.Any()).Return(),
		)

		now = nowMock
		suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	}
}

func (suite *TaskUpdaterTestSuite) TestProcessStatusUpdateSkipSameState() {
	defer suite.ctrl.Finish()

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_STARTING)
	taskInfo := createTestTaskInfo(task.TaskState_STARTING)

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
	)

	now = nowMock
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	suite.Equal(
		int64(0),
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
		PatchTasks(context.Background(), gomock.Any()).
		Do(func(ctx context.Context, runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff) {
			runtimeDiff := runtimeDiffs[_instanceID]
			suite.Equal(
				runtimeDiff[jobmgrcommon.StateField],
				task.TaskState_FAILED,
			)
			suite.Equal(
				runtimeDiff[jobmgrcommon.ReasonField],
				_mesosReason.String(),
			)
			suite.Equal(
				runtimeDiff[jobmgrcommon.MessageField],
				_failureMsg,
			)
		}).
		Return(nil)
	suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return()
	cachedJob.EXPECT().UpdateResourceUsage(gomock.Any()).Return()
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
		PatchTasks(context.Background(), gomock.Any()).
		Do(func(ctx context.Context, runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff) {
			runtimeDiff := runtimeDiffs[_instanceID]
			suite.Equal(
				runtimeDiff[jobmgrcommon.StateField],
				task.TaskState_INITIALIZED,
			)
			suite.Equal(
				runtimeDiff[jobmgrcommon.MessageField],
				rescheduleMsg,
			)
			suite.Equal(
				runtimeDiff[jobmgrcommon.HealthyField],
				task.HealthState_DISABLED,
			)
		}).
		Return(nil)
	suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return()
	cachedJob.EXPECT().UpdateResourceUsage(gomock.Any()).Return()
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

func (suite *TaskUpdaterTestSuite) TestProcessTaskFailureCountUpdate() {
	defer suite.ctrl.Finish()

	tt := []struct {
		mesosState          mesos.TaskState
		pelotnState         task.TaskState
		configVersion       uint64
		desiredVersion      uint64
		falureCount         uint32
		desiredFailureCount uint32
	}{
		{
			mesosState:          mesos.TaskState_TASK_KILLED,
			pelotnState:         task.TaskState_KILLED,
			configVersion:       1,
			desiredVersion:      1,
			falureCount:         3,
			desiredFailureCount: 4,
		},
		{
			mesosState:          mesos.TaskState_TASK_KILLED,
			pelotnState:         task.TaskState_KILLED,
			configVersion:       1,
			desiredVersion:      2,
			falureCount:         3,
			desiredFailureCount: 0,
		},
		{
			mesosState:          mesos.TaskState_TASK_FAILED,
			pelotnState:         task.TaskState_FAILED,
			configVersion:       1,
			desiredVersion:      1,
			falureCount:         3,
			desiredFailureCount: 4,
		},
		{
			mesosState:          mesos.TaskState_TASK_FINISHED,
			pelotnState:         task.TaskState_SUCCEEDED,
			configVersion:       1,
			desiredVersion:      1,
			falureCount:         3,
			desiredFailureCount: 4,
		},
	}

	for _, t := range tt {
		cachedJob := cachedmocks.NewMockJob(suite.ctrl)
		event := createTestTaskUpdateEvent(t.mesosState)
		taskInfo := createTestTaskInfoWithHealth(task.TaskState_RUNNING)

		taskInfo.Runtime.ConfigVersion = t.configVersion
		taskInfo.Runtime.DesiredConfigVersion = t.desiredVersion
		taskInfo.Runtime.FailureCount = t.falureCount

		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil)
		suite.jobFactory.EXPECT().
			AddJob(_pelotonJobID).Return(cachedJob)
		cachedJob.EXPECT().
			SetTaskUpdateTime(gomock.Any()).Return()
		cachedJob.EXPECT().
			PatchTasks(context.Background(), gomock.Any()).
			Do(func(ctx context.Context, runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff) {
				runtimeDiff := runtimeDiffs[_instanceID]
				suite.Equal(
					runtimeDiff[jobmgrcommon.StateField],
					t.pelotnState,
				)
				suite.Equal(
					runtimeDiff[jobmgrcommon.HealthyField],
					task.HealthState_INVALID,
				)
				suite.Equal(
					runtimeDiff[jobmgrcommon.FailureCountField],
					uint32(t.desiredFailureCount),
				)
			}).
			Return(nil)
		suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return()
		cachedJob.EXPECT().UpdateResourceUsage(gomock.Any()).Return()
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH)
		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(job.JobType_BATCH).
			Return(1 * time.Second)
		suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return()

		suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
		time.Sleep(_waitTime)
	}
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
		PatchTasks(context.Background(), gomock.Any()).
		Do(func(ctx context.Context, runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff) {
			runtimeDiff := runtimeDiffs[_instanceID]
			suite.Equal(
				runtimeDiff[jobmgrcommon.StateField],
				task.TaskState_LOST,
			)
			suite.Equal(
				runtimeDiff[jobmgrcommon.MessageField],
				_failureMsg,
			)
		}).
		Return(nil)
	suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return()
	cachedJob.EXPECT().UpdateResourceUsage(gomock.Any()).Return()
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
	taskInfo := createTestTaskInfoWithHealth(task.TaskState_RUNNING)
	taskInfo.Runtime.GoalState = task.TaskState_KILLED

	runtimeDiff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.StateField:          task.TaskState_KILLED,
		jobmgrcommon.ReasonField:         failureReason.String(),
		jobmgrcommon.MessageField:        "Stopped task LOST event: " + _failureMsg,
		jobmgrcommon.CompletionTimeField: _currentTime,
		jobmgrcommon.ResourceUsageField:  jobmgrtask.CreateEmptyResourceUsageMap(),
		jobmgrcommon.HealthyField:        task.HealthState_INVALID,
	}
	runtimeDiffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	runtimeDiffs[_instanceID] = runtimeDiff

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
		suite.jobFactory.EXPECT().
			AddJob(_pelotonJobID).Return(cachedJob),
		cachedJob.EXPECT().
			SetTaskUpdateTime(gomock.Any()).Return(),
		cachedJob.EXPECT().
			PatchTasks(context.Background(), runtimeDiffs).
			Return(nil),
		suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return(),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(job.JobType_BATCH).
			Return(1*time.Second),
		suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return(),
		cachedJob.EXPECT().UpdateResourceUsage(gomock.Any()).Return(),
	)

	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
	time.Sleep(_waitTime)
}

// Test processing task status update failure because of error in resource usage calculation.
func (suite *TaskUpdaterTestSuite) TestProcessStatusUpdateResourceUsageError() {
	defer suite.ctrl.Finish()

	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)
	taskInfo.Runtime.GoalState = task.TaskState_SUCCEEDED
	taskInfo.Runtime.StartTime = "not-valid"

	// LOST task has different code path to update resource usage than rest of
	// terminal states. So test for both LOST and FINISHED states
	states := []mesos.TaskState{
		mesos.TaskState_TASK_LOST,
		mesos.TaskState_TASK_FINISHED,
	}
	for _, state := range states {
		event := createTestTaskUpdateEvent(state)

		gomock.InOrder(
			suite.mockTaskStore.EXPECT().
				GetTaskByID(context.Background(), _pelotonTaskID).
				Return(taskInfo, nil),
			suite.jobFactory.EXPECT().
				AddJob(_pelotonJobID).Return(cachedJob),
			cachedJob.EXPECT().
				SetTaskUpdateTime(gomock.Any()).Return(),
			cachedJob.EXPECT().
				PatchTasks(context.Background(), gomock.Any()).
				Return(nil),
			suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return(),
			cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
			suite.goalStateDriver.EXPECT().
				JobRuntimeDuration(job.JobType_BATCH).
				Return(1*time.Second),
			suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return(),
			cachedJob.EXPECT().UpdateResourceUsage(gomock.Any()).Return(),
		)

		// simulate error in CreateResourceUsageMap due to invalid start time
		// This should be only logged and ProcessStatusUpdate should still
		// succeed
		suite.NoError(suite.updater.ProcessStatusUpdate(
			context.Background(), event))
	}
}

// Test processing task status update when there is a task with resource usage
// map as nil. This could happen for in-flight tasks created before the feature
// was introduced
func (suite *TaskUpdaterTestSuite) TestProcessStatusUpdateResourceUsageNil() {
	defer suite.ctrl.Finish()

	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)
	taskInfo.Runtime.GoalState = task.TaskState_SUCCEEDED
	taskInfo.Runtime.ResourceUsage = nil

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_LOST)

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
		suite.jobFactory.EXPECT().
			AddJob(_pelotonJobID).Return(cachedJob),
		cachedJob.EXPECT().
			SetTaskUpdateTime(gomock.Any()).Return(),
		cachedJob.EXPECT().
			PatchTasks(context.Background(), gomock.Any()).
			Return(nil),
		suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return(),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(job.JobType_BATCH).
			Return(1*time.Second),
		suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return(),
		cachedJob.EXPECT().UpdateResourceUsage(gomock.Any()).Return(),
	)

	suite.NoError(suite.updater.ProcessStatusUpdate(
		context.Background(), event))
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

// Test processing status update for a task failed to be fetched frmm DB.
func (suite *TaskUpdaterTestSuite) TestProcessTaskIDFetchError() {
	defer suite.ctrl.Finish()

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_LOST)
	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(nil, fmt.Errorf("fake db error"))
	suite.Error(suite.updater.ProcessStatusUpdate(context.Background(), event))
}

// Test processing status update for a task missing from DB.
func (suite *TaskUpdaterTestSuite) TestProcessMissingTaskStatusUpdate() {
	defer suite.ctrl.Finish()

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_RUNNING)
	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(nil, yarpcerrors.NotFoundErrorf("task:%s not found", _pelotonTaskID))
	suite.mockHostMgrClient.EXPECT().
		KillTasks(gomock.Any(), gomock.Any()).
		Return(&hostsvc.KillTasksResponse{}, nil)
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
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
	runtimeDiff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.StateField:          task.TaskState_RUNNING,
		jobmgrcommon.StartTimeField:      _currentTime,
		jobmgrcommon.MessageField:        "testFailure",
		jobmgrcommon.CompletionTimeField: "",
		jobmgrcommon.ReasonField:         "",
	}
	runtimeDiffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	runtimeDiffs[_instanceID] = runtimeDiff

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
			PatchTasks(context.Background(), runtimeDiffs).
			Return(nil),
		suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return(),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(job.JobType_BATCH).
			Return(1*time.Second),
		suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return(),
		cachedJob.EXPECT().UpdateResourceUsage(gomock.Any()).Return(),
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
	runtimeDiff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.StateField:          task.TaskState_RUNNING,
		jobmgrcommon.StartTimeField:      _currentTime,
		jobmgrcommon.MessageField:        "testFailure",
		jobmgrcommon.CompletionTimeField: "",
		jobmgrcommon.ReasonField:         "",
	}
	runtimeDiffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	runtimeDiffs[_instanceID] = runtimeDiff

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
			PatchTasks(context.Background(), runtimeDiffs).
			Return(nil),
		suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return(),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(job.JobType_BATCH).
			Return(1*time.Second),
		suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return(),
		cachedJob.EXPECT().UpdateResourceUsage(gomock.Any()).Return(),
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
		PatchTasks(context.Background(), gomock.Any()).
		Do(func(ctx context.Context, runtimeDiffs map[uint32]jobmgrcommon.RuntimeDiff) {
			runtimeDiff := runtimeDiffs[_instanceID]
			suite.Equal(
				runtimeDiff[jobmgrcommon.StateField],
				task.TaskState_RUNNING,
			)
			suite.Equal(
				runtimeDiff[jobmgrcommon.CompletionTimeField],
				"",
			)
		}).
		Return(nil)
	suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return()
	cachedJob.EXPECT().UpdateResourceUsage(gomock.Any()).Return()
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

func (suite *TaskUpdaterTestSuite) TestUpdaterStartStop() {
	defer suite.ctrl.Finish()

	suite.mockListener1.EXPECT().Start()
	suite.mockListener2.EXPECT().Start()

	suite.updater.Start()

	suite.mockListener1.EXPECT().Stop()
	suite.mockListener2.EXPECT().Stop()

	suite.updater.Stop()
}
