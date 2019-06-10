// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v0/volume"
	pb_eventstream "github.com/uber/peloton/.gen/peloton/private/eventstream"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	host_mocks "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"

	"github.com/uber/peloton/pkg/common"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	goalstatemocks "github.com/uber/peloton/pkg/jobmgr/goalstate/mocks"
	jobmgrtask "github.com/uber/peloton/pkg/jobmgr/task"
	event_mocks "github.com/uber/peloton/pkg/jobmgr/task/event/mocks"
	store_mocks "github.com/uber/peloton/pkg/storage/mocks"
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
	_failureMsg            = "testFailure"
	_failureMsgExitCode    = "Command exited with status 250"
	_failureMsgBadExitCode = "Command exited with status abc"
	_failureMsgSignal      = "Command terminated with signal Segmentation fault"
	_currentTime           = "2017-01-02T15:04:05.456789016Z"
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

func createTestTaskUpdateHealthCheckEvent(
	state mesos.TaskState, healthy bool) *pb_eventstream.Event {
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

func createTestTaskInfoWithHealth(
	state task.TaskState,
	healthState task.HealthState,
	healthCheckEnabled bool) *task.TaskInfo {
	taskInfo := &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			Healthy:       healthState,
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
				Enabled:                healthCheckEnabled,
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
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_RUNNING)
	timeNow := float64(time.Now().UnixNano())
	event.MesosTaskStatus.Timestamp = &timeNow
	taskInfo := createTestTaskInfo(task.TaskState_INITIALIZED)

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
		suite.jobFactory.EXPECT().AddJob(_pelotonJobID).Return(cachedJob),
		cachedJob.EXPECT().SetTaskUpdateTime(event.MesosTaskStatus.Timestamp).Return(),
		cachedJob.EXPECT().AddTask(gomock.Any(), _instanceID).Return(cachedTask, nil),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		cachedTask.EXPECT().CompareAndSetTask(context.Background(), gomock.Any(), job.JobType_BATCH).Return(nil, nil).
			Do(func(_ context.Context, runtime *task.RuntimeInfo, _ job.JobType) {
				suite.Equal(runtime.GetMessage(), "testFailure")
				suite.Empty(runtime.GetCompletionTime())
				suite.Equal(runtime.GetState(), task.TaskState_RUNNING)
				suite.Equal(runtime.GetStartTime(), _currentTime)
				suite.Empty(runtime.GetReason())
				suite.Empty(runtime.GetDesiredHost())
			}),
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

// Test case of processing status update for a task going through in-place update
func (suite *TaskUpdaterTestSuite) TestProcessStatusUpdateInPlaceUpdateTask() {
	defer suite.ctrl.Finish()

	hostname1 := "hostname1"
	hostname2 := "hostname2"
	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)

	// the task is placed on the desired host
	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_RUNNING)
	timeNow := float64(time.Now().UnixNano())
	event.MesosTaskStatus.Timestamp = &timeNow
	taskInfo := createTestTaskInfo(task.TaskState_INITIALIZED)
	taskInfo.Runtime.Host = hostname1
	taskInfo.Runtime.DesiredHost = hostname1

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
		suite.jobFactory.EXPECT().AddJob(_pelotonJobID).Return(cachedJob),
		cachedJob.EXPECT().SetTaskUpdateTime(event.MesosTaskStatus.Timestamp).Return(),
		cachedJob.EXPECT().AddTask(gomock.Any(), _instanceID).Return(cachedTask, nil),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_SERVICE),
		cachedTask.EXPECT().CompareAndSetTask(context.Background(), gomock.Any(), job.JobType_SERVICE).
			Do(func(_ context.Context, runtime *task.RuntimeInfo, _ job.JobType) {
				suite.Equal(runtime.GetMessage(), "testFailure")
				suite.Empty(runtime.GetCompletionTime())
				suite.Equal(runtime.GetState(), task.TaskState_RUNNING)
				suite.Equal(runtime.GetStartTime(), _currentTime)
				suite.Empty(runtime.GetReason())
				suite.Empty(runtime.GetDesiredHost())
			}).
			Return(nil, nil),
		suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return(),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_SERVICE),
		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(job.JobType_SERVICE).
			Return(1*time.Second),
		suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return(),
		cachedJob.EXPECT().UpdateResourceUsage(gomock.Any()).Return(),
	)

	now = nowMock
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))

	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["status_updater.tasks_in_place_placement_success+"].Value())
	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["status_updater.tasks_in_place_placement_total+"].Value())

	// the task is not placed on the desired host
	event = createTestTaskUpdateEvent(mesos.TaskState_TASK_RUNNING)
	event.MesosTaskStatus.Timestamp = &timeNow
	taskInfo = createTestTaskInfo(task.TaskState_INITIALIZED)
	taskInfo.Runtime.Host = hostname1
	taskInfo.Runtime.DesiredHost = hostname2

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
		suite.jobFactory.EXPECT().AddJob(_pelotonJobID).Return(cachedJob),
		cachedJob.EXPECT().SetTaskUpdateTime(event.MesosTaskStatus.Timestamp).Return(),
		cachedJob.EXPECT().AddTask(gomock.Any(), _instanceID).Return(cachedTask, nil),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_SERVICE),
		cachedTask.EXPECT().CompareAndSetTask(context.Background(), gomock.Any(), job.JobType_SERVICE).
			Do(func(_ context.Context, runtime *task.RuntimeInfo, _ job.JobType) {
				suite.Equal(runtime.GetMessage(), "testFailure")
				suite.Empty(runtime.GetCompletionTime())
				suite.Equal(runtime.GetState(), task.TaskState_RUNNING)
				suite.Equal(runtime.GetStartTime(), _currentTime)
				suite.Empty(runtime.GetReason())
				suite.Empty(runtime.GetDesiredHost())
			}).
			Return(nil, nil),
		suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return(),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_SERVICE),
		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(job.JobType_SERVICE).
			Return(1*time.Second),
		suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return(),
		cachedJob.EXPECT().UpdateResourceUsage(gomock.Any()).Return(),
	)

	now = nowMock
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))

	// success count does not increase, while total count increases
	suite.Equal(
		int64(1),
		suite.testScope.Snapshot().Counters()["status_updater.tasks_in_place_placement_success+"].Value())
	suite.Equal(
		int64(2),
		suite.testScope.Snapshot().Counters()["status_updater.tasks_in_place_placement_total+"].Value())

}

// Test processing Health check event
func (suite *TaskUpdaterTestSuite) TestProcessStatusUpdateHealthy() {
	defer suite.ctrl.Finish()

	tt := []struct {
		previousState       task.TaskState
		newState            mesos.TaskState
		previousHealthState task.HealthState
		newHealthState      bool

		healthyCounter   int64
		unHealthyCounter int64

		msg string
	}{
		{
			previousState:       task.TaskState_RUNNING,
			previousHealthState: task.HealthState_HEALTH_UNKNOWN,

			newState:       mesos.TaskState_TASK_RUNNING,
			newHealthState: true,

			healthyCounter:   1,
			unHealthyCounter: 0,

			msg: "Before Task RUNNING + HEALTHY_UNKNOWN, " +
				"After TASK RUNNING + HEALTHY",
		},
		{
			previousState:       task.TaskState_RUNNING,
			previousHealthState: task.HealthState_HEALTHY,

			newState:       mesos.TaskState_TASK_RUNNING,
			newHealthState: false,

			healthyCounter:   1,
			unHealthyCounter: 1,

			msg: "Before TASK RUNNING + HEALTHY, " +
				"After TASK RUNNING + UNHEALTHY",
		},
		{
			previousState:       task.TaskState_RUNNING,
			previousHealthState: task.HealthState_UNHEALTHY,

			newState:       mesos.TaskState_TASK_RUNNING,
			newHealthState: true,

			healthyCounter:   2,
			unHealthyCounter: 1,

			msg: "Before TASK RUNNING + UNHEALTHY, " +
				"After TASK RUNNING + HEALTHY",
		},
		{
			previousState:       task.TaskState_RUNNING,
			previousHealthState: task.HealthState_HEALTHY,

			newState:       mesos.TaskState_TASK_FAILED,
			newHealthState: false,

			healthyCounter:   2,
			unHealthyCounter: 1,

			msg: "Before TASK RUNNING + HEALTHY, " +
				"After TASK FAILED + UNHEALTHY, " +
				"health check max retries exhausted",
		},
		{
			previousState:       task.TaskState_RUNNING,
			previousHealthState: task.HealthState_UNHEALTHY,

			newState:       mesos.TaskState_TASK_RUNNING,
			newHealthState: false,

			healthyCounter:   2,
			unHealthyCounter: 2,

			msg: "Before TASK RUNNING + UNHEALTHY, " +
				"After TASK RUNNING + UNHEALTHY, " +
				"Consecutive negative health check result",
		},
	}

	for _, t := range tt {
		cachedJob := cachedmocks.NewMockJob(suite.ctrl)
		cachedTask := cachedmocks.NewMockTask(suite.ctrl)

		taskInfo := createTestTaskInfoWithHealth(
			t.previousState,
			t.previousHealthState,
			true)

		event := createTestTaskUpdateHealthCheckEvent(
			t.newState,
			t.newHealthState)
		timeNow := float64(time.Now().UnixNano())
		event.MesosTaskStatus.Timestamp = &timeNow

		gomock.InOrder(
			suite.mockTaskStore.EXPECT().
				GetTaskByID(context.Background(), _pelotonTaskID).
				Return(taskInfo, nil),
			suite.jobFactory.EXPECT().AddJob(_pelotonJobID).Return(cachedJob),
			cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH).MaxTimes(2),
			cachedJob.EXPECT().SetTaskUpdateTime(event.MesosTaskStatus.Timestamp).Return(),
			cachedJob.EXPECT().AddTask(gomock.Any(), _instanceID).Return(cachedTask, nil),
			cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
			cachedTask.EXPECT().CompareAndSetTask(gomock.Any(), gomock.Any(), job.JobType_BATCH).
				Do(func(_ context.Context, runtime *task.RuntimeInfo, _ job.JobType) {
					if t.newHealthState {
						suite.Equal(runtime.GetHealthy(), task.HealthState_HEALTHY)
					}
				}).Return(nil, nil),

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
			t.healthyCounter,
			suite.testScope.Snapshot().Counters()["status_updater.tasks_healthy_total+"].Value(),
			t.msg)

		suite.Equal(
			t.unHealthyCounter,
			suite.testScope.Snapshot().Counters()["status_updater.tasks_unhealthy_total+"].Value(),
			t.msg)
	}
}

// Test processing health check configured, configured but enabled or not
func (suite *TaskUpdaterTestSuite) TestProcessStatusUpdateSkipSameStateWithHealthy() {
	defer suite.ctrl.Finish()

	tt := []struct {
		previousState            task.TaskState
		newState                 mesos.TaskState
		previousHealthState      task.HealthState
		newHealthState           bool
		healthCheckEnabled       bool
		newStateByReconciliation bool
		msg                      string
	}{
		{
			previousHealthState: task.HealthState_HEALTH_UNKNOWN,
			previousState:       task.TaskState_STARTING,
			healthCheckEnabled:  false,

			newHealthState: false,
			newState:       mesos.TaskState_TASK_STARTING,

			msg: "Before/After TASK_STARTING",
		},
		{
			previousHealthState: task.HealthState_HEALTH_UNKNOWN,
			previousState:       task.TaskState_FAILED,
			healthCheckEnabled:  false,

			newHealthState: false,
			newState:       mesos.TaskState_TASK_FAILED,

			msg: "Before/After TASK_FAILED",
		},
		{
			previousHealthState: task.HealthState_INVALID,
			previousState:       task.TaskState_RUNNING,
			healthCheckEnabled:  false,

			newHealthState: false,
			newState:       mesos.TaskState_TASK_RUNNING,

			msg: "Before/After is TASK_RUNNING and health check not configured",
		},
		{
			previousHealthState: task.HealthState_HEALTH_UNKNOWN,
			previousState:       task.TaskState_RUNNING,
			healthCheckEnabled:  false,

			newHealthState: false,
			newState:       mesos.TaskState_TASK_RUNNING,

			msg: "Before/After is TASK_RUNNING " +
				"health check disabled",
		},
		{
			previousHealthState: task.HealthState_HEALTHY,
			previousState:       task.TaskState_RUNNING,
			healthCheckEnabled:  true,

			newHealthState:           false,
			newState:                 mesos.TaskState_TASK_RUNNING,
			newStateByReconciliation: true,

			msg: "Before/After is TASK_RUNNING, health check is enabled " +
				"but status update received is not due to health check",
		},
		{
			previousHealthState: task.HealthState_HEALTHY,
			previousState:       task.TaskState_RUNNING,
			healthCheckEnabled:  true,

			newHealthState:           true,
			newState:                 mesos.TaskState_TASK_RUNNING,
			newStateByReconciliation: false,

			msg: "Before/After is TASK_RUNNING, health is enabled, " +
				"status update is received using health check " +
				"but health check result is healthy consecutively",
		},
	}

	for _, t := range tt {

		var taskInfo *task.TaskInfo
		var event *pb_eventstream.Event

		if t.previousHealthState == task.HealthState_INVALID {
			taskInfo = createTestTaskInfo(t.previousState)
			event = createTestTaskUpdateEvent(t.newState)
		} else {
			taskInfo = createTestTaskInfoWithHealth(
				t.previousState,
				t.previousHealthState,
				t.healthCheckEnabled)

			event = createTestTaskUpdateHealthCheckEvent(
				t.newState, t.newHealthState)
		}

		if t.newStateByReconciliation {
			event = createTestTaskUpdateEvent(t.newState)
		}

		gomock.InOrder(
			suite.mockTaskStore.EXPECT().
				GetTaskByID(context.Background(), _pelotonTaskID).
				Return(taskInfo, nil),
		)
		now = nowMock
		suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))

		if t.newState == mesos.TaskState_TASK_RUNNING {
			suite.NotZero(suite.testScope.Snapshot().Counters()["status_updater.tasks_running_total+"].Value())
		}

		// Same status update with healthy is not processed so respective counters for
		// healthy or unhealthy status update is not increment in any scenario
		suite.Equal(
			int64(0),
			suite.testScope.Snapshot().Counters()["status_updater.tasks_healthy_total+"].Value())
		suite.Equal(
			int64(0),
			suite.testScope.Snapshot().Counters()["status_updater.tasks_unhealthy_total+"].Value())
	}
}

// Test processing task failure status update w/ retry.
func (suite *TaskUpdaterTestSuite) TestProcessTaskFailedStatusUpdate() {
	suite.doTestProcessTaskFailedStatusUpdate(_failureMsg,
		&task.TerminationStatus{
			Reason: task.TerminationStatus_TERMINATION_STATUS_REASON_FAILED,
		})
}

// Test processing task failure status update with exit-code.
func (suite *TaskUpdaterTestSuite) TestProcessTaskFailedStatusUpdateCode() {
	suite.doTestProcessTaskFailedStatusUpdate(_failureMsgExitCode,
		&task.TerminationStatus{
			Reason:   task.TerminationStatus_TERMINATION_STATUS_REASON_FAILED,
			ExitCode: 250,
		})
}

// Test processing task failure status update with invalid exit-code.
func (suite *TaskUpdaterTestSuite) TestProcessTaskFailedStatusUpdateBadCode() {
	suite.doTestProcessTaskFailedStatusUpdate(_failureMsgBadExitCode,
		&task.TerminationStatus{
			Reason: task.TerminationStatus_TERMINATION_STATUS_REASON_FAILED,
		})
}

// Test processing task failure status update with signal.
func (suite *TaskUpdaterTestSuite) TestProcessTaskFailedStatusUpdateSignal() {
	suite.doTestProcessTaskFailedStatusUpdate(_failureMsgSignal,
		&task.TerminationStatus{
			Reason: task.TerminationStatus_TERMINATION_STATUS_REASON_FAILED,
			Signal: "Segmentation fault",
		})
}

func (suite *TaskUpdaterTestSuite) doTestProcessTaskFailedStatusUpdate(
	failureMsg string, expectedTermStatus *task.TerminationStatus) {
	defer suite.ctrl.Finish()

	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_FAILED)
	event.MesosTaskStatus.Message = &failureMsg
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)

	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.jobFactory.EXPECT().
		AddJob(_pelotonJobID).Return(cachedJob)
	cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH)
	cachedJob.EXPECT().
		SetTaskUpdateTime(gomock.Any()).Return()
	cachedJob.EXPECT().AddTask(gomock.Any(), _instanceID).Return(cachedTask, nil)
	cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH)
	cachedTask.EXPECT().CompareAndSetTask(context.Background(), gomock.Any(), job.JobType_BATCH).
		Do(func(_ context.Context, runtime *task.RuntimeInfo, _ job.JobType) {
			suite.Equal(runtime.GetState(), task.TaskState_FAILED)
			suite.Equal(runtime.GetReason(), _mesosReason.String())
			suite.Equal(runtime.GetMessage(), failureMsg)
			suite.Equal(runtime.GetTerminationStatus(), expectedTermStatus)
		}).Return(nil, nil)
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
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_LOST)
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)

	rescheduleMsg := "Task LOST: testFailure"
	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.jobFactory.EXPECT().
		AddJob(_pelotonJobID).Return(cachedJob)
	cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH)
	cachedJob.EXPECT().
		SetTaskUpdateTime(gomock.Any()).Return()
	cachedJob.EXPECT().AddTask(gomock.Any(), _instanceID).Return(cachedTask, nil)
	cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH)
	cachedTask.EXPECT().CompareAndSetTask(context.Background(), gomock.Any(), job.JobType_BATCH).
		Do(func(_ context.Context, runtime *task.RuntimeInfo, _ job.JobType) {
			suite.Equal(runtime.GetState(), task.TaskState_LOST)
			suite.Equal(runtime.GetMessage(), rescheduleMsg)
		}).Return(nil, nil)
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
		cachedTask := cachedmocks.NewMockTask(suite.ctrl)
		event := createTestTaskUpdateEvent(t.mesosState)
		taskInfo := createTestTaskInfoWithHealth(
			task.TaskState_RUNNING,
			task.HealthState_HEALTHY,
			true)

		taskInfo.Runtime.ConfigVersion = t.configVersion
		taskInfo.Runtime.DesiredConfigVersion = t.desiredVersion
		taskInfo.Runtime.FailureCount = t.falureCount

		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil)
		suite.jobFactory.EXPECT().
			AddJob(_pelotonJobID).Return(cachedJob)
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH)
		cachedJob.EXPECT().
			SetTaskUpdateTime(gomock.Any()).Return()
		cachedJob.EXPECT().AddTask(gomock.Any(), _instanceID).Return(cachedTask, nil)
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH)
		cachedTask.EXPECT().CompareAndSetTask(context.Background(), gomock.Any(), job.JobType_BATCH).
			Do(func(_ context.Context, runtime *task.RuntimeInfo, _ job.JobType) {
				suite.Equal(runtime.GetState(), t.pelotnState)
				suite.Equal(runtime.GetHealthy(), task.HealthState_INVALID)
				suite.Equal(runtime.GetFailureCount(), t.desiredFailureCount)
			}).Return(nil, nil)
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
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
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
	cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH)
	cachedJob.EXPECT().
		SetTaskUpdateTime(gomock.Any()).Return()
	cachedJob.EXPECT().AddTask(gomock.Any(), _instanceID).Return(cachedTask, nil)
	cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH)
	cachedTask.EXPECT().CompareAndSetTask(context.Background(), gomock.Any(), job.JobType_BATCH).
		Do(func(_ context.Context, runtime *task.RuntimeInfo, _ job.JobType) {
			suite.Equal(runtime.GetState(), task.TaskState_LOST)
			suite.Equal(runtime.GetMessage(), _failureMsg)
		}).Return(nil, nil)
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
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	failureReason := mesos.TaskStatus_REASON_RECONCILIATION
	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_LOST)
	event.MesosTaskStatus.Reason = &failureReason
	taskInfo := createTestTaskInfoWithHealth(
		task.TaskState_RUNNING,
		task.HealthState_HEALTHY,
		true)
	taskInfo.Runtime.GoalState = task.TaskState_KILLED

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
		suite.jobFactory.EXPECT().
			AddJob(_pelotonJobID).Return(cachedJob),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		cachedJob.EXPECT().
			SetTaskUpdateTime(gomock.Any()).Return(),
		cachedJob.EXPECT().AddTask(gomock.Any(), _instanceID).Return(cachedTask, nil),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		cachedTask.EXPECT().CompareAndSetTask(context.Background(), gomock.Any(), job.JobType_BATCH).
			Do(func(_ context.Context, runtime *task.RuntimeInfo, _ job.JobType) {
				suite.Equal(runtime.GetState(), task.TaskState_KILLED)
				suite.Equal(runtime.GetReason(), failureReason.String())
				suite.Equal(runtime.GetMessage(), "Stopped task LOST event: "+_failureMsg)
				suite.Equal(runtime.GetCompletionTime(), _currentTime)
				suite.Equal(runtime.GetResourceUsage(), jobmgrtask.CreateEmptyResourceUsageMap())
				suite.Equal(runtime.GetHealthy(), task.HealthState_INVALID)
			}).Return(nil, nil),
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
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
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
			cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
			cachedJob.EXPECT().
				SetTaskUpdateTime(gomock.Any()).Return(),
			cachedJob.EXPECT().AddTask(gomock.Any(), _instanceID).Return(cachedTask, nil),
			cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
			cachedTask.EXPECT().CompareAndSetTask(context.Background(), gomock.Any(), job.JobType_BATCH).Return(nil, nil),
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

// Test service job would not update resource usage upon terminal state event
func (suite *TaskUpdaterTestSuite) TestProcessStatusUpdateWithTerminalStateEventForServiceJob() {
	defer suite.ctrl.Finish()

	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)

	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_KILLED)

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
		suite.jobFactory.EXPECT().
			AddJob(_pelotonJobID).Return(cachedJob),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_SERVICE),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_SERVICE),
		cachedJob.EXPECT().
			SetTaskUpdateTime(gomock.Any()).Return(),
		cachedJob.EXPECT().AddTask(gomock.Any(), _instanceID).Return(cachedTask, nil),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_SERVICE),
		cachedTask.EXPECT().CompareAndSetTask(context.Background(), gomock.Any(), job.JobType_SERVICE).Return(nil, nil),
		suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return(),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_SERVICE),
		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(job.JobType_SERVICE).
			Return(1*time.Second),
		suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return(),
		cachedJob.EXPECT().UpdateResourceUsage(nil),
	)

	suite.NoError(suite.updater.ProcessStatusUpdate(
		context.Background(), event))
}

// Test processing task status update when there is a task with resource usage
// map as nil. This could happen for in-flight tasks created before the feature
// was introduced
func (suite *TaskUpdaterTestSuite) TestProcessStatusUpdateResourceUsageNil() {
	defer suite.ctrl.Finish()

	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
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
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		cachedJob.EXPECT().
			SetTaskUpdateTime(gomock.Any()).Return(),
		cachedJob.EXPECT().AddTask(gomock.Any(), _instanceID).Return(cachedTask, nil),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		cachedTask.EXPECT().CompareAndSetTask(context.Background(), gomock.Any(), job.JobType_BATCH).Return(nil, nil),
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

// TestProcessOrphanTaskKillError tests getting an error on trying to kill orphan task
func (suite *TaskUpdaterTestSuite) TestProcessOrphanTaskKillError() {
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
		Return(nil, fmt.Errorf("fake db error")).
		Times(_numOrphanTaskKillAttempts)

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
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_RUNNING)
	taskInfo := createTestTaskInfo(task.TaskState_LAUNCHED)
	taskInfo.GetConfig().Volume = &task.PersistentVolumeConfig{}
	testVolumeID := &peloton.VolumeID{
		Value: "testVolume",
	}
	taskInfo.GetRuntime().VolumeID = testVolumeID

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
		cachedJob.EXPECT().AddTask(gomock.Any(), _instanceID).Return(cachedTask, nil),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		cachedTask.EXPECT().CompareAndSetTask(context.Background(), gomock.Any(), job.JobType_BATCH).
			Do(func(_ context.Context, runtime *task.RuntimeInfo, _ job.JobType) {
				suite.Equal(runtime.GetState(), task.TaskState_RUNNING)
				suite.Equal(runtime.GetStartTime(), _currentTime)
				suite.Equal(runtime.GetMessage(), "testFailure")
				suite.Empty(runtime.GetReason())
				suite.Empty(runtime.GetDesiredHost())
			}).Return(nil, nil),
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
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_RUNNING)
	taskInfo := createTestTaskInfo(task.TaskState_LAUNCHED)
	taskInfo.GetConfig().Volume = &task.PersistentVolumeConfig{}
	testVolumeID := &peloton.VolumeID{
		Value: "testVolume",
	}
	taskInfo.GetRuntime().VolumeID = testVolumeID

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
		cachedJob.EXPECT().AddTask(gomock.Any(), _instanceID).Return(cachedTask, nil),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		cachedTask.EXPECT().CompareAndSetTask(context.Background(), gomock.Any(), job.JobType_BATCH).
			Do(func(_ context.Context, runtime *task.RuntimeInfo, _ job.JobType) {
				suite.Equal(runtime.GetState(), task.TaskState_RUNNING)
				suite.Equal(runtime.GetStartTime(), _currentTime)
				suite.Equal(runtime.GetMessage(), "testFailure")
				suite.Empty(runtime.GetCompletionTime())
				suite.Empty(runtime.GetReason())
				suite.Empty(runtime.GetDesiredHost())
			}).Return(nil, nil),
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
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_RUNNING)
	taskInfo := createTestTaskInfo(task.TaskState_FAILED)
	taskInfo.GetRuntime().CompletionTime = _currentTime

	suite.mockTaskStore.EXPECT().
		GetTaskByID(context.Background(), _pelotonTaskID).
		Return(taskInfo, nil)
	suite.jobFactory.EXPECT().AddJob(_pelotonJobID).Return(cachedJob)
	cachedJob.EXPECT().AddTask(gomock.Any(), _instanceID).Return(cachedTask, nil)
	cachedJob.EXPECT().
		SetTaskUpdateTime(gomock.Any()).Return()
	cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH)
	cachedTask.EXPECT().
		CompareAndSetTask(context.Background(), gomock.Any(), job.JobType_BATCH).
		Do(func(_ context.Context, runtime *task.RuntimeInfo, _ job.JobType) {
			suite.Equal(runtime.GetState(), task.TaskState_RUNNING)
			suite.Empty(runtime.GetCompletionTime())
		}).
		Return(nil, nil)
	suite.goalStateDriver.EXPECT().EnqueueTask(_pelotonJobID, _instanceID, gomock.Any()).Return()
	cachedJob.EXPECT().UpdateResourceUsage(gomock.Any()).Return()
	cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH)
	suite.goalStateDriver.EXPECT().
		JobRuntimeDuration(job.JobType_BATCH).
		Return(1 * time.Second)
	suite.goalStateDriver.EXPECT().EnqueueJob(_pelotonJobID, gomock.Any()).Return()
	suite.NoError(suite.updater.ProcessStatusUpdate(context.Background(), event))
}

// Test case of processing status update for lost event.
func (suite *TaskUpdaterTestSuite) TestProcessLostEventStatusUpdate() {
	defer suite.ctrl.Finish()

	cachedJob := cachedmocks.NewMockJob(suite.ctrl)
	cachedTask := cachedmocks.NewMockTask(suite.ctrl)
	event := createTestTaskUpdateEvent(mesos.TaskState_TASK_LOST)
	timeNow := float64(time.Now().UnixNano())
	event.MesosTaskStatus.Timestamp = &timeNow
	taskInfo := createTestTaskInfo(task.TaskState_RUNNING)

	gomock.InOrder(
		suite.mockTaskStore.EXPECT().
			GetTaskByID(context.Background(), _pelotonTaskID).
			Return(taskInfo, nil),
		suite.jobFactory.EXPECT().AddJob(_pelotonJobID).Return(cachedJob),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		cachedJob.EXPECT().SetTaskUpdateTime(event.MesosTaskStatus.Timestamp).Return(),
		cachedJob.EXPECT().AddTask(gomock.Any(), _instanceID).Return(cachedTask, nil),
		cachedJob.EXPECT().GetJobType().Return(job.JobType_BATCH),
		cachedTask.EXPECT().CompareAndSetTask(context.Background(), gomock.Any(), job.JobType_BATCH).Return(nil, nil).
			Do(func(_ context.Context, runtime *task.RuntimeInfo, _ job.JobType) {
				suite.NotEmpty(runtime.GetCompletionTime())
			}),
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
