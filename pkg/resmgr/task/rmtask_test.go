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

package task

import (
	"context"
	"fmt"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pb_respool "github.com/uber/peloton/.gen/peloton/api/v0/respool"
	resp "github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common/statemachine"
	sm_mock "github.com/uber/peloton/pkg/common/statemachine/mocks"
	rc "github.com/uber/peloton/pkg/resmgr/common"
	"github.com/uber/peloton/pkg/resmgr/respool"
	"github.com/uber/peloton/pkg/resmgr/respool/mocks"
	"github.com/uber/peloton/pkg/resmgr/scalar"
	store_mocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type RMTaskTestSuite struct {
	suite.Suite

	tracker       Tracker
	task          *resmgr.Task
	ctrl          *gomock.Controller
	resTree       respool.Tree
	taskScheduler Scheduler
	context       context.Context
}

func (s *RMTaskTestSuite) SetupSuite() {
	s.ctrl = gomock.NewController(s.T())
	mockResPoolOps := objectmocks.NewMockResPoolOps(s.ctrl)
	mockResPoolOps.EXPECT().GetAll(context.Background()).
		Return(s.getResPools(), nil).AnyTimes()
	mockJobStore := store_mocks.NewMockJobStore(s.ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(s.ctrl)

	s.resTree = respool.NewTree(tally.NoopScope, mockResPoolOps, mockJobStore,
		mockTaskStore, rc.PreemptionConfig{
			Enabled: false,
		})

	InitTaskTracker(tally.NoopScope, &Config{
		EnablePlacementBackoff: true,
	})
	s.tracker = GetTracker()
	InitScheduler(
		tally.NoopScope,
		s.resTree,
		1*time.Second,
		s.tracker)
	s.taskScheduler = GetScheduler()
}

func (s *RMTaskTestSuite) SetupTest() {
	s.context = context.Background()
	err := s.resTree.Start()
	s.NoError(err)
	err = s.taskScheduler.Start()
	s.NoError(err)
}

func (s *RMTaskTestSuite) TearDownTest() {
	s.NoError(s.resTree.Stop())
	s.NoError(GetScheduler().Stop())
}

func (s *RMTaskTestSuite) getResPools() map[string]*pb_respool.ResourcePoolConfig {

	rootID := peloton.ResourcePoolID{Value: "root"}
	policy := pb_respool.SchedulingPolicy_PriorityFIFO

	return map[string]*pb_respool.ResourcePoolConfig{
		"root": {
			Name:      "root",
			Parent:    nil,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool1": {
			Name:      "respool1",
			Parent:    &rootID,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool2": {
			Name:      "respool2",
			Parent:    &rootID,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool3": {
			Name:      "respool3",
			Parent:    &rootID,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool11": {
			Name:      "respool11",
			Parent:    &peloton.ResourcePoolID{Value: "respool1"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool12": {
			Name:      "respool12",
			Parent:    &peloton.ResourcePoolID{Value: "respool1"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool21": {
			Name:      "respool21",
			Parent:    &peloton.ResourcePoolID{Value: "respool2"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool22": {
			Name:      "respool22",
			Parent:    &peloton.ResourcePoolID{Value: "respool2"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
	}
}

func (s *RMTaskTestSuite) createTask(instance int) *resmgr.Task {
	taskID := fmt.Sprintf("job1-%d", instance)
	mesosID := "mesosTaskID"
	return &resmgr.Task{
		Name:     taskID,
		Priority: 0,
		JobId:    &peloton.JobID{Value: "job1"},
		Id:       &peloton.TaskID{Value: taskID},
		Resource: &task.ResourceConfig{
			CpuLimit:    1,
			DiskLimitMb: 10,
			GpuLimit:    0,
			MemLimitMb:  100,
		},
		TaskId: &mesos.TaskID{
			Value: &mesosID,
		},
	}
}

func (s *RMTaskTestSuite) createTask0(cycleCount, attemptCount float64) *resmgr.Task {
	uuidStr := "uuidstr-1"
	jobID := "job1"
	instance := 1
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, instance, uuidStr)
	return &resmgr.Task{
		Name:     "job1-1",
		Priority: 0,
		JobId:    &peloton.JobID{Value: "job1"},
		Id:       &peloton.TaskID{Value: fmt.Sprintf("%s-%d", jobID, instance)},
		Resource: &task.ResourceConfig{
			CpuLimit:    1,
			DiskLimitMb: 10,
			GpuLimit:    0,
			MemLimitMb:  100,
		},
		TaskId: &mesos.TaskID{
			Value: &mesosTaskID,
		},
		Preemptible:             true,
		PlacementTimeoutSeconds: 60,
		PlacementRetryCount:     cycleCount,
		PlacementAttemptCount:   attemptCount,
	}
}

// Returns resource configs
func (s *RMTaskTestSuite) getResourceConfig() []*resp.ResourceConfig {

	resConfigs := []*resp.ResourceConfig{
		{
			Share:       1,
			Kind:        "cpu",
			Reservation: 100,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "memory",
			Reservation: 1000,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "disk",
			Reservation: 100,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "gpu",
			Reservation: 2,
			Limit:       4,
		},
	}
	return resConfigs
}

func (s *RMTaskTestSuite) pendingGang0() *resmgrsvc.Gang {
	var gang resmgrsvc.Gang
	gang.Tasks = []*resmgr.Task{
		s.createTask0(0, 1),
	}

	return &gang
}

func (s *RMTaskTestSuite) pendingGangs() []*resmgrsvc.Gang {
	gangs := make([]*resmgrsvc.Gang, 1)
	gangs[0] = s.pendingGang0()
	return gangs
}

func (s *RMTaskTestSuite) getEntitlement() *scalar.Resources {
	return &scalar.Resources{
		CPU:    100,
		MEMORY: 1000,
		DISK:   100,
		GPU:    2,
	}
}

func TestRMTask(t *testing.T) {
	suite.Run(t, new(RMTaskTestSuite))
}

// This tests CreateRMTask initializes the task properties properly
func (s *RMTaskTestSuite) TestCreateRMTasks() {

	respool, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)

	rmTask, err := CreateRMTask(
		tally.NoopScope,
		s.createTask(1),
		nil,
		respool,
		&Config{
			LaunchingTimeout:      2 * time.Second,
			PlacingTimeout:        2 * time.Second,
			PreemptingTimeout:     2 * time.Second,
			PlacementRetryCycle:   3,
			PlacementRetryBackoff: 1 * time.Second,
			PolicyName:            ExponentialBackOffPolicy,
		},
	)

	s.NoError(err)
	s.EqualValues(0, rmTask.Task().PlacementRetryCount)
	s.EqualValues(2, rmTask.Task().PlacementTimeoutSeconds)

	rmTask, err = CreateRMTask(
		tally.NoopScope,
		s.createTask0(1, 2),
		nil,
		respool,
		&Config{
			LaunchingTimeout:      2 * time.Second,
			PlacingTimeout:        2 * time.Second,
			PreemptingTimeout:     2 * time.Second,
			PlacementRetryCycle:   3,
			PlacementRetryBackoff: 1 * time.Second,
			PolicyName:            ExponentialBackOffPolicy,
		},
	)

	s.NoError(err)
	s.EqualValues(1, rmTask.Task().PlacementRetryCount)
	s.EqualValues(2, rmTask.Task().PlacementAttemptCount)
	s.EqualValues(2, rmTask.Task().PlacementTimeoutSeconds)
}

// This tests the requeue of the same task with same mesos task id as well
// as the different mesos task id
func (s *RMTaskTestSuite) TestStateChanges() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	var gangs []*resmgrsvc.Gang
	gangs = append(gangs, s.pendingGang0())

	s.tracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		&Config{
			LaunchingTimeout: 1 * time.Minute,
			PlacingTimeout:   1 * time.Minute,
			PolicyName:       ExponentialBackOffPolicy,
		})

	rmtask := s.tracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String(), statemachine.WithInfo("mesos_task_id",
		*s.pendingGang0().Tasks[0].TaskId.Value))
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_READY.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACED.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_LAUNCHING.String())
	s.NoError(err)
}

func (s *RMTaskTestSuite) TestReadyBackoff() {

	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	node.SetNonSlackEntitlement(s.getEntitlement())

	s.tracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		&Config{
			LaunchingTimeout:       1 * time.Minute,
			PlacingTimeout:         2 * time.Second,
			PlacementRetryCycle:    3,
			PlacementRetryBackoff:  1 * time.Second,
			PolicyName:             ExponentialBackOffPolicy,
			EnablePlacementBackoff: true,
		})

	respool, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	respool.EnqueueGang(s.pendingGang0())
	rmtask := s.tracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String(), statemachine.WithInfo("mesos_task_id",
		*s.pendingGang0().Tasks[0].TaskId.Value))
	s.NoError(err)

	// There is a race condition in the test due to the Scheduler.scheduleTasks
	// method is run asynchronously.
	time.Sleep(2 * time.Second)
	s.EqualValues(rmtask.GetCurrentState().State, task.TaskState_READY)
	rmtask.AddBackoff()
	err = rmtask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	// Testing the timeout hence sleep
	time.Sleep(5 * time.Second)
	// First Backoff and transition it should go to Ready state
	s.EqualValues(rmtask.GetCurrentState().State, task.TaskState_READY)
}

func (s *RMTaskTestSuite) TestPendingBackoff() {

	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	node.SetNonSlackEntitlement(s.getEntitlement())

	s.tracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		&Config{
			LaunchingTimeout:          1 * time.Minute,
			PlacingTimeout:            2 * time.Second,
			PlacementAttemptsPerCycle: 3,
			PlacementRetryCycle:       3,
			PlacementRetryBackoff:     1 * time.Second,
			PolicyName:                ExponentialBackOffPolicy,
			EnablePlacementBackoff:    true,
		})

	pool, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	pool.EnqueueGang(s.pendingGang0())
	rmtask := s.tracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String(), statemachine.WithInfo("mesos_task_id",
		*s.pendingGang0().Tasks[0].TaskId.Value))
	s.NoError(err)

	// There is a race condition in the test due to the Scheduler.scheduleTasks
	// method is run asynchronously.
	time.Sleep(2 * time.Second)
	s.EqualValues(rmtask.GetCurrentState().State, task.TaskState_READY)
	rmtask.task.PlacementAttemptCount = 2
	rmtask.AddBackoff()
	err = rmtask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	s.taskScheduler.Stop()
	// Testing the timeout hence sleep
	time.Sleep(5 * time.Second)
	// Third Backoff and transition it should go to Pending state
	s.EqualValues(rmtask.GetCurrentState().State, task.TaskState_PENDING)
}

func (s *RMTaskTestSuite) TestHostReservation() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	node.SetNonSlackEntitlement(s.getEntitlement())

	s.tracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		&Config{
			LaunchingTimeout:          1 * time.Minute,
			PlacingTimeout:            2 * time.Second,
			PlacementAttemptsPerCycle: 3,
			PlacementRetryCycle:       3,
			PlacementRetryBackoff:     1 * time.Second,
			PolicyName:                ExponentialBackOffPolicy,
			EnablePlacementBackoff:    true,
			EnableHostReservation:     true,
		})

	pool, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	pool.EnqueueGang(s.pendingGang0())
	rmTask := s.tracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmTask.TransitTo(task.TaskState_PENDING.String(), statemachine.WithInfo("mesos_task_id",
		*s.pendingGang0().Tasks[0].TaskId.Value))
	s.NoError(err)

	s.EqualValues(false, rmTask.Task().ReadyForHostReservation)

	// There is a race condition in the test due to the Scheduler.scheduleTasks
	// method is run asynchronously.
	// After third cycles, state transits to Ready state and is ready for host reservation
	time.Sleep(2 * time.Second)
	s.EqualValues(task.TaskState_READY, rmTask.GetCurrentState().State)
	rmTask.task.PlacementRetryCount = 2
	rmTask.task.PlacementAttemptCount = 3
	rmTask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	s.taskScheduler.Stop()
	// Testing the timeout hence sleep
	time.Sleep(5 * time.Second)
	s.EqualValues(true, rmTask.Task().ReadyForHostReservation)
	s.EqualValues(task.TaskState_READY, rmTask.GetCurrentState().State)
}

func (s *RMTaskTestSuite) TestHostReservationStateTransistion() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	node.SetNonSlackEntitlement(s.getEntitlement())

	s.tracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		&Config{
			LaunchingTimeout:          1 * time.Minute,
			PlacingTimeout:            2 * time.Second,
			PlacementAttemptsPerCycle: 3,
			PlacementRetryCycle:       3,
			PlacementRetryBackoff:     1 * time.Second,
			PolicyName:                ExponentialBackOffPolicy,
			EnablePlacementBackoff:    true,
			EnableHostReservation:     true,
		})

	pool, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	pool.EnqueueGang(s.pendingGang0())
	rmTask := s.tracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmTask.TransitTo(task.TaskState_PENDING.String(), statemachine.WithInfo("mesos_task_id",
		*s.pendingGang0().Tasks[0].TaskId.Value))
	s.NoError(err)

	err = rmTask.TransitTo(task.TaskState_READY.String())
	s.NoError(err)

	// Task can transit from READY to RESERVED
	err = rmTask.TransitTo(task.TaskState_RESERVED.String())
	s.NoError(err)
	rule := rmTask.stateMachine.GetTimeOutRules()[statemachine.State(task.TaskState_RESERVED.String())]
	s.Equal(rmTask.config.ReservingTimeout, rule.Timeout)
	s.Nil(rule.PreCallback)
	s.NotNil(rule.Callback)

	err = rmTask.TransitTo(task.TaskState_PLACED.String())
	s.NoError(err)
}

func (s *RMTaskTestSuite) TestBackOffDisabled() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	node.SetNonSlackEntitlement(s.getEntitlement())

	s.tracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		&Config{
			LaunchingTimeout:      1 * time.Minute,
			PlacingTimeout:        2 * time.Second,
			PlacementRetryCycle:   3,
			PlacementRetryBackoff: 1 * time.Second,
			PolicyName:            ExponentialBackOffPolicy,
			// Making backoff policy disabled
			EnablePlacementBackoff: false,
		})

	respool, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	respool.EnqueueGang(s.pendingGang0())
	rmtask := s.tracker.GetTask(s.pendingGang0().Tasks[0].Id)
	// Transiting task to Pending state
	err = rmtask.TransitTo(task.TaskState_PENDING.String(), statemachine.WithInfo("mesos_task_id",
		*s.pendingGang0().Tasks[0].TaskId.Value))
	s.NoError(err)

	// Waiting for scheduler to kick in by that the Scheduler.scheduleTasks runs
	// and we can test time out.
	time.Sleep(2 * time.Second)
	s.EqualValues(rmtask.GetCurrentState().State, task.TaskState_READY)
	// Making PlacementRetryCount to 2 by that next backoff can make it to PENDING
	// In case of backoff enabled and if thats not enabled it should make to READY
	rmtask.task.PlacementRetryCount = 2
	// Adding backoff but that should not be any effect.
	rmtask.AddBackoff()
	err = rmtask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	s.taskScheduler.Stop()
	// Testing the timeout hence sleep
	time.Sleep(5 * time.Second)
	// Third Backoff and transition it should go to READY state
	s.EqualValues(rmtask.GetCurrentState().State, task.TaskState_READY)
}

func (s *RMTaskTestSuite) TestReservingTimeout() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	node.SetNonSlackEntitlement(s.getEntitlement())

	s.tracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		&Config{
			ReservingTimeout: 1 * time.Second,
		})
	s.NoError(err)
	rmtask := s.tracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String(), statemachine.WithInfo("mesos_task_id",
		*s.pendingGang0().Tasks[0].TaskId.Value))
	s.NoError(err)

	err = rmtask.TransitTo(task.TaskState_READY.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_RESERVED.String())
	s.taskScheduler.Stop()

	// Testing the reserving timeout hence sleep
	time.Sleep(3 * time.Second)
	s.EqualValues(task.TaskState_PENDING, rmtask.GetCurrentState().State)
}

func (s *RMTaskTestSuite) TestLaunchingTimeout() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	node.SetNonSlackEntitlement(s.getEntitlement())

	s.tracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		&Config{
			LaunchingTimeout:      2 * time.Second,
			PlacingTimeout:        2 * time.Second,
			PlacementRetryCycle:   3,
			PlacementRetryBackoff: 1 * time.Second,
			PolicyName:            ExponentialBackOffPolicy,
		})
	s.NoError(err)
	rmtask := s.tracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String(), statemachine.WithInfo("mesos_task_id",
		*s.pendingGang0().Tasks[0].TaskId.Value))
	s.NoError(err)

	err = rmtask.TransitTo(task.TaskState_READY.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACED.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_LAUNCHING.String())
	s.NoError(err)
	// Testing the launching timeout hence sleep
	time.Sleep(3 * time.Second)
	s.EqualValues(rmtask.GetCurrentState().State, task.TaskState_READY)
}

func (s *RMTaskTestSuite) TestPreemptingTimeout() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	node.SetNonSlackEntitlement(s.getEntitlement())

	s.tracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		&Config{
			LaunchingTimeout:      2 * time.Second,
			PlacingTimeout:        2 * time.Second,
			PreemptingTimeout:     2 * time.Second,
			PlacementRetryCycle:   3,
			PlacementRetryBackoff: 1 * time.Second,
			PolicyName:            ExponentialBackOffPolicy,
		})
	s.NoError(err)
	rmtask := s.tracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String(), statemachine.WithInfo("mesos_task_id",
		*s.pendingGang0().Tasks[0].TaskId.Value))
	s.NoError(err)

	err = rmtask.TransitTo(task.TaskState_READY.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACED.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_LAUNCHING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_RUNNING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PREEMPTING.String())
	s.NoError(err)
	// Testing the preempting timeout hence sleep
	// TODO remove sleep
	time.Sleep(3 * time.Second)
	s.EqualValues(rmtask.GetCurrentState().State, task.TaskState_RUNNING)
}

func (s *RMTaskTestSuite) TestRunTimeStats() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	node.SetNonSlackEntitlement(s.getEntitlement())

	s.tracker.AddTask(
		s.pendingGang0().Tasks[0],
		nil,
		node,
		&Config{
			LaunchingTimeout:      2 * time.Second,
			PlacingTimeout:        2 * time.Second,
			PlacementRetryCycle:   3,
			PlacementRetryBackoff: 1 * time.Second,
			PolicyName:            ExponentialBackOffPolicy,
		})
	s.NoError(err)
	rmtask := s.tracker.GetTask(s.pendingGang0().Tasks[0].Id)
	err = rmtask.TransitTo(task.TaskState_PENDING.String(), statemachine.WithInfo("mesos_task_id",
		*s.pendingGang0().Tasks[0].TaskId.Value))
	s.NoError(err)

	err = rmtask.TransitTo(task.TaskState_READY.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_PLACED.String())
	s.NoError(err)
	err = rmtask.TransitTo(task.TaskState_LAUNCHING.String())
	s.NoError(err)

	s.Equal(rmtask.RunTimeStats().StartTime, time.Time{})

	before := time.Now()
	err = rmtask.TransitTo(task.TaskState_RUNNING.String())
	s.NoError(err)

	s.True(rmtask.RunTimeStats().StartTime.After(before))
}

func (s *RMTaskTestSuite) TestPushTaskForReadmissionError() {
	runWithMockNode := func(mnode respool.ResPool, err error) {
		rmTask, err := CreateRMTask(
			tally.NoopScope,
			s.createTask(1),
			nil,
			mnode,
			&Config{
				LaunchingTimeout:      2 * time.Second,
				PlacingTimeout:        2 * time.Second,
				PlacementRetryCycle:   3,
				PlacementRetryBackoff: 1 * time.Second,
				PolicyName:            ExponentialBackOffPolicy,
			},
		)

		s.NoError(err)

		err = rmTask.pushTaskForReadmission()
		s.EqualError(err, err.Error())
	}

	fErr := errors.New("fake error")
	mockNode := mocks.NewMockResPool(s.ctrl)

	// Mock Enqueue Failure
	mockNode.EXPECT().EnqueueGang(gomock.Any()).Return(fErr).Times(1)
	mockNode.EXPECT().GetPath().Return("/mocknode").Times(2)
	runWithMockNode(mockNode, fErr)

	// Mock Subtract Allocation failure
	mockNode.EXPECT().EnqueueGang(gomock.Any()).Return(nil).Times(1)
	mockNode.EXPECT().SubtractFromAllocation(gomock.Any()).Return(fErr).Times(1)
	runWithMockNode(mockNode, fErr)
}

func (s *RMTaskTestSuite) TestRMTaskTransitToError() {
	fErr := errors.New("fake error")

	errStateMachine := sm_mock.NewMockStateMachine(s.ctrl)
	errStateMachine.EXPECT().GetCurrentState().Return(statemachine.State("RUNNING"))
	errStateMachine.EXPECT().GetReason().Return("")
	errStateMachine.EXPECT().GetLastUpdateTime()
	errStateMachine.EXPECT().TransitTo(gomock.Any()).Return(fErr)

	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	rmTask, err := CreateRMTask(
		tally.NoopScope,
		s.createTask(1),
		nil,
		node,
		&Config{
			LaunchingTimeout:      2 * time.Second,
			PlacingTimeout:        2 * time.Second,
			PlacementRetryCycle:   3,
			PlacementRetryBackoff: 1 * time.Second,
			PolicyName:            ExponentialBackOffPolicy,
		})
	s.NotNil(rmTask.stateMachine)
	rmTask.stateMachine = errStateMachine

	err = rmTask.TransitTo(task.TaskState_RUNNING.String())
	s.EqualValues("failed to transition rmtask: fake error", err.Error())
}

func (s *RMTaskTestSuite) TestAddBackOffError() {
	errStateMachine := sm_mock.NewMockStateMachine(s.ctrl)
	errStateMachine.EXPECT().GetTimeOutRules().Return(make(map[statemachine.
		State]*statemachine.TimeoutRule))

	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	rmTask, err := CreateRMTask(
		tally.NoopScope,
		s.createTask(1),
		nil,
		node,
		&Config{
			LaunchingTimeout:       2 * time.Second,
			PlacingTimeout:         2 * time.Second,
			PlacementRetryCycle:    3,
			PlacementRetryBackoff:  1 * time.Second,
			PolicyName:             ExponentialBackOffPolicy,
			EnablePlacementBackoff: true,
		})
	s.NotNil(rmTask.stateMachine)
	rmTask.stateMachine = errStateMachine

	err = rmTask.AddBackoff()
	s.EqualError(err, "could not add backoff to task job1-1")
}

func (s *RMTaskTestSuite) TestRMTaskCallBackNilChecks() {
	var rmTask *RMTask
	tt := []struct {
		f   statemachine.Callback
		err error
	}{
		{
			rmTask.transitionCallBack,
			errTaskIsNotPresent,
		},
		{
			rmTask.timeoutCallbackFromPlacing,
			errTaskIsNotPresent,
		},
		{
			rmTask.timeoutCallbackFromLaunching,
			errTaskIsNotPresent,
		},
		{
			rmTask.preTimeoutCallback,
			errTaskIsNotPresent,
		},
		{
			rmTask.timeoutCallbackFromReserving,
			errTaskIsNotPresent,
		},
	}

	for _, t := range tt {
		s.Error(t.f(nil), t.err.Error())
	}
}

func (s *RMTaskTestSuite) TestRMTaskPreTimeoutCallback() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	rmTask, err := CreateRMTask(
		tally.NoopScope,
		s.createTask(1),
		nil,
		node,
		&Config{
			LaunchingTimeout:          2 * time.Second,
			PlacingTimeout:            2 * time.Second,
			PlacementRetryCycle:       3,
			PlacementAttemptsPerCycle: 3,
			PlacementRetryBackoff:     1 * time.Second,
			PolicyName:                ExponentialBackOffPolicy,
			EnablePlacementBackoff:    true,
			EnableHostReservation:     false,
		})

	// Host reservation is disabled and attempts are not exhausted
	rmTask.task.ReadyForHostReservation = false
	rmTask.task.PlacementRetryCount = 3
	rmTask.task.PlacementAttemptCount = 1
	transition := statemachine.Transition{}
	rmTask.preTimeoutCallback(&transition)
	s.Equal(false, rmTask.task.ReadyForHostReservation)
	s.Equal(statemachine.State(task.TaskState_READY.String()), transition.To)

	// Host reservation is disabled and attempts are exhausted
	rmTask.task.ReadyForHostReservation = false
	rmTask.task.PlacementRetryCount = 3
	rmTask.task.PlacementAttemptCount = 3
	transition = statemachine.Transition{}
	rmTask.preTimeoutCallback(&transition)
	s.Equal(false, rmTask.task.ReadyForHostReservation)
	s.Equal(statemachine.State(task.TaskState_PENDING.String()), transition.To)

	// Host reservation is enabled and retries are not exhausted
	rmTask.config.EnableHostReservation = true
	rmTask.task.ReadyForHostReservation = false
	rmTask.task.PlacementRetryCount = 1
	rmTask.task.PlacementAttemptCount = 3
	transition = statemachine.Transition{}
	rmTask.preTimeoutCallback(&transition)
	s.Equal(false, rmTask.task.ReadyForHostReservation)
	s.Equal(statemachine.State(task.TaskState_PENDING.String()), transition.To)

	// Host reservation is enabled and retries are exhausted
	rmTask.config.EnableHostReservation = true
	rmTask.task.ReadyForHostReservation = false
	rmTask.task.PlacementRetryCount = 2
	rmTask.task.PlacementAttemptCount = 3
	transition = statemachine.Transition{}
	rmTask.preTimeoutCallback(&transition)
	s.Equal(true, rmTask.task.ReadyForHostReservation)
	s.Equal(statemachine.State(task.TaskState_READY.String()), transition.To)
}

func (s *RMTaskTestSuite) TestRMTaskRequeueUnPlacedTaskNotInPlacing() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	rmTask, err := CreateRMTask(
		tally.NoopScope,
		s.createTask(1),
		nil,
		node,
		&Config{
			LaunchingTimeout:       2 * time.Second,
			PlacingTimeout:         2 * time.Second,
			PlacementRetryCycle:    3,
			PlacementRetryBackoff:  1 * time.Second,
			PolicyName:             ExponentialBackOffPolicy,
			EnablePlacementBackoff: true,
		})
	s.NotNil(rmTask.stateMachine)

	tt := []struct {
		tState task.TaskState
		err    error
		test   string
	}{
		{
			tState: task.TaskState_READY,
			err:    nil,
			test:   "task in ready state should return",
		},
		{
			tState: task.TaskState_PENDING,
			err:    nil,
			test:   "task in pending state should return",
		},
		{
			tState: task.TaskState_RUNNING,
			err:    errUnplacedTaskInWrongState,
			test:   "task not in placing state should return error",
		},
	}

	for _, t := range tt {
		// setup mock
		mockStateMachine := sm_mock.NewMockStateMachine(s.ctrl)
		mockStateMachine.EXPECT().GetCurrentState().Return(
			statemachine.State(t.tState.String()))
		mockStateMachine.
			EXPECT().GetReason().
			Return("testing").AnyTimes()
		mockStateMachine.
			EXPECT().GetLastUpdateTime().
			Return(time.Now()).AnyTimes()

		rmTask.stateMachine = mockStateMachine
		err := rmTask.RequeueUnPlaced("")
		s.Equal(t.err, err, t.test)
	}
}

func (s *RMTaskTestSuite) TestRMTaskRequeueUnPlacedTaskInPlacingToReady() {
	// Tests a task is PLACING state whose placement cycle hasn't finished is
	// successfully re-enqueued to the ready queue.

	runWithMockNode := func(
		mnode respool.ResPool,
		config *Config,
		sm statemachine.StateMachine) error {
		rmTask, err := CreateRMTask(
			tally.NoopScope,
			s.createTask(1),
			nil,
			mnode,
			config,
		)
		rmTask.stateMachine = sm
		s.NoError(err)

		return rmTask.RequeueUnPlaced("")
	}

	mockNode := mocks.NewMockResPool(s.ctrl)
	mockNode.EXPECT().GetPath().Return("/mocknode").Times(1)

	mockStateMachine := sm_mock.NewMockStateMachine(s.ctrl)

	// task is in PLACING state
	mockStateMachine.
		EXPECT().GetCurrentState().
		Return(statemachine.State(task.TaskState_PLACING.String()))
	mockStateMachine.
		EXPECT().GetReason().
		Return("testing").AnyTimes()
	mockStateMachine.
		EXPECT().GetLastUpdateTime().
		Return(time.Now()).AnyTimes()
	// transit to READY
	mockStateMachine.
		EXPECT().TransitTo(
		statemachine.State(task.TaskState_READY.String()),
		gomock.Any(),
	).Return(nil)
	// task is in READY state
	mockStateMachine.
		EXPECT().GetCurrentState().
		Return(statemachine.State(task.TaskState_READY.String()))

	err := runWithMockNode(
		mockNode,
		&Config{
			PolicyName: ExponentialBackOffPolicy,
		},
		mockStateMachine)
	s.Nil(err, "placing to ready requeue should not fail")
}

func (s *RMTaskTestSuite) TestRMTaskRequeueUnPlacedTaskInPlacingToReadyErr() {
	// Tests a task is PLACING state can't be requeued because of error in
	// state machine transition.
	runWithMockNode := func(
		mnode respool.ResPool,
		config *Config,
		sm statemachine.StateMachine) error {
		rmTask, err := CreateRMTask(
			tally.NoopScope,
			s.createTask(1),
			nil,
			mnode,
			config,
		)
		rmTask.stateMachine = sm
		s.NoError(err)

		return rmTask.RequeueUnPlaced("")
	}

	mockNode := mocks.NewMockResPool(s.ctrl)
	mockNode.EXPECT().GetPath().Return("/mocknode").Times(1)

	mockStateMachine := sm_mock.NewMockStateMachine(s.ctrl)
	fakeError := errors.New("fake error")

	// task is in PLACING state
	mockStateMachine.
		EXPECT().GetCurrentState().
		Return(statemachine.State(task.TaskState_PLACING.String())).AnyTimes()
	mockStateMachine.
		EXPECT().GetReason().
		Return("testing").AnyTimes()
	mockStateMachine.
		EXPECT().GetLastUpdateTime().
		Return(time.Now()).AnyTimes()
	// transit to READY
	mockStateMachine.
		EXPECT().TransitTo(
		statemachine.State(task.TaskState_READY.String()),
		gomock.Any(),
	).Return(fakeError)

	err := runWithMockNode(
		mockNode,
		&Config{
			PolicyName: ExponentialBackOffPolicy,
		},
		mockStateMachine)

	s.Equal(
		"failed to transition rmtask: fake error",
		err.Error(),
		"placing to ready requeue should fail")
	s.Equal(
		"fake error",
		errors.Cause(err).Error(),
		"placing to ready requeue should fail because of fake error")
}

func (s *RMTaskTestSuite) TestRMTaskRequeueUnPlacedTaskInPlacingToPending() {
	// Tests a task is PLACING state can't be requeued because of error in
	// state machine transition.
	runWithMockNode := func(
		mnode respool.ResPool,
		config *Config,
		sm statemachine.StateMachine) error {
		rmTask, err := CreateRMTask(
			tally.NoopScope,
			s.createTask0(0, 3),
			nil,
			mnode,
			config,
		)
		rmTask.stateMachine = sm
		s.NoError(err)

		return rmTask.RequeueUnPlaced("")
	}

	mockNode := mocks.NewMockResPool(s.ctrl)
	mockNode.EXPECT().GetPath().Return("/mocknode").Times(1)

	mockStateMachine := sm_mock.NewMockStateMachine(s.ctrl)

	// task is in PLACING state
	mockStateMachine.
		EXPECT().GetCurrentState().
		Return(statemachine.State(task.TaskState_PLACING.String())).AnyTimes()
	mockStateMachine.
		EXPECT().GetReason().
		Return("testing").AnyTimes()
	mockStateMachine.
		EXPECT().GetLastUpdateTime().
		Return(time.Now()).AnyTimes()
	// Enqueue gang
	mockNode.EXPECT().
		EnqueueGang(gomock.Any()).Return(nil)
	// Remove allocation
	mockNode.EXPECT().
		SubtractFromAllocation(gomock.Any()).Return(nil)
	// transit to READY
	mockStateMachine.
		EXPECT().TransitTo(
		statemachine.State(task.TaskState_PENDING.String()),
		gomock.Any(),
	).Return(nil)

	err := runWithMockNode(
		mockNode,
		&Config{
			LaunchingTimeout:          2 * time.Second,
			PlacingTimeout:            2 * time.Second,
			PlacementRetryCycle:       3,
			PlacementAttemptsPerCycle: 3,
			PlacementRetryBackoff:     1 * time.Second,
			PolicyName:                ExponentialBackOffPolicy,
			EnablePlacementBackoff:    true,
		},
		mockStateMachine)
	s.NoError(err, "placing to pending requeue should not fail")
}
