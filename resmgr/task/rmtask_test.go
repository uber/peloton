package task

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pb_respool "code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool"
	resp "code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/statemachine"
	sm_mock "code.uber.internal/infra/peloton/common/statemachine/mocks"
	rc "code.uber.internal/infra/peloton/resmgr/common"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/resmgr/respool/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
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
}

func (s *RMTaskTestSuite) SetupSuite() {
	s.ctrl = gomock.NewController(s.T())

	mockResPoolStore := store_mocks.NewMockResourcePoolStore(s.ctrl)
	mockResPoolStore.
		EXPECT().
		GetAllResourcePools(context.Background()).
		Return(s.getResPools(), nil).AnyTimes()
	mockJobStore := store_mocks.NewMockJobStore(s.ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(s.ctrl)

	respool.InitTree(
		tally.NoopScope,
		mockResPoolStore,
		mockJobStore,
		mockTaskStore,
		rc.PreemptionConfig{
			Enabled: false,
		})
	s.resTree = respool.GetTree()

	InitTaskTracker(tally.NoopScope, &Config{
		EnablePlacementBackoff: true,
	})
	s.tracker = GetTracker()

	InitScheduler(tally.NoopScope, 1*time.Second, s.tracker)
	s.taskScheduler = GetScheduler()
}

func (s *RMTaskTestSuite) SetupTest() {
	s.NoError(s.resTree.Start())
	s.NoError(s.taskScheduler.Start())
}

func (s *RMTaskTestSuite) TearDownTest() {
	s.NoError(respool.GetTree().Stop())
	s.NoError(GetScheduler().Stop())
}

var defaultConfig = &Config{
	LaunchingTimeout:       1 * time.Minute,
	PlacingTimeout:         2 * time.Second,
	PlacementRetryCycle:    3,
	PlacementRetryBackoff:  1 * time.Second,
	PolicyName:             ExponentialBackOffPolicy,
	EnablePlacementBackoff: true,
}

func (s *RMTaskTestSuite) withRMTask(config *Config) *RMTask {
	if config == nil {
		config = defaultConfig
	}

	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	node.SetEntitlement(s.getEntitlement())

	gang := s.pendingGang0()
	ttask := gang.Tasks[0]
	s.tracker.AddTask(
		ttask,
		nil,
		node,
		config,
	)

	node.EnqueueGang(gang)
	return s.tracker.GetTask(ttask.Id)
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
	jobID := uuid.NewRandom()
	taskID := fmt.Sprintf("%s-%d", jobID, instance)
	mesosID := fmt.Sprintf("%s-%d-%s", jobID, instance, uuid.NewRandom())
	return &resmgr.Task{
		Name:     taskID,
		Priority: 0,
		JobId:    &peloton.JobID{Value: jobID.String()},
		Id:       &peloton.TaskID{Value: taskID},
		Resource: &task.ResourceConfig{
			CpuLimit:    1,
			DiskLimitMb: 10,
			GpuLimit:    0,
			MemLimitMb:  100,
		},
		TaskId: &mesos_v1.TaskID{
			Value: &mesosID,
		},
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
	jobID := uuid.NewRandom()
	instance := 1
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, instance, uuid.NewRandom())
	gang.Tasks = []*resmgr.Task{
		{
			Name:     fmt.Sprintf("%s-%d", jobID, instance),
			Priority: 0,
			JobId:    &peloton.JobID{Value: jobID.String()},
			Id:       &peloton.TaskID{Value: fmt.Sprintf("%s-%d", jobID, instance)},
			Resource: &task.ResourceConfig{
				CpuLimit:    1,
				DiskLimitMb: 10,
				GpuLimit:    0,
				MemLimitMb:  100,
			},
			TaskId: &mesos_v1.TaskID{
				Value: &mesosTaskID,
			},
			Preemptible:             true,
			PlacementTimeoutSeconds: 60,
			PlacementRetryCount:     1,
		},
	}
	return &gang
}

func (s *RMTaskTestSuite) getEntitlement() map[string]float64 {
	mapEntitlement := make(map[string]float64)
	mapEntitlement[common.CPU] = float64(100)
	mapEntitlement[common.MEMORY] = float64(1000)
	mapEntitlement[common.DISK] = float64(100)
	mapEntitlement[common.GPU] = float64(2)
	return mapEntitlement
}

func TestRMTask(t *testing.T) {
	suite.Run(t, new(RMTaskTestSuite))
}

// This tests the requeue of the same task with same mesos task id as well
// as the different mesos task id
func (s *RMTaskTestSuite) TestStateChanges() {
	rmtask := s.withRMTask(nil)
	err := rmtask.TransitTo(task.TaskState_PENDING.String(),
		statemachine.WithInfo("mesos_task_id",
			rmtask.Task().GetTaskId().GetValue()))
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
	rmTask := s.withRMTask(nil)

	err := rmTask.TransitTo(
		task.TaskState_PENDING.String(),
		statemachine.WithInfo("mesos_task_id",
			rmTask.Task().GetTaskId().GetValue()))
	s.NoError(err)

	// There is a race condition in the test due to the Scheduler.scheduleTasks
	// method is run asynchronously.
	time.Sleep(2 * time.Second)
	s.EqualValues(rmTask.GetCurrentState().String(), task.TaskState_READY.String())
	rmTask.AddBackoff()
	err = rmTask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	// Testing the timeout hence sleep
	time.Sleep(5 * time.Second)
	// First Backoff and transition it should go to Ready state
	s.EqualValues(rmTask.GetCurrentState().String(), task.TaskState_READY.String())
}

func (s *RMTaskTestSuite) TestPendingBackoff() {
	rmTask := s.withRMTask(nil)

	err := rmTask.TransitTo(task.TaskState_PENDING.String(),
		statemachine.WithInfo("mesos_task_id",
			rmTask.Task().GetTaskId().GetValue()))
	s.NoError(err)

	// There is a race condition in the test due to the Scheduler.scheduleTasks
	// method is run asynchronously.
	time.Sleep(2 * time.Second)
	s.EqualValues(rmTask.GetCurrentState().String(), task.TaskState_READY.String())
	rmTask.task.PlacementRetryCount = 2
	rmTask.AddBackoff()
	err = rmTask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	s.taskScheduler.Stop()
	// Testing the timeout hence sleep
	time.Sleep(5 * time.Second)
	// Third Backoff and transition it should go to Pending state
	s.EqualValues(rmTask.GetCurrentState().String(), task.TaskState_PENDING.String())
}

func (s *RMTaskTestSuite) TestBackOffDisabled() {
	rmTask := s.withRMTask(&Config{
		LaunchingTimeout:      1 * time.Minute,
		PlacingTimeout:        2 * time.Second,
		PlacementRetryCycle:   3,
		PlacementRetryBackoff: 1 * time.Second,
		PolicyName:            ExponentialBackOffPolicy,
		// Making backoff policy disabled
		EnablePlacementBackoff: false,
	})

	// Transiting task to Pending state
	err := rmTask.TransitTo(
		task.TaskState_PENDING.String(),
		statemachine.WithInfo("mesos_task_id", rmTask.Task().GetTaskId().GetValue()))
	s.NoError(err)

	// Waiting for scheduler to kick in by that the Scheduler.scheduleTasks runs
	// and we can test time out.
	time.Sleep(2 * time.Second)
	s.EqualValues(rmTask.GetCurrentState().String(), task.TaskState_READY.String())
	// Making PlacementRetryCount to 2 by that next backoff can make it to PENDING
	// In case of backoff enabled and if thats not enabled it should make to READY
	rmTask.task.PlacementRetryCount = 2
	// Adding backoff but that should not be any effect.
	rmTask.AddBackoff()
	err = rmTask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	s.taskScheduler.Stop()
	// Testing the timeout hence sleep
	time.Sleep(5 * time.Second)
	// Third Backoff and transition it should go to READY state
	s.EqualValues(rmTask.GetCurrentState().String(), task.TaskState_READY.String())
}

func (s *RMTaskTestSuite) TestLaunchingTimeout() {

	rmTask := s.withRMTask(&Config{
		LaunchingTimeout:      2 * time.Second,
		PlacingTimeout:        2 * time.Second,
		PlacementRetryCycle:   3,
		PlacementRetryBackoff: 1 * time.Second,
		PolicyName:            ExponentialBackOffPolicy,
	})
	err := rmTask.TransitTo(task.TaskState_PENDING.String(),
		statemachine.WithInfo("mesos_task_id",
			rmTask.Task().GetTaskId().GetValue()))
	s.NoError(err)

	err = rmTask.TransitTo(task.TaskState_READY.String())
	s.NoError(err)
	err = rmTask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	err = rmTask.TransitTo(task.TaskState_PLACED.String())
	s.NoError(err)
	err = rmTask.TransitTo(task.TaskState_LAUNCHING.String())
	s.NoError(err)
	// Testing the launching timeout hence sleep
	time.Sleep(4 * time.Second)
	s.EqualValues(rmTask.GetCurrentState().String(), task.TaskState_READY.String())
}

func (s *RMTaskTestSuite) TestRunTimeStats() {
	rmTask := s.withRMTask(nil)

	err := rmTask.TransitTo(task.TaskState_PENDING.String(),
		statemachine.WithInfo("mesos_task_id",
			rmTask.Task().GetTaskId().GetValue()))
	s.NoError(err)

	err = rmTask.TransitTo(task.TaskState_READY.String())
	s.NoError(err)
	err = rmTask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	err = rmTask.TransitTo(task.TaskState_PLACED.String())
	s.NoError(err)
	err = rmTask.TransitTo(task.TaskState_LAUNCHING.String())
	s.NoError(err)

	s.Equal(rmTask.RunTimeStats().StartTime, time.Time{})

	before := time.Now()
	err = rmTask.TransitTo(task.TaskState_RUNNING.String())
	s.NoError(err)

	s.True(rmTask.RunTimeStats().StartTime.After(before))
}

func (s *RMTaskTestSuite) TestPushTaskForReadmissionError() {
	runWithMockNode := func(mnode respool.ResPool, err error) {
		rmTask, err := CreateRMTask(s.createTask(1),
			nil,
			mnode,
			tally.NoopScope,
			&Config{
				LaunchingTimeout:      2 * time.Second,
				PlacingTimeout:        2 * time.Second,
				PlacementRetryCycle:   3,
				PlacementRetryBackoff: 1 * time.Second,
				PolicyName:            ExponentialBackOffPolicy,
			},
		)

		s.NoError(err)

		err = rmTask.PushTaskForReadmission()
		s.EqualError(err, err.Error())
	}

	fErr := errors.New("fake error")
	mockNode := mocks.NewMockResPool(s.ctrl)
	mockNode.EXPECT().GetPath().Return("/mock/node").AnyTimes()

	// Mock Enqueue Failure
	mockNode.EXPECT().EnqueueGang(gomock.Any()).Return(fErr).Times(1)
	runWithMockNode(mockNode, fErr)

	// Mock Subtract Allocation failure
	mockNode.EXPECT().EnqueueGang(gomock.Any()).Return(nil).Times(1)
	mockNode.EXPECT().SubtractFromAllocation(gomock.Any()).Return(fErr).Times(1)
	runWithMockNode(mockNode, fErr)

	// Mock Add To Allocation failure
	mockNode.EXPECT().EnqueueGang(gomock.Any()).Return(nil).Times(1)
	mockNode.EXPECT().SubtractFromAllocation(gomock.Any()).Return(nil).Times(1)
	mockNode.EXPECT().AddToDemand(gomock.Any()).Return(fErr).Times(1)
	runWithMockNode(mockNode, fErr)
}

func (s *RMTaskTestSuite) TestRMTaskTransitToError() {
	fErr := errors.New("fake error")

	errStateMachine := sm_mock.NewMockStateMachine(s.ctrl)
	errStateMachine.EXPECT().TransitTo(gomock.Any()).Return(fErr)

	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	rmTask, err := CreateRMTask(s.createTask(1),
		nil,
		node,
		tally.NoopScope,
		&Config{
			LaunchingTimeout:      2 * time.Second,
			PlacingTimeout:        2 * time.Second,
			PlacementRetryCycle:   3,
			PlacementRetryBackoff: 1 * time.Second,
			PolicyName:            ExponentialBackOffPolicy,
		})
	s.NotNil(rmTask.StateMachine())
	rmTask.stateMachine = errStateMachine

	err = rmTask.TransitTo(task.TaskState_RUNNING.String())
	s.EqualError(err, fErr.Error())
}

func (s *RMTaskTestSuite) TestAddBackOffError() {
	errStateMachine := sm_mock.NewMockStateMachine(s.ctrl)
	errStateMachine.EXPECT().GetTimeOutRules().Return(make(map[statemachine.
		State]*statemachine.TimeoutRule))

	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	rmTask, err := CreateRMTask(s.createTask(1),
		nil,
		node,
		tally.NoopScope,
		&Config{
			LaunchingTimeout:       2 * time.Second,
			PlacingTimeout:         2 * time.Second,
			PlacementRetryCycle:    3,
			PlacementRetryBackoff:  1 * time.Second,
			PolicyName:             ExponentialBackOffPolicy,
			EnablePlacementBackoff: true,
		})
	s.NotNil(rmTask.StateMachine())
	rmTask.stateMachine = errStateMachine

	err = rmTask.AddBackoff()
	s.True(strings.Contains(err.Error(), "could not add backoff"))
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
	}

	for _, t := range tt {
		s.Error(t.f(nil), t.err.Error())
	}
}
