package task

import (
	"context"
	"fmt"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_respool "code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	resp "code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/statemachine"
	rc "code.uber.internal/infra/peloton/resmgr/common"
	"code.uber.internal/infra/peloton/resmgr/respool"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
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
	mockResPoolStore := store_mocks.NewMockResourcePoolStore(s.ctrl)
	mockResPoolStore.EXPECT().GetAllResourcePools(context.Background()).
		Return(s.getResPools(), nil).AnyTimes()
	mockJobStore := store_mocks.NewMockJobStore(s.ctrl)
	mockTaskStore := store_mocks.NewMockTaskStore(s.ctrl)

	respool.InitTree(tally.NoopScope, mockResPoolStore, mockJobStore,
		mockTaskStore, rc.PreemptionConfig{
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
	s.context = context.Background()
	err := s.resTree.Start()
	s.NoError(err)
	err = s.taskScheduler.Start()
	s.NoError(err)
}

func (s *RMTaskTestSuite) TearDownTest() {
	log.Info("tearing down")
	err := respool.GetTree().Stop()
	s.NoError(err)
	err = GetScheduler().Stop()
	s.NoError(err)
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
	uuidStr := "uuidstr-1"
	jobID := "job1"
	instance := 1
	mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, instance, uuidStr)
	gang.Tasks = []*resmgr.Task{
		{
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
			TaskId: &mesos_v1.TaskID{
				Value: &mesosTaskID,
			},
			Preemptible:         true,
			PlacementTimeout:    60,
			PlacementRetryCount: 1,
		},
	}
	return &gang
}

func (s *RMTaskTestSuite) pendingGangs() []*resmgrsvc.Gang {
	gangs := make([]*resmgrsvc.Gang, 1)
	gangs[0] = s.pendingGang0()
	return gangs
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
	node.SetEntitlement(s.getEntitlement())

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
	s.EqualValues(rmtask.GetCurrentState().String(), task.TaskState_READY.String())
	rmtask.AddBackoff()
	err = rmtask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	// Testing the timeout hence sleep
	time.Sleep(5 * time.Second)
	// First Backoff and transition it should go to Ready state
	s.EqualValues(rmtask.GetCurrentState().String(), task.TaskState_READY.String())
}

func (s *RMTaskTestSuite) TestPendingBackoff() {

	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	node.SetEntitlement(s.getEntitlement())

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
	s.EqualValues(rmtask.GetCurrentState().String(), task.TaskState_READY.String())
	rmtask.task.PlacementRetryCount = 2
	rmtask.AddBackoff()
	err = rmtask.TransitTo(task.TaskState_PLACING.String())
	s.NoError(err)
	s.taskScheduler.Stop()
	// Testing the timeout hence sleep
	time.Sleep(5 * time.Second)
	// Third Backoff and transition it should go to Pending state
	s.EqualValues(rmtask.GetCurrentState().String(), task.TaskState_PENDING.String())
}

func (s *RMTaskTestSuite) TestBackOffDisabled() {
	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	node.SetEntitlement(s.getEntitlement())

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
	s.EqualValues(rmtask.GetCurrentState().String(), task.TaskState_READY.String())
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
	s.EqualValues(rmtask.GetCurrentState().String(), task.TaskState_READY.String())
}

func (s *RMTaskTestSuite) TestLaunchingTimeout() {

	node, err := s.resTree.Get(&peloton.ResourcePoolID{Value: "respool3"})
	s.NoError(err)
	node.SetEntitlement(s.getEntitlement())

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
	s.EqualValues(rmtask.GetCurrentState().String(), task.TaskState_READY.String())

}
