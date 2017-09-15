package preemption

import (
	"fmt"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	peloton_respool "code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/resmgr/scalar"
	rm_task "code.uber.internal/infra/peloton/resmgr/task"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type RankerTestSuite struct {
	suite.Suite
	mockCtrl *gomock.Controller

	tracker            rm_task.Tracker
	eventStreamHandler *eventstream.Handler
	task               *resmgr.Task
	respool            respool.ResPool
}

func (suite *RankerTestSuite) SetupTest() {
	suite.mockCtrl = gomock.NewController(suite.T())

	rm_task.InitTaskTracker(tally.NoopScope)
	suite.tracker = rm_task.GetTracker()
	suite.eventStreamHandler = eventstream.NewEventStreamHandler(
		1000,
		[]string{
			common.PelotonJobManager,
			common.PelotonResourceManager,
		},
		nil,
		tally.Scope(tally.NoopScope))
}

func TestRanker(t *testing.T) {
	suite.Run(t, new(RankerTestSuite))
}

func (suite *RankerTestSuite) addTasktotracker(task *resmgr.Task) {
	rootID := peloton.ResourcePoolID{Value: respool.RootResPoolID}
	policy :=
		peloton_respool.SchedulingPolicy_PriorityFIFO
	respoolConfig := &peloton_respool.ResourcePoolConfig{
		Name:      "respool-1",
		Parent:    &rootID,
		Resources: suite.getResourceConfig(),
		Policy:    policy,
	}
	suite.respool, _ = respool.NewRespool(tally.NoopScope, "respool-1", nil, respoolConfig)
	suite.tracker.AddTask(task, suite.eventStreamHandler, suite.respool, &rm_task.Config{})
}

// Returns resource configs
func (suite *RankerTestSuite) getResourceConfig() []*peloton_respool.ResourceConfig {

	resConfigs := []*peloton_respool.ResourceConfig{
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

func (suite *RankerTestSuite) createTask(instance int, priority uint32) *resmgr.Task {
	taskID := fmt.Sprintf("job1-%d", instance)
	return &resmgr.Task{
		Name:     taskID,
		Priority: priority,
		JobId:    &peloton.JobID{Value: "job1"},
		Id:       &peloton.TaskID{Value: taskID},
		Hostname: "hostname",
		Resource: &task.ResourceConfig{
			CpuLimit:    1,
			DiskLimitMb: 10,
			GpuLimit:    0,
			MemLimitMb:  100,
		},
	}
}

func (suite *RankerTestSuite) addTasks() {
	// Add 10 tasks to tracker(3 READY, 6 RUNNING, 1 PENDING)
	// 3 READY with different priorities
	for i := 0; i < 3; i++ {
		suite.addTasktotracker(suite.createTask(i, uint32(i)))
		taskIDStr := fmt.Sprintf("job1-%d", i)
		taskID := &peloton.TaskID{Value: taskIDStr}
		suite.transitToReady(taskID)
	}
	// 3 RUNNING with different priorities
	for i := 3; i < 6; i++ {
		suite.addTasktotracker(suite.createTask(i, uint32(i)))
		taskIDStr := fmt.Sprintf("job1-%d", i)
		taskID := &peloton.TaskID{Value: taskIDStr}
		suite.transitToRunning(taskID)
	}
	// 3 RUNNING with same priority and different start times
	priority := uint32(6)
	for i := 6; i < 9; i++ {
		suite.addTasktotracker(suite.createTask(i, priority))
		taskIDStr := fmt.Sprintf("job1-%d", i)
		taskID := &peloton.TaskID{Value: taskIDStr}
		suite.transitToRunning(taskID)
		rmTask := suite.tracker.GetTask(taskID)
		rmTask.RunTimeStats().StartTime = time.Now()
		time.Sleep(1 * time.Second)
	}
	// 1 PENDING Tasks which should be ignored
	suite.addTasktotracker(suite.createTask(9, 1))
	taskIDStr := fmt.Sprintf("job1-%d", 9)
	taskID := &peloton.TaskID{Value: taskIDStr}
	suite.transitToPending(taskID)
}

func (suite *RankerTestSuite) transitToPending(taskID *peloton.TaskID) {
	rmTask := suite.tracker.GetTask(taskID)
	err := rmTask.TransitTo(task.TaskState_PENDING.String())
	suite.NoError(err)
}

func (suite *RankerTestSuite) transitToReady(taskID *peloton.TaskID) {
	rmTask := suite.tracker.GetTask(taskID)
	err := rmTask.TransitTo(task.TaskState_PENDING.String())
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_READY.String())
	suite.NoError(err)
}

func (suite *RankerTestSuite) transitToRunning(taskID *peloton.TaskID) {
	rmTask := suite.tracker.GetTask(taskID)
	err := rmTask.TransitTo(task.TaskState_PENDING.String())
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_PLACED.String())
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_LAUNCHING.String())
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_RUNNING.String())
	suite.NoError(err)
}

func (suite *RankerTestSuite) TestStatePriorityRuntimeRanker_GetTasksToEvict() {
	suite.addTasks()

	ranker := newStatePriorityRuntimeRanker(suite.tracker)
	tasksToEvict := ranker.GetTasksToEvict("respool-1", &scalar.Resources{
		CPU:    10,
		MEMORY: 1000,
		GPU:    0,
		DISK:   100,
	})
	suite.Equal(9, len(tasksToEvict))

	expectedTasks := []string{
		// READY TASKS sorted by priority
		"job1-0",
		"job1-1",
		"job1-2",

		// RUNNING tasks sorted by priority
		"job1-3",
		"job1-4",
		"job1-5",

		// RUNNING tasks sorted by start times
		"job1-8",
		"job1-7",
		"job1-6",
	}

	for i, taskToEvict := range tasksToEvict {
		suite.Equal(expectedTasks[i], taskToEvict.Task().GetId().Value)
	}
}

func (suite *RankerTestSuite) TestStatePriorityRuntimeRanker_GetTasksToEvictLimitResource() {
	suite.addTasks()

	ranker := newStatePriorityRuntimeRanker(suite.tracker)
	tasksToEvict := ranker.GetTasksToEvict("respool-1", &scalar.Resources{
		CPU:    5.5,
		MEMORY: 550,
		GPU:    0,
		DISK:   55,
	})

	// should only contain 6 tasks since resource limit is met by those tasks
	suite.Equal(6, len(tasksToEvict))
	expectedTasks := []string{
		// READY TASKS sorted by priority
		"job1-0",
		"job1-1",
		"job1-2",

		// RUNNING tasks sorted by priority
		"job1-3",
		"job1-4",
		"job1-5",
	}

	for i, taskToEvict := range tasksToEvict {
		suite.Equal(expectedTasks[i], taskToEvict.Task().GetId().Value)
	}
}
