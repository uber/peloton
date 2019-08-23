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
	"fmt"
	"sync"
	"testing"
	"time"

	mesos_v1 "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	resp "github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	hostsvc_mocks "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/eventstream"
	"github.com/uber/peloton/pkg/common/statemachine/mocks"
	rc "github.com/uber/peloton/pkg/resmgr/common"
	"github.com/uber/peloton/pkg/resmgr/respool"
	"github.com/uber/peloton/pkg/resmgr/scalar"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

var (
	unknownMesosTaskID = "941ff353-ba82-49fe-8f80-fb5bc649b04d-1-2"
)

type trackerTestSuite struct {
	suite.Suite
	mockCtrl *gomock.Controller

	tracker            Tracker
	eventStreamHandler *eventstream.Handler
	task               *resmgr.Task
	respool            respool.ResPool
	hostname           string
	mockHostmgr        *hostsvc_mocks.MockInternalHostServiceYARPCClient
}

func (suite *trackerTestSuite) SetupTest() {
	suite.setup(&Config{
		EnablePlacementBackoff: true,
	}, false)
}

func (suite *trackerTestSuite) setup(conf *Config, invalid bool) {
	InitTaskTracker(tally.NoopScope, conf)
	suite.tracker = GetTracker()
	suite.eventStreamHandler = eventstream.NewEventStreamHandler(
		1000,
		[]string{
			common.PelotonJobManager,
			common.PelotonResourceManager,
		},
		nil,
		tally.Scope(tally.NoopScope))
	suite.hostname = "hostname"
	suite.task = suite.createTask(0)
	if invalid {
		suite.addTaskToTrackerWithTimeoutConfig(suite.task, &Config{
			PolicyName:             ExponentialBackOffPolicy,
			PlacingTimeout:         -1 * time.Minute,
			EnablePlacementBackoff: true,
		})

	}
	suite.addTaskToTracker(suite.task)
}

func (suite *trackerTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
}

func (suite *trackerTestSuite) TearDownTest() {
	suite.tracker.Clear()
	for s := range task.TaskState_name {
		suite.tracker.(*tracker).
			resourcesHeldByTaskState[task.TaskState(s)] = scalar.ZeroResource
	}
}

func (suite *trackerTestSuite) addTaskToTracker(task *resmgr.Task) {
	suite.addTaskToTrackerWithTimeoutConfig(task, &Config{
		PolicyName: ExponentialBackOffPolicy,
	})
}

func (suite *trackerTestSuite) addTaskToTrackerWithTimeoutConfig(task *resmgr.
	Task, cfg *Config) {
	rootID := peloton.ResourcePoolID{Value: common.RootResPoolID}
	policy := resp.SchedulingPolicy_PriorityFIFO
	respoolConfig := &resp.ResourcePoolConfig{
		Name:      "respool-1",
		Parent:    &rootID,
		Resources: suite.getResourceConfig(),
		Policy:    policy,
	}
	suite.respool, _ = respool.NewRespool(tally.NoopScope, "respool-1",
		nil, respoolConfig, rc.PreemptionConfig{Enabled: false})
	suite.tracker.AddTask(task, suite.eventStreamHandler, suite.respool, cfg)
}

// Returns resource configs
func (suite *trackerTestSuite) getResourceConfig() []*resp.ResourceConfig {

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

func (suite *trackerTestSuite) createTask(instance int) *resmgr.Task {
	jobID := uuid.New()
	taskID := fmt.Sprintf("%s-%d", jobID, instance)
	mesosID := fmt.Sprintf("%s-%d-1", jobID, instance)
	return &resmgr.Task{
		Name:     taskID,
		Priority: 0,
		JobId:    &peloton.JobID{Value: jobID},
		Id:       &peloton.TaskID{Value: taskID},
		Hostname: suite.hostname,
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

func TestTracker(t *testing.T) {
	suite.Run(t, new(trackerTestSuite))
}

func (suite *trackerTestSuite) TestTasksByHosts() {
	result := suite.tracker.TasksByHosts([]string{suite.hostname}, suite.task.Type)
	suite.Equal(1, len(result))
	suite.Equal(1, len(result[suite.hostname]))
	suite.Equal(suite.task, result[suite.hostname][0].task)
}

func (suite *trackerTestSuite) TestTransition() {
	rmTask := suite.tracker.GetTask(suite.task.Id)
	err := rmTask.TransitTo(task.TaskState_PENDING.String())
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_READY.String())
	suite.NoError(err)
}

func (suite *trackerTestSuite) TestSetPlacement() {
	for i := 0; i < 5; i++ {
		newHostname := fmt.Sprintf("new-hostname-%v", i)
		suite.tracker.SetPlacement(&resmgr.Placement{
			TaskIDs: []*resmgr.Placement_Task{
				{
					PelotonTaskID: suite.task.GetId(),
					MesosTaskID:   suite.task.GetTaskId(),
				},
			},
			Hostname: newHostname,
		})

		result := suite.tracker.TasksByHosts([]string{newHostname}, suite.task.Type)
		suite.Equal(1, len(result))
		suite.Equal(1, len(result[newHostname]))
		suite.Equal(suite.task, result[newHostname][0].task)
	}
}

func (suite *trackerTestSuite) TestSetPlacementHost() {
	suite.tracker.Clear()
	var tasks []*resmgr.Placement_Task
	for i := 0; i < 5; i++ {
		rmTask := suite.createTask(i)
		tasks = append(tasks, &resmgr.Placement_Task{
			PelotonTaskID: rmTask.GetId(),
			MesosTaskID:   rmTask.GetTaskId(),
		})
		suite.addTaskToTracker(rmTask)
	}
	suite.tracker.SetPlacement(&resmgr.Placement{
		TaskIDs:  tasks,
		Hostname: suite.hostname,
	})
	result := suite.tracker.TasksByHosts([]string{suite.hostname}, suite.task.Type)
	suite.Equal(5, len(result[suite.hostname]))
	suite.tracker.Clear()
}

func (suite *trackerTestSuite) TestDelete() {
	suite.tracker.DeleteTask(suite.task.Id)
	rmTask := suite.tracker.GetTask(suite.task.Id)
	suite.Nil(rmTask)
	result := suite.tracker.TasksByHosts([]string{suite.hostname}, suite.task.Type)
	suite.Equal(0, len(result))
}

func (suite *trackerTestSuite) TestClear() {
	suite.tracker.Clear()
	suite.Equal(suite.tracker.GetSize(), int64(0))
}

func (suite *trackerTestSuite) TestAddResources() {
	res := suite.respool.GetTotalAllocatedResources()
	suite.Equal(res.GetCPU(), float64(0))
	suite.tracker.AddResources(suite.task.GetId())
	res = suite.respool.GetTotalAllocatedResources()
	suite.Equal(res.GetCPU(), float64(1))
}

func (suite *trackerTestSuite) TestGetTaskStates() {
	result := suite.tracker.GetActiveTasks("", "", nil)
	suite.Equal(1, len(result))

	result = suite.tracker.GetActiveTasks("foo", "", nil)
	suite.Equal(0, len(result))

	rmTask := suite.tracker.GetTask(suite.task.Id)
	err := rmTask.TransitTo(task.TaskState_PENDING.String())
	suite.NoError(err)

	states := make([]string, 2)
	states[0] = task.TaskState_PENDING.String()
	states[1] = task.TaskState_PLACING.String()
	result = suite.tracker.GetActiveTasks("", "", states)
	suite.Equal(1, len(result))

	result = suite.tracker.GetActiveTasks(suite.task.GetJobId().GetValue(), "", states)
	suite.Equal(1, len(result))

	result = suite.tracker.GetActiveTasks("foo", "", states)
	suite.Equal(0, len(result))
}

// TestGetActiveTasksOrphanTasks tests fetching the task state map for orphan tasks
func (suite *trackerTestSuite) TestGetActiveTasksOrphanTasks() {
	tr := suite.tracker.(*tracker)

	// move tasks to orphan tasks
	for _, rmTask := range tr.tasks {
		tr.orphanTasks[rmTask.task.GetTaskId().GetValue()] = rmTask
	}

	rmTask := suite.tracker.GetTask(suite.task.GetId())
	err := rmTask.TransitTo(task.TaskState_PENDING.String())
	suite.NoError(err)

	// remove all other tasks from tracker
	for k := range tr.tasks {
		delete(tr.tasks, k)
	}

	result := suite.tracker.GetActiveTasks("", "", nil)
	suite.Equal(1, len(result))

	result = suite.tracker.GetActiveTasks("foo", "", nil)
	suite.Equal(0, len(result))

	result = suite.tracker.GetActiveTasks("", "bar", nil)
	suite.Equal(0, len(result))

	result = suite.tracker.GetActiveTasks("", "", []string{task.TaskState_PLACING.String()})
	suite.Equal(0, len(result))

	result = suite.tracker.GetActiveTasks(suite.task.GetJobId().GetValue(), "", nil)
	suite.Len(result, len(tr.orphanTasks))
	for _, tasks := range result {
		for _, t := range tasks {
			suite.Equal(tr.orphanTasks[t.task.GetTaskId().GetValue()], t)
		}
	}
}

func (suite *trackerTestSuite) TestMarkItDone_Allocation() {
	suite.tracker.Clear()
	var tasks []*resmgr.Task
	for i := 0; i < 5; i++ {
		t := suite.createTask(i)
		tasks = append(tasks, t)
		suite.addTaskToTracker(t)
	}
	// Task 1
	// Trying to remove the first Task which is in initialized state
	// As initialized task can not be subtracted from allocation so
	// no change in respool allocation
	t := tasks[0].GetId()

	rmTask := suite.tracker.GetTask(t)

	resources := &scalar.Resources{
		CPU:    float64(1),
		DISK:   float64(10),
		GPU:    float64(0),
		MEMORY: float64(100),
	}
	rmTask.respool.AddToAllocation(scalar.GetTaskAllocation(rmTask.Task()))

	res := rmTask.respool.GetTotalAllocatedResources()

	suite.Equal(res, resources)

	suite.tracker.MarkItDone(rmTask.Task().GetTaskId().GetValue())

	res = rmTask.respool.GetTotalAllocatedResources()
	suite.Equal(res, resources)

	// FOR TASK 2
	// Trying to remove the Second Task which is in Pending state
	// As pending task can not be subtracted from allocation so
	// no change in respool allocation

	t = tasks[1].GetId()

	rmTask = suite.tracker.GetTask(t)

	rmTask.respool.AddToAllocation(scalar.GetTaskAllocation(rmTask.Task()))

	res = rmTask.respool.GetTotalAllocatedResources()

	err := rmTask.TransitTo(task.TaskState_PENDING.String())
	suite.NoError(err)

	suite.tracker.MarkItDone(rmTask.Task().GetTaskId().GetValue())

	res = rmTask.respool.GetTotalAllocatedResources()

	suite.Equal(res, resources)

	// TASK 3
	// Trying to remove the Third Task which is in Ready state
	// As READY task should subtracted from allocation so
	// so respool allocation is zero
	t = tasks[2].GetId()
	rmTask = suite.tracker.GetTask(t)
	rmTask.respool.AddToAllocation(scalar.GetTaskAllocation(rmTask.Task()))

	res = rmTask.respool.GetTotalAllocatedResources()

	err = rmTask.TransitTo(task.TaskState_PENDING.String())
	suite.NoError(err)

	err = rmTask.TransitTo(task.TaskState_READY.String())
	suite.NoError(err)

	suite.tracker.MarkItDone(rmTask.Task().GetTaskId().GetValue())

	res = rmTask.respool.GetTotalAllocatedResources()

	zeroResource := &scalar.Resources{
		CPU:    float64(0),
		DISK:   float64(0),
		GPU:    float64(0),
		MEMORY: float64(0),
	}
	suite.Equal(res, zeroResource)

	suite.tracker.Clear()
}

func (suite *trackerTestSuite) TestMarkItDone_WithDifferentMesosTaskID() {
	rmTask := suite.tracker.GetTask(suite.task.GetId())
	suite.NotNil(rmTask)

	// transit to a timeout state
	rmTask.TransitTo(task.TaskState_LAUNCHING.String())

	err := suite.tracker.MarkItDone(unknownMesosTaskID)
	suite.NoError(err)
}

// TestMarkItDoneOrphanTask tests the action of MarkItDone for an orphan RMTask
func (suite *trackerTestSuite) TestMarkItDoneOrphanTask() {
	testTracker := &tracker{
		tasks:         make(map[string]*RMTask),
		placements:    map[string]map[resmgr.TaskType]map[string]*RMTask{},
		orphanTasks:   make(map[string]*RMTask),
		metrics:       NewMetrics(tally.NoopScope),
		counters:      make(map[task.TaskState]float64),
		hostMgrClient: suite.mockHostmgr,
		scope:         tally.NoopScope,
	}
	t := suite.createTask(1)
	testTracker.AddTask(t, suite.eventStreamHandler, suite.respool, &Config{})

	tt := proto.Clone(t).(*resmgr.Task)

	orphanRMTask, err := CreateRMTask(tally.NoopScope, tt, suite.eventStreamHandler, suite.respool, &Config{})
	suite.NoError(err)
	testTracker.orphanTasks[unknownMesosTaskID] = orphanRMTask

	err = testTracker.MarkItDone(unknownMesosTaskID)
	suite.NoError(err)

	suite.Nil(testTracker.orphanTasks[unknownMesosTaskID])
}

// TestMarkItDoneOrphanTaskNoTaskInTracker tests the action of MarkItDone for an
// old run of a task after the newer run has been cleaned up
func (suite *trackerTestSuite) TestMarkItDoneOrphanTaskNoTaskInTracker() {
	testTracker := &tracker{
		tasks:         make(map[string]*RMTask),
		placements:    map[string]map[resmgr.TaskType]map[string]*RMTask{},
		orphanTasks:   make(map[string]*RMTask),
		metrics:       NewMetrics(tally.NoopScope),
		counters:      make(map[task.TaskState]float64),
		hostMgrClient: suite.mockHostmgr,
		scope:         tally.NoopScope,
	}
	t := suite.createTask(1)
	testTracker.AddTask(t, suite.eventStreamHandler, suite.respool, &Config{})

	tt := proto.Clone(t).(*resmgr.Task)

	orphanRMTask, err := CreateRMTask(tally.NoopScope, tt, suite.eventStreamHandler, suite.respool, &Config{})
	suite.NoError(err)
	testTracker.orphanTasks[unknownMesosTaskID] = orphanRMTask

	err = testTracker.MarkItDone(t.GetTaskId().GetValue())
	suite.NoError(err)
	suite.Nil(testTracker.GetTask(t.GetId()))

	err = testTracker.MarkItDone(unknownMesosTaskID)
	suite.NoError(err)

	suite.Nil(testTracker.orphanTasks[unknownMesosTaskID])
}

func (suite *trackerTestSuite) TestMarkItDone_StateMachine() {
	suite.addTaskToTrackerWithTimeoutConfig(suite.createTask(1), &Config{
		LaunchingTimeout: 1 * time.Second,
	})
	rmTask := suite.tracker.GetTask(suite.task.GetId())
	suite.NotNil(rmTask)

	// transit to a timeout state
	rmTask.TransitTo(task.TaskState_LAUNCHING.String())

	suite.tracker.MarkItDone(rmTask.Task().GetTaskId().GetValue())

	// wait for LaunchingTimeout
	time.Sleep(1 * time.Second)

	// the state machine's timer should be stopped
	suite.Equal(task.TaskState_LAUNCHING,
		rmTask.GetCurrentState().State)
}

func (suite *trackerTestSuite) TestMarkItInvalid() {
	rmTask := suite.tracker.GetTask(suite.task.GetId())
	suite.NotNil(rmTask)

	// transit to a timeout state
	rmTask.TransitTo(task.TaskState_LAUNCHING.String())

	err := suite.tracker.MarkItInvalid(unknownMesosTaskID)
	suite.NoError(err)

	err = suite.tracker.MarkItInvalid(rmTask.Task().GetTaskId().GetValue())
	suite.NoError(err)
}

// TestAddDeleteTasks tests the concurrency issues between add task and delete
// task from tracker this happens when add tasks and MarkItDone been called at
// the same time
func (suite *trackerTestSuite) TestAddDeleteTasks() {
	suite.tracker.Clear()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			suite.addTaskToTracker(suite.createTask(i))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			suite.tracker.MarkItDone("mesosTaskID")
		}

	}()

	wg.Wait()
	suite.tracker.Clear()
}

func (suite *trackerTestSuite) TestBackoffDisabled() {
	suite.tracker.Clear()
	rmtracker = nil
	suite.setup(&Config{
		EnablePlacementBackoff: false,
	}, false)

	rmTask := suite.tracker.GetTask(suite.task.GetId())
	suite.NotNil(rmTask)

	// transit to a timeout state
	rmTask.TransitTo(task.TaskState_LAUNCHING.String())

	err := suite.tracker.MarkItInvalid(unknownMesosTaskID)
	suite.NoError(err)

	err = suite.tracker.MarkItInvalid(rmTask.Task().GetTaskId().GetValue())
	suite.NoError(err)
	suite.tracker.Clear()
}

func (suite *trackerTestSuite) TestInitializeError() {
	suite.tracker.Clear()
	rmtracker = nil
	suite.setup(&Config{
		EnablePlacementBackoff: true,
		PlacingTimeout:         -1 * time.Minute,
	}, true)
	rmTask := suite.tracker.GetTask(suite.task.GetId())
	suite.NotNil(rmTask)
	suite.tracker.Clear()
}

/*
Tests the following scenario to check it doesn't result in a deadlock
1. A RMTask state machine is rolling back
   1.1 This acquires a write lock in the state machine
2. GetActiveTask is called.
   2.1 This acquires RLock on Tracker
   2.2 This acquires RLock on state machine(GetCurrentState).
3. SetPlacementHost is called which tries to acquire write lock on the
   tracker (subsequent requests to acquire RLock on the tracker will be put in
   a  queue).
4. The rollback(1) continues and tries to acquire RLock on the tracker but
   should not be blocked.

This test should complete if there is no deadlock
*/
func (suite *trackerTestSuite) TestGetActiveTasksDeadlock() {
	testTracker := &tracker{
		tasks:         make(map[string]*RMTask),
		placements:    map[string]map[resmgr.TaskType]map[string]*RMTask{},
		orphanTasks:   make(map[string]*RMTask),
		metrics:       NewMetrics(tally.NoopScope),
		counters:      make(map[task.TaskState]float64),
		hostMgrClient: suite.mockHostmgr,
		scope:         tally.NoopScope,
	}
	t := suite.createTask(1)
	testTracker.AddTask(
		t,
		suite.eventStreamHandler,
		suite.respool, &Config{
			PolicyName: ExponentialBackOffPolicy,
		})

	// channels to coordinate the deadlock behaviour
	startSetPlacement := make(chan struct{})
	continueGATask := make(chan struct{})
	resumeSMRollback := make(chan struct{})
	startSMRollBack := make(chan struct{})
	defer func() {
		close(startSetPlacement)
		close(continueGATask)
		close(resumeSMRollback)
		close(startSMRollBack)
	}()

	ctrl := gomock.NewController(suite.T())
	defer ctrl.Finish()

	// mock the GetCurrentState
	smLock := sync.RWMutex{}
	mSm := mocks.NewMockStateMachine(ctrl)
	mSm.EXPECT().GetReason().Return("testing").AnyTimes()
	mSm.EXPECT().GetLastUpdateTime().Return(time.Now()).AnyTimes()
	mSm.EXPECT().GetCurrentState().Do(func() {
		defer smLock.RUnlock()

		startSMRollBack <- struct{}{}
		<-continueGATask
		fmt.Println("acquiring statemachine read lock")
		// see 2.2 in test comments
		smLock.RLock()
		fmt.Println("statemachine read lock acquired")
	})

	// set the mock state machine for the task
	tesTask := testTracker.GetTask(t.GetId())
	tesTask.stateMachine = mSm

	wg := sync.WaitGroup{}
	wg.Add(3)

	// simulate state machine rollback for a task which acquires a write lock
	// on the statemachine.
	go func() {
		defer wg.Done()
		defer smLock.Unlock()

		<-startSMRollBack
		// acquire write lock on state machine (see 1.1 in test comments)
		fmt.Println("acquiring statemachine write lock")
		smLock.Lock()
		fmt.Println("statemachine write lock acquired")

		continueGATask <- struct{}{}

		// signal to call SetPlacement
		startSetPlacement <- struct{}{}
	}()

	// simulate calling GetActiveTasks which acquires a read lock on the
	// tracker and a read lock on the statemachine.
	go func() {
		defer wg.Done()

		// call GetActiveTasks which acquires RLock on tracker(see #2.1
		// in test comments)
		fmt.Println("calling GetActiveTasks")
		testTracker.GetActiveTasks("", "", []string{})
		fmt.Println("GetActiveTasks returned")
	}()

	// simulate calling SetPlacement which acquires a write lock on the tracker.
	go func() {
		defer wg.Done()

		<-startSetPlacement
		fmt.Println("calling SetPlacement")
		// call SetPlacement which acquires Lock on tracker(see #3
		// in test comments)
		testTracker.SetPlacement(&resmgr.Placement{
			TaskIDs: []*resmgr.Placement_Task{
				{
					PelotonTaskID: suite.task.GetId(),
					MesosTaskID:   suite.task.GetTaskId(),
				},
			},
			Hostname: "hostname",
		})
		fmt.Println("SetPlacement returned")
	}()

	// a deadlock would cause this to wait indefinitely
	wg.Wait()
}

// TestGetOrphanTask tests getting an orphan rm task
func (suite *trackerTestSuite) TestGetOrphanTask() {
	tr := suite.tracker.(*tracker)

	// move tasks to orphan tasks
	for _, rmTask := range tr.tasks {
		tr.orphanTasks[rmTask.task.GetTaskId().GetValue()] = rmTask
	}

	// remove all other tasks from tracker
	for k := range tr.tasks {
		delete(tr.tasks, k)
	}

	for _, rmTask := range tr.orphanTasks {
		suite.Equal(rmTask, tr.GetOrphanTask(rmTask.Task().GetTaskId().GetValue()))
	}
}

// TestGetOrphanTaskNoTask tests getting an unknown orphan rm Task
func (suite *trackerTestSuite) TestGetOrphanTaskNoTask() {
	tr := suite.tracker.(*tracker)

	var mesosTasks []string
	// move tasks to orphan tasks
	for _, rmTask := range tr.tasks {
		tr.orphanTasks[rmTask.task.GetTaskId().GetValue()] = rmTask
		mesosTasks = append(mesosTasks, rmTask.task.GetTaskId().GetValue())
	}

	// remove all other tasks from tracker
	for k := range tr.tasks {
		delete(tr.tasks, k)
	}

	suite.Nil(suite.tracker.GetOrphanTask("unknown-task"))
}

// TestGetOrphanTasks tests getting all orphan rm tasks
func (suite *trackerTestSuite) TestGetOrphanTasks() {
	tr := suite.tracker.(*tracker)

	var mesosTasks []string
	// move tasks to orphan tasks
	for _, rmTask := range tr.tasks {
		tr.orphanTasks[rmTask.task.GetTaskId().GetValue()] = rmTask
		mesosTasks = append(mesosTasks, rmTask.task.GetTaskId().GetValue())
	}

	// remove all other tasks from tracker
	for k := range tr.tasks {
		delete(tr.tasks, k)
	}

	for _, t := range suite.tracker.GetOrphanTasks("") {
		suite.Equal(tr.orphanTasks[t.Task().GetTaskId().GetValue()], t)
	}
}

// TestResourcesHeldByTaskState tests resources held by task state. It checks
// if the resource map is updated correctly after each state transition
func (suite *trackerTestSuite) TestResourcesHeldByTaskState() {
	rmTask1 := suite.tracker.GetTask(suite.task.Id)
	task2 := suite.createTask(1)
	suite.addTaskToTracker(task2)
	rmTask2 := suite.tracker.GetTask(task2.GetId())

	testRMTasks := []*RMTask{rmTask1, rmTask2}

	for i, t := range testRMTasks {
		err := t.TransitTo(task.TaskState_PENDING.String())
		suite.NoError(err)

		err = t.TransitTo(task.TaskState_READY.String())
		suite.NoError(err)
		resourcesHeld := suite.tracker.(*tracker).resourcesHeldByTaskState[task.TaskState_READY]
		suite.Equal(t.task.GetResource().GetCpuLimit(), resourcesHeld.GetCPU())
		suite.Equal(t.task.GetResource().GetMemLimitMb(), resourcesHeld.GetMem())
		suite.Equal(t.task.GetResource().GetDiskLimitMb(), resourcesHeld.GetDisk())
		suite.Equal(t.task.GetResource().GetGpuLimit(), resourcesHeld.GetGPU())

		err = t.TransitTo(task.TaskState_PLACING.String())
		suite.NoError(err)
		suite.Equal(scalar.ZeroResource, suite.tracker.(*tracker).resourcesHeldByTaskState[task.TaskState_READY])
		resourcesHeld = suite.tracker.(*tracker).resourcesHeldByTaskState[task.TaskState_PLACING]
		suite.Equal(t.task.GetResource().GetCpuLimit(), resourcesHeld.GetCPU())
		suite.Equal(t.task.GetResource().GetMemLimitMb(), resourcesHeld.GetMem())
		suite.Equal(t.task.GetResource().GetDiskLimitMb(), resourcesHeld.GetDisk())
		suite.Equal(t.task.GetResource().GetGpuLimit(), resourcesHeld.GetGPU())

		err = t.TransitTo(task.TaskState_PLACED.String())
		suite.NoError(err)
		suite.Equal(scalar.ZeroResource, suite.tracker.(*tracker).resourcesHeldByTaskState[task.TaskState_PLACING])
		resourcesHeld = suite.tracker.(*tracker).resourcesHeldByTaskState[task.TaskState_PLACED]
		suite.Equal(t.task.GetResource().GetCpuLimit(), resourcesHeld.GetCPU())
		suite.Equal(t.task.GetResource().GetMemLimitMb(), resourcesHeld.GetMem())
		suite.Equal(t.task.GetResource().GetDiskLimitMb(), resourcesHeld.GetDisk())
		suite.Equal(t.task.GetResource().GetGpuLimit(), resourcesHeld.GetGPU())

		err = t.TransitTo(task.TaskState_LAUNCHING.String())
		suite.NoError(err)
		suite.Equal(scalar.ZeroResource, suite.tracker.(*tracker).resourcesHeldByTaskState[task.TaskState_PLACED])
		resourcesHeld = suite.tracker.(*tracker).resourcesHeldByTaskState[task.TaskState_LAUNCHING]
		suite.Equal(t.task.GetResource().GetCpuLimit(), resourcesHeld.GetCPU())
		suite.Equal(t.task.GetResource().GetMemLimitMb(), resourcesHeld.GetMem())
		suite.Equal(t.task.GetResource().GetDiskLimitMb(), resourcesHeld.GetDisk())
		suite.Equal(t.task.GetResource().GetGpuLimit(), resourcesHeld.GetGPU())

		err = t.TransitTo(task.TaskState_LAUNCHED.String())
		suite.NoError(err)
		suite.Equal(scalar.ZeroResource, suite.tracker.(*tracker).resourcesHeldByTaskState[task.TaskState_LAUNCHING])
		resourcesHeld = suite.tracker.(*tracker).resourcesHeldByTaskState[task.TaskState_LAUNCHED]
		suite.Equal(t.task.GetResource().GetCpuLimit(), resourcesHeld.GetCPU())
		suite.Equal(t.task.GetResource().GetMemLimitMb(), resourcesHeld.GetMem())
		suite.Equal(t.task.GetResource().GetDiskLimitMb(), resourcesHeld.GetDisk())
		suite.Equal(t.task.GetResource().GetGpuLimit(), resourcesHeld.GetGPU())

		err = t.TransitTo(task.TaskState_RUNNING.String())
		suite.NoError(err)
		suite.Equal(scalar.ZeroResource, suite.tracker.(*tracker).resourcesHeldByTaskState[task.TaskState_LAUNCHED])
		resourcesHeld = suite.tracker.(*tracker).resourcesHeldByTaskState[task.TaskState_RUNNING]
		if i == 0 {
			suite.Equal(t.task.GetResource().GetCpuLimit(), resourcesHeld.GetCPU())
			suite.Equal(t.task.GetResource().GetMemLimitMb(), resourcesHeld.GetMem())
			suite.Equal(t.task.GetResource().GetDiskLimitMb(), resourcesHeld.GetDisk())
			suite.Equal(t.task.GetResource().GetGpuLimit(), resourcesHeld.GetGPU())
		} else {
			suite.Equal(rmTask1.task.GetResource().GetCpuLimit()+
				rmTask2.task.GetResource().GetCpuLimit(), resourcesHeld.GetCPU())
			suite.Equal(rmTask1.task.GetResource().GetMemLimitMb()+
				rmTask2.task.GetResource().GetMemLimitMb(), resourcesHeld.GetMem())
			suite.Equal(rmTask1.task.GetResource().GetDiskLimitMb()+
				rmTask2.task.GetResource().GetDiskLimitMb(), resourcesHeld.GetDisk())
			suite.Equal(rmTask1.task.GetResource().GetGpuLimit()+
				rmTask2.task.GetResource().GetGpuLimit(), resourcesHeld.GetGPU())
		}
	}
}

// TestResourcesHeldByTaskStateConcurrencyControl tests for race and deadlock
// while updating ResourcesHeldByTaskState tracker metric
func (suite *trackerTestSuite) TestResourcesHeldByTaskStateConcurrencyControl() {
	numTasks := 50
	tasks := []*resmgr.Task{suite.task}
	for i := 1; i < numTasks; i++ {
		t := suite.createTask(i)
		tasks = append(tasks, t)
		suite.addTaskToTracker(t)
	}

	wg := sync.WaitGroup{}
	wg.Add(numTasks)
	for i := 0; i < numTasks; i++ {
		if i%2 == 0 {
			go func(id int) {
				defer wg.Done()

				suite.tracker.AddResources(tasks[id].GetId())
			}(i)
		} else {
			go func(id int) {
				defer wg.Done()

				t := suite.tracker.GetTask(tasks[id].GetId())
				suite.NotNil(t)
				t.TransitTo(task.TaskState_PENDING.String())
				t.TransitTo(task.TaskState_READY.String())
				t.TransitTo(task.TaskState_PLACING.String())
				t.TransitTo(task.TaskState_PLACED.String())
				t.TransitTo(task.TaskState_LAUNCHING.String())
				t.TransitTo(task.TaskState_LAUNCHED.String())
				t.TransitTo(task.TaskState_RUNNING.String())
			}(i)
		}
	}

	wg.Wait()
}

// TestResourcesHeldByTaskStateRelease ensures ResourcesHeldByTaskState is
// updated when a task is cleaned up from tracker
func (suite *trackerTestSuite) TestResourcesHeldByTaskStateRelease() {
	rmTask := suite.tracker.GetTask(suite.task.GetId())

	// transit the RMTask to RUNNING state
	err := rmTask.TransitTo(task.TaskState_PENDING.String())
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_READY.String())
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_PLACING.String())
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_PLACED.String())
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_LAUNCHING.String())
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_LAUNCHED.String())
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_RUNNING.String())
	suite.NoError(err)
	resourcesHeld := suite.tracker.(*tracker).resourcesHeldByTaskState[task.TaskState_RUNNING]
	suite.Equal(rmTask.task.GetResource().GetCpuLimit(), resourcesHeld.GetCPU())
	suite.Equal(rmTask.task.GetResource().GetMemLimitMb(), resourcesHeld.GetMem())
	suite.Equal(rmTask.task.GetResource().GetDiskLimitMb(), resourcesHeld.GetDisk())
	suite.Equal(rmTask.task.GetResource().GetGpuLimit(), resourcesHeld.GetGPU())

	err = suite.tracker.MarkItDone(rmTask.task.GetTaskId().GetValue())
	suite.NoError(err)

	// verify that the metrics are updated
	suite.Equal(
		scalar.ZeroResource,
		suite.tracker.(*tracker).resourcesHeldByTaskState[task.TaskState_RUNNING],
	)
}

// TestResourcesHeldByTaskStateReleaseResourceOrphanTasks ensures
// ResourcesHeldByTaskState is updated when orphan task resources are released
func (suite *trackerTestSuite) TestResourcesHeldByTaskStateReleaseResourceOrphanTasks() {
	rmTask := suite.tracker.GetTask(suite.task.GetId())

	// transit the RMTask to LAUNCHED state
	err := rmTask.TransitTo(task.TaskState_PENDING.String())
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_READY.String())
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_PLACING.String())
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_PLACED.String())
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_LAUNCHING.String())
	suite.NoError(err)
	err = rmTask.TransitTo(task.TaskState_LAUNCHED.String())
	suite.NoError(err)
	resourcesHeld := suite.tracker.(*tracker).resourcesHeldByTaskState[task.TaskState_LAUNCHED]
	suite.Equal(rmTask.task.GetResource().GetCpuLimit(), resourcesHeld.GetCPU())
	suite.Equal(rmTask.task.GetResource().GetMemLimitMb(), resourcesHeld.GetMem())
	suite.Equal(rmTask.task.GetResource().GetDiskLimitMb(), resourcesHeld.GetDisk())
	suite.Equal(rmTask.task.GetResource().GetGpuLimit(), resourcesHeld.GetGPU())

	// Add new run of the task so that an orphan task is created
	newMesosTaskID := fmt.Sprintf("%s-%d", suite.task.GetId().GetValue(), 2)
	newResmgrTask := proto.Clone(rmTask.task).(*resmgr.Task)
	newResmgrTask.TaskId.Value = &newMesosTaskID
	err = suite.tracker.AddTask(
		newResmgrTask,
		suite.eventStreamHandler,
		suite.respool,
		&Config{},
	)
	suite.NoError(err)

	// transit the new run of the RMTask to RUNNING
	newRMTask := suite.tracker.GetTask(suite.task.GetId())
	suite.Equal(newResmgrTask, newRMTask.task)
	err = newRMTask.TransitTo(task.TaskState_PENDING.String())
	suite.NoError(err)
	err = newRMTask.TransitTo(task.TaskState_READY.String())
	suite.NoError(err)
	err = newRMTask.TransitTo(task.TaskState_PLACING.String())
	suite.NoError(err)
	err = newRMTask.TransitTo(task.TaskState_PLACED.String())
	suite.NoError(err)
	err = newRMTask.TransitTo(task.TaskState_LAUNCHING.String())
	suite.NoError(err)
	err = newRMTask.TransitTo(task.TaskState_LAUNCHED.String())
	suite.NoError(err)
	err = newRMTask.TransitTo(task.TaskState_RUNNING.String())
	suite.NoError(err)

	// verify that the resources of the orphan task are still held
	resourcesHeld = suite.tracker.(*tracker).resourcesHeldByTaskState[task.TaskState_LAUNCHED]
	suite.Equal(rmTask.task.GetResource().GetCpuLimit(), resourcesHeld.GetCPU())
	suite.Equal(rmTask.task.GetResource().GetMemLimitMb(), resourcesHeld.GetMem())
	suite.Equal(rmTask.task.GetResource().GetDiskLimitMb(), resourcesHeld.GetDisk())
	suite.Equal(rmTask.task.GetResource().GetGpuLimit(), resourcesHeld.GetGPU())

	// clean up orphan task
	err = suite.tracker.MarkItDone(rmTask.task.GetTaskId().GetValue())
	suite.NoError(err)

	// verify that the metrics are updated
	suite.Equal(
		scalar.ZeroResource,
		suite.tracker.(*tracker).resourcesHeldByTaskState[task.TaskState_LAUNCHED],
	)
}
