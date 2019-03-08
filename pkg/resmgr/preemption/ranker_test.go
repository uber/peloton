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

package preemption

import (
	"fmt"
	"testing"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	peloton_respool "github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/eventstream"
	rc "github.com/uber/peloton/pkg/resmgr/common"
	"github.com/uber/peloton/pkg/resmgr/respool"
	"github.com/uber/peloton/pkg/resmgr/scalar"
	rm_task "github.com/uber/peloton/pkg/resmgr/task"
	"github.com/uber/peloton/pkg/resmgr/tasktestutil"

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

func (suite *RankerTestSuite) SetupSuite() {
	rm_task.InitTaskTracker(
		tally.NoopScope,
		&rm_task.Config{})
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

func (suite *RankerTestSuite) TearDownTest() {
	suite.tracker.Clear()
}

func TestRanker(t *testing.T) {
	suite.Run(t, new(RankerTestSuite))
}

func (suite *RankerTestSuite) addTaskToTracker(task *resmgr.Task) {
	rootID := peloton.ResourcePoolID{Value: common.RootResPoolID}
	policy :=
		peloton_respool.SchedulingPolicy_PriorityFIFO
	respoolConfig := &peloton_respool.ResourcePoolConfig{
		Name:      "respool-1",
		Parent:    &rootID,
		Resources: suite.getResourceConfig(),
		Policy:    policy,
	}
	suite.respool, _ = respool.NewRespool(tally.NoopScope, "respool-1",
		nil, respoolConfig, rc.PreemptionConfig{Enabled: true})
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
			DiskLimitMb: 9,
			GpuLimit:    0,
			MemLimitMb:  100,
		},
		Preemptible: true,
	}
}

func (suite *RankerTestSuite) addTasks() {
	// Add 13 tasks to tracker(3 READY, 6 RUNNING, 1 PENDING, 3 PLACING)
	// 3 READY with different priorities
	for i := 0; i < 3; i++ {
		suite.addTaskToTracker(suite.createTask(i, uint32(i)))
		taskIDStr := fmt.Sprintf("job1-%d", i)
		taskID := &peloton.TaskID{Value: taskIDStr}
		suite.transitToReady(taskID)
	}
	// 3 RUNNING with different priorities
	for i := 3; i < 6; i++ {
		suite.addTaskToTracker(suite.createTask(i, uint32(i)))
		taskIDStr := fmt.Sprintf("job1-%d", i)
		taskID := &peloton.TaskID{Value: taskIDStr}
		suite.transitToRunning(taskID)
	}
	// 3 RUNNING with same priority and different start times
	priority := uint32(6)
	for i := 6; i < 9; i++ {
		suite.addTaskToTracker(suite.createTask(i, priority))
		taskIDStr := fmt.Sprintf("job1-%d", i)
		taskID := &peloton.TaskID{Value: taskIDStr}
		suite.transitToRunning(taskID)
		rmTask := suite.tracker.GetTask(taskID)
		rmTask.RunTimeStats().StartTime = time.Now()
		time.Sleep(1 * time.Second)
	}
	// 1 PENDING Tasks which should be ignored
	suite.addTaskToTracker(suite.createTask(9, 1))
	taskIDStr := fmt.Sprintf("job1-%d", 9)
	taskID := &peloton.TaskID{Value: taskIDStr}
	suite.transitToPending(taskID)

	// 3 PLACING with different priorities
	for i := 10; i < 13; i++ {
		suite.addTaskToTracker(suite.createTask(i, uint32(i)))
		taskIDStr := fmt.Sprintf("job1-%d", i)
		taskID := &peloton.TaskID{Value: taskIDStr}
		suite.transitToPlacing(taskID)
	}
}

func (suite *RankerTestSuite) transitToPending(taskID *peloton.TaskID) {
	rmTask := suite.tracker.GetTask(taskID)
	tasktestutil.ValidateStateTransitions(rmTask,
		[]task.TaskState{task.TaskState_PENDING})
}

func (suite *RankerTestSuite) transitToReady(taskID *peloton.TaskID) {
	rmTask := suite.tracker.GetTask(taskID)
	tasktestutil.ValidateStateTransitions(rmTask,
		[]task.TaskState{task.TaskState_PENDING, task.TaskState_READY})
}

func (suite *RankerTestSuite) transitToRunning(taskID *peloton.TaskID) {
	rmTask := suite.tracker.GetTask(taskID)
	tasktestutil.ValidateStateTransitions(rmTask,
		[]task.TaskState{
			task.TaskState_PENDING,
			task.TaskState_PLACED,
			task.TaskState_LAUNCHING,
			task.TaskState_RUNNING,
		})
}

func (suite *RankerTestSuite) transitToPlacing(taskID *peloton.TaskID) {
	rmTask := suite.tracker.GetTask(taskID)
	suite.NotNil(rmTask)
	tasktestutil.ValidateStateTransitions(rmTask,
		[]task.TaskState{
			task.TaskState_PENDING,
			task.TaskState_READY,
			task.TaskState_PLACING,
		})
}

func (suite *RankerTestSuite) TestStatePriorityRuntimeRanker_GetTasksToEvict() {
	suite.addTasks()

	ranker := newStatePriorityRuntimeRanker(suite.tracker)
	tasksToEvict := ranker.GetTasksToEvict(
		"respool-1",
		scalar.ZeroResource,
		&scalar.Resources{
			CPU:    10,
			MEMORY: 1000,
			GPU:    0,
			DISK:   100,
		})
	suite.Equal(12, len(tasksToEvict))

	expectedTasks := []string{
		// READY TASKS sorted by priority
		"job1-0",
		"job1-1",
		"job1-2",

		// PLACING tasks sorted by priority
		"job1-10",
		"job1-11",
		"job1-12",

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

func (suite *RankerTestSuite) TestStatePriorityRuntimeRanker_FilterTasks() {
	// create CPU tasks
	var tasks []*rm_task.RMTask
	for i := 0; i < 3; i++ {
		taskIDStr := fmt.Sprintf("job1-%d", i)
		task := suite.createTask(i, 1)
		suite.addTaskToTracker(task)
		tasks = append(tasks, suite.tracker.GetTask(&peloton.TaskID{Value: taskIDStr}))
	}

	// create GPU task
	task := suite.createTask(3, 2)
	gpuTaskIDStr := fmt.Sprintf("job1-%d", 3)
	task.Resource.GpuLimit = 3
	suite.addTaskToTracker(task)
	tasks = append(tasks, suite.tracker.GetTask(&peloton.TaskID{Value: gpuTaskIDStr}))

	// Create CPU task
	task = suite.createTask(4, 3)
	taskIDStr := fmt.Sprintf("job1-%d", 3)
	suite.addTaskToTracker(task)
	tasks = append(tasks, suite.tracker.GetTask(&peloton.TaskID{Value: taskIDStr}))

	// Only GPU is required to be freed
	resourceLimit := &scalar.Resources{
		CPU:    0,
		GPU:    3,
		MEMORY: 0,
		DISK:   0,
	}

	// should only contain the GPU task
	tasksToEvict := filterTasks(resourceLimit, tasks)
	suite.Equal(1, len(tasksToEvict))
	suite.Equal(gpuTaskIDStr, tasksToEvict[0].Task().GetId().Value)
}

func (suite *RankerTestSuite) TestStatePriorityRuntimeRanker_GetTasksToEvictLimitResource() {
	suite.addTasks()

	ranker := newStatePriorityRuntimeRanker(suite.tracker)
	tasksToEvict := ranker.GetTasksToEvict(
		"respool-1",
		scalar.ZeroResource,
		&scalar.Resources{
			CPU:    5.5,
			MEMORY: 550,
			GPU:    0,
			DISK:   55,
		})

	// should only contain 7 tasks since resource limit is met by those tasks
	suite.Equal(7, len(tasksToEvict))
	expectedTasks := []string{
		// READY TASKS sorted by priority
		"job1-0",
		"job1-1",
		"job1-2",

		// PLACING tasks sorted by priority
		"job1-10",
		"job1-11",
		"job1-12",

		// RUNNING tasks sorted by priority
		"job1-3",
	}

	for i, taskToEvict := range tasksToEvict {
		suite.Equal(expectedTasks[i], taskToEvict.Task().GetId().Value)
	}
}

func (suite *RankerTestSuite) addTaskWithID(
	tid string,
	jid string,
	preemptible bool) {
	suite.addTaskToTracker(&resmgr.Task{
		Name:     tid,
		Priority: 0,
		JobId:    &peloton.JobID{Value: jid},
		Id:       &peloton.TaskID{Value: tid},
		Hostname: "hostname",
		Resource: &task.ResourceConfig{
			CpuLimit:    1,
			DiskLimitMb: 9,
			GpuLimit:    0,
			MemLimitMb:  100,
		},
		Preemptible: preemptible,
	})
}

func (suite *RankerTestSuite) addRevocableTaskWithID(
	tid string,
	jid string) {
	suite.addTaskToTracker(&resmgr.Task{
		Name:     tid,
		Priority: 0,
		JobId:    &peloton.JobID{Value: jid},
		Id:       &peloton.TaskID{Value: tid},
		Hostname: "hostname",
		Resource: &task.ResourceConfig{
			CpuLimit:    1,
			DiskLimitMb: 9,
			GpuLimit:    0,
			MemLimitMb:  100,
		},
		Preemptible: true,
		Revocable:   true,
	})
}

func (suite *RankerTestSuite) TestRankerForRevocableTasks() {
	// Add three non-revocable + non-preemptible tasks
	suite.addTaskWithID("j3-t1", "j3", false)
	suite.transitToRunning(&peloton.TaskID{Value: "j3-t1"})
	suite.addTaskWithID("j3-t2", "j3", false)
	suite.transitToRunning(&peloton.TaskID{Value: "j3-t2"})
	suite.addTaskWithID("j3-t3", "j3", false)
	suite.transitToRunning(&peloton.TaskID{Value: "j3-t3"})

	// Add three revocable tasks
	suite.addRevocableTaskWithID("j2-t1", "j2")
	suite.transitToReady(&peloton.TaskID{Value: "j2-t1"})
	suite.addRevocableTaskWithID("j2-t2", "j2")
	suite.transitToPlacing(&peloton.TaskID{Value: "j2-t2"})
	suite.addRevocableTaskWithID("j2-t3", "j2")
	suite.transitToRunning(&peloton.TaskID{Value: "j2-t3"})

	// Add three non-revocable + preemptible tasks
	suite.addTaskWithID("j1-t1", "j1", true)
	suite.transitToReady(&peloton.TaskID{Value: "j1-t1"})
	suite.addTaskWithID("j1-t2", "j1", true)
	suite.transitToPlacing(&peloton.TaskID{Value: "j1-t2"})
	suite.addTaskWithID("j1-t3", "j1", true)
	suite.transitToRunning(&peloton.TaskID{Value: "j1-t3"})

	// tasks to evict have revocable tasks before non-revocable tasks
	ranker := newStatePriorityRuntimeRanker(suite.tracker)
	tasksToEvict := ranker.GetTasksToEvict(
		"respool-1",
		&scalar.Resources{
			CPU:    3,
			MEMORY: 300,
			GPU:    0,
			DISK:   25,
		},
		&scalar.Resources{
			CPU:    2.5,
			MEMORY: 250,
			GPU:    0,
			DISK:   25,
		})
	suite.Equal(len(tasksToEvict), 6)
	suite.Equal(tasksToEvict[0].Task().GetId().GetValue(), "j2-t1") // revocable + ready
	suite.Equal(tasksToEvict[1].Task().GetId().GetValue(), "j2-t2") // revocable + placing
	suite.Equal(tasksToEvict[2].Task().GetId().GetValue(), "j2-t3") // revocable + running
	suite.Equal(tasksToEvict[3].Task().GetId().GetValue(), "j1-t1") // non-revocable + ready
	suite.Equal(tasksToEvict[4].Task().GetId().GetValue(), "j1-t2") // non-revocable + placing
	suite.Equal(tasksToEvict[5].Task().GetId().GetValue(), "j1-t3") // non-revocable + running
}

func (suite *RankerTestSuite) TestStatePriorityRuntimeRanker_FilterNonPreemptible() {
	tt := []struct {
		tid         string
		jid         string
		preemptible bool
	}{
		{
			tid:         "job1-1",
			jid:         "job1",
			preemptible: true,
		},
		{
			tid:         "job2-1",
			jid:         "job2",
			preemptible: false,
		},
	}

	for _, t := range tt {
		suite.addTaskWithID(t.tid, t.jid, t.preemptible)
		suite.transitToRunning(&peloton.TaskID{Value: t.tid})

		ranker := newStatePriorityRuntimeRanker(suite.tracker)
		tasksToEvict := ranker.GetTasksToEvict("respool-1",
			scalar.ZeroResource,
			&scalar.Resources{
				CPU:    5.5,
				MEMORY: 550,
				GPU:    0,
				DISK:   55,
			})

		// should always contain just the preemptible task(s)s
		suite.Equal(1, len(tasksToEvict))
		for _, taskToEvict := range tasksToEvict {
			suite.Equal("job1-1", taskToEvict.Task().GetId().Value)
		}
	}
}
