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
	"sort"

	"github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/uber/peloton/pkg/resmgr/scalar"
	rm_task "github.com/uber/peloton/pkg/resmgr/task"

	log "github.com/sirupsen/logrus"
)

// Represents the task states in the order in which they should be
// evaluated for preemption.
// For each task revocable tasks are first selected then
// preemptible non-revocable tasks
var taskStatesPreemptionOrder = []task.TaskState{
	task.TaskState_READY,
	task.TaskState_PLACING,
	task.TaskState_RUNNING,
}

// ranker sorts revocable tasks first for eviction to satisfy slackResourcesToFree and
// then sort non-revocable tasks to satisfy nonSlackResourcesToFree
type ranker interface {
	GetTasksToEvict(
		respoolID string,
		slackResourcesToFree,
		nonSlackResourcesToFree *scalar.Resources) []*rm_task.RMTask
}

// statePriorityRuntimeRanker sorts the tasks in the following order
// * Task State : READY > PLACING > RUNNING
// * If task state is the same it sorts on the task Priority
// * If the priority is the same it sorts on the task runtime(how long the task has been running)
type statePriorityRuntimeRanker struct {
	tracker rm_task.Tracker
	sorter  taskSorter
}

// newStatePriorityRuntimeRanker returns a new instance of the statePriorityRuntimeRanker
func newStatePriorityRuntimeRanker(tracker rm_task.Tracker) ranker {
	return &statePriorityRuntimeRanker{
		tracker: tracker,
		sorter: taskSorter{
			cmpFuncs: []cmpFunc{
				priorityCmp,
				startTimeCmp,
			},
		},
	}
}

// GetTasksToEvict returns the tasks in the order in which they should be evicted from
// the resource pool such that the cumulative resources of those tasks >= requiredResources
func (r *statePriorityRuntimeRanker) GetTasksToEvict(
	respoolID string,
	slackResourcesToFree, nonSlackResourcesToFree *scalar.Resources) []*rm_task.RMTask {

	// get all active tasks for this resource pool
	stateTaskMap := r.tracker.GetActiveTasks("", respoolID, nil)

	// get revocable tasks to preempt and filter on slack resources to free
	revocableTasks := r.rankAllRevocableTasks(stateTaskMap)
	revocableTasksToEvict := filterTasks(slackResourcesToFree, revocableTasks)

	// get non-revocable preemptible tasks to preempt and filter on
	// non-slack resources to free
	nonRevocTasks := r.rankAllNonRevocableTasks(stateTaskMap)
	nonRevocTasksToEvict := filterTasks(nonSlackResourcesToFree, nonRevocTasks)
	return append(revocableTasksToEvict, nonRevocTasksToEvict...)
}

// rankAllRevocableTasks returns a ranked list of revocable tasks
// in which order they will be preempted to free up slack resources
func (r *statePriorityRuntimeRanker) rankAllRevocableTasks(
	stateTaskMap map[string][]*rm_task.RMTask) []*rm_task.RMTask {
	var allTasks []*rm_task.RMTask
	for _, taskState := range taskStatesPreemptionOrder {
		var tasksInState []*rm_task.RMTask
		// tracker contains *all*(revocable + preemptible + non-preemptible) tasks,
		// so we need to filer out the non-revocable tasks before ranking
		// them.
		tasksInState = filterRevocableTasks(stateTaskMap[taskState.String()])

		r.sorter.Sort(tasksInState)
		allTasks = append(allTasks, tasksInState...)
	}
	return allTasks
}

// rankAllNonRevocableTasks retuns a ranked order/list of non-revocable preemptible
// tasks in which order they will be preempted to free up non-slack resources
func (r *statePriorityRuntimeRanker) rankAllNonRevocableTasks(
	stateTaskMap map[string][]*rm_task.RMTask) []*rm_task.RMTask {
	var allTasks []*rm_task.RMTask
	for _, taskState := range taskStatesPreemptionOrder {
		var tasksInState []*rm_task.RMTask
		// tracker contains *all*(revocable + preemptible + non-preemptible) tasks,
		// so we need to filer out the non-preemptible tasks before ranking
		// them.
		tasksInState = filterNonRevocableTasks(stateTaskMap[taskState.String()])
		r.sorter.Sort(tasksInState)
		allTasks = append(allTasks, tasksInState...)
	}
	return allTasks
}

// returns only revocable tasks
func filterRevocableTasks(
	allTasks []*rm_task.RMTask) []*rm_task.RMTask {
	var p []*rm_task.RMTask
	for _, t := range allTasks {
		if t.Task().Preemptible && t.Task().Revocable {
			p = append(p, t)
		}
	}

	return p
}

// returns only preemptible non-revocable tasks
func filterNonRevocableTasks(
	allTasks []*rm_task.RMTask) []*rm_task.RMTask {
	var p []*rm_task.RMTask
	for _, t := range allTasks {
		if t.Task().Preemptible && !t.Task().Revocable {
			p = append(p, t)
		}
	}

	return p
}

// filterTasks filters tasks which satisfy the resourcesLimit
// This method assumes the list of tasks supplied is already sorted in the preferred order
func filterTasks(
	resourcesLimit *scalar.Resources,
	allTasks []*rm_task.RMTask) []*rm_task.RMTask {
	var tasksToEvict []*rm_task.RMTask
	resourceRunningCount := scalar.ZeroResource
	for _, task := range allTasks {
		// Check how many resource we need to free
		resourceToFree := resourcesLimit.Subtract(resourceRunningCount)
		if resourceToFree.Equal(scalar.ZeroResource) {
			// we have enough tasks
			break
		}
		// get task resources
		taskResources := scalar.ConvertToResmgrResource(task.Task().Resource)

		// check if the task resource helps in satisfying resourceToFree
		newResourceToFree := resourcesLimit.Subtract(taskResources)
		if newResourceToFree.Equal(resourcesLimit) {
			// this task doesn't help with meeting the resourcesLimit
			continue
		}
		// we can add more tasks
		tasksToEvict = append(tasksToEvict, task)
		// Add the task resource to the running count
		resourceRunningCount = resourceRunningCount.Add(taskResources)
	}
	return tasksToEvict
}

// return 0  if  t1 == t2
// return <0 if  t1 < t2
// return >0 if  t1 > t2
type cmpFunc func(t1, t2 *rm_task.RMTask) int

// compares tasks based on their priority
func priorityCmp(t1, t2 *rm_task.RMTask) int {
	log.WithField("task_1_ID", t1.Task().Id.Value).
		WithField("task_2_ID", t2.Task().Id.Value).
		Debug("comparing priority of tasks")
	return int(t1.Task().GetPriority()) - int(t2.Task().GetPriority())
}

// startTimeCmp compares tasks based on their start time
func startTimeCmp(t1, t2 *rm_task.RMTask) int {
	log.WithField("task_1_ID", t1.Task().Id.Value).
		WithField("task_2_ID", t2.Task().Id.Value).
		Debug("comparing start times of tasks")

	if t1.GetCurrentState().State == task.TaskState_READY ||
		t2.GetCurrentState().State == task.TaskState_READY {
		// ready tasks don't have a start time
		return 0
	}

	t1StartTime := t1.RunTimeStats().StartTime
	t2StartTime := t2.RunTimeStats().StartTime

	if t1StartTime.After(t2StartTime) {
		// t1 started after t2 so we want to evict t1 first
		return -1
	}
	return 1
}

// taskSorter implements the Sort interface
type taskSorter struct {
	tasks    []*rm_task.RMTask
	cmpFuncs []cmpFunc
}

// Sort sorts the tasks based on the cmpFuncs
func (ts *taskSorter) Sort(tasks []*rm_task.RMTask) {
	ts.tasks = tasks
	sort.Sort(ts)
}

// Len is part of sort.Interface.
func (ts *taskSorter) Len() int {
	return len(ts.tasks)
}

// Swap is part of sort.Interface.
func (ts *taskSorter) Swap(i, j int) {
	ts.tasks[i], ts.tasks[j] = ts.tasks[j], ts.tasks[i]
}

// Less is part of sort.Interface.
func (ts *taskSorter) Less(i, j int) bool {
	t1, t2 := ts.tasks[i], ts.tasks[j]
	for _, cmp := range ts.cmpFuncs {
		if r := cmp(t1, t2); r < 0 {
			return true
		} else if r > 0 {
			return false
		}
	}
	return false
}
