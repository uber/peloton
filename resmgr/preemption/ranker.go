package preemption

import (
	"sort"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/resmgr/scalar"
	rm_task "code.uber.internal/infra/peloton/resmgr/task"

	log "github.com/sirupsen/logrus"
)

// ranker sorts the tasks in eviction order such that the resourcesLimit is satisfied
type ranker interface {
	GetTasksToEvict(respoolID string, resourcesLimit *scalar.Resources) []*rm_task.RMTask
}

// statePriorityRuntimeRanker sorts the tasks in the following order
// * Task State : READY > RUNNING
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
			cmpFuncs: []cmpFunc{priorityCmp, startTimeCmp},
		},
	}
}

// GetTasksToEvict returns the tasks in the order in which they should be evicted from
// the resource pool such that the cumulative resources of those tasks >= resourcesLimit
func (r *statePriorityRuntimeRanker) GetTasksToEvict(respoolID string, resourcesLimit *scalar.Resources) []*rm_task.RMTask {
	stateTaskMap := r.toStateTaskMap(r.tracker.GetActiveTasks("", respoolID))

	//TODO PLACING, PLACED, LAUNCHING, LAUNCHED
	readyTasks := stateTaskMap[task.TaskState_READY.String()]
	runningTasks := stateTaskMap[task.TaskState_RUNNING.String()]

	r.sorter.Sort(readyTasks)
	r.sorter.Sort(runningTasks)

	var allTasks []*rm_task.RMTask
	allTasks = append(append(allTasks, readyTasks...), runningTasks...)

	var tasksToEvict []*rm_task.RMTask
	taskResource := scalar.ZeroResource
	for _, task := range allTasks {
		if taskResource.LessThanOrEqual(resourcesLimit) {
			// we can add more tasks
			taskResource = taskResource.Add(scalar.ConvertToResmgrResource(task.Task().Resource))
			tasksToEvict = append(tasksToEvict, task)
		} else {
			// we have enough tasks
			break
		}
	}
	return tasksToEvict
}

func (r *statePriorityRuntimeRanker) toStateTaskMap(taskToStateMap map[string]string) map[string][]*rm_task.RMTask {
	stateTaskMap := make(map[string][]*rm_task.RMTask)
	for taskeID, state := range taskToStateMap {
		if _, ok := stateTaskMap[state]; !ok {
			stateTaskMap[state] = []*rm_task.RMTask{}
		}
		rmTask := r.tracker.GetTask(&peloton.TaskID{Value: taskeID})
		stateTaskMap[state] = append(stateTaskMap[state], rmTask)
	}
	return stateTaskMap
}

// return 0  if  t1 == t2
// return <0 if  t1 < t2
// return >0 if  t1 > t2
type cmpFunc func(t1, t2 *rm_task.RMTask) int

// compares tasks based on their priority
func priorityCmp(t1, t2 *rm_task.RMTask) int {
	log.WithField("task_1_id", t1.Task().Id.Value).WithField("task_2_id", t2.Task().Id.Value).
		Debug("comparing priority of tasks")
	return int(t1.Task().GetPriority()) - int(t2.Task().GetPriority())
}

// startTimeCmp compares tasks based on their start time
func startTimeCmp(t1, t2 *rm_task.RMTask) int {
	log.WithField("task_1_id", t1.Task().Id.Value).WithField("task_2_id", t2.Task().Id.Value).
		Debug("comparing start times of tasks")

	if t1.GetCurrentState() == task.TaskState_READY ||
		t2.GetCurrentState() == task.TaskState_READY {
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
