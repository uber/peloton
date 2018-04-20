package task

import (
	"sync"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/resmgr/scalar"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// Tracker is the interface for resource manager to
// track all the tasks
type Tracker interface {

	// AddTask adds the task to state machine
	AddTask(
		t *resmgr.Task,
		handler *eventstream.Handler,
		respool respool.ResPool,
		config *Config) error

	// GetTask gets the RM task for taskID
	GetTask(t *peloton.TaskID) *RMTask

	// Sets the hostname where the task is placed.
	SetPlacement(t *peloton.TaskID, hostname string)

	// SetPlacementHost Sets the hostname for the placement
	SetPlacementHost(placement *resmgr.Placement, hostname string)

	// DeleteTask deletes the task from the map
	DeleteTask(t *peloton.TaskID)

	// MarkItDone marks the task done and add back those
	// resources to respool
	MarkItDone(taskID *peloton.TaskID, mesosTaskID string) error

	// MarkItInvalid marks the task done and invalidate them
	// in to respool by that they can be removed from the queue
	MarkItInvalid(taskID *peloton.TaskID, mesosTaskID string) error

	// TasksByHosts returns all tasks of the given type running on the given hosts.
	TasksByHosts(hosts []string, taskType resmgr.TaskType) map[string][]*RMTask

	// AddResources adds the task resources to respool
	AddResources(taskID *peloton.TaskID) error

	// GetSize returns the number of the tasks in tracker
	GetSize() int64

	// Clear cleans the tracker with all the tasks
	Clear()

	// GetActiveTasks returns task states map
	GetActiveTasks(jobID string, respoolID string, states []string) map[string][]*RMTask

	// UpdateCounters updates the counters for each state
	UpdateCounters(from string, to string)
}

// tracker is the rmtask tracker
// map[taskid]*rmtask
type tracker struct {
	sync.Mutex

	// TODO: After go 1.9 we need to use
	// https://golang.org/doc/go1.9#sync-map
	// Maps task id -> rm task
	tasks map[string]*RMTask

	// Maps hostname -> task type -> task id -> rm task
	placements map[string]map[resmgr.TaskType]map[string]*RMTask

	metrics *Metrics

	counters map[string]float64
}

// singleton object
var rmtracker *tracker

// InitTaskTracker initialize the task tracker
func InitTaskTracker(parent tally.Scope) {
	if rmtracker != nil {
		log.Info("Resource Manager Tracker is already initialized")
		return
	}
	rmtracker = &tracker{
		tasks:      make(map[string]*RMTask),
		placements: map[string]map[resmgr.TaskType]map[string]*RMTask{},
		metrics:    NewMetrics(parent.SubScope("tracker")),
		counters:   make(map[string]float64),
	}
	log.Info("Resource Manager Tracker is initialized")
}

// GetTracker gets the singleton object of the tracker
func GetTracker() Tracker {
	if rmtracker == nil {
		log.Fatal("Tracker is not initialized")
	}
	return rmtracker
}

// AddTask adds task to resmgr task tracker
func (tr *tracker) AddTask(
	t *resmgr.Task,
	handler *eventstream.Handler,
	respool respool.ResPool,
	config *Config) error {
	tr.Lock()
	defer tr.Unlock()
	rmTask, err := CreateRMTask(t, handler, respool, config)
	if err != nil {
		return err
	}
	tr.tasks[rmTask.task.Id.Value] = rmTask
	if rmTask.task.Hostname != "" {
		tr.setPlacement(rmTask.task.Id, rmTask.task.Hostname)
	}
	tr.metrics.TaskLeninTracker.Update(float64(tr.GetSize()))
	return nil
}

// GetTask gets the RM task for taskID
// this locks the tracker and get the Task
func (tr *tracker) GetTask(t *peloton.TaskID) *RMTask {
	tr.Lock()
	defer tr.Unlock()
	return tr.getTask(t)
}

// getTask gets the RM task for taskID
// this method is not protected, we need to lock tracker
// before we use this
func (tr *tracker) getTask(t *peloton.TaskID) *RMTask {
	if rmTask, ok := tr.tasks[t.Value]; ok {
		return rmTask
	}
	return nil
}

func (tr *tracker) setPlacement(t *peloton.TaskID, hostname string) {
	rmTask, ok := tr.tasks[t.Value]
	if !ok {
		return
	}
	tr.clearPlacement(rmTask)
	rmTask.task.Hostname = hostname
	if _, exists := tr.placements[hostname]; !exists {
		tr.placements[hostname] = map[resmgr.TaskType]map[string]*RMTask{}
	}
	if _, exists := tr.placements[hostname][rmTask.task.Type]; !exists {
		tr.placements[hostname][rmTask.task.Type] = map[string]*RMTask{}
	}
	if _, exists := tr.placements[hostname][rmTask.task.Type][t.Value]; !exists {
		tr.placements[hostname][rmTask.task.Type][t.Value] = rmTask
	}
}

// clearPlacement will remove the task from the placements map.
func (tr *tracker) clearPlacement(rmTask *RMTask) {
	if rmTask.task.Hostname == "" {
		return
	}
	delete(tr.placements[rmTask.task.Hostname][rmTask.task.Type], rmTask.task.Id.Value)
	if len(tr.placements[rmTask.task.Hostname][rmTask.task.Type]) == 0 {
		delete(tr.placements[rmTask.task.Hostname], rmTask.task.Type)
	}
	if len(tr.placements[rmTask.task.Hostname]) == 0 {
		delete(tr.placements, rmTask.task.Hostname)
	}
}

// SetPlacement will set the hostname that the task is currently placed on.
func (tr *tracker) SetPlacement(t *peloton.TaskID, hostname string) {
	tr.Lock()
	defer tr.Unlock()
	tr.setPlacement(t, hostname)
}

// SetPlacementHost will set the hostname that the task is currently placed on.
func (tr *tracker) SetPlacementHost(placement *resmgr.Placement, hostname string) {
	tr.Lock()
	defer tr.Unlock()
	for _, t := range placement.GetTasks() {
		tr.setPlacement(t, hostname)
	}
}

// DeleteTask deletes the task from the map after
// locking the tracker , this is interface call
func (tr *tracker) DeleteTask(t *peloton.TaskID) {
	tr.Lock()
	defer tr.Unlock()
	tr.deleteTask(t)
}

// deleteTask deletes the task from the map
// this method is not protected, we need to lock tracker
// before we use this
func (tr *tracker) deleteTask(t *peloton.TaskID) {
	if rmTask, exists := tr.tasks[t.Value]; exists {
		tr.clearPlacement(rmTask)
	}
	delete(tr.tasks, t.Value)
	tr.metrics.TaskLeninTracker.Update(float64(tr.GetSize()))
}

// MarkItDone updates the resources in resmgr and removes the task
// from the tracker
func (tr *tracker) MarkItDone(
	tID *peloton.TaskID,
	mesosTaskID string) error {
	tr.Lock()
	defer tr.Unlock()
	t := tr.getTask(tID)
	if t == nil {
		return errors.Errorf("task %s is not in tracker", tID)
	}

	// Checking mesos ID again if thats not changed
	if *t.Task().TaskId.Value != mesosTaskID {
		return errors.Errorf("for task %s: mesos id %s in tracker is different id %s from event",
			tID.Value, *t.Task().TaskId.Value, mesosTaskID)
	}

	// We need to skip the tasks from resource counting which are in pending and
	// and initialized state
	if !(t.GetCurrentState().String() == task.TaskState_PENDING.String() ||
		t.GetCurrentState().String() == task.TaskState_INITIALIZED.String()) {
		err := t.respool.SubtractFromAllocation(scalar.GetTaskAllocation(t.Task()))
		if err != nil {
			return errors.Errorf("failed update task %s ", tID)
		}
	}

	// stop the state machine
	t.StateMachine().Terminate()

	log.WithField("task_id", tID.Value).Info("Deleting the task from Tracker")
	tr.deleteTask(tID)
	return nil
}

// MarkItInvalid marks the task done and invalidate them
// in to respool by that they can be removed from the queue
func (tr *tracker) MarkItInvalid(tID *peloton.TaskID, mesosTaskID string) error {
	t := tr.GetTask(tID)
	if t == nil {
		return errors.Errorf("task %s is not in tracker", tID)
	}
	err := tr.MarkItDone(tID, mesosTaskID)
	if err != nil {
		return err
	}
	// We only need to invalidate tasks if they are in PENDING or
	// INITIALIZED STATE as Pending queue only will have these tasks
	if t.GetCurrentState() == task.TaskState_PENDING ||
		t.GetCurrentState() == task.TaskState_INITIALIZED {
		t.respool.AddInvalidTask(tID)
	}
	return nil
}

// TasksByHosts returns all tasks of the given type running on the given hosts.
func (tr *tracker) TasksByHosts(hosts []string, taskType resmgr.TaskType) map[string][]*RMTask {
	result := map[string][]*RMTask{}
	var types []resmgr.TaskType
	if taskType == resmgr.TaskType_UNKNOWN {
		for t := range resmgr.TaskType_name {
			types = append(types, resmgr.TaskType(t))
		}
	} else {
		types = append(types, taskType)
	}
	for _, hostname := range hosts {
		for _, tType := range types {
			for _, rmTask := range tr.placements[hostname][tType] {
				result[hostname] = append(result[hostname], rmTask)
			}
		}
	}
	return result
}

// AddResources adds the task resources to respool
func (tr *tracker) AddResources(
	tID *peloton.TaskID) error {
	rmTask := tr.GetTask(tID)
	if rmTask == nil {
		return errors.Errorf("rmTask %s is not in tracker", tID)
	}
	res := scalar.ConvertToResmgrResource(rmTask.Task().GetResource())
	err := rmTask.respool.AddToAllocation(scalar.GetTaskAllocation(rmTask.Task()))
	if err != nil {
		return errors.Errorf("Not able to add resources for "+
			"rmTask %s for respool %s ", tID, rmTask.respool.Name())
	}
	log.WithFields(log.Fields{
		"respool_id": rmTask.respool.ID(),
		"resources":  res,
	}).Debug("Added resources to Respool")
	return nil
}

// GetSize gets the number of tasks in tracker
func (tr *tracker) GetSize() int64 {
	return int64(len(tr.tasks))
}

// Clear cleans the tracker with all the existing tasks
func (tr *tracker) Clear() {
	tr.Lock()
	defer tr.Unlock()
	// Cleaning the tasks
	for k := range tr.tasks {
		delete(tr.tasks, k)
	}
	// Cleaning the placements
	for k := range tr.placements {
		delete(tr.placements, k)
	}
}

func (tr *tracker) isStateInRequest(state string, reqStates []string) bool {
	for _, req := range reqStates {
		if state == req {
			return true
		}
	}
	return false
}

// GetActiveTasks returns task to states map, if jobID or respoolID is provided,
// only tasks for that job or respool will be returned
func (tr *tracker) GetActiveTasks(jobID string, respoolID string, states []string) map[string][]*RMTask {
	tr.Lock()
	defer tr.Unlock()
	taskStates := make(map[string][]*RMTask)

	for _, t := range tr.tasks {
		taskState := t.GetCurrentState().String()
		if jobID != "" || respoolID != "" || len(states) != 0 {
			if jobID != "" && t.Task().GetJobId().GetValue() != jobID {
				continue
			}

			if respoolID != "" && t.Respool().ID() != respoolID {
				continue
			}

			if len(states) > 0 && !tr.isStateInRequest(taskState, states) {
				continue
			}
		}
		if _, ok := taskStates[taskState]; !ok {
			taskStates[taskState] = []*RMTask{}
		}
		taskStates[taskState] = append(taskStates[taskState], t)
	}
	return taskStates
}

// UpdateCounters updates the counters for each state
func (tr *tracker) UpdateCounters(from string, to string) {
	tr.Lock()
	defer tr.Unlock()
	// Reducing the count from state
	if val, ok := tr.counters[from]; ok {
		if val > 0 {
			tr.counters[from] = val - 1
		}
	}
	// Incrementing the state counter to +1
	if val, ok := tr.counters[to]; ok {
		tr.counters[to] = val + 1
	} else {
		tr.counters[to] = 1
	}
	// publishing the counters
	tr.publishCounters()
}

// publishes the counters for all task states
func (tr *tracker) publishCounters() {
	for state, counter := range tr.counters {
		switch state {
		case task.TaskState_PENDING.String():
			tr.metrics.pendingTasks.Update(counter)
		case task.TaskState_READY.String():
			tr.metrics.readyTasks.Update(counter)
		case task.TaskState_PLACING.String():
			tr.metrics.placingTasks.Update(counter)
		case task.TaskState_PLACED.String():
			tr.metrics.placedTasks.Update(counter)
		case task.TaskState_LAUNCHING.String():
			tr.metrics.launchingTasks.Update(counter)
		case task.TaskState_LAUNCHED.String():
			tr.metrics.launchedTasks.Update(counter)
		case task.TaskState_RUNNING.String():
			tr.metrics.runningTasks.Update(counter)
		case task.TaskState_SUCCEEDED.String():
			tr.metrics.succeededTasks.Update(counter)
		case task.TaskState_FAILED.String():
			tr.metrics.failedTasks.Update(counter)
		case task.TaskState_LOST.String():
			tr.metrics.lostTasks.Update(counter)
		case task.TaskState_KILLED.String():
			tr.metrics.killedTasks.Update(counter)
		case task.TaskState_PREEMPTING.String():
			tr.metrics.preemptingTasks.Update(counter)
		}
	}
}
