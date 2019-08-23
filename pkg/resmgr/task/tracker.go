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
	"sync"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"

	"github.com/uber/peloton/pkg/common/eventstream"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/resmgr/respool"
	"github.com/uber/peloton/pkg/resmgr/scalar"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// Tracker is the interface for resource manager to
// track all the tasks
// TODO: Get rid of peloton-task-id from tracker
type Tracker interface {

	// AddTask adds the task to state machine
	AddTask(
		t *resmgr.Task,
		handler *eventstream.Handler,
		respool respool.ResPool,
		config *Config) error

	// GetTask gets the RM task for taskID
	GetTask(t *peloton.TaskID) *RMTask

	// SetPlacementHost Sets the placement for the tasks.
	SetPlacement(placement *resmgr.Placement)

	// DeleteTask deletes the task from the map
	DeleteTask(t *peloton.TaskID)

	// MarkItDone marks the task done and add back those
	// resources to respool
	MarkItDone(mesosTaskID string) error

	// MarkItInvalid marks the task done and invalidate them
	// in to respool by that they can be removed from the queue
	MarkItInvalid(mesosTaskID string) error

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

	// UpdateMetrics updates the task metrics
	UpdateMetrics(from task.TaskState, to task.TaskState, taskResources *scalar.Resources)

	// GetOrphanTask gets the orphan RMTask for the given mesos-task-id
	GetOrphanTask(mesosTaskID string) *RMTask

	// GetOrphanTasks returns orphan tasks
	GetOrphanTasks(respoolID string) []*RMTask
}

// tracker is the rmtask tracker
// map[taskid]*rmtask
// TODO: Get rid of peloton-task-id from tracker
type tracker struct {
	lock sync.RWMutex

	// Map of peloton task ID to the resource manager task
	tasks map[string]*RMTask

	// Maps hostname -> task type -> mesos task id -> rm task
	placements map[string]map[resmgr.TaskType]map[string]*RMTask

	// Map of mesos task ID to rm task
	// Orphan tasks are those whose resources are not released but
	// are no longer tracked by the tracker (since the RMTask in tracker
	// was replaced by a RMTask with new mesos task id). When we receive a
	// terminal event for a task that is not present in the tracker,
	// we use this map to release held resources, if any.
	// TODO: Move `placements` and `orphanTasks` out of tracker
	orphanTasks map[string]*RMTask

	scope   tally.Scope
	metrics *Metrics

	// mutex for the task state metrics
	metricsLock sync.Mutex

	// map of task state to the count of tasks in the tracker
	counters map[task.TaskState]float64

	// map of task state to the resources held
	resourcesHeldByTaskState map[task.TaskState]*scalar.Resources

	// host manager client
	hostMgrClient hostsvc.InternalHostServiceYARPCClient
}

// singleton object
var rmtracker *tracker

// InitTaskTracker initialize the task tracker
func InitTaskTracker(
	parent tally.Scope,
	config *Config) {
	if rmtracker != nil {
		log.Info("Resource Manager Tracker is already initialized")
		return
	}

	scope := parent.SubScope("tracker")
	rmtracker = &tracker{
		tasks:                    make(map[string]*RMTask),
		placements:               map[string]map[resmgr.TaskType]map[string]*RMTask{},
		orphanTasks:              make(map[string]*RMTask),
		metrics:                  NewMetrics(scope),
		scope:                    scope,
		counters:                 make(map[task.TaskState]float64),
		resourcesHeldByTaskState: make(map[task.TaskState]*scalar.Resources),
	}

	// Initialize resources held by each non-terminal task state to zero resource.
	for s := range task.TaskState_name {
		taskState := task.TaskState(s)
		// skip terminal states
		if !util.IsPelotonStateTerminal(taskState) {
			rmtracker.resourcesHeldByTaskState[taskState] = scalar.ZeroResource
		}
	}

	// Checking placement back off is enabled , if yes then initialize
	// policy factory. Explicitly checking, anything related to
	// back off policies should come inside this code path.
	if config.EnablePlacementBackoff {
		err := InitPolicyFactory()
		if err != nil {
			log.Error("Error initializing back off policy")
		}
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
	config *Config,
) error {

	rmTask, err := CreateRMTask(
		tr.scope.SubScope("rmtask"),
		t,
		handler,
		respool,
		config)
	if err != nil {
		return err
	}

	tr.lock.Lock()
	defer tr.lock.Unlock()

	if prevRMTask, ok := tr.tasks[rmTask.task.GetId().GetValue()]; ok &&
		prevRMTask.task.GetTaskId().GetValue() != rmTask.task.GetTaskId().GetValue() {
		// If EnqueueGangs request for a new run of the task is received
		// before the terminal event for the last run is processed by resmgr
		// mark the prev RMTask as an orphan task so that we can release
		// resources when the terminal event is processed
		tr.orphanTasks[prevRMTask.task.GetTaskId().GetValue()] = prevRMTask
		tr.metrics.OrphanTasks.Update(float64(len(tr.orphanTasks)))
	}

	tr.tasks[rmTask.task.GetId().GetValue()] = rmTask
	if rmTask.task.Hostname != "" {
		tr.setPlacement(rmTask.task.GetTaskId(), rmTask.task.GetHostname())
	}
	tr.metrics.TasksCountInTracker.Update(float64(tr.GetSize()))
	return nil
}

// GetTask gets the RM task for taskID
// this locks the tracker and get the Task
func (tr *tracker) GetTask(t *peloton.TaskID) *RMTask {
	tr.lock.RLock()
	defer tr.lock.RUnlock()
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

// setPlacement writes the host:task mapping for the given hostname and mesos-task-id
// in the placements map of the tracker. Before writing to placements map it checks
// if the task is present in the tracker. If not present, the mapping is not set
func (tr *tracker) setPlacement(t *mesos.TaskID, hostname string) {
	taskID, err := util.ParseTaskIDFromMesosTaskID(t.GetValue())
	if err != nil {
		log.WithError(err).
			Error("error while setting placement")
		return
	}

	rmTask, ok := tr.tasks[taskID]
	if !ok {
		return
	}

	rmTask.task.Hostname = hostname
	if _, exists := tr.placements[hostname]; !exists {
		tr.placements[hostname] = map[resmgr.TaskType]map[string]*RMTask{}
	}
	if _, exists := tr.placements[hostname][rmTask.task.GetType()]; !exists {
		tr.placements[hostname][rmTask.task.GetType()] = map[string]*RMTask{}
	}
	if _, exists := tr.placements[hostname][rmTask.task.GetType()][t.GetValue()]; !exists {
		tr.placements[hostname][rmTask.task.GetType()][t.GetValue()] = rmTask
	}
}

// clearPlacement will remove the task from the placements map.
func (tr *tracker) clearPlacement(
	hostname string,
	taskType resmgr.TaskType,
	mesosTaskID string,
) {
	if hostname == "" {
		return
	}

	placements := tr.placements[hostname]

	delete(placements[taskType], mesosTaskID)
	if len(placements[taskType]) == 0 {
		delete(placements, taskType)
	}

	if len(tr.placements[hostname]) == 0 {
		delete(tr.placements, hostname)
	}
}

// SetPlacementHost will set the hostname that the task is currently placed on.
func (tr *tracker) SetPlacement(placement *resmgr.Placement) {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	for _, t := range placement.GetTaskIDs() {
		tr.setPlacement(t.GetMesosTaskID(), placement.GetHostname())
	}
}

// DeleteTask deletes the task from the map after
// locking the tracker , this is interface call
func (tr *tracker) DeleteTask(t *peloton.TaskID) {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	tr.deleteTask(t)
}

// deleteTask deletes the task from the map
// this method is not protected, we need to lock tracker
// before we use this.
func (tr *tracker) deleteTask(t *peloton.TaskID) {
	rmTask, exists := tr.tasks[t.Value]
	if !exists {
		return
	}

	tr.clearPlacement(
		rmTask.task.GetHostname(),
		rmTask.task.GetType(),
		rmTask.task.GetTaskId().GetValue(),
	)

	delete(tr.tasks, t.Value)
	tr.metrics.TasksCountInTracker.Update(float64(tr.GetSize()))
}

// MarkItDone updates the resources in resmgr and removes the task
// from the tracker
func (tr *tracker) MarkItDone(
	mesosTaskID string) error {
	tr.lock.Lock()
	defer tr.lock.Unlock()

	return tr.markItDone(mesosTaskID)
}

// MarkItInvalid marks the task done and invalidate them
// in to respool by that they can be removed from the queue
func (tr *tracker) MarkItInvalid(mesosTaskID string) error {
	tr.lock.Lock()
	defer tr.lock.Unlock()

	taskID, err := util.ParseTaskIDFromMesosTaskID(mesosTaskID)
	if err != nil {
		return err
	}

	tID := &peloton.TaskID{Value: taskID}
	t := tr.getTask(tID)

	// remove from the tracker
	if err = tr.markItDone(mesosTaskID); err != nil {
		return err
	}

	if t == nil {
		return nil
	}

	switch t.GetCurrentState().State {
	case task.TaskState_PENDING, task.TaskState_INITIALIZED:
		// If task is in INITIALIZED or PENDING state we need to invalidate
		// it from in pending queue
		t.respool.AddInvalidTask(tID)
	case task.TaskState_READY:
		// If task is in READY state we need to invalidate
		// it from in ready queue
		GetScheduler().AddInvalidTask(tID)
	}

	return nil
}

// tracker needs to be locked before calling this.
func (tr *tracker) markItDone(mesosTaskID string) error {
	taskID, err := util.ParseTaskIDFromMesosTaskID(mesosTaskID)
	if err != nil {
		return err
	}

	tID := &peloton.TaskID{Value: taskID}
	t := tr.getTask(tID)

	if t == nil || t.Task().GetTaskId().GetValue() != mesosTaskID {
		// If task is not in tracker or if the mesos ID has changed, clear the
		// placement and free up the held resources.
		// 1. `task not in tracker` - This can happen if resource manager receives
		//     the task event for an older run after the latest run of the task
		//     has been cleaned up from tracker.
		// 2. `mesos ID has changed` - This can happen when jobmgr processes the
		//     mesos event faster than resmgr causing EnqueueGangs to be called
		//     before the task termination event is processed by resmgr.
		return tr.deleteOrphanTask(mesosTaskID)
	}

	// We need to skip the tasks from resource counting which are in pending and
	// and initialized state
	if !(t.GetCurrentState().State == task.TaskState_PENDING ||
		t.GetCurrentState().State == task.TaskState_INITIALIZED) {
		err := t.respool.SubtractFromAllocation(scalar.GetTaskAllocation(t.Task()))
		if err != nil {
			return errors.Errorf("failed update task %s ", taskID)
		}

		tr.metricsLock.Lock()
		defer tr.metricsLock.Unlock()

		// update metrics
		taskState := t.GetCurrentState().State
		if val, ok := tr.resourcesHeldByTaskState[taskState]; ok {
			tr.resourcesHeldByTaskState[taskState] = val.Subtract(
				scalar.ConvertToResmgrResource(t.task.GetResource()),
			)
		}

		// publish metrics
		if gauge, ok := tr.metrics.ResourcesHeldByTaskState[taskState]; ok {
			gauge.Update(tr.resourcesHeldByTaskState[taskState])
		}
	}

	// terminate the rm task
	t.Terminate()

	log.WithField("task_id", t.Task().GetTaskId().GetValue()).
		Info("Deleting the task from Tracker")
	tr.deleteTask(tID)
	return nil
}

// TasksByHosts returns all tasks of the given type running on the given hosts.
func (tr *tracker) TasksByHosts(hosts []string, taskType resmgr.TaskType) map[string][]*RMTask {
	tr.lock.RLock()
	defer tr.lock.RUnlock()

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

	tr.metricsLock.Lock()
	defer tr.metricsLock.Unlock()

	taskState := rmTask.GetCurrentState().State
	if val, ok := tr.resourcesHeldByTaskState[taskState]; ok {
		tr.resourcesHeldByTaskState[taskState] = val.Add(res)
	}

	// publish metrics
	if gauge, ok := tr.metrics.ResourcesHeldByTaskState[taskState]; ok {
		gauge.Update(tr.resourcesHeldByTaskState[taskState])
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
	tr.lock.Lock()
	defer tr.lock.Unlock()

	// Clearing the tasks
	for k := range tr.tasks {
		delete(tr.tasks, k)
	}
	// Clearing the placements
	for k := range tr.placements {
		delete(tr.placements, k)
	}

	// Clearing the orphan tasks
	for k := range tr.orphanTasks {
		delete(tr.orphanTasks, k)
	}
	// publish metrics
	tr.metrics.OrphanTasks.Update(float64(len(tr.orphanTasks)))
}

// GetActiveTasks returns task to states map, if jobID or respoolID is provided,
// only tasks for that job or respool will be returned
func (tr *tracker) GetActiveTasks(
	jobID string,
	respoolID string,
	states []string) map[string][]*RMTask {
	tr.lock.RLock()
	defer tr.lock.RUnlock()

	taskStates := make(map[string][]*RMTask)

	for _, t := range filterTasks(tr.tasks, jobID, respoolID, states) {
		taskState := t.GetCurrentState().State.String()
		taskStates[taskState] = append(taskStates[taskState], t)
	}

	for _, t := range filterTasks(tr.orphanTasks, jobID, respoolID, states) {
		taskState := t.GetCurrentState().State.String()
		taskStates[taskState] = append(taskStates[taskState], t)
	}

	return taskStates
}

// UpdateMetrics updates the task metrics. This can be called from
// multiple goroutines.
func (tr *tracker) UpdateMetrics(
	from task.TaskState,
	to task.TaskState,
	taskResources *scalar.Resources,
) {
	tr.metricsLock.Lock()
	defer tr.metricsLock.Unlock()

	// Reducing the count from state
	tr.counters[from]--
	if tr.counters[from] < 0 {
		tr.counters[from] = 0
	}

	// Incrementing the state counter to +1
	tr.counters[to]++

	// Subtract resources from 'from' state
	if res, ok := tr.resourcesHeldByTaskState[from]; ok {
		tr.resourcesHeldByTaskState[from] = res.Subtract(taskResources)
	}

	// Add resources to 'to' state
	if res, ok := tr.resourcesHeldByTaskState[to]; ok {
		tr.resourcesHeldByTaskState[to] = res.Add(taskResources)
	}

	// publish metrics
	if gauge, ok := tr.metrics.TaskStatesGauge[from]; ok {
		gauge.Update(tr.counters[from])
	}

	if gauge, ok := tr.metrics.TaskStatesGauge[to]; ok {
		gauge.Update(tr.counters[to])
	}

	if gauge, ok := tr.metrics.ResourcesHeldByTaskState[from]; ok {
		gauge.Update(tr.resourcesHeldByTaskState[from])
	}

	if gauge, ok := tr.metrics.ResourcesHeldByTaskState[to]; ok {
		gauge.Update(tr.resourcesHeldByTaskState[to])
	}
}

// GetOrphanTask gets the orphan RMTask for the given mesos-task-id
func (tr *tracker) GetOrphanTask(mesosTaskID string) *RMTask {
	tr.lock.RLock()
	defer tr.lock.RUnlock()

	if rmTask, ok := tr.orphanTasks[mesosTaskID]; ok {
		return rmTask
	}
	return nil
}

// GetOrphanTasks returns all orphan tasks known to resource manager
func (tr *tracker) GetOrphanTasks(respoolID string) []*RMTask {
	tr.lock.RLock()
	defer tr.lock.RUnlock()

	return filterTasks(tr.orphanTasks, "", respoolID, nil)
}

// deleteOrphanTask is a helper that cleans up the task from the
// host-to-tasks map and releases the resources held by the task
func (tr *tracker) deleteOrphanTask(mesosTaskID string) error {
	rmTask, ok := tr.orphanTasks[mesosTaskID]
	if !ok {
		// If the mesos task ID is not a known orphan task then
		// it means there are no resources held for this task.
		// We can simply return here
		return nil
	}

	tr.clearPlacement(
		rmTask.task.GetHostname(),
		rmTask.task.GetType(),
		mesosTaskID,
	)

	err := rmTask.respool.SubtractFromAllocation(scalar.GetTaskAllocation(rmTask.task))
	if err != nil {
		log.WithField("mesos_task", mesosTaskID).
			WithField("resources", rmTask.task.GetResource()).
			WithError(err).
			Error("failed to release held resources for task")
		err = errors.Wrapf(err, "failed to release held resources for task %s", mesosTaskID)
	}

	delete(tr.orphanTasks, mesosTaskID)

	// update metrics
	tr.metricsLock.Lock()
	defer tr.metricsLock.Unlock()

	taskState := rmTask.GetCurrentState().State
	if val, ok := tr.resourcesHeldByTaskState[taskState]; ok {
		tr.resourcesHeldByTaskState[taskState] = val.Subtract(
			scalar.ConvertToResmgrResource(rmTask.task.GetResource()),
		)
	}

	// publish metrics
	if gauge, ok := tr.metrics.ResourcesHeldByTaskState[taskState]; ok {
		gauge.Update(tr.resourcesHeldByTaskState[taskState])
	}

	log.WithFields(log.Fields{
		"orphan_task": mesosTaskID,
	}).Debug("Orphan task deleted")

	tr.metrics.OrphanTasks.Update(float64(len(tr.orphanTasks)))
	return err
}

// filterTasks filters the tasks based on the jobID, respoolID and states filters
func filterTasks(
	tasks map[string]*RMTask,
	jobID string,
	respoolID string,
	states []string,
) []*RMTask {
	var result []*RMTask

	for _, t := range tasks {
		// filter by jobID
		if jobID != "" && t.Task().GetJobId().GetValue() != jobID {
			continue
		}

		// filter by resource pool ID
		if respoolID != "" && t.Respool().ID() != respoolID {
			continue
		}

		// filter by task states
		if len(states) > 0 &&
			!util.Contains(states, t.GetCurrentState().State.String()) {
			continue
		}

		result = append(result, t)
	}

	return result
}
