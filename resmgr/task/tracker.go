package task

import (
	"sync"

	log "github.com/Sirupsen/logrus"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/resmgr/scalar"
	"github.com/pkg/errors"
	"github.com/uber-go/tally"
)

// Tracker is the interface for resource manager to
// track all the tasks in rm
type Tracker interface {

	// AddTask adds the task to state machine
	AddTask(
		t *resmgr.Task,
		handler *eventstream.Handler,
		respool respool.ResPool) error

	// GetTask gets the RM task for taskID
	GetTask(t *peloton.TaskID) *RMTask

	// DeleteTask deletes the task from the map
	DeleteTask(t *peloton.TaskID)

	// MarkItDone marks the task done and add back those
	// resources to respool
	MarkItDone(taskID *peloton.TaskID) error

	// AddResources adds the task resources to respool
	AddResources(taskID *peloton.TaskID) error

	// GetSize returns the number of the tasks in tracker
	GetSize() int64

	// Clear cleans the tracker with all the tasks
	Clear()
}

// tracker is the rmtask tracker
// map[taskid]*rmtask
type tracker struct {
	sync.Mutex

	tasks   map[string]*RMTask
	metrics *Metrics
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
		tasks:   make(map[string]*RMTask),
		metrics: NewMetrics(parent.SubScope("tracker")),
	}
}

// GetTracker gets the singelton object of the tracker
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
	respool respool.ResPool) error {
	tr.Lock()
	defer tr.Unlock()
	rmTask, err := CreateRMTask(t, handler, respool)
	if err != nil {
		return err
	}
	tr.tasks[rmTask.task.Id.Value] = rmTask
	tr.metrics.TaskLeninTracker.Update(float64(tr.GetSize()))
	return nil
}

// GetTask gets the RM task for taskID
func (tr *tracker) GetTask(t *peloton.TaskID) *RMTask {
	tr.Lock()
	defer tr.Unlock()
	if rmTask, ok := tr.tasks[t.Value]; ok {
		return rmTask
	}
	return nil
}

// DeleteTask deletes the task from the map
func (tr *tracker) DeleteTask(t *peloton.TaskID) {
	tr.Lock()
	defer tr.Unlock()
	delete(tr.tasks, t.Value)
	tr.metrics.TaskLeninTracker.Update(float64(tr.GetSize()))
}

// MarkItDone updates the resources in resmgr
func (tr *tracker) MarkItDone(
	tID *peloton.TaskID) error {
	task := tr.GetTask(tID)
	if task == nil {
		return errors.Errorf("task %s is not in tracker", tID)
	}
	err := task.respool.MarkItDone(
		scalar.ConvertToResmgrResource(
			task.task.GetResource()))
	if err != nil {
		return errors.Errorf("Not able to update task %s ", tID)
	}
	log.WithField("Task", tID.Value).Info("Deleting the task from Tracker")
	tr.DeleteTask(tID)
	return nil
}

// AddResources adds the task resources to respool
func (tr *tracker) AddResources(
	tID *peloton.TaskID) error {
	task := tr.GetTask(tID)
	if task == nil {
		return errors.Errorf("task %s is not in tracker", tID)
	}
	res := scalar.ConvertToResmgrResource(task.task.GetResource())
	err := task.respool.AddToAllocation(res)
	if err != nil {
		return errors.Errorf("Not able to add resources for "+
			"task %s for respool %s ", tID, task.respool.Name())
	}
	log.WithFields(log.Fields{
		"Respool":   task.respool.Name(),
		"Resources": res,
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
	for k := range tr.tasks {
		delete(tr.tasks, k)
	}
}
