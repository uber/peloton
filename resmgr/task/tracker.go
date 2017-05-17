package task

import (
	"sync"

	"github.com/prometheus/common/log"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/resmgr/respool"
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
}

// tracker is the rmtask tracker
// map[taskid]*rmtask
type tracker struct {
	sync.Mutex

	tasks map[string]*RMTask
}

// singleton object
var rmtracker *tracker

// InitTaskTracker initialize the task tracker
func InitTaskTracker() {

	if rmtracker != nil {
		log.Info("Resource Manager Tracker is already initialized")
		return
	}
	rmtracker = &tracker{
		tasks: make(map[string]*RMTask),
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
}
