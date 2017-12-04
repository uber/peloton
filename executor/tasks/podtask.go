package task

import (
	"errors"
	"sync"
	"time"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	agent "code.uber.internal/infra/peloton/.gen/mesos/v1/agent"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"
)

// PodTask is the core structure of the executor. It is responsible for managing and monitoring
// the lifecycle of the task and all of the containers inside the task.
// This includes healthchecks and status checks that need to be returned to the executor
// creating those tasks.
//
// The PodTask creates 1 long-running goroutine when launched. This goroutine will be performing
// the operations below when necessary, or remain sleeping:
//
// - A HealthCheck for the task as a whole, or a healthcheck as described in the PodSpec
//
// - A StatusCheck for the task (this is still a maybe).
//
// In addition, we need another goroutine for each of the nested containers that will make sure to wait
// for the container to possibly finish or fail; so that we can relaunch that container if need be.
// That goroutine will also take care of retrying the calls to WaitNestedContainer if the connection
// is closed for some reason
type PodTask struct {
	info mesos.TaskInfo // The taskinfo that this task was created with

	monitoring *atomic.Bool // Whether or not the monitoring of this task was told to be stopped
	killing    *atomic.Bool // Whether or not the task is being killed. This is used by all goroutines as a sign to return
	healthy    *atomic.Bool

	state    mesos.TaskState
	watchers map[string]chan<- mesos.TaskState // A mapping of watcher name to its channel

	Sleeper     func(time.Duration)
	MesosClient PodTaskMesosClient

	sync.Mutex
}

const (
	// StateChannelSize is the capacity of the state change channels. This needs to be larger than 1
	// since tasks can go from STARTING to RUNNING quicker than the watcher may be able to process.
	// 10 is a reasonably large number that should indicate that the watcher has stopped watching if
	// the channel gets capped out.
	StateChannelSize = 10
)

// PodTaskMesosClient is the interface needed by the podtask to talk to the mesos agent.
// It is used to spin up nested containers, wait for them, and kill them if need be.
type PodTaskMesosClient interface {
	LaunchNestedContainer(*agent.Call_LaunchNestedContainer) error
	KillNestedContainer(*agent.Call_KillNestedContainer) error

	// Returns the exit-code of the nested container as well as an error if the
	// call failed.
	WaitNestedContainer(*agent.Call_WaitNestedContainer) (int, error)
}

// Launch is called when creating the task object. It will spin up a new nested container
// and start healthchecking and status checking it.
func (task *PodTask) Launch() error {
	task.Lock()
	defer task.Unlock()
	log.WithField("task_id", task.getID()).Debugf("Launching task")

	task.setState(mesos.TaskState_TASK_STARTING)
	// TODO: Launch the nested containers
	// TODO: Run the goroutines that wait for the nested containers and retry if need be
	// TODO: Run the goroutine for healthchecks/status checks
	task.Sleeper(1 * time.Second)
	task.healthy.Store(true)
	task.setState(mesos.TaskState_TASK_RUNNING)
	return nil
}

// Kill is called when the executor decides to kill the particular task, and therefore
// all of the containers inside it.
// Makes sure that the goroutines that Launch spawned properly return via state in the task.
// NOOPs when the task was already killed.
func (task *PodTask) Kill() error {
	task.Lock()
	defer task.Unlock()

	if task.state == mesos.TaskState_TASK_KILLED {
		return nil
	}
	log.WithField("task_id", task.getID()).Debugf("Killing task")

	task.setState(mesos.TaskState_TASK_KILLING)
	// TODO: Kill all containers and goroutines synchroneously
	task.Sleeper(1 * time.Second)
	task.healthy.Store(false)
	task.setState(mesos.TaskState_TASK_KILLED)
	return nil
}

// Update is called to update the information about the task. This essentially sets the goal
// state of the task, and the task should try to evolve towards that goal state as best it can.
func (task *PodTask) Update(mesos.TaskInfo) error {
	task.Lock()
	defer task.Unlock()
	// TODO
	return nil
}

// IsHealthy returns whatever the latest policy evaluation of the healthcheck is at the time being.
func (task *PodTask) IsHealthy() bool { return task.healthy.Load() }

// GetState returns the latest TaskState for this task.
func (task *PodTask) GetState() mesos.TaskState { return task.state }

// GetInfo returns the latest TaskInfo for that task.
func (task *PodTask) GetInfo() mesos.TaskInfo { return task.info }

// Monitor tells the task whether or not to start/restart/stop monitoring the containers
// (healthcheck, checkstatus, wait_nested_container...)
func (task *PodTask) Monitor(enabled bool) error {
	task.monitoring.Store(enabled)
	return nil
}

// AddWatcher returns a channel that will receive a TaskState value when the TaskState changes.
// If the reader falls more than StateChannelSize state changes behind, the channel will be closed.
// If there is already a watcher under that name, we return a conflict error.
func (task *PodTask) AddWatcher(key string) (<-chan mesos.TaskState, error) {
	if _, found := task.watchers[key]; found {
		// TODO: constant error
		return nil, errors.New("Watcher key conflict")
	}
	output := make(chan mesos.TaskState, StateChannelSize)
	task.watchers[key] = output
	return output, nil
}

// setState is used internally to notify all watcher channels of the state change. It will also
// take care of closing any of the channels that the reader fell too far behind on (usually when the reader
// goroutine gets returns).
func (task *PodTask) setState(state mesos.TaskState) {
	if state == task.state {
		return
	}
	task.state = state
	for name, watcher := range task.watchers {
		select {
		case watcher <- task.state:
			// Successfully updated that watcher
			break
		default:
			// The state did not go through the channel. The reader goroutine most likely returned.
			log.WithField("watcher", name).Debugf("Closing channel")
			delete(task.watchers, name)
			close(watcher)
		}
	}
}

func (task *PodTask) getID() string {
	return *task.info.TaskId.Value
}

// NewPodTask creates a new PodTask from the TaskInfo. It will expect a
// PodSpec to be present in the data field of the TaskInfo.
// This will also attach a watcher that prints out the task state changes.
func NewPodTask(info mesos.TaskInfo, client PodTaskMesosClient) *PodTask {
	// Populate task struct
	task := &PodTask{
		info:       info,
		monitoring: atomic.NewBool(false),
		killing:    atomic.NewBool(false),
		healthy:    atomic.NewBool(false),
		state:      mesos.TaskState_TASK_UNKNOWN,
		watchers:   map[string]chan<- mesos.TaskState{},

		Sleeper:     time.Sleep,
		MesosClient: client,
	}

	// Attach task state printer. Since it's the first watcher, this operation should never
	// return an error.
	// We attach a default state watcher with debug log statements, both as an example and for
	// actual logs that are usable.
	states, err := task.AddWatcher("state_printer")
	if err != nil {
		panic(err)
	}
	go func() {
		for state := range states {
			log.WithField("task_id", *info.TaskId.Value).WithField("new_state", state).
				Debugf("State changed")
		}
		log.WithField("task_id", *info.TaskId.Value).
			Debugf("State printer channel closed for task")
	}()
	return task
}
