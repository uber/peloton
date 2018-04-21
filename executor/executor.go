package executor

import (
	"sync"
	"time"

	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	executor "code.uber.internal/infra/peloton/.gen/mesos/v1/executor"
)

// State is an enumeration of the possible states of the executor.
type state int

const (
	waiting state = iota
	disconnected
	subscribed
	connected
	shutdown
)

func (s state) String() string {
	switch s {
	case waiting:
		return "WAITING"
	case disconnected:
		return "DISCONNECTED"
	case subscribed:
		return "SUBSCRIBED"
	case connected:
		return "CONNECTED"
	case shutdown:
		return "SHUTDOWN"
	}
	return "UNKNOWN"
}

// TaskFactory is the interface that Executor needs in order to create a
// new Task from the TaskInfo.
type TaskFactory func(*mesos.TaskInfo) Task

// Task is the interface that the Executor interacts with to launch, kill,
// and retrieve state about the task that it is aware of.
// The Launch, Update and Kill commands should return before doing any
// long operation such as network IO and waits.
// Monitor is meant to serve as a toggle for enabling/disabling healthchecks
// on the containers of a task.
// AddWatcher will create a channel that the task will send updates to when
// its state changes. The executor will register itself as a watcher on each
// of the tasks it manages and update the agent on the box anytime one of the
// tasks changes its status.
type Task interface {
	Launch() error
	Kill() error
	Update(mesos.TaskInfo) error

	IsHealthy() bool
	GetState() mesos.TaskState
	GetInfo() *mesos.TaskInfo

	Monitor(enabled bool) error
	AddWatcher(key string) (<-chan mesos.TaskState, error)
}

// MesosClient is the interface needed by the executor to interact
// with the mesos agent. The executor needs to be able to subscribe
// to the agent as well as update it with the states of the tasks.
type MesosClient interface {
	ExecutorCall(call *executor.Call) error
}

// Executor is the struct that is able to handle events coming from
// the mesos agent directed to an executor. It is responsible for
// creating the tasks objects through the taskfactory, handling updates
// and graceful shutdowns/disconnects with the agent.
//
// TBD: Should we periodically send all unacked updates to the agent after
// they have been outstanding for a while?
type Executor struct {
	sync.Mutex

	Factory     TaskFactory
	MesosClient MesosClient
	Sleeper     func(time.Duration)

	RecoveryTimeout     time.Duration // This field should come from the env variable "MESOS_RECOVERY_TIMEOUT" in production
	SubscriptionBackoff time.Duration // This field should come from the env variable "MESOS_SUBSCRIPTION_BACKOFF_MAX" in production

	info      *mesos.ExecutorInfo
	agent     *mesos.AgentInfo
	framework *mesos.FrameworkInfo
	container *mesos.ContainerID

	state          state
	tasks          map[string]Task       // A map from taskid to task
	pendingUpdates map[string]*ackUpdate // A map from taskid to the latest pending update for that task
	abortStart     chan error            // A channel that the executor listens to. It will log the error and return from the Start command
}

// ackUpdate is the tuple of an update and its ack-status.
type ackUpdate struct {
	acked bool
	*executor.Call_Update
}

// NewExecutor returns a new instance of the executor. Most of the fields will not be populated
// here but rather during the subscription phase.
// Some of the fields are taken from the spec of the Mesos executor:
// http://mesos.apache.org/documentation/latest/executor-http-api/
// TODO: Use options to pass in the values for some of the fields that are sure to be overwritten
// in production.
func NewExecutor(factory TaskFactory, client MesosClient) *Executor {
	exec := &Executor{
		Factory:     factory,
		MesosClient: client,
		Sleeper:     time.Sleep,

		RecoveryTimeout:     10 * time.Minute,
		SubscriptionBackoff: 10 * time.Second,

		state:          waiting,
		tasks:          map[string]Task{},
		pendingUpdates: map[string]*ackUpdate{},
		abortStart:     make(chan error, 1),
	}
	return exec
}

// Start runs the executor. The executor only stops when told to or when receiving a fatal error.
// Usually, the user should call Start and Wait on the executor to start it.
func (exec *Executor) Start() error {
	log.Debug("Executor starting")
	err := exec.subscribe()
	if err != nil {
		return errors.Wrapf(err, "failed to start executor")
	}
	return nil
}

// Wait returns once the executor is stopped or encounters a fatal failure.
func (exec *Executor) Wait() error {
	return <-exec.abortStart
}

// Connected is called when a successful connection to the mesos agent was achieved.
func (exec *Executor) Connected() {
	exec.Lock()
	defer exec.Unlock()
	log.Debug("Executor connected")
	exec.state = connected
}

// Disconnected is called when the connection to the mesos agent was severed. The executor will then try calling
// SUBSCRIBE again to re-establish a connection (after some backoff).
func (exec *Executor) Disconnected() {
	exec.Lock()
	log.Debug("Executor disconnected")
	exec.state = disconnected
	exec.Unlock()
	for _, task := range exec.tasks {
		task.Monitor(false)
	}
	// TODO: retry logic when subscribe fails
	var err error
	maxRetries := int(exec.RecoveryTimeout / exec.SubscriptionBackoff)
	for i := 0; i < maxRetries; i++ {
		exec.Sleeper(exec.SubscriptionBackoff)
		if err = exec.subscribe(); err == nil {
			break
		}
	}
	exec.Fatal(err)
}

// HandleEvent handles a generic executor event. Will return an error if the executor is shutting down.
func (exec *Executor) HandleEvent(event *executor.Event) error {
	log.WithField("event_type", event.GetType()).Debug("Received event")
	if exec.isShutdown() {
		return errors.Errorf("executor has shut down; cannot handle event %v", event.Type)
	}
	switch event.GetType() {
	case executor.Event_SUBSCRIBED:
		return exec.Subscribed(event.GetSubscribed())
	case executor.Event_KILL:
		return exec.Kill(event.GetKill())
	case executor.Event_SHUTDOWN:
		return exec.Shutdown()
	case executor.Event_LAUNCH:
		return exec.Launch(event.GetLaunch())
	case executor.Event_ACKNOWLEDGED:
		return exec.Acknowledged(event.GetAcknowledged())
	}

	log.Warningf("event not handled: %v", event.Type)
	return nil
}

// Shutdown kill all the tasks and shuts down the executor. This essentially means all events handled will
// return an error, and the start function (which was blocking) will return a nil error.
// TODO: Implement graceful shutdown behavior.
func (exec *Executor) Shutdown() error {
	exec.Lock()
	defer exec.Unlock()

	exec.state = shutdown
	for id, task := range exec.tasks {
		if err := task.Kill(); err != nil {
			return errors.Wrapf(err, "failed to kill task: %v", id)
		}
	}

	// Here we have successfully told all tasks to kill themselves, we can therefore
	// tell the thread that called Wait to return with a nil error.
	go func() {
		// TODO: check that all tasks are killed or that a graceful period of time
		// was exceeded.
		exec.abortStart <- nil
	}()
	return nil
}

// Subscribed gets called when the executor should handle a subscribed event. It resumes
// the monitoring of all tasks.
func (exec *Executor) Subscribed(event *executor.Event_Subscribed) error {
	exec.Lock()
	defer exec.Unlock()

	exec.state = subscribed
	for _, task := range exec.tasks {
		task.Monitor(true)
	}
	exec.info = event.GetExecutorInfo()
	exec.agent = event.GetAgentInfo()
	exec.framework = event.GetFrameworkInfo()
	exec.container = event.GetContainerId()
	log.WithField("agent_id", exec.agent.GetId().GetValue()).Debug("Subscribed to agent")
	return nil
}

// Launch gets called when the executor should handle a launch event. It should launch
// a task and ensure that all containers in that task are being healthchecked. It should
// also start sending updates about the task that was just started.
func (exec *Executor) Launch(event *executor.Event_Launch) error {
	exec.Lock()
	defer exec.Unlock()

	taskid := event.GetTask().GetTaskId().GetValue()
	if exec.isShutdown() {
		log.WithField("task_id", taskid).
			Warning("Ignoring launch event: executor is shutting down")
		return nil
	}

	if task, found := exec.tasks[taskid]; found {
		log.Debugf("Updating pre-existing task: %v", taskid)
		return task.Update(*event.Task)
	}

	log.WithField("task_id", taskid).Debug("Launching task")
	task := exec.Factory(event.GetTask())
	exec.tasks[taskid] = task
	exec.watch(task)

	// Launch that task, and set its monitoring to true.
	if err := task.Launch(); err != nil {
		return errors.Wrapf(err, "failed to launch task: %v", taskid)
	} else if err = task.Monitor(true); err != nil {
		return errors.Wrapf(err, "failed to turn on monitoring for task: %v", taskid)
	}
	return nil
}

// Kill gets called when the executor is asked to kill a particular task and all containers
// associated with it.
func (exec *Executor) Kill(event *executor.Event_Kill) error {
	exec.Lock()
	defer exec.Unlock()

	taskid := event.TaskId.GetValue()
	if exec.isShutdown() {
		log.WithField("task_id", taskid).
			Warning("Ignoring kill event: executor is shutting down")
		return nil
	}

	task, found := exec.tasks[taskid]
	if !found {
		return errors.Errorf("failed to find task: %v", taskid)
	}

	log.WithField("task_id", taskid).Debug("Killing task")
	if err := task.Kill(); err != nil {
		return errors.Wrapf(err, "failed to kill task: %v", taskid)
	}
	return nil
}

// Acknowledged gets called when the executor should be aware of an ack that the agent made
// about one of the updates that it received.
func (exec *Executor) Acknowledged(event *executor.Event_Acknowledged) error {
	exec.Lock()
	defer exec.Unlock()

	taskid := event.GetTaskId().GetValue()
	updateuuid := string(event.Uuid)

	// Get the pending update for that task.
	update, found := exec.pendingUpdates[taskid]
	if !found {
		// Got an ack for an update that we did not send.
		return errors.Errorf("got an ack for an unknown task: %v", taskid)
	}

	// If the latest update is the one that is getting acked, just mark as acked
	// and return.
	if string(update.GetStatus().Uuid) == updateuuid {
		log.WithField("task_id", taskid).Debug("Got an ack for updated task")
		update.acked = true
		return nil
	}

	// Otherwise, it means we just got an ack for an update that came before the latest update.
	// So we want to make sure that the latest update. So send the latest update again to make sure
	// the agent has the right last update in its state.
	log.WithField("task_id", taskid).Warning("Got out-of-order ack for task")
	update.acked = false
	call := exec.newCall(executor.Call_UPDATE)
	call.Update = update.Call_Update
	return exec.MesosClient.ExecutorCall(call)
}

// Fatal checks if the error is nil, and causes the executor Wait call to return that error
// if it isn't.
func (exec *Executor) Fatal(err error) {
	if err != nil {
		exec.abortStart <- err
	}
}

// IsConnected returns whether the executor is connected to the agent.
func (exec *Executor) IsConnected() bool {
	return exec.state == connected
}

// Subscribes to the agent to receive events. Sends any unacked Updates and TaskInfo along
// with the subscribe call.
func (exec *Executor) subscribe() error {
	exec.Lock()
	defer exec.Unlock()

	// Gather all of the infos and the updates from our tasks and pendingUpdates
	// respectively. These are needed to send the subscribe call.
	infos := []*mesos.TaskInfo{}
	for _, task := range exec.tasks {
		info := task.GetInfo()
		infos = append(infos, info)
	}

	updates := []*executor.Call_Update{}
	for _, update := range exec.pendingUpdates {
		updates = append(updates, update.Call_Update)
	}

	subscribe := &executor.Call_Subscribe{
		UnacknowledgedTasks:   infos,
		UnacknowledgedUpdates: updates,
	}
	call := exec.newCall(executor.Call_SUBSCRIBE)
	call.Subscribe = subscribe
	log.Debug("Subscribing to the agent")
	return exec.MesosClient.ExecutorCall(call)
}

// Watches a task and makes sure that the mesos agent gets updated when the task's state changes.
func (exec *Executor) watch(task Task) {
	// Start watching the state of that task
	taskid := task.GetInfo().GetTaskId().GetValue()
	states, err := task.AddWatcher("executor_main")
	if err != nil {
		log.WithError(err).Error("Failed to watch task")
		return
	}

	taskstate := task.GetState()
	go func() {
		// Update the agent every time there is a change in the state of that task.
		for state := range states {
			if taskstate == state {
				// Don't do anything if the state remains the same. We don't need to notify
				// the mesos agent that this happened.
				continue
			}

			log.WithFields(log.Fields{
				"task_id":   taskid,
				"old_state": taskstate,
				"new_state": state,
			}).Debug("Got a state change for task")
			update := exec.newUpdate(task, state)

			exec.Lock()
			exec.pendingUpdates[taskid] = &ackUpdate{acked: false, Call_Update: update}
			exec.Unlock()

			log.WithFields(log.Fields{
				"task_id":   taskid,
				"new_state": state,
				"update_id": update.Status.Uuid,
			}).Debug("Sending mesos an update")

			// Send that update to the mesos client
			// TODO: What if that update fails to send? Do we retry? Do we just assume that
			// this will get fixed the next time we subscribe?
			call := exec.newCall(executor.Call_UPDATE)
			call.Update = update
			if err := exec.MesosClient.ExecutorCall(call); err != nil {
				log.WithError(err).Error("Failed to issue update")
			}

			// Update the last known task state for this task.
			taskstate = state
		}
		log.WithField("task_id", taskid).Debug("Done watching task")
	}()
}

func (exec *Executor) newCall(callType executor.Call_Type) *executor.Call {
	call := &executor.Call{
		Type:        &callType,
		ExecutorId:  exec.info.GetExecutorId(),
		FrameworkId: exec.framework.GetId(),
	}
	return call
}

func (exec *Executor) newUpdate(task Task, state mesos.TaskState) *executor.Call_Update {
	updateid := uuid.Parse(uuid.New())

	// Create the update object and set a new pending update
	healthy := task.IsHealthy()
	source := mesos.TaskStatus_SOURCE_EXECUTOR
	status := &mesos.TaskStatus{
		TaskId:     task.GetInfo().TaskId,
		State:      &state,
		Uuid:       updateid,
		Healthy:    &healthy,
		ExecutorId: exec.info.GetExecutorId(),
		Source:     &source,
		Labels:     task.GetInfo().GetLabels(),
		AgentId:    exec.agent.GetId(),
	}
	update := &executor.Call_Update{
		Status: status,
	}
	return update
}

func (exec *Executor) isShutdown() bool {
	return exec.state == shutdown
}
