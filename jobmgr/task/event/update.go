package event

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/jobmgr/tracked"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

// Maximum retries on mesos system failures
const (
	MaxSystemFailureAttempts = 1
)

// NowFunc returns time.Time so that we can mock it in unit tests.
type NowFunc func() time.Time

// StatusUpdate is the interface for task status updates
type StatusUpdate interface {
	Start()
	Stop()
}

// Listener is the interface for StatusUpdate listener
type Listener interface {
	eventstream.EventHandler

	Start()
	Stop()
}

// StatusUpdate reads and processes the task state change events from HM
type statusUpdate struct {
	jobStore       storage.JobStore
	taskStore      storage.TaskStore
	eventClients   map[string]*eventstream.Client
	applier        *asyncEventProcessor
	trackedManager tracked.Manager
	listeners      []Listener
	rootCtx        context.Context
	metrics        *Metrics
	now            NowFunc
}

// Singleton task status updater
var statusUpdater *statusUpdate
var onceInitStatusUpdate sync.Once

// InitTaskStatusUpdate creates a statusUpdate
func InitTaskStatusUpdate(
	d *yarpc.Dispatcher,
	server string,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	trackedManager tracked.Manager,
	listeners []Listener,
	parentScope tally.Scope) {
	onceInitStatusUpdate.Do(func() {
		if statusUpdater != nil {
			log.Warning("Task updater has already been initialized")
			return
		}
		statusUpdater = &statusUpdate{
			jobStore:       jobStore,
			taskStore:      taskStore,
			rootCtx:        context.Background(),
			metrics:        NewMetrics(parentScope.SubScope("status_updater")),
			eventClients:   make(map[string]*eventstream.Client),
			trackedManager: trackedManager,
			listeners:      listeners,
			now:            time.Now,
		}
		// TODO: add config for BucketEventProcessor
		statusUpdater.applier = newBucketEventProcessor(statusUpdater, 100, 10000)

		eventClient := eventstream.NewEventStreamClient(
			d,
			common.PelotonJobManager,
			server,
			statusUpdater,
			parentScope.SubScope("HostmgrEventStreamClient"))
		statusUpdater.eventClients[common.PelotonJobManager] = eventClient
	})
}

// GetStatusUpdater returns the task status updater. This
// function assumes the updater has been initialized as part of the
// InitTaskStatusUpdate function.
func GetStatusUpdater() StatusUpdate {
	if statusUpdater == nil {
		log.Fatal("Status updater is not initialized")
	}
	return statusUpdater
}

// OnEvent is the callback function notifying an event
func (p *statusUpdate) OnEvent(event *pb_eventstream.Event) {
	log.WithField("event_offset", event.Offset).Debug("JobMgr receiving event")
	p.applier.addEvent(event)
}

// GetEventProgress returns the progress of the event progressing
func (p *statusUpdate) GetEventProgress() uint64 {
	return p.applier.GetEventProgress()
}

func (p *statusUpdate) isSystemFailure(event *pb_eventstream.Event) bool {
	if event.Type != pb_eventstream.Event_MESOS_TASK_STATUS {
		return false
	}
	state := util.MesosStateToPelotonState(event.MesosTaskStatus)
	if state != pb_task.TaskState_FAILED && state != pb_task.TaskState_KILLED {
		return false
	}
	if event.GetMesosTaskStatus().GetReason() == mesos_v1.TaskStatus_REASON_CONTAINER_LAUNCH_FAILED {
		return true
	}
	if event.GetMesosTaskStatus().GetReason() == mesos_v1.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED {
		if strings.Contains(event.MesosTaskStatus.GetMessage(), "Container terminated with signal Broken pipe") {
			return true
		}
	}
	return false
}

// ProcessStatusUpdate processes the actual task status
func (p *statusUpdate) ProcessStatusUpdate(ctx context.Context, event *pb_eventstream.Event) error {
	var taskID *peloton.TaskID
	var err error
	var mesosTaskID string
	var state pb_task.TaskState
	var statusMsg string

	if event.Type == pb_eventstream.Event_MESOS_TASK_STATUS {
		mesosTaskID = event.MesosTaskStatus.GetTaskId().GetValue()
		taskID, err = util.ParseTaskIDFromMesosTaskID(mesosTaskID)
		if err != nil {
			log.WithError(err).
				WithField("mesos_task_id", mesosTaskID).
				Error("Fail to parse taskID for mesostaskID")
			return err
		}
		state = util.MesosStateToPelotonState(event.MesosTaskStatus)
		statusMsg = event.MesosTaskStatus.GetMessage()
		log.WithFields(log.Fields{
			"task_id": taskID.Value,
			"state":   state.String(),
		}).Debug("Adding Mesos Event ")
	} else {
		log.WithFields(log.Fields{
			"task_id": taskID.Value,
			"state":   state.String(),
		}).Error("Unknown Event ")
		return errors.New("Unknown Event ")
	}

	jobID, instanceID, err := util.ParseTaskID(taskID.Value)
	if err != nil {
		log.WithError(err).WithField("task_id", taskID).Error("failed parsing task_id")
		return err
	}

	runtime, err := p.trackedManager.GetTaskRuntime(ctx, jobID, instanceID)
	if err != nil {
		log.WithError(err).
			WithField("task_id", taskID.Value).
			Error("Fail to find task runtime for taskID")
		return err
	}

	dbTaskID := runtime.GetMesosTaskId().GetValue()
	if dbTaskID != mesosTaskID {
		p.metrics.SkipOrphanTasksTotal.Inc(1)
		log.WithFields(log.Fields{
			"orphan_task_id":    mesosTaskID,
			"db_task_id":        dbTaskID,
			"task_status_event": event.MesosTaskStatus,
		}).Warn("skip status update for orphan mesos task")
		return nil
	}

	// TODO: Once https://issues.apache.org/jira/browse/MESOS-6417 is fixed, we
	// don't have to read the config.
	config, err := p.taskStore.GetTaskConfig(ctx, jobID, instanceID, runtime.GetConfigVersion())
	if err != nil {
		log.WithError(err).
			WithField("task_id", taskID).
			Error("Fail to find task config")
		return err
	}

	// If it's running and we have a health check configured, but no health check
	// status is available, we're waiting for the first health check to be
	// processed.
	// TODO: Move to util.MesosStateToPelotonState when
	// https://issues.apache.org/jira/browse/MESOS-6417 is fixed.
	if state == pb_task.TaskState_RUNNING && config.GetHealthCheck().GetEnabled() && event.GetMesosTaskStatus().Healthy == nil {
		state = pb_task.TaskState_PENDING_HEALTH
	}

	if state == runtime.GetState() {
		log.WithFields(log.Fields{
			"db_runtime":        runtime,
			"task_status_event": event.MesosTaskStatus,
		}).Debug("skip same status update for mesos task")
		return nil
	}

	// Update task state counter for non-reconcilication update.
	if event.GetMesosTaskStatus().GetReason() != mesos_v1.TaskStatus_REASON_RECONCILIATION {
		switch state {
		case pb_task.TaskState_RUNNING:
			p.metrics.TasksRunningTotal.Inc(1)
		case pb_task.TaskState_SUCCEEDED:
			p.metrics.TasksSucceededTotal.Inc(1)
		case pb_task.TaskState_FAILED:
			p.metrics.TasksFailedTotal.Inc(1)
		case pb_task.TaskState_KILLED:
			p.metrics.TasksKilledTotal.Inc(1)
		case pb_task.TaskState_LOST:
			p.metrics.TasksLostTotal.Inc(1)
		}
	}

	switch state {
	case pb_task.TaskState_FAILED, pb_task.TaskState_KILLED:
		// If the stop was not user invoked or caused by preemption,
		// count it as a failure.
		if runtime.GoalState != pb_task.TaskGoalState_RESTART && runtime.
			GoalState != pb_task.TaskGoalState_PREEMPT {
			if runtime.ConfigVersion == runtime.DesiredConfigVersion {
				runtime.FailureCount++
			}
		}

		maxAttempts := config.GetRestartPolicy().GetMaxFailures()
		if p.isSystemFailure(event) {
			if maxAttempts < MaxSystemFailureAttempts {
				maxAttempts = MaxSystemFailureAttempts
			}
			p.metrics.RetryFailedLaunchTotal.Inc(1)
		}

		if runtime.GetFailureCount() > maxAttempts {
			// Stop scheduling the task, max failures reached.
			runtime.GoalState = pb_task.TaskGoalState_KILL
			runtime.State = state
			statusMsg = fmt.Sprintf("Task killed after %d failure(s): %s",
				runtime.GetFailureCount(), statusMsg)
			break
		}

		p.metrics.RetryFailedTasksTotal.Inc(1)
		statusMsg = "Rescheduled due to task failure status: " + statusMsg
		// TODO: check for failing reason before rescheduling.

	case pb_task.TaskState_LOST:
		p.metrics.RetryLostTasksTotal.Inc(1)
		log.WithFields(log.Fields{
			"runtime":           runtime,
			"task_status_event": event.GetMesosTaskStatus(),
		}).Info("reschedule lost task")
		statusMsg = "Rescheduled due to task LOST: " + statusMsg
	}

	// TODO: figure out on what cases state updates should not be persisted
	// TODO: depends on the state, may need to put the task back to
	// the queue, or clear the pending task record from taskqueue
	runtime.State = state

	// Update task start and completion timestamps
	switch runtime.State {
	case pb_task.TaskState_RUNNING:
		runtime.StartTime = p.now().UTC().Format(time.RFC3339Nano)
	case pb_task.TaskState_SUCCEEDED,
		pb_task.TaskState_FAILED,
		pb_task.TaskState_KILLED:
		runtime.CompletionTime = p.now().UTC().Format(time.RFC3339Nano)
	}

	// clear message and reason
	runtime.Message = ""
	runtime.Reason = ""

	// Persist message to help end user figure out root cause
	if wasPreempted(state, runtime.GoalState) {
		runtime.Reason = "Task preempted"
		runtime.Message = "Task will be rescheduled"
		if config.GetPreemptionPolicy().GetKillOnPreempt() {
			runtime.Message = "Task will not be rescheduled"
		}
	}

	// Persist error message to help end user figure out root cause
	if isUnexpected(state) {
		runtime.Message = statusMsg
		runtime.Reason = event.GetMesosTaskStatus().GetReason().String()
		// TODO: Add metrics for unexpected task updates
		log.WithFields(log.Fields{
			"task_status_event": event.GetMesosTaskStatus(),
			"task_id":           taskID.Value,
			"state":             state,
			"runtime":           runtime},
		).Debug("Received unexpected update for task")
	}

	err = p.trackedManager.UpdateTaskRuntime(ctx, jobID, instanceID, runtime)
	if err != nil {
		log.WithError(err).
			WithFields(log.Fields{
				"task_id": taskID.Value,
				"State":   state}).
			Error("Fail to update runtime for taskID")
		return err
	}

	return nil
}

func wasPreempted(taskState pb_task.TaskState,
	goalState pb_task.TaskGoalState) bool {
	return taskState == pb_task.TaskState_KILLED &&
		goalState == pb_task.TaskGoalState_PREEMPT
}

func (p *statusUpdate) ProcessListeners(event *pb_eventstream.Event) {
	for _, listener := range p.listeners {
		listener.OnEvents([]*pb_eventstream.Event{event})
	}
}

// isUnexpected tells if taskState is unexpected or not
func isUnexpected(taskState pb_task.TaskState) bool {
	switch taskState {
	case pb_task.TaskState_FAILED,
		pb_task.TaskState_LOST:
		return true
	default:
		// TODO: we may want to treat unknown state as error
		return false
	}
}

// OnEvents is the callback function notifying a batch of events
func (p *statusUpdate) OnEvents(events []*pb_eventstream.Event) {}

// Start starts processing status update events
func (p *statusUpdate) Start() {
	for _, client := range p.eventClients {
		client.Start()
	}
	log.Info("Task status updater started")
	for _, listener := range p.listeners {
		listener.Start()
	}
}

// Stop stops processing status update events
func (p *statusUpdate) Stop() {
	for _, client := range p.eventClients {
		client.Stop()
	}
	log.Info("Task status updater stopped")
	for _, listener := range p.listeners {
		listener.Stop()
	}
}
