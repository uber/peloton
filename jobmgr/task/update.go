package task

import (
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"

	mesos "mesos/v1"
	pb_task "peloton/api/task"
	pb_eventstream "peloton/private/eventstream"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
)

// StatusUpdate is the interface for task status updates
type StatusUpdate interface {
	Start()
	Stop()
}

// StatusUpdateListener is the interface for StatusUpdate listener
type StatusUpdateListener interface {
	Start()
	Stop()
	eventstream.EventHandler
}

// StatusUpdate reads and processes the task state change events from HM
type statusUpdate struct {
	taskStore         storage.TaskStore
	eventClient       *eventstream.Client
	applier           *asyncEventProcessor
	jobRuntimeUpdater StatusUpdateListener
}

// Singleton task status updater
var statusUpdater *statusUpdate
var onceInitStatusUpdate sync.Once

// InitTaskStatusUpdate creates a statusUpdate
func InitTaskStatusUpdate(
	d yarpc.Dispatcher,
	server string,
	taskStore storage.TaskStore,
	jobRuntimeUpdater StatusUpdateListener,
	parentScope tally.Scope) {
	onceInitStatusUpdate.Do(func() {
		if statusUpdater != nil {
			log.Warning("Task updater has already been initialized")
			return
		}
		statusUpdater = &statusUpdate{
			taskStore: taskStore,
		}
		// TODO: add config for BucketEventProcessor
		statusUpdater.applier = newBucketEventProcessor(statusUpdater, 100, 10000)

		eventClient := eventstream.NewEventStreamClient(
			d,
			common.PelotonJobManager,
			server,
			statusUpdater,
			parentScope.SubScope("HostmgrEventStreamClient"))
		statusUpdater.eventClient = eventClient

		statusUpdater.jobRuntimeUpdater = jobRuntimeUpdater
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

// ProcessStatusUpdate processes the actual task status
func (p *statusUpdate) ProcessStatusUpdate(taskStatus *mesos.TaskStatus) error {
	mesosTaskID := taskStatus.GetTaskId().GetValue()
	taskID, err := util.ParseTaskIDFromMesosTaskID(mesosTaskID)
	if err != nil {
		log.WithError(err).
			WithField("task_id", mesosTaskID).
			Error("Fail to parse taskID for mesostaskID")
		return err
	}
	taskInfo, err := p.taskStore.GetTaskByID(taskID)
	if err != nil {
		log.WithError(err).
			WithField("task_id", taskID).
			Error("Fail to find taskInfo for taskID")
		return err
	}
	state := util.MesosStateToPelotonState(taskStatus.GetState())

	// TODO: figure out on what cases state updates should not be persisted

	// TODO: depends on the state, may need to put the task back to
	// the queue, or clear the pending task record from taskqueue
	taskInfo.GetRuntime().State = state

	// persist error message to help end user figure out root cause
	if isUnexpected(state) {
		taskInfo.GetRuntime().Message = taskStatus.GetMessage()
		taskInfo.GetRuntime().Reason = taskStatus.GetReason().String()
		// TODO: Add metrics for unexpected task updates
		log.WithFields(log.Fields{
			"task_id": taskID,
			"state":   state,
			"message": taskStatus.GetMessage(),
			"reason":  taskStatus.GetReason().String()}).
			Debug("Received unexpected update for task")
	}

	err = p.taskStore.UpdateTask(taskInfo)
	if err != nil {
		log.WithError(err).
			WithFields(log.Fields{
				"task_id": taskID,
				"State":   state}).
			Error("Fail to update taskInfo for taskID")
		return err
	}
	return nil
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
func (p *statusUpdate) OnEvents(events []*pb_eventstream.Event) {
	p.jobRuntimeUpdater.OnEvents(events)
}

// Start starts processing status update events
func (p *statusUpdate) Start() {
	p.eventClient.Start()
	log.Info("Task status updater started")
	if p.jobRuntimeUpdater != nil {
		p.jobRuntimeUpdater.Start()
	}
}

// Stop stops processing status update events
func (p *statusUpdate) Stop() {
	p.eventClient.Stop()
	log.Info("Task status updater stopped")
	if p.jobRuntimeUpdater != nil {
		p.jobRuntimeUpdater.Stop()
	}

}
