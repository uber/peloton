package task

import (
	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	mesos "mesos/v1"
	pb_eventstream "peloton/private/eventstream"
)

// StatusUpdate reads and processes the task state change events from HM
type StatusUpdate struct {
	taskStore   storage.TaskStore
	eventClient *eventstream.Client
	applier     *asyncEventProcessor
}

// InitTaskStatusUpdate creates a StatusUpdate
func InitTaskStatusUpdate(
	d yarpc.Dispatcher,
	server string,
	taskStore storage.TaskStore) *StatusUpdate {

	result := &StatusUpdate{
		taskStore: taskStore,
	}
	eventClient := eventstream.NewEventStreamClient(d, common.PelotonJobManager, server, result)
	// TODO: add config for BucketEventProcessor
	result.applier = newBucketEventProcessor(result, 100, 10000)

	result.eventClient = eventClient
	eventClient.Start()
	return result
}

// OnEvent is the callback function notifying an event
func (p *StatusUpdate) OnEvent(event *pb_eventstream.Event) {
	log.WithField("event_offset", event.Offset).Debug("Receiving event")
	p.applier.addEvent(event)
}

// GetEventProgress returns the progress of the event progressing
func (p *StatusUpdate) GetEventProgress() uint64 {
	return p.applier.GetEventProgress()
}

// ProcessStatusUpdate processes the actual task status
func (p *StatusUpdate) ProcessStatusUpdate(taskStatus *mesos.TaskStatus) error {
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

	// TODO: depends on the state, may need to put the task back to
	// the queue, or clear the pending task record from taskqueue
	taskInfo.GetRuntime().State = state
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
