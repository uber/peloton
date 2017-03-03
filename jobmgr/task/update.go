package task

import (
	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/storage"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	pb_eventstream "peloton/private/eventstream"
	"sync/atomic"
)

// StatusUpdate reads and processes the task state change events from HM
type StatusUpdate struct {
	taskStore   storage.TaskStore
	eventClient *eventstream.Client
	// TODO: enable this on next change
	// applier     *taskUpdateApplier
	progress *uint64
}

// InitTaskStatusUpdate creates a StatusUpdate
func InitTaskStatusUpdate(
	d yarpc.Dispatcher,
	server string,
	taskStore storage.TaskStore) *StatusUpdate {

	var progress uint64
	result := &StatusUpdate{
		taskStore: taskStore,
		progress:  &progress,
	}
	eventClient := eventstream.NewEventStreamClient(d, common.PelotonJobManager, server, result)
	result.eventClient = eventClient
	// result.applier = newTaskStateUpdateApplier(result, 100, 10000)
	eventClient.Start()
	return result
}

// OnEvent is the callback function notifying an event
func (p *StatusUpdate) OnEvent(event *pb_eventstream.Event) {
	// TODO: make this debug
	log.WithField("EventOffset", event.Offset).Info("Receiving event")
	atomic.StoreUint64(p.progress, event.Offset)
	// p.applier.addEvent(event)
}

// GetEventProgress returns the progress of the event progressing
func (p *StatusUpdate) GetEventProgress() uint64 {
	// return p.applier.GetEventProgress()
	return atomic.LoadUint64(p.progress)
}
