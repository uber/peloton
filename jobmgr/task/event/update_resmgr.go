package event

import (
	"context"
	"errors"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/tally"

	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/storage"
	"go.uber.org/yarpc"
)

// StatusUpdateRM is the interface for task status updates
type StatusUpdateRM interface {
	Start()
	Stop()
}

// StatusUpdateListenerRM is the interface for StatusUpdate listener
type StatusUpdateListenerRM interface {
	Start()
	Stop()
	eventstream.EventHandler
}

// StatusUpdate reads and processes the task state change events from HM
type statusUpdateRM struct {
	jobStore          storage.JobStore
	taskStore         storage.TaskStore
	eventClients      map[string]*eventstream.Client
	applier           *asyncEventProcessor
	jobRuntimeUpdater StatusUpdateListenerRM
	rootCtx           context.Context
	resmgrClient      resmgrsvc.ResourceManagerServiceYarpcClient
	metrics           *Metrics
}

// Singleton task status updater
var statusUpdaterRM *statusUpdateRM
var onceInitStatusUpdateRM sync.Once

// InitTaskStatusUpdateRM creates a statusUpdate
func InitTaskStatusUpdateRM(
	d *yarpc.Dispatcher,
	server string,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	jobRuntimeUpdater StatusUpdateListenerRM,
	resmgrClientName string,
	parentScope tally.Scope) {
	onceInitStatusUpdateRM.Do(func() {
		if statusUpdaterRM != nil {
			log.Warning("Task updater for RM has already been initialized")
			return
		}
		statusUpdaterRM = &statusUpdateRM{
			jobStore:     jobStore,
			taskStore:    taskStore,
			rootCtx:      context.Background(),
			resmgrClient: resmgrsvc.NewResourceManagerServiceYarpcClient(d.ClientConfig(resmgrClientName)),
			metrics:      NewMetrics(parentScope.SubScope("status_updater")),
			eventClients: make(map[string]*eventstream.Client),
		}
		// TODO: add config for BucketEventProcessor
		statusUpdaterRM.applier = newBucketEventProcessor(statusUpdater, 100, 10000)
		eventClientRM := eventstream.NewEventStreamClient(
			d,
			common.PelotonJobManager,
			common.PelotonResourceManager,
			statusUpdaterRM,
			parentScope.SubScope("ResmgrEventStreamClient"))
		statusUpdaterRM.eventClients[common.PelotonResourceManager] = eventClientRM
		statusUpdaterRM.jobRuntimeUpdater = jobRuntimeUpdater
	})
}

// GetStatusUpdaterRM returns the task status updater. This
// function assumes the updater has been initialized as part of the
// InitTaskStatusUpdate function.
func GetStatusUpdaterRM() StatusUpdateRM {
	if statusUpdaterRM == nil {
		log.Fatal("Status updater is not initialized")
	}
	return statusUpdaterRM
}

// OnEvent is the callback function notifying an event
func (p *statusUpdateRM) OnEvent(event *pb_eventstream.Event) {
	log.WithField("event_offset", event.Offset).Debug("JobMgr receiving event")
	p.applier.addEvent(event)
}

// GetEventProgress returns the progress of the event progressing
func (p *statusUpdateRM) GetEventProgress() uint64 {
	return p.applier.GetEventProgress()
}

// ProcessStatusUpdate processes the actual task status
func (p *statusUpdateRM) ProcessStatusUpdate(ctx context.Context, event *pb_eventstream.Event) error {
	var taskID string
	var err error
	var state pb_task.TaskState

	if event.Type == pb_eventstream.Event_PELOTON_TASK_EVENT {
		taskID = event.PelotonTaskEvent.TaskId.Value
		state = event.PelotonTaskEvent.State
		statusMsg := event.PelotonTaskEvent.Message
		log.WithFields(log.Fields{
			"taskID":  taskID,
			"state":   state.String(),
			"Message": statusMsg,
		}).Debug("Adding Peloton Event ")
	} else {
		log.WithFields(log.Fields{
			"taskID": taskID,
			"state":  state.String(),
		}).Error("Unknown Event ")
		return errors.New("Unknown Event ")
	}

	taskInfo, err := p.taskStore.GetTaskByID(ctx, taskID)
	if err != nil {
		log.WithError(err).
			WithField("task_id", taskID).
			Error("Fail to find taskInfo for taskID")
		return err
	}

	taskInfo.GetRuntime().State = state

	err = p.taskStore.UpdateTask(ctx, taskInfo)
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

// OnEvents is the callback function notifying a batch of events
func (p *statusUpdateRM) OnEvents(events []*pb_eventstream.Event) {
	p.jobRuntimeUpdater.OnEvents(events)
}

// Start starts processing status update events
func (p *statusUpdateRM) Start() {
	for _, client := range p.eventClients {
		client.Start()
	}
	log.Info("Task status updater started")
	if p.jobRuntimeUpdater != nil {
		p.jobRuntimeUpdater.Start()
	}
}

// Stop stops processing status update events
func (p *statusUpdateRM) Stop() {
	for _, client := range p.eventClients {
		client.Stop()
	}
	log.Info("Task status updater stopped")
	if p.jobRuntimeUpdater != nil {
		p.jobRuntimeUpdater.Stop()
	}

}
