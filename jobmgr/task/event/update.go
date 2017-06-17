package event

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"

	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
	"github.com/pkg/errors"
)

// Declare a Now function so that we can mock it in unit tests.
var now = time.Now

// StatusUpdate is the interface for task status updates
type StatusUpdate interface {
	Start()
	Stop()
}

// Listener is the interface for StatusUpdate listener
type Listener interface {
	Start()
	Stop()
	eventstream.EventHandler
}

// StatusUpdate reads and processes the task state change events from HM
type statusUpdate struct {
	jobStore          storage.JobStore
	taskStore         storage.TaskStore
	eventClients      map[string]*eventstream.Client
	applier           *asyncEventProcessor
	jobRuntimeUpdater Listener
	rootCtx           context.Context
	resmgrClient      resmgrsvc.ResourceManagerServiceYarpcClient
	metrics           *Metrics
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
	jobRuntimeUpdater Listener,
	resmgrClientName string,
	parentScope tally.Scope) {
	onceInitStatusUpdate.Do(func() {
		if statusUpdater != nil {
			log.Warning("Task updater has already been initialized")
			return
		}
		statusUpdater = &statusUpdate{
			jobStore:     jobStore,
			taskStore:    taskStore,
			rootCtx:      context.Background(),
			resmgrClient: resmgrsvc.NewResourceManagerServiceYarpcClient(d.ClientConfig(resmgrClientName)),
			metrics:      NewMetrics(parentScope.SubScope("status_updater")),
			eventClients: make(map[string]*eventstream.Client),
		}
		// TODO: add config for BucketEventProcessor
		statusUpdater.applier = newBucketEventProcessor(statusUpdater, 100, 10000)

		statusUpdaterRM := &statusUpdate{
			jobStore:     jobStore,
			taskStore:    taskStore,
			rootCtx:      context.Background(),
			resmgrClient: resmgrsvc.NewResourceManagerServiceYarpcClient(d.ClientConfig(resmgrClientName)),
			metrics:      NewMetrics(parentScope.SubScope("status_updater")),
			eventClients: make(map[string]*eventstream.Client),
		}
		// TODO: add config for BucketEventProcessor
		statusUpdaterRM.applier = newBucketEventProcessor(statusUpdaterRM, 100, 10000)

		eventClient := eventstream.NewEventStreamClient(
			d,
			common.PelotonJobManager,
			server,
			statusUpdater,
			parentScope.SubScope("HostmgrEventStreamClient"))
		statusUpdater.eventClients[common.PelotonJobManager] = eventClient

		eventClientRM := eventstream.NewEventStreamClient(
			d,
			common.PelotonJobManager,
			common.PelotonResourceManager,
			statusUpdaterRM,
			parentScope.SubScope("ResmgrEventStreamClient"))
		statusUpdaterRM.eventClients[common.PelotonResourceManager] = eventClientRM

		statusUpdater.jobRuntimeUpdater = jobRuntimeUpdater
		statusUpdaterRM.jobRuntimeUpdater = jobRuntimeUpdater
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
func (p *statusUpdate) ProcessStatusUpdate(ctx context.Context, event *pb_eventstream.Event) error {
	var taskID string
	var err error
	var mesosTaskID string
	var state pb_task.TaskState
	var statusMsg string
	isMesosStatus := false

	if event.Type == pb_eventstream.Event_MESOS_TASK_STATUS {
		mesosTaskID = event.MesosTaskStatus.GetTaskId().GetValue()
		taskID, err = util.ParseTaskIDFromMesosTaskID(mesosTaskID)
		if err != nil {
			log.WithError(err).
				WithField("task_id", mesosTaskID).
				Error("Fail to parse taskID for mesostaskID")
			return err
		}
		state = util.MesosStateToPelotonState(event.MesosTaskStatus.GetState())
		statusMsg = event.MesosTaskStatus.GetMessage()
		isMesosStatus = true
		log.WithFields(log.Fields{
			"taskID": taskID,
			"state":  state.String(),
		}).Debug("Adding Mesos Event ")
	} else if event.Type == pb_eventstream.Event_PELOTON_TASK_EVENT {
		// Peloton task event is used for task status update from resmgr.
		taskID = event.PelotonTaskEvent.TaskId.Value
		state = event.PelotonTaskEvent.State
		statusMsg = event.PelotonTaskEvent.Message
		log.WithFields(log.Fields{
			"taskID": taskID,
			"state":  state.String(),
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

	runtime := taskInfo.GetRuntime()
	dbTaskID := runtime.GetMesosTaskId().GetValue()
	if isMesosStatus && dbTaskID != mesosTaskID {
		p.metrics.SkipOrphanTasksTotal.Inc(1)
		log.WithFields(log.Fields{
			"orphan_task_id":    mesosTaskID,
			"db_task_id":        dbTaskID,
			"db_task_info":      taskInfo,
			"task_status_event": event.MesosTaskStatus,
		}).Warn("skip status update for orphan mesos task")
		return nil
	}

	needRetrySchedulingTask := false
	maxFailures := taskInfo.GetConfig().GetRestartPolicy().GetMaxFailures()
	if state == pb_task.TaskState_FAILED &&
		runtime.GetFailureCount() < maxFailures {

		p.metrics.RetryFailedTasksTotal.Inc(1)
		statusMsg = "Rescheduled due to task failure status: " + statusMsg
		p.regenerateMesosTaskID(taskInfo)
		runtime.FailureCount++

		// TODO: check for failing reason and do backoff before
		// rescheduling.
		needRetrySchedulingTask = true
	} else if state == pb_task.TaskState_LOST {
		p.metrics.RetryLostTasksTotal.Inc(1)
		statusMsg = "Rescheduled due to task LOST: " + statusMsg
		p.regenerateMesosTaskID(taskInfo)
		needRetrySchedulingTask = true
	} else {
		// TODO: figure out on what cases state updates should not be persisted
		// TODO: depends on the state, may need to put the task back to
		// the queue, or clear the pending task record from taskqueue
		runtime.State = state
	}

	// Update task start and completion timestamps
	switch runtime.State {
	case pb_task.TaskState_RUNNING:
		runtime.StartTime = now().UTC().Format(time.RFC3339Nano)
	case pb_task.TaskState_SUCCEEDED,
		pb_task.TaskState_FAILED,
		pb_task.TaskState_KILLED:
		runtime.CompletionTime = now().UTC().Format(time.RFC3339Nano)
	}

	// Persist error message to help end user figure out root cause
	if isUnexpected(state) && isMesosStatus {
		runtime.Message = statusMsg
		runtime.Reason = event.MesosTaskStatus.GetReason().String()
		// TODO: Add metrics for unexpected task updates
		log.WithFields(log.Fields{
			"task_id": taskID,
			"state":   state,
			"message": statusMsg,
			"reason":  event.MesosTaskStatus.GetReason().String()}).
			Debug("Received unexpected update for task")
	}

	err = p.taskStore.UpdateTask(ctx, taskInfo)
	if err != nil {
		log.WithError(err).
			WithFields(log.Fields{
				"task_id": taskID,
				"State":   state}).
			Error("Fail to update taskInfo for taskID")
		return err
	}

	if needRetrySchedulingTask {
		go p.retrySchedulingTask(ctx, proto.Clone(taskInfo).(*pb_task.TaskInfo))
	}
	return nil
}

// regenerateMesosTaskID generates a new mesos task ID and update task info.
func (p *statusUpdate) regenerateMesosTaskID(taskInfo *pb_task.TaskInfo) {
	newMesosTaskID := fmt.Sprintf(
		"%s-%d-%s",
		taskInfo.GetJobId().GetValue(),
		taskInfo.GetInstanceId(),
		uuid.NewUUID().String())
	taskInfo.GetRuntime().GetMesosTaskId().Value = &newMesosTaskID
	taskInfo.GetRuntime().State = pb_task.TaskState_INITIALIZED
}

// retrySchedulingTask retries scheduling task by enqueue task to resmgr.
func (p *statusUpdate) retrySchedulingTask(ctx context.Context, taskInfo *pb_task.TaskInfo) {
	jobConfig, err := p.jobStore.GetJobConfig(ctx, taskInfo.GetJobId())
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"task":  taskInfo,
		}).Error("jobstore getjobconfig failed")
		return
	}
	p.enqueueTask(taskInfo, jobConfig)
}

// enqueueTask enqueues given task to respool in resmgr.
func (p *statusUpdate) enqueueTask(
	taskInfo *pb_task.TaskInfo,
	jobConfig *pb_job.JobConfig) {

	tasks := []*pb_task.TaskInfo{taskInfo}
	task.EnqueueGangs(p.rootCtx, tasks, jobConfig, p.resmgrClient)
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
	for _, client := range p.eventClients {
		client.Start()
	}
	log.Info("Task status updater started")
	if p.jobRuntimeUpdater != nil {
		p.jobRuntimeUpdater.Start()
	}
}

// Stop stops processing status update events
func (p *statusUpdate) Stop() {
	for _, client := range p.eventClients {
		client.Stop()
	}
	log.Info("Task status updater stopped")
	if p.jobRuntimeUpdater != nil {
		p.jobRuntimeUpdater.Stop()
	}

}
