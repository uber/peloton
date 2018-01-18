package event

import (
	"context"
	"strings"
	"sync"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/jobmgr/tracked"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

// Declare a Now function so that we can mock it in unit tests.
var now = time.Now

// Maximum retries on mesos system failures
const (
	MaxSystemFailureAttempts = 4
)

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
	volumeStore    storage.PersistentVolumeStore
	eventClients   map[string]*eventstream.Client
	hostmgrClient  hostsvc.InternalHostServiceYARPCClient
	applier        *asyncEventProcessor
	trackedManager tracked.Manager
	listeners      []Listener
	rootCtx        context.Context
	metrics        *Metrics
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
	volumeStore storage.PersistentVolumeStore,
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
			volumeStore:    volumeStore,
			rootCtx:        context.Background(),
			metrics:        NewMetrics(parentScope.SubScope("status_updater")),
			eventClients:   make(map[string]*eventstream.Client),
			trackedManager: trackedManager,
			listeners:      listeners,
			hostmgrClient:  hostsvc.NewInternalHostServiceYARPCClient(d.ClientConfig(common.PelotonHostManager)),
		}
		// TODO: add config for BucketEventProcessor
		statusUpdater.applier = newBucketEventProcessor(statusUpdater, 100, 10000)

		statusUpdaterRM := &statusUpdate{
			jobStore:     jobStore,
			taskStore:    taskStore,
			rootCtx:      context.Background(),
			metrics:      NewMetrics(parentScope.SubScope("status_updater")),
			eventClients: make(map[string]*eventstream.Client),
			listeners:    listeners,
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
	state := util.MesosStateToPelotonState(event.MesosTaskStatus.GetState())
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

	// Update task state counter for non-reconcilication update.
	if isMesosStatus && event.GetMesosTaskStatus().GetReason() != mesos_v1.TaskStatus_REASON_RECONCILIATION {
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
	} else {
		p.metrics.TasksReconciledTotal.Inc(1)
	}

	taskInfo, err := p.taskStore.GetTaskByID(ctx, taskID)
	if err != nil {
		log.WithError(err).
			WithField("task_id", taskID).
			WithField("task_status_event", event.GetMesosTaskStatus()).
			WithField("state", state.String()).
			Error("fail to find taskInfo for taskID for mesos event")
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
			"task_status_event": event.GetMesosTaskStatus(),
		}).Info("received status update for orphan mesos task")

		if taskInfo.GetConfig().GetVolume() != nil &&
			len(taskInfo.GetRuntime().GetVolumeID().GetValue()) != 0 {
			// Do not kill stateful orphan task.
			return nil
		}

		if !util.IsPelotonStateTerminal(state) {
			// Only kill task if state is not terminal.
			err := jobmgr_task.KillTask(ctx, p.hostmgrClient, event.MesosTaskStatus.GetTaskId())
			if err != nil {
				log.WithError(err).
					WithField("orphan_task_id", mesosTaskID).
					WithField("db_task_id", dbTaskID).
					Error("failed to kill orphan task")
			}
		}
		return nil
	}

	if state == runtime.GetState() {
		log.WithFields(log.Fields{
			"db_task_info":      taskInfo,
			"task_status_event": event.GetMesosTaskStatus(),
		}).Debug("skip same status update for mesos task")
		return nil
	}

	if state == pb_task.TaskState_RUNNING &&
		taskInfo.GetConfig().GetVolume() != nil &&
		len(taskInfo.GetRuntime().GetVolumeID().GetValue()) != 0 {
		// Update volume state to be CREATED upon task RUNNING.
		if err := p.updatePersistentVolumeState(ctx, taskInfo); err != nil {
			return err
		}
	}

	// Persist the reason and message for mesos updates
	runtime.Message = statusMsg
	runtime.Reason = ""

	switch state {
	case pb_task.TaskState_KILLED:
		if runtime.GetGoalState() == pb_task.TaskState_PREEMPTING {
			runtime.Reason = "Task preempted"
			runtime.Message = "Task will not be rescheduled"
			pp := taskInfo.GetConfig().GetPreemptionPolicy()
			if pp == nil || !pp.GetKillOnPreempt() {
				runtime.Message = "Task will be rescheduled"
			}
		}
		runtime.State = state

	case pb_task.TaskState_FAILED:
		runtime.Reason = event.GetMesosTaskStatus().GetReason().String()
		maxAttempts := taskInfo.GetConfig().GetRestartPolicy().GetMaxFailures()
		if p.isSystemFailure(event) {
			if maxAttempts < MaxSystemFailureAttempts {
				maxAttempts = MaxSystemFailureAttempts
			}
			p.metrics.RetryFailedLaunchTotal.Inc(1)
		}

		if runtime.GetFailureCount() >= maxAttempts {
			// Stop scheduling the task, max failures reached.
			runtime.GoalState = pb_task.TaskState_FAILED
			runtime.State = state
			break
		}

		// TODO(mu): check for failing reason before rescheduling.
		p.metrics.RetryFailedTasksTotal.Inc(1)
		runtime.Message = "Rescheduled due to task failure status: " + runtime.Message
		util.RegenerateMesosTaskID(taskInfo.JobId, taskInfo.InstanceId, taskInfo.Runtime)
		runtime.FailureCount++

	case pb_task.TaskState_LOST:
		runtime.Reason = event.GetMesosTaskStatus().GetReason().String()
		if util.IsPelotonStateTerminal(runtime.GetState()) {
			// Skip LOST status update if current state is terminal state.
			log.WithFields(log.Fields{
				"db_task_info":      taskInfo,
				"task_status_event": event.GetMesosTaskStatus(),
			}).Debug("skip reschedule lost task as it is already in terminal state")
			return nil
		}
		if runtime.GetGoalState() == pb_task.TaskState_KILLED {
			// Do not take any action for killed tasks, just mark it killed.
			// Same message will go to resource manager which will release the placement.
			log.WithFields(log.Fields{
				"db_task_info":      taskInfo,
				"task_status_event": event.GetMesosTaskStatus(),
			}).Debug("mark stopped task as killed due to LOST")
			runtime.State = pb_task.TaskState_KILLED
			runtime.Message = "Stopped task LOST event: " + statusMsg
			break
		}

		if taskInfo.GetConfig().GetVolume() != nil &&
			len(taskInfo.GetRuntime().GetVolumeID().GetValue()) != 0 {
			// Do not reschedule stateful task. Storage layer will decide
			// whether to start or replace this task.
			runtime.State = pb_task.TaskState_LOST
			break
		}

		log.WithFields(log.Fields{
			"db_task_info":      taskInfo,
			"task_status_event": event.GetMesosTaskStatus(),
		}).Info("reschedule lost task")
		p.metrics.RetryLostTasksTotal.Inc(1)
		runtime.Message = "Rescheduled due to task LOST: " + statusMsg
		util.RegenerateMesosTaskID(taskInfo.JobId, taskInfo.InstanceId, taskInfo.Runtime)

	default:
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

	err = p.trackedManager.UpdateTaskRuntime(ctx, taskInfo.GetJobId(), taskInfo.GetInstanceId(), runtime, tracked.UpdateAndSchedule)
	if err != nil {
		log.WithError(err).
			WithFields(log.Fields{
				"task_id": taskID,
				"state":   state}).
			Error("Fail to update runtime for taskID")
		return err
	}

	return nil
}

// updatePersistentVolumeState updates volume state to be CREATED.
func (p *statusUpdate) updatePersistentVolumeState(ctx context.Context, taskInfo *pb_task.TaskInfo) error {
	// Update volume state to be created if task enters RUNNING state.
	volumeInfo, err := p.volumeStore.GetPersistentVolume(ctx, taskInfo.GetRuntime().GetVolumeID())
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"db_task_info": taskInfo,
			"volume_id":    taskInfo.GetRuntime().GetVolumeID(),
		}).Error("Failed to read db for given volume")
		_, ok := err.(*storage.VolumeNotFoundError)
		if !ok {
			// Do not ack status update running if db read error.
			return err
		}
		return nil
	}

	// Do not update volume db if state is already CREATED or goalstate is DELETED.
	if volumeInfo.GetState() == volume.VolumeState_CREATED ||
		volumeInfo.GetGoalState() == volume.VolumeState_DELETED {
		return nil
	}

	volumeInfo.State = volume.VolumeState_CREATED
	return p.volumeStore.UpdatePersistentVolume(ctx, volumeInfo)
}

func (p *statusUpdate) ProcessListeners(event *pb_eventstream.Event) {
	for _, listener := range p.listeners {
		listener.OnEvents([]*pb_eventstream.Event{event})
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
