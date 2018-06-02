package event

import (
	"context"
	"strings"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	"code.uber.internal/infra/peloton/jobmgr/goalstate"
	jobmgr_task "code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
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
	eventstream.EventHandler

	Start()
	Stop()
}

// StatusUpdate reads and processes the task state change events from HM
type statusUpdate struct {
	jobStore        storage.JobStore
	taskStore       storage.TaskStore
	volumeStore     storage.PersistentVolumeStore
	eventClients    map[string]*eventstream.Client
	hostmgrClient   hostsvc.InternalHostServiceYARPCClient
	applier         *asyncEventProcessor
	jobFactory      cached.JobFactory
	goalStateDriver goalstate.Driver
	listeners       []Listener
	rootCtx         context.Context
	metrics         *Metrics
}

// NewTaskStatusUpdate creates a statusUpdate
func NewTaskStatusUpdate(
	d *yarpc.Dispatcher,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	volumeStore storage.PersistentVolumeStore,
	jobFactory cached.JobFactory,
	goalStateDriver goalstate.Driver,
	listeners []Listener,
	parentScope tally.Scope) StatusUpdate {

	statusUpdater := &statusUpdate{
		jobStore:        jobStore,
		taskStore:       taskStore,
		volumeStore:     volumeStore,
		rootCtx:         context.Background(),
		metrics:         NewMetrics(parentScope.SubScope("status_updater")),
		eventClients:    make(map[string]*eventstream.Client),
		jobFactory:      jobFactory,
		goalStateDriver: goalStateDriver,
		listeners:       listeners,
		hostmgrClient:   hostsvc.NewInternalHostServiceYARPCClient(d.ClientConfig(common.PelotonHostManager)),
	}
	// TODO: add config for BucketEventProcessor
	statusUpdater.applier = newBucketEventProcessor(statusUpdater, 100, 10000)

	eventClient := eventstream.NewEventStreamClient(
		d,
		common.PelotonJobManager,
		common.PelotonHostManager,
		statusUpdater,
		parentScope.SubScope("HostmgrEventStreamClient"))
	statusUpdater.eventClients[common.PelotonHostManager] = eventClient

	eventClientRM := eventstream.NewEventStreamClient(
		d,
		common.PelotonJobManager,
		common.PelotonResourceManager,
		statusUpdater,
		parentScope.SubScope("ResmgrEventStreamClient"))
	statusUpdater.eventClients[common.PelotonResourceManager] = eventClientRM
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
	var reason mesos_v1.TaskStatus_Reason
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
			"task_id": taskID,
			"state":   state.String(),
		}).Debug("Adding Mesos Event ")
		reason = event.GetMesosTaskStatus().GetReason()
	} else if event.Type == pb_eventstream.Event_PELOTON_TASK_EVENT {
		// Peloton task event is used for task status update from resmgr.
		taskID = event.PelotonTaskEvent.TaskId.Value
		state = event.PelotonTaskEvent.State
		statusMsg = event.PelotonTaskEvent.Message
		log.WithFields(log.Fields{
			"task_id": taskID,
			"state":   state.String(),
		}).Debug("Adding Peloton Event ")
	} else {
		log.WithFields(log.Fields{
			"task_id": taskID,
			"state":   state.String(),
		}).Error("Unknown Event ")
		return errors.New("Unknown Event ")
	}

	// Update task state counter for non-reconcilication update.
	if isMesosStatus && reason != mesos_v1.TaskStatus_REASON_RECONCILIATION {
		switch state {
		case pb_task.TaskState_RUNNING:
			p.metrics.TasksRunningTotal.Inc(1)
		case pb_task.TaskState_SUCCEEDED:
			p.metrics.TasksSucceededTotal.Inc(1)
		case pb_task.TaskState_FAILED:
			p.metrics.TasksFailedTotal.Inc(1)
			p.metrics.TasksFailedReason[int32(reason)].Inc(1)
			log.WithFields(log.Fields{
				"task_id":       mesosTaskID,
				"failed_reason": mesos_v1.TaskStatus_Reason_name[int32(reason)],
			}).Debug("received failed task")
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
	updatedRuntime := &pb_task.RuntimeInfo{}
	dbTaskID := runtime.GetMesosTaskId().GetValue()
	if isMesosStatus && dbTaskID != mesosTaskID {
		p.metrics.SkipOrphanTasksTotal.Inc(1)
		log.WithFields(log.Fields{
			"orphan_task_id":    mesosTaskID,
			"db_task_id":        dbTaskID,
			"db_task_runtime":   taskInfo.GetRuntime(),
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
			"db_task_runtime":   taskInfo.GetRuntime(),
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
	updatedRuntime.Message = statusMsg
	updatedRuntime.Reason = ""

	switch state {
	case pb_task.TaskState_KILLED:
		if runtime.GetGoalState() == pb_task.TaskState_PREEMPTING {
			updatedRuntime.Reason = "Task preempted"
			updatedRuntime.Message = "Task will not be rescheduled"
			pp := taskInfo.GetConfig().GetPreemptionPolicy()
			if pp == nil || !pp.GetKillOnPreempt() {
				updatedRuntime.Message = "Task will be rescheduled"
			}
		}
		updatedRuntime.State = state

	case pb_task.TaskState_FAILED:
		if event.GetMesosTaskStatus().GetReason() == mesos_v1.TaskStatus_REASON_TASK_INVALID && strings.Contains(event.GetMesosTaskStatus().GetMessage(), "Task has duplicate ID") {
			log.WithField("task_id", taskID).
				Info("ignoring duplicate task id failure")
			return nil
		}
		updatedRuntime.Reason = event.GetMesosTaskStatus().GetReason().String()
		updatedRuntime.State = state

	case pb_task.TaskState_LOST:
		updatedRuntime.Reason = event.GetMesosTaskStatus().GetReason().String()
		if util.IsPelotonStateTerminal(runtime.GetState()) {
			// Skip LOST status update if current state is terminal state.
			log.WithFields(log.Fields{
				"task_id":           taskID,
				"db_task_runtime":   taskInfo.GetRuntime(),
				"task_status_event": event.GetMesosTaskStatus(),
			}).Debug("skip reschedule lost task as it is already in terminal state")
			return nil
		}
		if runtime.GetGoalState() == pb_task.TaskState_KILLED {
			// Do not take any action for killed tasks, just mark it killed.
			// Same message will go to resource manager which will release the placement.
			log.WithFields(log.Fields{
				"task_id":           taskID,
				"db_task_runtime":   taskInfo.GetRuntime(),
				"task_status_event": event.GetMesosTaskStatus(),
			}).Debug("mark stopped task as killed due to LOST")
			updatedRuntime.State = pb_task.TaskState_KILLED
			updatedRuntime.Message = "Stopped task LOST event: " + statusMsg
			break
		}

		if taskInfo.GetConfig().GetVolume() != nil &&
			len(taskInfo.GetRuntime().GetVolumeID().GetValue()) != 0 {
			// Do not reschedule stateful task. Storage layer will decide
			// whether to start or replace this task.
			updatedRuntime.State = pb_task.TaskState_LOST
			break
		}

		log.WithFields(log.Fields{
			"task_id":           taskID,
			"db_task_runtime":   taskInfo.GetRuntime(),
			"task_status_event": event.GetMesosTaskStatus(),
		}).Info("reschedule lost task")
		p.metrics.RetryLostTasksTotal.Inc(1)
		updatedRuntime = util.RegenerateMesosTaskID(taskInfo.JobId, taskInfo.InstanceId, taskInfo.Runtime.GetMesosTaskId())
		updatedRuntime.Message = "Rescheduled due to task LOST: " + statusMsg
		updatedRuntime.Reason = event.GetMesosTaskStatus().GetReason().String()

	default:
		updatedRuntime.State = state
	}

	// Update task start and completion timestamps
	switch updatedRuntime.State {
	case pb_task.TaskState_RUNNING:
		// CompletionTime may have been set (e.g. task has been set),
		// which could make StartTime larger than CompletionTime.
		// Reset CompletionTime every time a task transits to RUNNING state.
		updatedRuntime.StartTime = now().UTC().Format(time.RFC3339Nano)
		updatedRuntime.CompletionTime = ""
	case pb_task.TaskState_SUCCEEDED,
		pb_task.TaskState_FAILED,
		pb_task.TaskState_KILLED:
		updatedRuntime.CompletionTime = now().UTC().Format(time.RFC3339Nano)
	}

	// Update the task update times in job cache and then update the task runtime in cache and DB
	cachedJob := p.jobFactory.AddJob(taskInfo.GetJobId())
	cachedJob.SetTaskUpdateTime(event.MesosTaskStatus.Timestamp)
	err = cachedJob.UpdateTasks(
		ctx,
		map[uint32]*pb_task.RuntimeInfo{taskInfo.GetInstanceId(): updatedRuntime},
		cached.UpdateCacheAndDB,
	)
	if err != nil {
		log.WithError(err).
			WithFields(log.Fields{
				"task_id": taskID,
				"state":   state}).
			Error("Fail to update runtime for taskID")
		return err
	}

	// Enqueue task to goal state
	p.goalStateDriver.EnqueueTask(
		taskInfo.GetJobId(),
		taskInfo.GetInstanceId(),
		time.Now())
	// Enqueue job to goal state as well
	goalstate.EnqueueJobWithDefaultDelay(
		taskInfo.GetJobId(), p.goalStateDriver, cachedJob)

	return nil
}

// updatePersistentVolumeState updates volume state to be CREATED.
func (p *statusUpdate) updatePersistentVolumeState(ctx context.Context, taskInfo *pb_task.TaskInfo) error {
	// Update volume state to be created if task enters RUNNING state.
	volumeInfo, err := p.volumeStore.GetPersistentVolume(ctx, taskInfo.GetRuntime().GetVolumeID())
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"job_id":          taskInfo.GetJobId().GetValue(),
			"instance_id":     taskInfo.GetInstanceId(),
			"db_task_runtime": taskInfo.GetRuntime(),
			"volume_id":       taskInfo.GetRuntime().GetVolumeID(),
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
