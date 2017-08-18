package goalstate

import (
	"context"
	"math"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/atomic"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"
	"code.uber.internal/infra/peloton/jobmgr/tracked"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
)

const (
	_indefDelay time.Duration = math.MaxInt64
)

// Engine manages task state -> goalstate convergence. For every task it tracks,
// it will derive actions based on state/goalstate difference. Eventually it tries
// to converge state to goalstate. It will only run on jobmgr leader instance.
type Engine interface {
	// OnEvent callback
	OnEvent(event *pb_eventstream.Event)

	// OnEvents is the implementation of the event stream handler callback
	OnEvents(events []*pb_eventstream.Event)

	// GetEventProgress returns the progress
	GetEventProgress() uint64

	// Start starts processing status update events
	Start()

	// Stop stops processing status update events
	Stop()
}

// NewEngine creates a new task goalstate Engine.
func NewEngine(
	cfg Config,
	trackedManager tracked.Manager,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	parentScope tally.Scope) Engine {
	cfg.normalize()
	return &engine{
		cfg:            cfg,
		trackedManager: trackedManager,
		jobStore:       jobStore,
		taskStore:      taskStore,
		metrics:        NewMetrics(parentScope.SubScope("goalstate_engine")),
	}
}

type engine struct {
	sync.Mutex

	cfg            Config
	trackedManager tracked.Manager

	jobStore  storage.JobStore
	taskStore storage.TaskStore

	progress atomic.Uint64
	stopChan chan struct{}

	metrics *Metrics
}

// OnEvent callback
func (e *engine) OnEvent(event *pb_eventstream.Event) {
	log.Error("Not implemented")
}

// OnEvents is the implementation of the event stream handler callback
func (e *engine) OnEvents(events []*pb_eventstream.Event) {
	for _, event := range events {
		if event.GetType() != pb_eventstream.Event_MESOS_TASK_STATUS {
			e.progress.Store(event.Offset)
			log.WithField("type", event.GetType()).
				Warn("unhandled event type in goalstate engine")
			continue
		}

		mesosTaskID := event.GetMesosTaskStatus().GetTaskId().GetValue()
		jobID, instanceID, err := util.ParseJobAndInstanceID(mesosTaskID)
		if err != nil {
			e.progress.Store(event.Offset)
			log.WithError(err).
				WithField("mesos_task_id", mesosTaskID).
				Error("Failed to ParseTaskIDFromMesosTaskID")
			continue
		}

		if j := e.trackedManager.GetJob(&peloton.JobID{Value: jobID}); j != nil {
			j.UpdateTaskState(uint32(instanceID), util.MesosStateToPelotonState(event.GetMesosTaskStatus().GetState()))
		}

		e.progress.Store(event.Offset)
	}
}

// GetEventProgress returns the progress
func (e *engine) GetEventProgress() uint64 {
	return e.progress.Load()
}

// Start starts processing status update events
func (e *engine) Start() {
	e.Lock()
	defer e.Unlock()

	if e.stopChan != nil {
		return
	}

	e.stopChan = make(chan struct{})
	go e.run(e.stopChan)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := e.syncFromDB(ctx); err != nil {
			log.WithError(err).Warn("failed to sync with DB in goalstate engine")
		}
	}()

	log.Info("goalstate.Engine started")
}

// Stop stops processing status update events
func (e *engine) Stop() {
	e.Lock()
	defer e.Unlock()

	if e.stopChan == nil {
		return
	}

	close(e.stopChan)
	e.stopChan = nil

	log.Info("goalstate.Engine stopped")
}

func (e *engine) syncFromDB(ctx context.Context) error {
	log.Info("syncing goalstate engine with DB goalstates")

	// TODO: Skip completed jobs.
	jobs, err := e.jobStore.GetAllJobs(ctx)
	if err != nil {
		return err
	}

	for id := range jobs {
		jobID := &peloton.JobID{Value: id}
		tasks, err := e.taskStore.GetTasksForJob(ctx, jobID)
		if err != nil {
			return err
		}

		for instanceID, task := range tasks {
			e.trackedManager.AddJob(jobID).UpdateTask(instanceID, task.GetRuntime())
		}
	}

	return nil
}

func (e *engine) run(stopChan <-chan struct{}) {
	for {
		t := e.trackedManager.WaitForScheduledTask(stopChan)
		if t == nil {
			return
		}

		e.processTask(t)
	}
}
