package goalstate

import (
	"context"
	"math"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/jobmgr/tracked"
	"code.uber.internal/infra/peloton/storage"
)

const (
	_indefDelay time.Duration = math.MaxInt64
)

// Engine manages current state -> goalstate convergence. For every task in the
// tracked Manager, it will derive actions based on state/goalstate difference.
// Eventually it tries to converge current state to goalstate. It will only run
// on the jobmgr leader instance.
type Engine interface {
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
		metrics:        NewMetrics(parentScope.SubScope("goalstate").SubScope("engine")),
	}
}

type engine struct {
	sync.Mutex

	cfg            Config
	trackedManager tracked.Manager

	jobStore  storage.JobStore
	taskStore storage.TaskStore

	stopChan chan struct{}

	metrics *Metrics
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
			e.trackedManager.SetTask(jobID, instanceID, task.GetRuntime())
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
