package goalstate

import (
	"math"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/jobmgr/tracked"
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
	parentScope tally.Scope) Engine {
	cfg.normalize()
	return &engine{
		cfg:            cfg,
		trackedManager: trackedManager,
		metrics:        NewMetrics(parentScope.SubScope("goalstate").SubScope("engine")),
	}
}

type engine struct {
	sync.Mutex

	cfg            Config
	trackedManager tracked.Manager

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

func (e *engine) run(stopChan <-chan struct{}) {
	for {
		t := e.trackedManager.WaitForScheduledTask(stopChan)
		if t == nil {
			return
		}

		e.processTask(t)
	}
}
