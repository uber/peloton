package background

import (
	"sync"
	"time"

	"errors"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"
)

const (
	_stopRetryInterval = 1 * time.Millisecond
)

var (
	errEmptyName     = errors.New("background work name cannot be empty")
	errDuplicateName = errors.New("duplicate background work name")
)

// Work refers to a piece of background work which needs to happen
// periodically.
type Work struct {
	Name         string
	Func         func(*atomic.Bool)
	Period       time.Duration
	InitialDelay time.Duration
}

// Manager allows multiple background Works to be registered and
// started/stopped together.
type Manager interface {
	// Start starts all registered background works.
	Start()
	// Stop starts all registered background works.
	Stop()
	// RegisterWork registers a background work against the Manager
	RegisterWork(work Work) error
}

// manager implements Manager interface.
type manager struct {
	runners map[string]*runner
}

// NewManager creates a new instance of Manager with registered background works.
func NewManager(works ...Work) (Manager, error) {
	r := &manager{
		runners: make(map[string]*runner),
	}

	for _, work := range works {
		if err := r.RegisterWork(work); err != nil {
			return nil, err
		}
	}

	return r, nil
}

// RegisterWork registers a background work against the Manager
func (r *manager) RegisterWork(work Work) error {
	if work.Name == "" {
		return errEmptyName
	}

	if _, ok := r.runners[work.Name]; ok {
		return errDuplicateName
	}

	r.runners[work.Name] = &runner{
		work:     work,
		stopChan: make(chan struct{}, 1),
	}

	return nil
}

// Start all registered works.
func (r *manager) Start() {
	for _, runner := range r.runners {
		runner.start()
	}
}

// Stop all registered runners.
func (r *manager) Stop() {
	for _, runner := range r.runners {
		runner.stop()
	}
}

type runner struct {
	sync.Mutex

	work Work

	running  atomic.Bool
	stopChan chan struct{}
}

func (r *runner) start() {
	log.WithField("name", r.work.Name).Info("Starting Background work.")
	r.Lock()
	defer r.Unlock()
	if r.running.Swap(true) {
		log.WithField("name", r.work.Name).
			WithField("interval_secs", r.work.Period.Seconds()).
			Info("Background work is already running, no-op.")
		return
	}

	go func() {
		defer r.running.Store(false)

		// Non empty initial delay
		if r.work.InitialDelay.Nanoseconds() > 0 {
			log.WithField("name", r.work.Name).
				WithField("initial_delay", r.work.InitialDelay).
				Info("Initial delay for background work")

			initialTimer := time.NewTimer(r.work.InitialDelay)
			select {
			case <-r.stopChan:
				log.Info("Periodic reconcile stopped before first run.")
				return
			case <-initialTimer.C:
				log.Debug("Initial delay passed")
			}

			r.work.Func(&r.running)
		}

		ticker := time.NewTicker(r.work.Period)
		defer ticker.Stop()
		for {
			select {
			case <-r.stopChan:
				log.WithField("name", r.work.Name).
					Info("Background work stopped.")
				return
			case t := <-ticker.C:
				log.WithField("tick", t).
					WithField("name", r.work.Name).
					Debug("Background work triggered.")
				r.work.Func(&r.running)
			}
		}
	}()
}

func (r *runner) stop() {
	log.WithField("name", r.work.Name).Info("Stopping Background work.")

	if !r.running.Load() {
		log.WithField("name", r.work.Name).
			Warn("Background work is not running, no-op.")
		return
	}

	r.Lock()
	defer r.Unlock()

	r.stopChan <- struct{}{}

	// TODO: Make this non-blocking.
	for r.running.Load() {
		time.Sleep(_stopRetryInterval)
	}
	log.WithField("name", r.work.Name).Info("Background work stop confirmed.")
}
