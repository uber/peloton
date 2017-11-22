package async

import (
	"context"
	"sync"

	log "github.com/sirupsen/logrus"
)

// Daemon represents a function that we want to start and run continuously until stopped.
type Daemon interface {
	// Start starts the daemon. The daemon is running when the underlying
	// runnable is started. Start blocks until the runnable is in the running
	// state. Otherwise it returns and does not block.
	Start()

	// Stop stops the daemon. The daemon is running until the underlying
	// runnable returns. Stop blocks until the runnable is in state stopped.
	// Otherwise it returns and does not block.
	Stop()
}

// Runnable represents a runnable function that can return an error.
type Runnable interface {
	// Run will run the runnable with a context and return any errors that
	// might occur.
	Run(ctx context.Context) (err error)
}

type runnable struct {
	runFunc func(context.Context) error
}

func (r *runnable) Run(ctx context.Context) (err error) {
	return r.runFunc(ctx)
}

// NewRunnable creates a new runnable from a function type.
func NewRunnable(runFunc func(context.Context) error) Runnable {
	return &runnable{
		runFunc: runFunc,
	}
}

// NewDaemon will create a new daemon.
func NewDaemon(name string, runnable Runnable) Daemon {
	return &daemon{
		condition: sync.NewCond(&sync.Mutex{}),
		name:      name,
		runnable:  runnable,
	}
}

type status uint

func (s status) String() string {
	switch s {
	case running:
		return "running"
	case cancelled:
		return "cancelled"
	case stopped:
		return "stopped"
	default:
		return "unknown"
	}
}

const (
	stopped status = iota
	running
	cancelled
)

type daemon struct {
	cancelFunc context.CancelFunc
	condition  *sync.Cond
	status     status
	name       string
	runnable   Runnable
}

// notifyOfStop will notify the daemon that the runnable stopped running and update the running flag.
func (d *daemon) notifyOfStop() {
	d.condition.L.Lock()
	defer d.condition.L.Unlock()
	d.status = stopped
	d.condition.Broadcast()
}

func (d *daemon) Start() {
	d.condition.L.Lock()
	defer d.condition.L.Unlock()
	loop := true
	for loop {
		switch d.status {
		case running:
			return
		case cancelled:
			d.condition.Wait()
		case stopped:
			loop = false
			continue
		}
	}

	// Status is stopped => launch the runnable
	ctx, cancelFunc := context.WithCancel(context.Background())
	d.cancelFunc = cancelFunc
	// Start the runnable
	go func() {
		defer d.notifyOfStop()
		d.runnable.Run(ctx)
	}()
	d.status = running
	d.condition.Broadcast()
	log.WithField("name", d.name).
		WithField("status", d.status).
		Info("Daemon started")
}

func (d *daemon) Stop() {
	d.condition.L.Lock()
	defer d.condition.L.Unlock()
	for {
		switch d.status {
		case running:
			d.status = cancelled
			if d.cancelFunc != nil {
				d.cancelFunc()
				d.cancelFunc = nil
			}
			d.condition.Wait()
		case cancelled:
			d.condition.Wait()
		case stopped:
			log.WithField("name", d.name).
				WithField("status", d.status).
				Info("Daemon stopped")
			return
		}
	}
}
