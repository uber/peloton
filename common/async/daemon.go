package async

import (
	"context"
	"sync"

	log "github.com/sirupsen/logrus"
)

// Daemon represents a function that we want to start and run continuously until stopped.
type Daemon interface {
	// Start starts the daemon.
	// The daemon is running when the underlying step function is started.
	Start()

	// Stop stops the daemon.
	// The daemon is running until the underlying step function returns.
	Stop()
}

// Runnable represents a runnable function that can return an error.
type Runnable interface {
	// Run will run the runnable with an context and log any errors that it might return.
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
		name:     name,
		runnable: runnable,
	}
}

type daemon struct {
	lock       sync.Mutex
	started    bool
	running    bool
	cancelFunc context.CancelFunc
	name       string
	runnable   Runnable
}

func (d *daemon) Start() {
	d.lock.Lock()
	defer d.lock.Unlock()
	log.Infof("Daemon %s starting", d.name)
	if !d.started && !d.running {
		go func() {
			ctx, cancelFunc := context.WithCancel(context.Background())
			d.startRunning(cancelFunc)
			defer d.stopRunning()
			d.runnable.Run(ctx)
		}()
	}
	d.started = true
}

func (d *daemon) Stop() {
	d.lock.Lock()
	defer d.lock.Unlock()
	log.Infof("Daemon %s stopping", d.name)
	d.started = false
	if d.cancelFunc != nil {
		d.cancelFunc()
		d.cancelFunc = nil
	}
}

func (d *daemon) startRunning(cancelFunc context.CancelFunc) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.running = true
	d.cancelFunc = cancelFunc
	log.Infof("Daemon %s started running", d.name)
}

func (d *daemon) stopRunning() {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.running = false
	d.cancelFunc = nil
	log.Infof("Daemon %s stopped running", d.name)
}
