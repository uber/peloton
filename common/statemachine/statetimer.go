package statemachine

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	// NotStarted state of task scheduler
	runningStateNotStarted = iota
	// Running state of task scheduler
	runningStateRunning
)

// StateTimer is the interface for recovering the states
type StateTimer interface {
	// Start starts the state recovery
	Start(timeout time.Duration) error
	// Stop stops the state recovery
	Stop() error
}

// statetimer is the timer object which is used to start the state recovery
type statetimer struct {
	// To synchronize state machine operations
	sync.RWMutex

	// runningState is the current state for recovery state thread
	runningState int32

	// stopChan for stopping the recovery thread
	stopChan chan struct{}

	// state machine reference
	statemachine *statemachine
}

// NewTimer returns the object for the state timer
func NewTimer(sm *statemachine) StateTimer {
	return &statetimer{
		stopChan:     make(chan struct{}, 1),
		statemachine: sm,
	}
}

// Stop stops state recovery process
func (st *statetimer) Stop() error {
	st.Lock()
	defer st.Unlock()

	if st.runningState == runningStateNotStarted {
		log.WithField("task_id", st.statemachine.name).
			Warn("State Recovery is already stopped, " +
				"no action will be performed")
		return errors.New("State Timer is not running")
	}

	log.WithField("task_id", st.statemachine.name).
		Debug("Stopping State Recovery")
	st.stopChan <- struct{}{}

	// Wait for State recovery to be stopped
	for {
		runningState := atomic.LoadInt32(&st.runningState)
		if runningState == runningStateRunning {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
	log.WithField("task_id", st.statemachine.name).
		Debug("State Recovery Stopped")

	return nil
}

// Start starts the recovery process and recover the states
// from the timeout rules.
func (st *statetimer) Start(timeout time.Duration) error {
	st.Lock()
	defer st.Unlock()

	if st.runningState == runningStateRunning {
		log.WithField("task_id", st.statemachine.name).
			Warn("State Recovery is already running, no action will be performed")
		return errors.New("State Timer is already running")
	}

	started := make(chan int, 1)
	go func() {
		atomic.StoreInt32(&st.runningState, runningStateRunning)
		defer atomic.StoreInt32(&st.runningState, runningStateNotStarted)
		log.WithField("task_id", st.statemachine.name).
			Debug("Starting State recovery")
		started <- 0

		for {
			timer := time.NewTimer(timeout)
			select {
			case <-st.stopChan:
				log.WithField("task_id", st.statemachine.name).
					Debug("Exiting State Recovery")
			case <-timer.C:
				err := st.statemachine.rollbackState()
				if err != nil {
					log.WithField("task_id", st.statemachine.name).
						WithField("current_state", st.statemachine.current).
						WithError(err).
						Error("Error recovering state machine")
					timer.Stop()
					return
				}
				log.WithField("task_id", st.statemachine.name).
					WithField("current_state", st.statemachine.current).
					Info("Recovered state, stopping service")
			}
			timer.Stop()
			return
		}
	}()
	// Wait until go routine is started
	<-started
	return nil
}
