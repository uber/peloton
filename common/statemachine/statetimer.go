package statemachine

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// StateTimer is the interface for recovering the states
type StateTimer interface {
	// Start starts the state recovery
	Start(timeout time.Duration)
	// Stop stops the state recovery
	Stop()
}

// statetimer is the timer object which is used to start the state recovery
type statetimer struct {
	// To synchronize state machine operations
	sync.Mutex

	// stopChan for stopping the recovery thread
	stopChan chan struct{}

	// state machine reference
	statemachine *statemachine
}

// NewTimer returns the object for the state timer
func NewTimer(sm *statemachine) StateTimer {
	return &statetimer{
		statemachine: sm,
	}
}

// Stop stops state recovery process
func (st *statetimer) Stop() {
	st.Lock()
	defer st.Unlock()

	log.Debug("Stopping State Recovery")
	if st.stopChan != nil {
		close(st.stopChan)
		st.stopChan = nil
	}

	log.Debug("State Recovery Stopped")
}

// Start starts the recovery process and recover the states
// from the timeout rules.
func (st *statetimer) Start(timeout time.Duration) {
	st.Lock()
	defer st.Unlock()

	if st.stopChan != nil {
		close(st.stopChan)
	}

	st.stopChan = make(chan struct{})

	go func(stopChan chan struct{}) {
		timer := time.NewTimer(timeout)
		select {
		case <-stopChan:
			log.Debug("Exiting State Recovery")
		case <-timer.C:
			st.statemachine.rollbackState()
			log.Info("Recovered state, stopping service")
		}
		timer.Stop()
	}(st.stopChan)
}
