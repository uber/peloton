// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statemachine

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	// NotStarted state of state timer
	runningStateNotStarted = iota
	// Running state of state timer
	runningStateRunning
)

// StateTimer is the interface for recovering the states
type StateTimer interface {
	// Start starts the state recovery
	Start(timeout time.Duration) error
	// Stop stops the state recovery
	Stop()
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
func (st *statetimer) Stop() {
	st.Lock()
	defer st.Unlock()

	if st.runningState == runningStateNotStarted {
		log.WithField("task_id", st.statemachine.name).
			Warn("State Recovery is already stopped, " +
				"no action will be performed")
		return
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

	started := make(chan struct{})

	go func() {
		log.WithField("task_id", st.statemachine.name).
			Debug("Starting State recovery")

		atomic.StoreInt32(&st.runningState, runningStateRunning)
		timer := time.NewTimer(timeout)

		defer func() {
			atomic.StoreInt32(&st.runningState, runningStateNotStarted)
			timer.Stop()
		}()

		close(started)

		select {
		case <-st.stopChan:
			log.WithField("task_id", st.statemachine.name).
				Debug("Exiting State Recovery")
		case <-timer.C:
			st.recover()
		}
	}()

	// wait for goroutine to start
	<-started
	return nil
}

func (st *statetimer) recover() {
	// Acquiring the state machine lock here
	// The lock should be held for the entire duration of the rollback and
	// the termination of the timer.
	// Otherwise there could be cases where after the rollback and before the
	// timer termination the state machine can be updated and cause undefined
	// behaviour.
	st.statemachine.Lock()
	defer st.statemachine.Unlock()

	err := st.statemachine.rollbackState()
	if err != nil {
		log.WithField("task_id", st.statemachine.name).
			WithField("current_state", st.statemachine.current).
			WithError(err).
			Error("Error recovering state machine")
		return
	}
	log.WithField("task_id", st.statemachine.name).
		WithField("current_state", st.statemachine.current).
		Info("Recovered state, stopping service")
}
