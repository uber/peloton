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
	RegisterWorks(works ...Work) error
}

// manager implements Manager interface.
type manager struct {
	runners map[string]*runner
}

// NewManager creates a new instance of Manager with registered background works.
func NewManager() Manager {
	return &manager{
		runners: make(map[string]*runner),
	}
}

// RegisterWorks registers background works against the Manager
func (r *manager) RegisterWorks(works ...Work) error {
	for _, work := range works {
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
		}

		ticker := time.NewTicker(r.work.Period)
		defer ticker.Stop()
		for {
			r.work.Func(&r.running)

			select {
			case <-r.stopChan:
				log.WithField("name", r.work.Name).
					Info("Background work stopped.")
				return
			case t := <-ticker.C:
				log.WithField("tick", t).
					WithField("name", r.work.Name).
					Debug("Background work triggered.")
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
