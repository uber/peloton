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

package goalstate

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/peloton/pkg/common/goalstate"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// _sleepRetryCheckRunningState is the duration to wait during stop/start while waiting for
// the driver to be running/non-running..
const (
	_sleepRetryCheckRunningState = 10 * time.Millisecond
)

// driverState indicates whether driver is running or not
type driverState int32

const (
	notRunning driverState = iota + 1 // not running
	running                           // running
)

// Driver is the interface to enqueue hosts into the goal state engine
// for evaluation and then run the corresponding actions.
type Driver interface {
	// EnqueueHost is used to enqueue a host into the goal state.
	EnqueueHost(hostname string, deadline time.Time)

	// DeleteHost deletes the host from the goal state engine.
	DeleteHost(hostname string)

	// Start is used to start processing items in the goal state engine.
	Start()

	// Stop is used to clean all items and then stop the goal state engine.
	Stop()

	// Started returns true if goal state engine has finished start process
	Started() bool
}

// driver implements the Driver interface
type driver struct {
	sync.RWMutex

	hostEngine              goalstate.Engine         // goal state engine for processing hosts
	mesosMasterClient       mpb.MasterOperatorClient // mesos master client
	hostInfoOps             ormobjects.HostInfoOps   // DB ops for host_info table
	maintenanceScheduleLock sync.Mutex               // Lock for updating Mesos Master maintenance schedule
	cfg                     *Config                  // goal state engine configuration
	scope                   tally.Scope              // scope for overall goal state
	running                 int32                    // whether driver is running or not
}

// NewDriver returns a new goal state driver object.
func NewDriver(
	ormStore *ormobjects.Store,
	mesosMasterClient mpb.MasterOperatorClient,
	parentScope tally.Scope,
	cfg Config,
) Driver {
	cfg.normalize()
	scope := parentScope.SubScope("goalstate")
	hostScope := scope.SubScope("host")
	return &driver{
		hostEngine: goalstate.NewEngine(
			cfg.NumWorkerHostThreads,
			cfg.FailureRetryDelay,
			cfg.MaxRetryDelay,
			hostScope),
		mesosMasterClient: mesosMasterClient,
		hostInfoOps:       ormobjects.NewHostInfoOps(ormStore),
		cfg:               &cfg,
		scope:             scope,
	}
}

// syncHostsFromDB performs recovery from DB by fetching hostInfos
// and enqueuing them into goal state engine
func (d *driver) syncHostsFromDB(ctx context.Context) error {
	log.Info("syncing host goal state engine from DB")
	hostInfos, err := d.hostInfoOps.GetAll(ctx)
	if err != nil {
		return err
	}
	for _, h := range hostInfos {
		log.WithFields(log.Fields{
			"hostname":   h.GetHostname(),
			"state":      h.GetState().String(),
			"goal_state": h.GetGoalState().String(),
		}).Info("recovering host and enqueuing into goal state engine")
		d.EnqueueHost(h.GetHostname(), time.Now())
	}
	log.Info("syncing host goal state engine from DB is complete")
	return nil
}

func (d *driver) setState(state driverState) {
	atomic.StoreInt32(&d.running, int32(state))
}

// Start the goal state engine
func (d *driver) Start() {
	defer func() {
		atomic.StoreInt32(&d.running, int32(running))
		log.Info("goalstate driver started")
	}()

	// Ensure that driver is not already running
	for {
		if d.runningState() != int32(running) {
			break
		}
		time.Sleep(_sleepRetryCheckRunningState)
	}

	if err := d.syncHostsFromDB(context.Background()); err != nil {
		log.WithError(err).Fatal("failed to sync host goal state engine from DB")
	}

	d.Lock()
	d.hostEngine.Start()
	d.Unlock()

	log.Info("goalstate driver started")
}

// runningState returns the running state of the driver
// (1 is not running, 2 if runing and 0 is invalid).
func (d *driver) runningState() int32 {
	return atomic.LoadInt32(&d.running)
}

// Started returns whether the goal state engine is started
func (d *driver) Started() bool {
	return driverState(d.runningState()) == running
}

// Stop stops the goal state engine
func (d *driver) Stop() {
	defer func() {
		atomic.StoreInt32(&d.running, int32(notRunning))
		log.Info("goalstate driver stopped")
	}()

	// Ensure that driver is running
	for {
		if d.runningState() != int32(notRunning) {
			break
		}
		time.Sleep(_sleepRetryCheckRunningState)
	}

	d.Lock()
	d.hostEngine.Stop()
	d.Unlock()

	// Clean up the entity map backing the host goal state engine
	// by reading from DB all hosts and deleting them
	hostInfos, err := d.hostInfoOps.GetAll(context.Background())
	if err != nil {
		log.WithError(err).
			Fatal("failed to delete entities from goal state engine")
	}
	for _, h := range hostInfos {
		d.DeleteHost(h.GetHostname())
	}
}

// EnqueueHost is used to enqueue a host into the goal state.
func (d *driver) EnqueueHost(hostname string, deadline time.Time) {
	hostEntity := NewHostEntity(hostname, d)

	d.RLock()
	defer d.RUnlock()

	d.hostEngine.Enqueue(hostEntity, deadline)
}

// DeleteHost deletes the host from the goal state engine.
func (d *driver) DeleteHost(hostname string) {
	hostEntity := NewHostEntity(hostname, d)

	d.RLock()
	defer d.RUnlock()

	d.hostEngine.Delete(hostEntity)
}
