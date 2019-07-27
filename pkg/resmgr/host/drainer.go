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

package host

import (
	"context"
	"time"

	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"

	"github.com/uber/peloton/pkg/common/backoff"
	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/resmgr/preemption"
	rmtask "github.com/uber/peloton/pkg/resmgr/task"

	multierror "github.com/hashicorp/go-multierror"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

const (
	contextTimeout                      = 10 * time.Second
	drainingHostsLimit                  = 100                    // Maximum number of hosts to be polled from Maintenance Queue
	drainingHostsTimeout                = 1000                   // Maintenance Queue poll timeout
	markHostDrainedBackoffRetryCount    = 3                      // Retry count to mark host drained
	markHostDrainedBackoffRetryInterval = 100 * time.Millisecond // Retry interval to mark host drained
)

// Drainer defines the host drainer which drains
// the hosts which are to be put into maintenance
type Drainer struct {
	hostMgrClient   hostsvc.InternalHostServiceYARPCClient // Host Manager client
	metrics         *Metrics
	rmTracker       rmtask.Tracker      // Task Tracker
	started         int32               // State of the host drainer
	drainerPeriod   time.Duration       // Period to run host drainer
	preemptionQueue preemption.Queue    // Preemption Queue
	lifecycle       lifecycle.LifeCycle // Lifecycle manager
}

// NewDrainer creates a new Drainer
func NewDrainer(
	parent tally.Scope,
	hostMgrClient hostsvc.InternalHostServiceYARPCClient,
	drainerPeriod time.Duration,
	rmTracker rmtask.Tracker,
	preemptionQueue preemption.Queue) *Drainer {

	return &Drainer{
		hostMgrClient:   hostMgrClient,
		metrics:         NewMetrics(parent.SubScope("drainer")),
		rmTracker:       rmTracker,
		preemptionQueue: preemptionQueue,
		drainerPeriod:   drainerPeriod,
		lifecycle:       lifecycle.NewLifeCycle(),
	}
}

// Start starts the Drainer process
func (d *Drainer) Start() error {
	if !d.lifecycle.Start() {
		log.Warn("Host Drainer is already running, no action will be performed")
		return nil
	}
	started := make(chan int, 1)
	go func() {
		defer d.lifecycle.StopComplete()
		ticker := time.NewTicker(d.drainerPeriod)
		defer ticker.Stop()

		log.Info("Starting Host Drainer")
		close(started)
		for {
			select {
			case <-d.lifecycle.StopCh():
				log.Info("Exiting Host Drainer")
				return
			case <-ticker.C:
				err := d.performDrainCycle()
				if err != nil {
					d.metrics.HostDrainFail.Inc(1)
					log.WithError(err).Error("Host Drain cycle failed")
					continue
				}
				d.metrics.HostDrainSuccess.Inc(1)
			}
		}
	}()
	<-started
	return nil
}

// Stop stops the Drainer process
func (d *Drainer) Stop() error {
	if !d.lifecycle.Stop() {
		log.Warn("Host Drainer is already stopped, no action will be performed")
		return nil
	}
	log.Info("Stopping Host Drainer")
	// Wait for drainer to be stopped
	d.lifecycle.Wait()
	log.Info("Host Drainer Stopped")
	return nil
}

func (d *Drainer) performDrainCycle() error {
	request := &hostsvc.GetDrainingHostsRequest{
		Limit:   drainingHostsLimit,
		Timeout: drainingHostsTimeout,
	}
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	response, err := d.hostMgrClient.GetDrainingHosts(ctx, request)
	if err != nil {
		return err
	}

	return d.drainHosts(response.GetHostnames())
}

func (d *Drainer) drainHosts(hosts []string) error {
	var errs error

	log.WithField("hosts", hosts).Info("Draining hosts")
	// No-op if there are no hosts to drain
	if len(hosts) == 0 {
		return nil
	}
	// Get all tasks on the DRAINING hosts
	tasksByHost := d.rmTracker.TasksByHosts(hosts, resmgr.TaskType_UNKNOWN)
	var drainedHosts []string
	for _, host := range hosts {
		tasks, ok := tasksByHost[host]
		if !ok || len(tasks) == 0 {
			drainedHosts = append(drainedHosts, host)
			continue
		}

		err := d.preemptionQueue.EnqueueTasks(
			tasks,
			resmgr.PreemptionReason_PREEMPTION_REASON_HOST_MAINTENANCE)
		if err != nil {
			log.WithField("host", host).
				WithError(err).
				Error("Failed to enqueue some tasks")
			errs = multierror.Append(errs, err)
		}
	}
	if len(drainedHosts) != 0 {
		for _, host := range drainedHosts {
			if err := d.markHostDrained(host); err != nil {
				errs = multierror.Append(err, errs)
				return errs
			}
			log.WithField("hostname", host).Info("Marked host as drained")
		}
	}
	return errs
}

func (d *Drainer) markHostDrained(host string) error {
	err := backoff.Retry(
		func() error {
			log.WithField("hostname", host).
				Info("Attempting to mark host as drained")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err := d.hostMgrClient.MarkHostDrained(
				ctx,
				&hostsvc.MarkHostDrainedRequest{
					Hostname: host,
				},
			)
			if err != nil {
				return err
			}
			log.WithField("hostname", host).
				Info("successfully marked host as drained, removing from queue")
			return nil
		},
		backoff.NewRetryPolicy(
			markHostDrainedBackoffRetryCount,
			time.Duration(markHostDrainedBackoffRetryInterval),
		),
		func(error) bool {
			return true // isRetryable
		},
	)

	return err
}
