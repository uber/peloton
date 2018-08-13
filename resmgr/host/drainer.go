package host

import (
	"context"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common/backoff"
	"code.uber.internal/infra/peloton/common/lifecycle"
	"code.uber.internal/infra/peloton/common/stringset"
	"code.uber.internal/infra/peloton/resmgr/preemption"
	rmtask "code.uber.internal/infra/peloton/resmgr/task"

	"github.com/hashicorp/go-multierror"
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
	hostMgrClient hostsvc.InternalHostServiceYARPCClient // Host Manager client
	metrics       *Metrics
	rmTracker     rmtask.Tracker       // Task Tracker
	started       int32                // State of the host drainer
	drainerPeriod time.Duration        // Period to run host drainer
	preemptor     preemption.Preemptor // Task Preemptor
	lifecycle     lifecycle.LifeCycle  // Lifecycle manager
	drainingHosts stringset.StringSet  // Set of hosts currently being drained
}

// NewDrainer creates a new Drainer
func NewDrainer(
	parent tally.Scope,
	hostMgrClient hostsvc.InternalHostServiceYARPCClient,
	drainerPeriod time.Duration,
	rmTracker rmtask.Tracker,
	preemptor preemption.Preemptor) *Drainer {

	return &Drainer{
		hostMgrClient: hostMgrClient,
		metrics:       NewMetrics(parent.SubScope("drainer")),
		rmTracker:     rmTracker,
		preemptor:     preemptor,
		drainerPeriod: drainerPeriod,
		lifecycle:     lifecycle.NewLifeCycle(),
		drainingHosts: stringset.New(),
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
	// Clear the set
	d.drainingHosts.Clear()
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

	for _, host := range response.GetHostnames() {
		d.drainingHosts.Add(host)
	}
	return d.drainHosts()
}

func (d *Drainer) drainHosts() error {
	var errs error

	log.WithField("hosts", d.drainingHosts).Info("Draining hosts")
	drainingHosts := d.drainingHosts.ToSlice()
	// No-op if there are no hosts to drain
	if len(drainingHosts) == 0 {
		return nil
	}
	// Get all tasks on the DRAINING hosts
	tasksByHost := d.rmTracker.TasksByHosts(drainingHosts, resmgr.TaskType_UNKNOWN)
	var drainedHosts []string
	for _, host := range drainingHosts {
		if _, ok := tasksByHost[host]; !ok {
			drainedHosts = append(drainedHosts, host)
			continue
		}
		err := d.preemptor.EnqueueTasks(
			tasksByHost[host],
			resmgr.PreemptionReason_PREEMPTION_REASON_HOST_MAINTENANCE)
		if err != nil {
			log.WithField("host", host).
				WithError(err).
				Error("Failed to enqueue some tasks")
			errs = multierror.Append(errs, err)
		}
	}
	if len(drainedHosts) != 0 {
		err := d.markHostsDrained(drainedHosts)
		if err != nil {
			errs = multierror.Append(err, errs)
		}
	}
	return errs
}

func (d *Drainer) markHostsDrained(hosts []string) error {
	err := backoff.Retry(
		func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			response, err := d.hostMgrClient.MarkHostsDrained(
				ctx,
				&hostsvc.MarkHostsDrainedRequest{
					Hostnames: hosts,
				})
			for _, host := range response.GetMarkedHosts() {
				d.drainingHosts.Remove(host)
			}
			return err
		},
		backoff.NewRetryPolicy(
			markHostDrainedBackoffRetryCount,
			time.Duration(markHostDrainedBackoffRetryInterval),
		),
		func(error) bool {
			return true
		},
	)
	return err
}
