package host

import (
	"context"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common/lifecycle"
	"code.uber.internal/infra/peloton/resmgr/preemption"
	rmtask "code.uber.internal/infra/peloton/resmgr/task"

	"github.com/hashicorp/go-multierror"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

const (
	contextTimeout      = 10 * time.Second
	hostsToDrainLimit   = 100  // Maximum number of hosts to be polled from Maintenance Queue
	hostsToDrainTimeout = 1000 // Maintenance Queue poll timeout
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
		Limit:   hostsToDrainLimit,
		Timeout: hostsToDrainTimeout,
	}
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	response, err := d.hostMgrClient.GetDrainingHosts(ctx, request)
	if err != nil {
		return err
	}

	hosts := response.GetHostnames()
	if len(hosts) == 0 {
		return nil
	}

	err = d.drainHosts(hosts)
	if err != nil {
		return err
	}

	return nil
}

func (d *Drainer) drainHosts(hosts []string) error {
	errs := new(multierror.Error)

	log.WithField("hosts", hosts).Info("Draining hosts")
	tasksByHost := d.rmTracker.TasksByHosts(hosts, resmgr.TaskType_UNKNOWN)
	for _, host := range hosts {
		if _, ok := tasksByHost[host]; !ok {
			ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
			defer cancel()

			_, err := d.hostMgrClient.MarkHostDrained(ctx, &hostsvc.MarkHostDrainedRequest{
				Hostname: host,
			})
			if err != nil {
				log.WithError(err).Error("Error while downing host: " + host)
				errs = multierror.Append(errs, err)
			}
			continue
		}
		err := d.preemptor.EnqueueTasks(tasksByHost[host], resmgr.PreemptionReason_PREEMPTION_REASON_HOST_MAINTENANCE)
		if err != nil {
			log.WithError(err).Error("Failed to enqueue some tasks")
			errs = multierror.Append(errs, err)
		}
	}

	return errs.ErrorOrNil()
}
