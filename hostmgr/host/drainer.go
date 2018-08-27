package host

import (
	"time"

	"code.uber.internal/infra/peloton/common/lifecycle"
	"code.uber.internal/infra/peloton/hostmgr/queue"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"

	log "github.com/sirupsen/logrus"
)

// drainer defines the host drainer which drains
// the hosts which are to be put into maintenance
type drainer struct {
	drainerPeriod        time.Duration
	masterOperatorClient mpb.MasterOperatorClient
	maintenanceQueue     queue.MaintenanceQueue
	lifecycle            lifecycle.LifeCycle // lifecycle manager
}

// Drainer defines the interface for host drainer
type Drainer interface {
	Start()
	Stop()
}

// NewDrainer creates a new host drainer
func NewDrainer(
	drainerPeriod time.Duration,
	masterOperatorClient mpb.MasterOperatorClient,
	maintenanceQueue queue.MaintenanceQueue,
) Drainer {
	return &drainer{
		drainerPeriod:        drainerPeriod,
		masterOperatorClient: masterOperatorClient,
		maintenanceQueue:     maintenanceQueue,
		lifecycle:            lifecycle.NewLifeCycle(),
	}
}

// Start starts the host drainer
func (d *drainer) Start() {
	if !d.lifecycle.Start() {
		log.Warn("drainer is already started, no" +
			" action will be performed")
		return
	}

	go func() {
		defer d.lifecycle.StopComplete()

		ticker := time.NewTicker(d.drainerPeriod)
		defer ticker.Stop()

		log.Info("Starting Host drainer")

		for {
			select {
			case <-d.lifecycle.StopCh():
				log.Info("Exiting Host drainer")
				return
			case <-ticker.C:
				err := d.enqueueDrainingHosts()
				if err != nil {
					log.WithError(err).Warn("Draining host enqueue unsuccessful")
				}
			}
		}
	}()
}

// Stop stops the host drainer
func (d *drainer) Stop() {
	if !d.lifecycle.Stop() {
		log.Warn("drainer is already stopped, no" +
			" action will be performed")
		return
	}
	// Wait for drainer to be stopped
	d.lifecycle.Wait()
	log.Info("drainer stopped")
}

// Enqueue draining hosts into maintenance queue
func (d *drainer) enqueueDrainingHosts() error {
	maintenanceStatus, err := d.masterOperatorClient.GetMaintenanceStatus()
	if err != nil {
		return err
	}
	var drainingHosts []string
	for _, machine := range maintenanceStatus.GetStatus().GetDrainingMachines() {
		drainingHosts = append(drainingHosts, machine.GetId().GetHostname())
	}
	err = d.maintenanceQueue.Enqueue(drainingHosts)
	if err != nil {
		return err
	}
	return nil
}
