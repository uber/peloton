package hostmgr

import (
	"code.uber.internal/infra/peloton/hostmgr/metrics"
	"code.uber.internal/infra/peloton/hostmgr/queue"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// RecoveryHandler defines the interface to
// be called by leader election callbacks.
type RecoveryHandler interface {
	Start() error
	Stop() error
}

// recoveryHandler restores the contents of MaintenanceQueue
// from Mesos Maintenance Status
type recoveryHandler struct {
	metrics              *metrics.Metrics
	maintenanceQueue     queue.MaintenanceQueue
	masterOperatorClient mpb.MasterOperatorClient
}

// NewRecoveryHandler creates a recoveryHandler
func NewRecoveryHandler(parent tally.Scope,
	maintenanceQueue queue.MaintenanceQueue,
	masterOperatorClient mpb.MasterOperatorClient) RecoveryHandler {
	recovery := &recoveryHandler{
		metrics:              metrics.NewMetrics(parent),
		maintenanceQueue:     maintenanceQueue,
		masterOperatorClient: masterOperatorClient,
	}
	return recovery
}

// Stop is a no-op for recovery handler
func (r *recoveryHandler) Stop() error {
	log.Info("Stopping recovery")
	return nil
}

// Start requeues all 'DRAINING' hosts into maintenance queue
func (r *recoveryHandler) Start() error {
	err := r.restoreMaintenanceQueue()
	if err != nil {
		r.metrics.RecoveryFail.Inc(1)
		return err
	}

	r.metrics.RecoverySuccess.Inc(1)
	return nil
}

func (r *recoveryHandler) restoreMaintenanceQueue() error {
	// Clear contents of maintenance queue before
	// enqueuing, to ensure removal of stale data
	r.maintenanceQueue.Clear()

	// Get Maintenance Status from Mesos Master
	response, err := r.masterOperatorClient.GetMaintenanceStatus()
	if err != nil {
		return err
	}

	clusterStatus := response.GetStatus()
	if clusterStatus == nil {
		log.Info("Empty maintenance status received")
		return nil
	}

	// Extract hostnames of draining machines
	var drainingHosts []string
	for _, drainingMachine := range clusterStatus.DrainingMachines {
		drainingHosts = append(drainingHosts, drainingMachine.GetId().GetHostname())
	}

	// Enqueue hostnames
	err = r.maintenanceQueue.Enqueue(drainingHosts)
	if err != nil {
		return err
	}

	return nil
}
