package hostmgr

import (
	"sync"

	"code.uber.internal/infra/peloton/hostmgr/metrics"
	maintenance_queue "code.uber.internal/infra/peloton/hostmgr/queue"
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
	sync.Mutex
	metrics              *metrics.Metrics
	maintenanceQueue     maintenance_queue.MaintenanceQueue
	operatorMasterClient mpb.MasterOperatorClient
}

// NewRecoveryHandler creates a recoveryHandler
func NewRecoveryHandler(parent tally.Scope,
	operatorMasterClient mpb.MasterOperatorClient,
	maintenanceQueue maintenance_queue.MaintenanceQueue) RecoveryHandler {
	recovery := &recoveryHandler{
		metrics:              metrics.NewMetrics(parent),
		maintenanceQueue:     maintenanceQueue,
		operatorMasterClient: operatorMasterClient,
	}
	return recovery
}

// Stop is a no-op for recovery handler
func (r *recoveryHandler) Stop() error {
	//no-op
	log.Info("Stopping recovery")
	return nil
}

// Start requeues all 'DRAINING' hosts into MaintenanceQueue
func (r *recoveryHandler) Start() error {
	r.Lock()
	defer r.Unlock()

	// Get Maintenance Status from Mesos Master
	response, err := r.operatorMasterClient.GetMaintenanceStatus()
	if err != nil {
		r.metrics.RecoveryFail.Inc(1)
		log.WithError(err).Error("failed to get maintenance status")
		return err
	}

	clusterStatus := response.GetStatus()
	if clusterStatus == nil {
		log.Info("Empty maintenance status received")
		r.metrics.RecoverySuccess.Inc(1)
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
		r.metrics.RecoveryFail.Inc(1)
		log.WithError(err).Error("failed to enqueue some hostnames")
		return err
	}

	r.metrics.RecoverySuccess.Inc(1)
	return nil
}
