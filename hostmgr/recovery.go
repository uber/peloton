package hostmgr

import (
	"context"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/hostmgr/metrics"
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
	metrics *metrics.Metrics
	handler *ServiceHandler
}

// NewRecoveryHandler creates a recoveryHandler
func NewRecoveryHandler(parent tally.Scope,
	handler *ServiceHandler) RecoveryHandler {
	recovery := &recoveryHandler{
		metrics: metrics.NewMetrics(parent),
		handler: handler,
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
	_, err := r.handler.RestoreMaintenanceQueue(
		context.Background(),
		&hostsvc.RestoreMaintenanceQueueRequest{})
	if err != nil {
		r.metrics.RecoveryFail.Inc(1)
		return err
	}

	r.metrics.RecoverySuccess.Inc(1)
	return nil
}
