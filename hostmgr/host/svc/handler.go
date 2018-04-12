package svc

import (
	"context"

	host_svc "code.uber.internal/infra/peloton/.gen/peloton/api/host/svc"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

// serviceHandler implements peloton.api.host.svc.HostService
type serviceHandler struct {
	metrics *Metrics
}

// InitServiceHandler initializes the HostService
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope) {
	handler := &serviceHandler{
		metrics: NewMetrics(parent.SubScope("hostsvc")),
	}
	d.Register(host_svc.BuildHostServiceYARPCProcedures(handler))
	log.Info("Hostsvc handler initialized")
}

// QueryHosts returns the hosts which are in one of the specified states.
func (m *serviceHandler) QueryHosts(ctx context.Context, request *host_svc.QueryHostsRequest) (*host_svc.QueryHostsResponse, error) {
	m.metrics.QueryHostsAPI.Inc(1)
	return &host_svc.QueryHostsResponse{
		Hosts: nil,
	}, nil
}

// StartMaintenance starts maintenance on the specified hosts.
func (m *serviceHandler) StartMaintenance(ctx context.Context, request *host_svc.StartMaintenanceRequest) (*host_svc.StartMaintenanceResponse, error) {
	m.metrics.StartMaintenanceAPI.Inc(1)
	return &host_svc.StartMaintenanceResponse{}, nil
}

// CompleteMaintenance completes maintenance on the specified hosts.
func (m *serviceHandler) CompleteMaintenance(ctx context.Context, request *host_svc.CompleteMaintenanceRequest) (*host_svc.CompleteMaintenanceResponse, error) {
	m.metrics.CompleteMaintenanceAPI.Inc(1)
	return &host_svc.CompleteMaintenanceResponse{}, nil
}
