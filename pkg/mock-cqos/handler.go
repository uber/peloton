package mock_cqos

import (
	"context"
	cqos "github.com/uber/peloton/.gen/qos/v1alpha1"
	"go.uber.org/yarpc"
)

// ServiceHandler mocks the implementation for QoSAdvisorService
// this is for testing purpose
type ServiceHandler struct {
	hostLoad map[string]int32
}

// NewServiceHandler creates a new ServiceHandler.
func NewServiceHandler(
	d *yarpc.Dispatcher,
	hostLoad map[string]int32,
) *ServiceHandler {
	handler := &ServiceHandler{
		hostLoad: hostLoad,
	}
	d.Register(cqos.BuildQoSAdvisorServiceYARPCProcedures(handler))
	return handler
}

// GetHostMetrics mocks the getting the metrics of all hosts in the cluster.
func (h *ServiceHandler) GetHostMetrics(
	ctx context.Context,
	in *cqos.GetHostMetricsRequest) (*cqos.GetHostMetricsResponse, error) {
	hostMapResponse := make(map[string]*cqos.Metrics)
	for key, value := range h.hostLoad {
		hostMapResponse[key] = &cqos.Metrics{Score: value}
	}
	hostsLoadMap := &cqos.GetHostMetricsResponse{
		Hosts: hostMapResponse}
	return hostsLoadMap, nil
}

// GetServicesImpacted mocks getting all service instances in the cluster along
// with associated CPU and memory metrics.
// leave it blank now, since our test does not need this part yet.
func (h *ServiceHandler) GetServicesImpacted(
	ctx context.Context,
	in *cqos.GetServicesImpactedRequest) (*cqos.GetServicesImpactedResponse,
	error) {
	return &cqos.GetServicesImpactedResponse{}, nil
}

func (h *ServiceHandler) UpdateHostLoad(
	hostLoad map[string]int32) {
	h.hostLoad = hostLoad
}
