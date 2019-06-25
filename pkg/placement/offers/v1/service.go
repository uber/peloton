package offers

import (
	hostsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/host/svc"

	"github.com/uber/peloton/pkg/placement/metrics"
	"github.com/uber/peloton/pkg/placement/offers"
)

// NewService returns a new offer service that calls
// out to the v1 api of hostmanager.
func NewService(
	hostManager hostsvc.HostServiceYARPCClient,
	metrics *metrics.Metrics,
) offers.Service {
	// TODO: Implement this.
	return nil
}
