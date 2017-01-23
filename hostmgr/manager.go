package hostmgr

import (
	"context"
	"fmt"

	"github.com/prometheus/common/log"

	"code.uber.internal/infra/peloton/pbgen/src/peloton/hostmgr"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
)

// hostManager implements the peloton.hostmgr.HostManager service.
type hostManager struct {
	// TODO(zhitao): Add offer pool batching and implement OfferManager API.

	// TODO(zhitao): Add host batching.
}

// InitManager initialize the service handler for HostManager.
func InitManager(d yarpc.Dispatcher) {
	handler := hostManager{}

	json.Register(d, json.Procedure("HostManager.GetResources", handler.GetResources))
}

// GetResources implements HostManager.GetResources.
func (m *hostManager) GetResources(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *hostmgr.GetResourcesRequest) (*hostmgr.GetResourcesResponse, yarpc.ResMeta, error) {
	log.Info("GetResources called.")
	return nil, nil, fmt.Errorf("Unimplemented")
}
