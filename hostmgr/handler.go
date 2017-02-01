package hostmgr

import (
	"context"
	"fmt"

	"github.com/prometheus/common/log"

	"code.uber.internal/infra/peloton/pbgen/src/peloton/hostmgr"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
)

// serviceHandler implements peloton.hostmgr.HostService.
type serviceHandler struct {
	// TODO(zhitao): Add offer pool batching and implement OfferManager API.

	// TODO(zhitao): Add host batching.
}

// InitServiceHandler initialize serviceHandler.
func InitServiceHandler(d yarpc.Dispatcher) {
	handler := serviceHandler{}

	json.Register(d, json.Procedure("HostService.GetResources", handler.GetResources))
}

// GetResources implements HostService.GetResources.
func (m *serviceHandler) GetResources(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *hostmgr.GetResourcesRequest) (*hostmgr.GetResourcesResponse, yarpc.ResMeta, error) {
	log.Info("GetResources called.")
	return nil, nil, fmt.Errorf("Unimplemented")
}
