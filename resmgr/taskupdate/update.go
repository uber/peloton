package taskupdate

import (
	"context"
	"sync/atomic"

	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
)

// ServiceHandler implements API to receive task updates from Hostmgr
type ServiceHandler struct {
	dispatcher yarpc.Dispatcher
	maxOffset  *uint64
}

// InitServiceHandler initializes the resource pool manager APIs to receive
// task status updates
func InitServiceHandler(
	d yarpc.Dispatcher) *ServiceHandler {
	var maxOffset uint64
	handler := ServiceHandler{
		dispatcher: d,
		maxOffset:  &maxOffset,
	}
	json.Register(
		d, json.Procedure("ResourceManager.NotifyTaskUpdates", handler.NotifyTaskUpdates))
	return &handler
}

// NotifyTaskUpdates is called by HM to notify task updates
func (m *ServiceHandler) NotifyTaskUpdates(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	req *resmgrsvc.NotifyTaskUpdatesRequest) (*resmgrsvc.NotifyTaskUpdatesResponse, yarpc.ResMeta, error) {
	var response resmgrsvc.NotifyTaskUpdatesResponse

	if len(req.Events) > 0 {
		for _, event := range req.Events {
			// NOTE: hookup future task status update handling logic here
			log.WithField("Offset", event.Offset).
				Debug("Event received by resource manager")
			if event.Offset > atomic.LoadUint64(m.maxOffset) {
				atomic.StoreUint64(m.maxOffset, event.Offset)
			}
		}
		response.PurgeOffset = atomic.LoadUint64(m.maxOffset)
	} else {
		log.Warn("Empty events received by resource manager")
	}
	return &response, nil, nil
}
