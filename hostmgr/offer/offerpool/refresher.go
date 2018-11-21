package offerpool

import (
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"
)

// Refresher is the interface to refresh bin packed host list
type Refresher interface {
	Refresh(_ *atomic.Bool)
}

// refresher implements interface Refresher
type refresher struct {
	offerPool Pool
}

// NewRefresher initializes the refresher for an OfferPool
func NewRefresher(pool Pool) Refresher {
	return &refresher{
		offerPool: pool,
	}
}

// Refresh refreshes the bin packed list, we need to run refresh
// asynchronously to reduce the performance panality,
func (h *refresher) Refresh(_ *atomic.Bool) {
	log.Debug("Running Refresh")
	h.offerPool.GetBinPackingRanker().RefreshRanking(
		h.offerPool.GetHostOfferIndex())
}
