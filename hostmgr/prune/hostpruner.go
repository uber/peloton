package prune

import (
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/hostmgr/offer/offerpool"
)

// HostPruner is the interface to prune hosts set to PlacingOffer status for
// too long by resetting status to ReadyOffer
type HostPruner interface {
	Prune(_ *atomic.Bool)
}

// hostPruner implements interface HostPruner
type hostPruner struct {
	offerPool offerpool.Pool
	scope     tally.Scope
}

// NewHostPruner initializes the host pruner for an OfferPool
func NewHostPruner(pool offerpool.Pool, scope tally.Scope) HostPruner {
	return &hostPruner{
		offerPool: pool,
		scope:     scope,
	}
}

// For each host of the offerPool, the hostSummary status gets reset
// from PlacingOffer back to ReadyOffer if the PlacingOffer status has expired
func (h *hostPruner) Prune(_ *atomic.Bool) {
	log.Debug("Running host pruning")
	prunedHostnames := h.offerPool.ResetExpiredHostSummaries(time.Now())
	h.scope.Counter("pruned").Inc(int64(len(prunedHostnames)))
	if len(prunedHostnames) > 0 {
		log.WithField("hosts", prunedHostnames).Warn("Hosts pruned")
	}
}
