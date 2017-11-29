package cleaner

import (
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/hostmgr/offer/offerpool"
)

// Cleaner is the interface to recycle the to be deleted volumes and reserved resources.
type Cleaner interface {
	Run(isRunning *atomic.Bool)
}

// cleaner implements interface Cleaner to recycle reserved resources.
type cleaner struct {
	offerPool offerpool.Pool
	scope     tally.Scope
}

// NewCleaner initializes the reservation resource cleaner.
func NewCleaner(pool offerpool.Pool, scope tally.Scope) Cleaner {
	return &cleaner{
		offerPool: pool,
		scope:     scope,
	}
}

// Run will clean the unused reservation resources and volumes.
func (h *cleaner) Run(isRunning *atomic.Bool) {
	log.Info("Cleaning reserved resources and volumes")
	h.offerPool.CleanReservationResources()
}
