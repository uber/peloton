package offer

import (
	"context"
	"time"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"

	"code.uber.internal/infra/peloton/common/lifecycle"
	"code.uber.internal/infra/peloton/hostmgr/offer/offerpool"

	log "github.com/sirupsen/logrus"
)

const (
	runningStateNotStarted = 0
	runningStateRunning    = 1
)

// Pruner prunes offers
type Pruner interface {
	Start()
	Stop()
}

// NewOfferPruner initiates an instance of OfferPruner
func NewOfferPruner(
	pool offerpool.Pool,
	offerPruningPeriod time.Duration,
	metrics *offerpool.Metrics,
) Pruner {
	pruner := &offerPruner{
		pool:               pool,
		offerPruningPeriod: offerPruningPeriod,
		metrics:            metrics,
		lifeCycle:          lifecycle.NewLifeCycle(),
	}
	return pruner
}

// offerPruner implements OfferPruner
type offerPruner struct {
	pool               offerpool.Pool
	offerPruningPeriod time.Duration
	metrics            *offerpool.Metrics
	lifeCycle          lifecycle.LifeCycle // lifecycle manager
}

// Start starts offer pruning process
func (p *offerPruner) Start() {
	if !p.lifeCycle.Start() {
		log.Warn("Offer prunner is already running, no action will be performed")
		return
	}
	started := make(chan int, 1)
	go func() {
		defer p.lifeCycle.StopComplete()

		log.Info("Starting offer pruning loop")
		close(started)

		for {
			timer := time.NewTimer(p.offerPruningPeriod)
			select {
			case <-p.lifeCycle.StopCh():
				log.Info("Exiting the offer pruning loop")
				return
			case <-timer.C:
				log.Debug("Running offer pruning loop")
				expiredOffers, _ := p.pool.RemoveExpiredOffers()

				if len(expiredOffers) != 0 {
					var offerIDs []*mesos.OfferID
					for id := range expiredOffers {
						tmp := id
						offerIDs = append(offerIDs, &mesos.OfferID{
							Value: &tmp,
						})
					}
					log.WithField("offers", offerIDs).Debug("Offers to decline")
					if err := p.pool.DeclineOffers(context.Background(), offerIDs); err != nil {
						log.WithError(err).Error("Failed to decline offers")
					}
				}
			}
			timer.Stop()
		}
	}()
	// Wait until go routine is started
	<-started
}

// Stop stops offer pruning process
func (p *offerPruner) Stop() {
	if !p.lifeCycle.Stop() {
		log.Warn("Offer prunner is already stopped, no action will be performed")
		return
	}

	log.Info("Stopping offer pruner")
	// Wait for pruner to be stopped, should happen pretty quickly
	p.lifeCycle.Wait()

	log.Info("Offer pruner stopped")
}
