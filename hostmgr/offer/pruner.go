package offer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
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
		runningState:       runningStateNotStarted,
		offerPruningPeriod: offerPruningPeriod,
		stopPrunerChan:     make(chan struct{}, 1),
		metrics:            metrics,
	}
	return pruner
}

// offerPruner implements OfferPruner
type offerPruner struct {
	sync.Mutex

	runningState       int32
	pool               offerpool.Pool
	offerPruningPeriod time.Duration
	stopPrunerChan     chan struct{}
	metrics            *offerpool.Metrics
}

// Start starts offer pruning process
func (p *offerPruner) Start() {
	defer p.Unlock()
	p.Lock()

	if p.runningState == runningStateRunning {
		log.Warn("Offer prunner is already running, no action will be performed")
		return
	}

	started := make(chan int, 1)
	go func() {
		defer atomic.StoreInt32(&p.runningState, runningStateNotStarted)
		atomic.StoreInt32(&p.runningState, runningStateRunning)

		log.Info("Starting offer pruning loop")
		started <- 0

		for {
			timer := time.NewTimer(p.offerPruningPeriod)
			select {
			case <-p.stopPrunerChan:
				log.Info("Exiting the offer pruning loop")
				return
			case <-timer.C:
				log.Debug("Running offer pruning loop")
				expiredOffers, numValid := p.pool.RemoveExpiredOffers()

				p.metrics.Pruned.Inc(int64(len(expiredOffers)))
				p.metrics.PrunerValid.Update(float64(numValid))

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
	defer p.Unlock()
	p.Lock()

	if p.runningState == runningStateNotStarted {
		log.Warn("Offer prunner is already stopped, no action will be performed")
		return
	}

	log.Info("Stopping offer pruner")
	p.stopPrunerChan <- struct{}{}

	// Wait for pruner to be stopped, should happen pretty quickly
	for {
		runningState := atomic.LoadInt32(&p.runningState)
		if runningState == runningStateRunning {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}

	log.Info("Offer pruner stopped")
}
