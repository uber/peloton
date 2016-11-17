package offer

import (
	"go.uber.org/yarpc"
	"sync"
	"sync/atomic"
	"time"

	"code.uber.internal/go-common.git/x/log"
)

const (
	RunningState_NotStarted = 0
	RunningState_Running    = 1
)

// OfferPruner prunes offers
type OfferPruner interface {
	Start()
	Stop()
}

// NewOfferPruner initiates an instance of OfferPruner
func NewOfferPruner(pool OfferPool, offerPruningPeriod time.Duration, d yarpc.Dispatcher) OfferPruner {
	pruner := &offerPruner{
		pool:               pool,
		runningState:       RunningState_NotStarted,
		offerPruningPeriod: offerPruningPeriod,
		stopPrunerChan:     make(chan struct{}, 1),
	}
	return pruner
}

// offerPruner implements OfferPruner
type offerPruner struct {
	sync.Mutex

	runningState       int32
	pool               OfferPool
	offerPruningPeriod time.Duration
	stopPrunerChan     chan struct{}
}

// Start starts offer pruning process
func (p *offerPruner) Start() {
	defer p.Unlock()
	p.Lock()

	if p.runningState == RunningState_Running {
		log.Warn("Offer prunner is already running, no action will be performed")
		return
	}

	started := make(chan int, 1)
	go func() {
		defer atomic.StoreInt32(&p.runningState, RunningState_NotStarted)
		atomic.StoreInt32(&p.runningState, RunningState_Running)

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
				offersToDecline := p.pool.RemoveExpiredOffers()
				if len(offersToDecline) != 0 {
					log.Debugf("Offers to decline: %v", offersToDecline)
					p.pool.DeclineOffers(offersToDecline)
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

	if p.runningState == RunningState_NotStarted {
		log.Warn("Offer prunner is already stopped, no action will be performed")
		return
	}

	log.Info("Stopping offer pruner")
	p.stopPrunerChan <- struct{}{}

	// Wait for pruner to be stopped, should happen pretty quickly
	for {
		runningState := atomic.LoadInt32(&p.runningState)
		if runningState == RunningState_Running {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}

	log.Info("Offer pruner stopped")
}
