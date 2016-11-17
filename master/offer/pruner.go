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
		stopFlag:           0,
		offerPruningPeriod: offerPruningPeriod,
	}
	return pruner
}

// offerPruner implements OfferPruner
type offerPruner struct {
	sync.Mutex

	runningState       int32
	stopFlag           int32
	pool               OfferPool
	offerPruningPeriod time.Duration
}

// Start starts offer pruning process
func (p *offerPruner) Start() {
	defer p.Unlock()
	p.Lock()

	if p.runningState == RunningState_Running {
		log.Warn("Offer prunner is already running, no action")
		return
	}

	started := make(chan int, 1)
	go func() {
		defer atomic.StoreInt32(&p.runningState, RunningState_NotStarted)
		atomic.StoreInt32(&p.runningState, RunningState_Running)

		log.Info("Starting offer pruning loop")
		started <- 0
		for {
			log.Debug("Running offer pruning loop")

			stopFlag := atomic.LoadInt32(&p.stopFlag)
			if stopFlag != 0 {
				log.Info("Exiting offer pruning loop")
				return
			}

			offersIDsToDecline, offersToDecline := p.pool.RemoveExpiredOffers()
			if len(offersToDecline) != 0 {
				log.Debugf("Offers to decline: %v", offersIDsToDecline)
				p.pool.DeclineOffers(offersIDsToDecline, offersToDecline)
			}

			time.Sleep(p.offerPruningPeriod)
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
		log.Warn("Offer prunner is already stopped, no action")
		return
	}

	log.Info("Stopping offer pruner")
	p.stopFlag = 1

	// Wait for pruner to be stopped
	// TODO: see if we can improve the locking effciency
	for {
		runningState := atomic.LoadInt32(&p.runningState)
		if runningState == RunningState_Running {
			time.Sleep(5 * time.Second)
		} else {
			break
		}
	}
	p.stopFlag = 0
	log.Info("Offer pruner stopped")
}
