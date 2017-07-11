package health

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"
)

// Heartbeat is the heartbeat interface
type Heartbeat interface {
	Start()
	Stop()
}

type heartbeat struct {
	sync.Mutex

	Running  atomic.Bool
	stopChan chan struct{}

	metrics           *Metrics
	heartbeatInterval time.Duration
}

var hb *heartbeat
var onceInitHeartbeat sync.Once

// InitHeartbeat inits heartbeat
func InitHeartbeat(
	parent tally.Scope,
	config Config) {
	onceInitHeartbeat.Do(func() {
		hb = &heartbeat{
			metrics:           NewMetrics(parent.SubScope("health")),
			heartbeatInterval: config.HeartbeatInterval,
		}
		hb.metrics.Init.Inc(1)
		hb.Start()
	})
}

func (*heartbeat) Start() {
	log.Info("Heartbeat start called.")

	hb.Lock()
	defer hb.Unlock()

	if hb.Running.Swap(true) {
		log.Warn("Heartbeater is already running, no-op.")
		return
	}

	go func() {
		defer hb.Running.Store(false)

		for {
			ticker := time.NewTimer(hb.heartbeatInterval)
			select {
			case <-hb.stopChan:
				log.Info("Heartbeater stopped.")
				return
			case t := <-ticker.C:
				log.WithField("tick", t).
					Debug("Emitting heartbeat.")
				hb.metrics.Heartbeat.Update(1)
			}
			ticker.Stop()
		}
	}()

	log.Info("Heartbeater started.")
}

func (*heartbeat) Stop() {
	log.Info("Heartbeat stop called.")

	if !hb.Running.Load() {
		log.Warn("Heartbeat is not running, no-op.")
		return
	}

	hb.Lock()
	defer hb.Unlock()

	log.Info("Stopping Heartbeat.")
	hb.stopChan <- struct{}{}

	for hb.Running.Load() {
		time.Sleep(1 * time.Millisecond)
	}

	log.Info("Heartbeat stopped.")
}
