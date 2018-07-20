package cached

import (
	"sync"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbupdate "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"

	"code.uber.internal/infra/peloton/common/lifecycle"
	"code.uber.internal/infra/peloton/storage"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// UpdateFactory is the entrypoint object into the cache which stores job updates.
// This only runs in the job manager leader.
type UpdateFactory interface {
	// AddUpdate will create an Update if not present in cache,
	// else returns the current cached Update.
	AddUpdate(id *peloton.UpdateID) Update

	// ClearUpdate cleans up the update from the cache.
	ClearUpdate(id *peloton.UpdateID)

	// GetUpdate will return the current cached Update,
	// and nil if currently not in cache.
	GetUpdate(id *peloton.UpdateID) Update

	// GetAllUpdates returns a map of current updates in the cache.
	GetAllUpdates() map[string]Update

	// Start emitting metrics.
	Start()

	// Stop clears the current updates in cache, stops metrics.
	Stop()
}

type updateFactory struct {
	//  Mutex to acquire before accessing any variables
	// in the update factory object
	sync.RWMutex

	// map of active updates (update identifier -> cache update object)
	// in the system
	updates map[string]*update

	jobStore    storage.JobStore    // storage job store object
	taskStore   storage.TaskStore   // storage task store object
	updateStore storage.UpdateStore // storage update store object
	jobFactory  JobFactory          // the job factory in the cache
	mtx         *Metrics            // cache metrics

	// lifecycle manager
	lifeCycle lifecycle.LifeCycle
}

// InitUpdateFactory initializes the update factory object.
func InitUpdateFactory(
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	updateStore storage.UpdateStore,
	jobFactory JobFactory,
	parentScope tally.Scope) UpdateFactory {
	return &updateFactory{
		updates:     map[string]*update{},
		jobStore:    jobStore,
		taskStore:   taskStore,
		updateStore: updateStore,
		jobFactory:  jobFactory,
		mtx:         NewMetrics(parentScope.SubScope("cache")),
		lifeCycle:   lifecycle.NewLifeCycle(),
	}
}

func (f *updateFactory) AddUpdate(id *peloton.UpdateID) Update {
	f.Lock()
	defer f.Unlock()

	u, ok := f.updates[id.GetValue()]
	if !ok {
		u = newUpdate(id, f.jobFactory, f)
		f.updates[id.GetValue()] = u
	}

	return u
}

func (f *updateFactory) ClearUpdate(id *peloton.UpdateID) {
	f.Lock()
	defer f.Unlock()

	delete(f.updates, id.GetValue())
}

func (f *updateFactory) GetUpdate(id *peloton.UpdateID) Update {
	f.RLock()
	defer f.RUnlock()

	if update, ok := f.updates[id.GetValue()]; ok {
		return update
	}
	return nil
}

func (f *updateFactory) GetAllUpdates() map[string]Update {
	f.RLock()
	defer f.RUnlock()

	updateMap := make(map[string]Update)
	for k, v := range f.updates {
		updateMap[k] = v
	}
	return updateMap
}

// Start the update factory, starts emitting metrics.
func (f *updateFactory) Start() {
	if !f.lifeCycle.Start() {
		return
	}

	go f.runPublishMetrics(f.lifeCycle.StopCh())
	log.Info("update factory started")
}

// Stop clears the current updates in cache, stops emitting metrics.
func (f *updateFactory) Stop() {
	f.Lock()
	defer f.Unlock()

	// Do not do anything if not runnning
	if !f.lifeCycle.Stop() {
		log.Info("update factory stopped")
		return
	}

	f.updates = map[string]*update{}
	log.Info("update factory stopped")
}

//TODO Refactor to remove the metrics loop into a separate component.
// UpdateFactory should only implement an interface like MetricsProvides
// to periodically publish metrics instead of having its own go routine.
// runPublishMetrics is the entrypoint to start and stop publishing cache metrics
func (f *updateFactory) runPublishMetrics(stopChan <-chan struct{}) {
	ticker := time.NewTicker(_defaultMetricsUpdateTick)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			f.publishMetrics()
		case <-stopChan:
			return
		}
	}
}

// publishMetrics is the routine which publishes cache metrics to M3
func (f *updateFactory) publishMetrics() {
	// Initialise update count map for all possible states
	uCount := map[pbupdate.State]float64{}
	for s := range pbupdate.State_name {
		uCount[pbupdate.State(s)] = float64(0)
	}

	// Iterate through updates and count
	f.RLock()
	uTotal := float64(len(f.updates))
	for _, u := range f.updates {
		uCount[u.GetState().State]++
	}
	f.RUnlock()

	// Publish
	f.mtx.scope.Gauge("updates_total").Update(uTotal)
	for s, v := range uCount {
		f.mtx.scope.Tagged(map[string]string{"state": s.String()}).Gauge("updates_count").Update(v)
	}
}
