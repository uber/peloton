package cached

import (
	"sync"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/storage"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// JobFactory is the entrypoint object into the cache which stores job and tasks.
// This only runs in the job manager leader.
type JobFactory interface {
	// AddJob will create a Job if not present in cache, else returns the current cached Job.
	AddJob(id *peloton.JobID) Job

	// ClearJob cleans up the job from the cache
	ClearJob(jobID *peloton.JobID)

	// GetJob will return the current cached Job, and nil if currently not in cache.
	GetJob(id *peloton.JobID) Job

	// GetAllJobs returns the list of all jobs in cache
	GetAllJobs() map[string]Job

	// Start emitting metrics.
	Start()

	// Stop clears the current jobs and tasks in cache, stops metrics.
	Stop()
}

type jobFactory struct {
	sync.RWMutex //  Mutex to acquire before accessing any variables in the job factory object

	jobs        map[string]*job               // map of active jobs (job identifier -> cache job object) in the system
	running     bool                          // whether job factory is running
	jobStore    storage.JobStore              // storage job store object
	taskStore   storage.TaskStore             // storage task store object
	volumeStore storage.PersistentVolumeStore // storage volume store object
	mtx         *Metrics                      // cache metrics
	stopChan    chan struct{}                 // channel to indicate that the job factory needs to stop
}

// InitJobFactory initializes the singleton job factory object.
func InitJobFactory(
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	volumeStore storage.PersistentVolumeStore,
	parentScope tally.Scope) JobFactory {
	return &jobFactory{
		jobs:        map[string]*job{},
		jobStore:    jobStore,
		taskStore:   taskStore,
		volumeStore: volumeStore,
		mtx:         NewMetrics(parentScope.SubScope("cache")),
	}
}

func (f *jobFactory) AddJob(id *peloton.JobID) Job {
	f.Lock()
	defer f.Unlock()

	j, ok := f.jobs[id.GetValue()]
	if !ok {
		j = newJob(id, f)
		f.jobs[id.GetValue()] = j
	}

	return j
}

// ClearJob removes the job and all it tasks from inventory
func (f *jobFactory) ClearJob(id *peloton.JobID) {
	j := f.GetJob(id)
	if j == nil {
		return
	}

	j.ClearAllTasks()

	f.Lock()
	defer f.Unlock()
	delete(f.jobs, j.ID().GetValue())
}

func (f *jobFactory) GetJob(id *peloton.JobID) Job {
	f.RLock()
	defer f.RUnlock()

	if j, ok := f.jobs[id.GetValue()]; ok {
		return j
	}

	return nil
}

func (f *jobFactory) GetAllJobs() map[string]Job {
	f.RLock()
	defer f.RUnlock()

	jobMap := make(map[string]Job)
	for k, v := range f.jobs {
		jobMap[k] = v
	}
	return jobMap
}

// Start the job factory, starts emitting metrics.
func (f *jobFactory) Start() {
	f.Lock()
	defer f.Unlock()

	if f.running {
		return
	}
	f.running = true

	f.stopChan = make(chan struct{})
	go f.runPublishMetrics(f.stopChan)
	log.Info("job factory started")
}

// Stop clears the current jobs and tasks in cache, stops emitting metrics.
func (f *jobFactory) Stop() {
	f.Lock()
	defer f.Unlock()

	// Do not do anything if not runnning
	if !f.running {
		log.Info("job factory stopped")
		return
	}

	f.running = false
	for _, j := range f.jobs {
		j.ClearAllTasks()
	}
	f.jobs = map[string]*job{}
	close(f.stopChan)
	log.Info("job factory stopped")
}

//TODO Refactor to remove the metrics loop into a separate component.
// JobFactory should only implement an interface like MetricsProvides
// to periodically publish metrics instead of having its own go routine.
// runPublishMetrics is the entrypoint to start and stop publishing cache metrics
func (f *jobFactory) runPublishMetrics(stopChan <-chan struct{}) {
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
func (f *jobFactory) publishMetrics() {
	// Initialise tasks count map for all possible pairs of (state, goal_state)
	tCount := map[pbtask.TaskState]map[pbtask.TaskState]float64{}
	for s := range pbtask.TaskState_name {
		tCount[pbtask.TaskState(s)] = map[pbtask.TaskState]float64{}
		for gs := range pbtask.TaskState_name {
			tCount[pbtask.TaskState(s)][pbtask.TaskState(gs)] = 0.0
		}
	}

	// Iterate through jobs, tasks and count
	f.RLock()
	jCount := float64(len(f.jobs))
	for _, j := range f.jobs {
		j.RLock()
		for _, t := range j.tasks {
			t.RLock()
			tCount[t.runtime.GetState()][t.runtime.GetGoalState()]++
			t.RUnlock()
		}
		j.RUnlock()
	}
	f.RUnlock()

	// Publish
	f.mtx.scope.Gauge("jobs_count").Update(jCount)
	for s, sm := range tCount {
		for gs, tc := range sm {
			f.mtx.scope.Tagged(map[string]string{"state": s.String(), "goal_state": gs.String()}).Gauge("tasks_count").Update(tc)
		}
	}
}
