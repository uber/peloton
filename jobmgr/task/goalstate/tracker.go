package goalstate

import (
	"sync"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
)

// newTracker returns a new job tracker.
func newTracker() *tracker {
	return &tracker{
		jobMap: map[string]*trackedJob{},
	}
}

// tracker is the job tracker.
type tracker struct {
	sync.Mutex

	// jobMap is map from peloton job id -> job.
	jobMap map[string]*trackedJob
}

func (t *tracker) addOrGetJob(id *peloton.JobID, e *engine) *trackedJob {
	t.Lock()
	defer t.Unlock()

	j, ok := t.jobMap[id.GetValue()]
	if !ok {
		j = newTrackedJob(id, e)
		t.jobMap[id.GetValue()] = j
	}

	return j
}

func (t *tracker) getJob(id *peloton.JobID) *trackedJob {
	t.Lock()
	defer t.Unlock()

	return t.jobMap[id.GetValue()]
}

func (t *tracker) startAll(stopChan <-chan struct{}) {
	t.Lock()
	defer t.Unlock()

	for _, j := range t.jobMap {
		j.start(stopChan)
	}
}
