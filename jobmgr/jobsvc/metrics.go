package jobsvc

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track
// internal state of the job service
type Metrics struct {
	JobAPICreate  tally.Counter
	JobCreate     tally.Counter
	JobCreateFail tally.Counter
	JobAPIGet     tally.Counter
	JobGet        tally.Counter
	JobGetFail    tally.Counter
	JobAPIDelete  tally.Counter
	JobDelete     tally.Counter
	JobDeleteFail tally.Counter
	JobAPIQuery   tally.Counter
	JobQuery      tally.Counter
	JobQueryFail  tally.Counter
	JobUpdate     tally.Counter
	JobUpdateFail tally.Counter

	JobAPIGetByRespoolID  tally.Counter
	JobGetByRespoolID     tally.Counter
	JobGetByRespoolIDFail tally.Counter

	// TODO: find a better way of organizing metrics per package
	TaskCreate     tally.Counter
	TaskCreateFail tally.Counter
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	jobSuccessScope := scope.Tagged(map[string]string{"type": "success"})
	jobFailScope := scope.Tagged(map[string]string{"type": "fail"})
	jobAPIScope := scope.SubScope("api")

	// TODO: find a better way of organizing metrics per package so we
	// don't have to nest task scope under job scope here.
	taskScope := scope.SubScope("task")
	taskSuccessScope := taskScope.Tagged(map[string]string{"type": "success"})
	taskFailScope := taskScope.Tagged(map[string]string{"type": "fail"})

	return &Metrics{
		JobAPICreate:   jobAPIScope.Counter("create"),
		JobCreate:      jobSuccessScope.Counter("create"),
		JobCreateFail:  jobFailScope.Counter("create"),
		JobAPIGet:      jobAPIScope.Counter("get"),
		JobGet:         jobSuccessScope.Counter("get"),
		JobGetFail:     jobFailScope.Counter("get"),
		JobAPIDelete:   jobAPIScope.Counter("delete"),
		JobDelete:      jobSuccessScope.Counter("delete"),
		JobDeleteFail:  jobFailScope.Counter("delete"),
		JobAPIQuery:    jobAPIScope.Counter("query"),
		JobQuery:       jobSuccessScope.Counter("query"),
		JobQueryFail:   jobFailScope.Counter("query"),
		JobUpdate:      jobSuccessScope.Counter("update"),
		JobUpdateFail:  jobFailScope.Counter("update"),
		TaskCreate:     taskSuccessScope.Counter("create"),
		TaskCreateFail: taskFailScope.Counter("create"),

		JobAPIGetByRespoolID:  jobAPIScope.Counter("get_by_respool_id"),
		JobGetByRespoolID:     jobSuccessScope.Counter("get_by_respool_id"),
		JobGetByRespoolIDFail: jobFailScope.Counter("get_by_respool_id"),
	}
}
