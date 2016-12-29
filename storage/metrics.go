package storage

import (
	"github.com/uber-go/tally"
)

// Metrics is a struct for tracking all the general purpose counters that have relevance to the storage
// layer, i.e. how many jobs and tasks were created/deleted in the storage layer
type Metrics struct {
	JobCreate     tally.Counter
	JobCreateFail tally.Counter
	JobGet        tally.Counter
	JobGetFail    tally.Counter
	JobDelete     tally.Counter
	JobDeleteFail tally.Counter

	TaskCreate     tally.Counter
	TaskCreateFail tally.Counter
	TaskGet        tally.Counter
	TaskGetFail    tally.Counter
	TaskDelete     tally.Counter
	TaskDeleteFail tally.Counter
	TaskUpdate     tally.Counter
	TaskUpdateFail tally.Counter
}

// NewMetrics returns a new Metrics struct, with all metrics initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) Metrics {
	jobScope := scope.SubScope("job")
	jobSuccessScope := jobScope.Tagged(map[string]string{"type": "success"})
	jobFailScope := jobScope.Tagged(map[string]string{"type": "fail"})
	taskScope := scope.SubScope("task")
	taskSuccessScope := taskScope.Tagged(map[string]string{"type": "success"})
	taskFailScope := taskScope.Tagged(map[string]string{"type": "fail"})
	metrics := Metrics{
		JobCreate:      jobSuccessScope.Counter("create"),
		JobCreateFail:  jobFailScope.Counter("create"),
		JobDelete:      jobSuccessScope.Counter("delete"),
		JobDeleteFail:  jobFailScope.Counter("delete"),
		JobGet:         jobSuccessScope.Counter("get"),
		JobGetFail:     jobFailScope.Counter("get"),
		TaskCreate:     taskSuccessScope.Counter("create"),
		TaskCreateFail: taskFailScope.Counter("create"),
		TaskGet:        taskSuccessScope.Counter("get"),
		TaskGetFail:    taskFailScope.Counter("get"),
		TaskDelete:     taskSuccessScope.Counter("delete"),
		TaskDeleteFail: taskFailScope.Counter("delete"),
		TaskUpdate:     taskSuccessScope.Counter("update"),
		TaskUpdateFail: taskFailScope.Counter("update"),
	}
	return metrics
}
