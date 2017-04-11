package storage

import (
	"github.com/uber-go/tally"
)

// Metrics is a struct for tracking all the general purpose counters that have relevance to the storage
// layer, i.e. how many jobs and tasks were created/deleted in the storage layer
type Metrics struct {
	JobCreate     tally.Counter
	JobCreateFail tally.Counter

	JobGet      tally.Counter
	JobGetFail  tally.Counter
	JobNotFound tally.Counter

	JobDelete     tally.Counter
	JobDeleteFail tally.Counter

	JobGetRuntime     tally.Counter
	JobGetRuntimeFail tally.Counter

	JobGetByState     tally.Counter
	JobGetByStateFail tally.Counter

	JobUpdateRuntime     tally.Counter
	JobUpdateRuntimeFail tally.Counter

	TaskCreate     tally.Counter
	TaskCreateFail tally.Counter

	TaskGet      tally.Counter
	TaskGetFail  tally.Counter
	TaskNotFound tally.Counter

	TaskDelete     tally.Counter
	TaskDeleteFail tally.Counter

	TaskUpdate     tally.Counter
	TaskUpdateFail tally.Counter

	// resource pool metrics
	ResourcePoolCreate     tally.Counter
	ResourcePoolCreateFail tally.Counter

	ResourcePoolGet     tally.Counter
	ResourcePoolGetFail tally.Counter
}

// NewMetrics returns a new Metrics struct, with all metrics initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) Metrics {
	jobScope := scope.SubScope("job")
	jobSuccessScope := jobScope.Tagged(map[string]string{"type": "success"})
	jobFailScope := jobScope.Tagged(map[string]string{"type": "fail"})
	jobNotFoundScope := jobScope.Tagged(map[string]string{"type": "not_found"})

	jobRuntimeScope := scope.SubScope("job_runtime")
	jobRuntimeSuccessScope := jobRuntimeScope.Tagged(map[string]string{"type": "success"})
	jobRuntimeFailScope := jobRuntimeScope.Tagged(map[string]string{"type": "fail"})

	taskScope := scope.SubScope("task")
	taskSuccessScope := taskScope.Tagged(map[string]string{"type": "success"})
	taskFailScope := taskScope.Tagged(map[string]string{"type": "fail"})
	taskNotFoundScope := taskScope.Tagged(map[string]string{"type": "not_found"})

	resourcePoolScope := scope.SubScope("resource_pool")
	resourcePoolSuccessScope := resourcePoolScope.Tagged(map[string]string{"type": "success"})
	resourcePoolFailScope := resourcePoolScope.Tagged(map[string]string{"type": "fail"})

	metrics := Metrics{
		JobCreate:     jobSuccessScope.Counter("create"),
		JobCreateFail: jobFailScope.Counter("create"),
		JobDelete:     jobSuccessScope.Counter("delete"),
		JobDeleteFail: jobFailScope.Counter("delete"),
		JobGet:        jobSuccessScope.Counter("get"),
		JobGetFail:    jobFailScope.Counter("get"),
		JobNotFound:   jobNotFoundScope.Counter("get"),

		JobGetRuntime:        jobRuntimeSuccessScope.Counter("get"),
		JobGetRuntimeFail:    jobRuntimeFailScope.Counter("get"),
		JobGetByState:        jobRuntimeScope.Counter("get_job_by_state"),
		JobGetByStateFail:    jobRuntimeFailScope.Counter("get_job_by_state"),
		JobUpdateRuntime:     jobRuntimeSuccessScope.Counter("update"),
		JobUpdateRuntimeFail: jobRuntimeFailScope.Counter("update"),

		TaskCreate:     taskSuccessScope.Counter("create"),
		TaskCreateFail: taskFailScope.Counter("create"),
		TaskGet:        taskSuccessScope.Counter("get"),
		TaskGetFail:    taskFailScope.Counter("get"),
		TaskDelete:     taskSuccessScope.Counter("delete"),
		TaskDeleteFail: taskFailScope.Counter("delete"),
		TaskUpdate:     taskSuccessScope.Counter("update"),
		TaskUpdateFail: taskFailScope.Counter("update"),
		TaskNotFound:   taskNotFoundScope.Counter("get"),

		ResourcePoolCreate:     resourcePoolSuccessScope.Counter("create"),
		ResourcePoolCreateFail: resourcePoolFailScope.Counter("create"),
		ResourcePoolGet:        resourcePoolScope.Counter("get"),
		ResourcePoolGetFail:    resourcePoolFailScope.Counter("get"),
	}
	return metrics
}
