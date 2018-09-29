package cached

import (
	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
)

// JobTaskListener defines an interface that must to be implemented by
// a listener interested in job and task changes. The callbacks are
// invoked after updates to the cache are written through to the
// persistent store. Note that callbacks may not get invoked in the
// same order as the changes to objects in cache; the version field
// of the changed object (e.g. Changelog) is a better indicator of
// order. To keep things simple, the callbacks are invoked
// synchronously when the cached object is changed. Thus slow
// listeners can make operations on the cache slower, which must be
// avoided.
// Implementations must not
// - modify the provided objects in any way
// - do processing that can take a long time, such as blocking on
//   locks or making remote calls. Such activities must be done
//   in separate goroutines that are managed by the listener.
type JobTaskListener interface {
	// Name returns a user-friendly name for the listener
	Name() string

	// JobRuntimeChanged is invoked when the runtime for a job is updated
	// in cache and persistent store.
	JobRuntimeChanged(
		jobID *peloton.JobID,
		jobType pbjob.JobType,
		runtime *pbjob.RuntimeInfo)

	// TaskRuntimeChanged is invoked when the runtime for a task is updated
	// in cache and persistent store.
	TaskRuntimeChanged(
		jobID *peloton.JobID,
		instanceID uint32,
		jobType pbjob.JobType,
		runtime *pbtask.RuntimeInfo)
}
