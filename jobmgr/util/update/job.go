package update

import (
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
)

// HasUpdate returns if a job has any update going
func HasUpdate(jobRuntime *pbjob.RuntimeInfo) bool {
	return jobRuntime.GetUpdateID() != nil &&
		len(jobRuntime.GetUpdateID().GetValue()) > 0
}

// HasFailedUpdate returns if a task has failed during an update due
// to too many failures.
// If maxAttempts is 0, HasFailedUpdate always returns false.
func HasFailedUpdate(
	taskRuntime *pbtask.RuntimeInfo,
	maxAttempts uint32) bool {
	if maxAttempts == 0 {
		return false
	}

	return taskRuntime.GetFailureCount() >= maxAttempts
}
