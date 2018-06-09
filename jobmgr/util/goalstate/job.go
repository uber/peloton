package goalstate

import "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"

// GetDefaultJobGoalState returns the goal state of a job
// depending on the job type
func GetDefaultJobGoalState(jobType job.JobType) job.JobState {
	switch jobType {
	case job.JobType_BATCH:
		return job.JobState_SUCCEEDED
	default:
		return job.JobState_RUNNING
	}
}
