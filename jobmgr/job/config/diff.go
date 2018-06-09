package jobconfig

import (
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/jobmgr/task"
)

// JobDiff holds the difference between two job configs
type JobDiff struct {
	// instanceID->runtime info, which includes the
	// runtimeInfo of the new instances in the new job config
	InstancesToAdd map[uint32]*pbtask.RuntimeInfo
}

// CalculateJobDiff returns the difference between 2 job configs
func CalculateJobDiff(
	id *peloton.JobID,
	oldConfig *job.JobConfig,
	newConfig *job.JobConfig) JobDiff {
	var jobDiff JobDiff

	instanceToAdd := getInstancesToAdd(id, oldConfig, newConfig)

	jobDiff.InstancesToAdd = instanceToAdd
	return jobDiff
}

// IsNoop checks whether this diff contains no work to be done
func (jd *JobDiff) IsNoop() bool {
	return len(jd.InstancesToAdd) == 0
}

func getInstancesToAdd(
	jobID *peloton.JobID,
	oldConfig *job.JobConfig,
	newConfig *job.JobConfig) map[uint32]*pbtask.RuntimeInfo {

	instancesToAdd := make(map[uint32]*pbtask.RuntimeInfo)
	if oldConfig.InstanceCount == newConfig.InstanceCount {
		return instancesToAdd
	}

	for i := oldConfig.InstanceCount; i < newConfig.InstanceCount; i++ {
		instancesToAdd[i] = task.CreateInitializingTask(jobID, i, newConfig)
	}
	return instancesToAdd
}
