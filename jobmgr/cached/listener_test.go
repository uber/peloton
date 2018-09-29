package cached

import (
	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
)

type FakeJobListener struct {
	jobID      *peloton.JobID
	jobType    pbjob.JobType
	jobRuntime *pbjob.RuntimeInfo
}

func (l *FakeJobListener) Name() string {
	return "fake_job_listener"
}

func (l *FakeJobListener) JobRuntimeChanged(
	jobID *peloton.JobID,
	jobType pbjob.JobType,
	runtime *pbjob.RuntimeInfo) {
	l.jobID = jobID
	l.jobType = jobType
	l.jobRuntime = runtime
}

func (l *FakeJobListener) TaskRuntimeChanged(
	jobID *peloton.JobID,
	instanceID uint32,
	jobType pbjob.JobType,
	runtime *pbtask.RuntimeInfo) {
}

func (l *FakeJobListener) Reset() {
	l.jobID = nil
	l.jobRuntime = nil
}

type FakeTaskListener struct {
	jobID       *peloton.JobID
	jobType     pbjob.JobType
	instanceID  uint32
	taskRuntime *pbtask.RuntimeInfo
}

func (l *FakeTaskListener) Name() string {
	return "fake_task_listener"
}

func (l *FakeTaskListener) JobRuntimeChanged(
	jobID *peloton.JobID,
	jobType pbjob.JobType,
	runtime *pbjob.RuntimeInfo) {
}

func (l *FakeTaskListener) TaskRuntimeChanged(
	jobID *peloton.JobID,
	instanceID uint32,
	jobType pbjob.JobType,
	runtime *pbtask.RuntimeInfo) {
	l.jobID = jobID
	l.instanceID = instanceID
	l.jobType = jobType
	l.taskRuntime = runtime
}
