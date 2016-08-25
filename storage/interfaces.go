package storage

import (
	mesos_v1 "mesos/v1"
	"peloton/job"
	"peloton/task"
)

// JobStore is the interface to store job states
type JobStore interface {
	CreateJob(id *job.JobID, Config *job.JobConfig) (err error)
	GetJob(id *job.JobID) (job *job.JobConfig, err error)
	Query(Labels *mesos_v1.Labels) (jobs []*job.JobConfig, err error)
	DeleteJob(id *job.JobID) (err error)
	GetJobsByOwner(owner string) (jobs []*job.JobConfig, err error)
}

// TaskStore is the interface to store task states
type TaskStore interface {
	CreateTask(id *job.JobID, Config *job.JobConfig) (err error)
	GetTasksForJob(id *job.JobID) (tasks []*task.RuntimeInfo, err error)
	GetTasksForJobAndState(id *job.JobID, state string) (tasks []*task.RuntimeInfo, err error)
	UpdateTask(taskInfo *task.RuntimeInfo) (err error)
}
