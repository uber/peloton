package storage

import (
	mesos_v1 "mesos/v1"
	"peloton/job"
	"peloton/task"
)

// JobStore is the interface to store job states
type JobStore interface {
	CreateJob(id *job.JobID, Config *job.JobConfig, createBy string) error
	GetJob(id *job.JobID) (*job.JobConfig, error)
	Query(Labels *mesos_v1.Labels) (map[string]*job.JobConfig, error)
	DeleteJob(id *job.JobID) error
	GetJobsByOwner(owner string) (map[string]*job.JobConfig, error)
	GetAllJobs() (map[string]*job.JobConfig, error)
}

// TaskStore is the interface to store task states
type TaskStore interface {
	CreateTask(id *job.JobID, instanceID int, taskInfo *task.TaskInfo, createdBy string) error
	GetTasksForJob(id *job.JobID) (map[uint32]*task.TaskInfo, error)
	GetTasksForJobAndState(id *job.JobID, state string) (map[uint32]*task.TaskInfo, error)
	GetTasksForJobByRange(id *job.JobID, Range *task.InstanceRange) (map[uint32]*task.TaskInfo, error)
	GetTaskForJob(id *job.JobID, instanceID uint32) (map[uint32]*task.TaskInfo, error)
	UpdateTask(taskInfo *task.TaskInfo) error
	GetTaskByID(taskID string) (*task.TaskInfo, error)
}

// FrameworkInfoStore is the interface to store mesosStreamID for peloton frameworks
type FrameworkInfoStore interface {
	SetMesosStreamID(frameworkName string, mesosStreamID string) error
	SetMesosFrameworkID(frameworkName string, frameworkID string) error
	GetMesosStreamID(frameworkName string) (string, error)
	GetFrameworkID(frameworkName string) (string, error)
}
