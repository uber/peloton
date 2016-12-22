package stapi

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/stapi-go.git"
	"code.uber.internal/infra/stapi-go.git/api"
	//qb "code.uber.internal/infra/stapi-go.git/querybuilder"
	mesos "code.uber.internal/infra/peloton/pbgen/src/mesos/v1"
	"code.uber.internal/infra/peloton/pbgen/src/peloton/job"
	"code.uber.internal/infra/peloton/pbgen/src/peloton/task"
	sc "code.uber.internal/infra/stapi-go.git/config"
)

// Config is the config for STAPIStore
type Config struct {
	Stapi     sc.Configuration `yaml:"stapi"`
	StoreName string           `yaml:"store_name"`
}

// Store implements JobStore using a cassandra backend
type Store struct {
	DataStore api.DataStore
}

// NewStore creates a Store
func NewStore(config *Config) (*Store, error) {
	storage.Initialize(storage.Options{
		Cfg:    config.Stapi,
		AppID:  "peloton",
		Logger: log.DefaultLogger(),
	})
	dataStore, err := storage.OpenDataStore(config.StoreName)
	if err != nil {
		log.Errorf("Failed to NewSTAPIStore, err=%v", err)
		return nil, err
	}
	return &Store{DataStore: dataStore}, nil
}

// CreateJob creates a job with the job id and the config value
func (s *Store) CreateJob(id *job.JobID, Config *job.JobConfig, createBy string) error {
	return nil
}

// GetJob returns a job config given the job id
func (s *Store) GetJob(id *job.JobID) (*job.JobConfig, error) {
	return nil, nil
}

// Query returns all jobs that contains the Labels.
func (s *Store) Query(Labels *mesos.Labels) (map[string]*job.JobConfig, error) {
	return nil, nil
}

// DeleteJob deletes a job by id
func (s *Store) DeleteJob(id *job.JobID) error {
	return nil
}

// GetJobsByOwner returns jobs by owner
func (s *Store) GetJobsByOwner(owner string) (map[string]*job.JobConfig, error) {
	return nil, nil
}

// GetAllJobs returns all jobs
func (s *Store) GetAllJobs() (map[string]*job.JobConfig, error) {
	return nil, nil
}

// CreateTask creates a task for a peloton job
func (s *Store) CreateTask(id *job.JobID, instanceID int, taskInfo *task.TaskInfo, createdBy string) error {
	return nil
}

// GetTasksForJob returns the tasks (tasks.TaskInfo) for a peloton job
func (s *Store) GetTasksForJob(id *job.JobID) (map[uint32]*task.TaskInfo, error) {
	return nil, nil
}

// GetTasksForJobAndState returns the tasks (runtime_config) for a peloton job with certain state
func (s *Store) GetTasksForJobAndState(id *job.JobID, state string) (map[uint32]*task.TaskInfo, error) {
	return nil, nil
}

// GetTasksForJobByRange returns the tasks (tasks.TaskInfo) for a peloton job given instance id range
func (s *Store) GetTasksForJobByRange(id *job.JobID, Range *task.InstanceRange) (map[uint32]*task.TaskInfo, error) {
	return nil, nil
}

// GetTaskForJob returns all tasks for a job
func (s *Store) GetTaskForJob(id *job.JobID, instanceID uint32) (map[uint32]*task.TaskInfo, error) {
	return nil, nil
}

// UpdateTask updates a task for a peloton job
func (s *Store) UpdateTask(taskInfo *task.TaskInfo) error {
	return nil
}

// GetTaskByID returns the tasks (tasks.TaskInfo) for a peloton job
func (s *Store) GetTaskByID(taskID string) (*task.TaskInfo, error) {
	return nil, nil
}

//SetMesosStreamID stores the mesos framework id for a framework name
func (s *Store) SetMesosStreamID(frameworkName string, mesosStreamID string) error {
	return nil
}

//SetMesosFrameworkID stores the mesos framework id for a framework name
func (s *Store) SetMesosFrameworkID(frameworkName string, frameworkID string) error {
	return nil
}

//GetMesosStreamID reads the mesos stream id for a framework name
func (s *Store) GetMesosStreamID(frameworkName string) (string, error) {
	return "", nil
}

//GetFrameworkID reads the framework id for a framework name
func (s *Store) GetFrameworkID(frameworkName string) (string, error) {
	return "", nil
}
