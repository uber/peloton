package mysql

import (
	"database/sql"
	"reflect"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/util"
)

// BaseRowRecord contains the common fields of table tasks and table jobs
type BaseRowRecord struct {
	AddedID     int       `db:"added_id"`
	RowKey      string    `db:"row_key"`
	ColKey      string    `db:"col_key"`
	RefKey      int       `db:"ref_key"`
	Body        string    `db:"body"`
	Flags       int       `db:"flags"`
	CreatedBy   string    `db:"created_by"`
	CreatedTime time.Time `db:"create_time"`
	UpdateTime  time.Time `db:"update_time"`
}

// TaskRecord also contains the virtual generated columns for tasks table
type TaskRecord struct {
	BaseRowRecord
	JobID         sql.NullString `db:"job_id"`
	TaskGoalState sql.NullString `db:"task_goal_state"`
	TaskState     sql.NullString `db:"task_state"`
	TaskHost      sql.NullString `db:"task_host"`
	MesosTaskID   sql.NullString `db:"mesos_task_id"`
	InstanceID    int            `db:"instance_id"`
}

// JobRecord also contains the generated columns for jobs table
type JobRecord struct {
	BaseRowRecord
	OwningTeam    sql.NullString `db:"owning_team"`
	LabelsSummary sql.NullString `db:"labels_summary"`
}

// JobRuntimeRecord contains job runtime info
type JobRuntimeRecord struct {
	RowKey      string         `db:"row_key"`
	Runtime     string         `db:"runtime"`
	CreatedTime time.Time      `db:"create_time"`
	UpdateTime  time.Time      `db:"update_time"`
	JobState    sql.NullString `db:"job_state"`
}

// ResourcePoolRecord also contains the generated columns for respool table
type ResourcePoolRecord struct {
	BaseRowRecord
	OwningTeam sql.NullString `db:"owning_team"`
}

// GetTaskInfo returns the task.TaskInfo from a TaskRecord table record
func (t *TaskRecord) GetTaskInfo() (*task.TaskInfo, error) {
	result, err := util.UnmarshalToType(t.Body, reflect.TypeOf(task.TaskInfo{}))
	if err != nil {
		return nil, err
	}
	return result.(*task.TaskInfo), err
}

// GetJobConfig returns the job.JobConfig from a JobRecord table record
func (t *JobRecord) GetJobConfig() (*job.JobConfig, error) {
	result, err := util.UnmarshalToType(t.Body, reflect.TypeOf(job.JobConfig{}))
	if err != nil {
		return nil, err
	}
	return result.(*job.JobConfig), err
}

// GetJobRuntime returns the job.Runtime from a JobRecord table record
func (t *JobRuntimeRecord) GetJobRuntime() (*job.RuntimeInfo, error) {
	val, err := util.UnmarshalToType(t.Runtime, reflect.TypeOf(job.RuntimeInfo{}))
	if err != nil {
		return nil, err
	}
	result := val.(*job.RuntimeInfo)
	if result.TaskStats == nil {
		result.TaskStats = make(map[string]uint32)
	}
	return result, err
}

// GetResPoolConfig returns the respool.ResourceConfig from a resourcePoolTable
func (t *ResourcePoolRecord) GetResPoolConfig() (*respool.ResourcePoolConfig, error) {
	result, err := util.UnmarshalToType(t.Body, reflect.TypeOf(respool.ResourcePoolConfig{}))
	if err != nil {
		return nil, err
	}
	return result.(*respool.ResourcePoolConfig), err
}

// ToTaskInfos Convert a string array into an array of *task.TaskInfo
func ToTaskInfos(jsonStrings []string) ([]*task.TaskInfo, error) {
	resultType := reflect.TypeOf(task.TaskInfo{})
	results, err := util.UnmarshalStringArray(jsonStrings, resultType)
	if err != nil {
		return nil, err
	}
	var finalResult []*task.TaskInfo
	for _, result := range results {
		finalResult = append(finalResult, result.(*task.TaskInfo))
	}
	return finalResult, nil
}

// ToJobConfigs Convert a string array into an array of *job.JobConfig
func ToJobConfigs(jsonStrings []string) ([]*job.JobConfig, error) {
	resultType := reflect.TypeOf(job.JobConfig{})
	results, err := util.UnmarshalStringArray(jsonStrings, resultType)
	if err != nil {
		return nil, err
	}
	var finalResult []*job.JobConfig
	for _, result := range results {
		finalResult = append(finalResult, result.(*job.JobConfig))
	}
	return finalResult, nil
}

// MesosFrameworkInfo stores the framework id and mesos stream id information
type MesosFrameworkInfo struct {
	FrameworkName sql.NullString `db:"framework_name"`
	FrameworkID   sql.NullString `db:"framework_id"`
	MesosStreamID sql.NullString `db:"mesos_stream_id"`
	UpdateTime    time.Time      `db:"update_time"`
	UpdateHost    sql.NullString `db:"update_host"`
}
