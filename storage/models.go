package storage

import (
	"database/sql"
	"encoding/json"
	"peloton/api/job"
	"peloton/api/task"
	"reflect"
	"time"

	log "github.com/Sirupsen/logrus"
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
	JobID       sql.NullString `db:"job_id"`
	TaskState   sql.NullString `db:"task_state"`
	TaskHost    sql.NullString `db:"task_host"`
	MesosTaskID sql.NullString `db:"mesos_task_id"`
	InstanceID  int            `db:"instance_id"`
}

// JobRecord also contains the generated columns for jobs table
type JobRecord struct {
	BaseRowRecord
	OwningTeam    sql.NullString `db:"owning_team"`
	LabelsSummary sql.NullString `db:"labels_summary"`
}

// GetTaskInfo returns the task.TaskInfo from a TaskRecord table record
func (t *TaskRecord) GetTaskInfo() (*task.TaskInfo, error) {
	result, err := UnmarshalToType(t.Body, reflect.TypeOf(task.TaskInfo{}))
	if err != nil {
		return nil, nil
	}
	return result.(*task.TaskInfo), err
}

// GetJobConfig returns the job.JobConfig from a JobRecord table record
func (t *JobRecord) GetJobConfig() (*job.JobConfig, error) {
	result, err := UnmarshalToType(t.Body, reflect.TypeOf(job.JobConfig{}))
	if err != nil {
		return nil, nil
	}
	return result.(*job.JobConfig), err
}

// UnmarshalToType unmarshal a string to a typed interface{}
func UnmarshalToType(jsonString string, resultType reflect.Type) (interface{}, error) {
	result := reflect.New(resultType)
	err := json.Unmarshal([]byte(jsonString), result.Interface())
	if err != nil {
		log.Errorf("Unmarshal failed with error %v, type %v, jsonString %v", err, resultType, jsonString)
		return nil, nil
	}
	return result.Interface(), nil
}

// UnmarshalStringArray unmarshal a string array to a typed array, in interface{}
func UnmarshalStringArray(jsonStrings []string, resultType reflect.Type) ([]interface{}, error) {
	var results []interface{}
	for _, jsonString := range jsonStrings {
		result, err := UnmarshalToType(jsonString, resultType)
		if err != nil {
			return nil, nil
		}
		results = append(results, result)
	}
	return results, nil
}

// ToTaskInfos Convert a string array into an array of *task.TaskInfo
func ToTaskInfos(jsonStrings []string) ([]*task.TaskInfo, error) {
	resultType := reflect.TypeOf(task.TaskInfo{})
	results, err := UnmarshalStringArray(jsonStrings, resultType)
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
	results, err := UnmarshalStringArray(jsonStrings, resultType)
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
