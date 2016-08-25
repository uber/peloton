package storage

import (
	"database/sql"
	"encoding/json"
	"peloton/job"
	"peloton/task"
	"time"
)

type BaseRowRecord struct {
	AddedId     int       `db:"added_id"`
	RowKey      string    `db:"row_key"`
	ColKey      string    `db:"col_key"`
	RefKey      int       `db:"ref_key"`
	Body        string    `db:"body"`
	Flags       int       `db:"flags"`
	CreatedBy   string    `db:"created_by"`
	CreatedTime time.Time `db:"create_time"`
	UpdateTime  time.Time `db:"update_time"`
}

type TaskRecord struct {
	BaseRowRecord
	JobId     sql.NullString `db:"job_id"`
	TaskState sql.NullString `db:"task_state"`
	TaskHost  sql.NullString `db:"task_host"`
}

type JobRecord struct {
	BaseRowRecord
	OwningTeam    sql.NullString `db:"owning_team"`
	LabelsSummary sql.NullString `db:"labels_summary"`
}

// GetRuntimeInfo returns the task.RuntimeInfo from a TaskRecord table record
func (t *TaskRecord) GetRuntimeInfo() (*task.RuntimeInfo, error) {
	buffer := []byte(t.Body)
	var result task.RuntimeInfo
	err := json.Unmarshal(buffer, &result)
	if err != nil {
		return nil, nil
	}
	return &result, err
}

// GetJobConfig returns the job.JobConfig from a TaskRecord table record
func (t *JobRecord) GetJobConfig() (*job.JobConfig, error) {
	buffer := []byte(t.Body)
	var result job.JobConfig
	err := json.Unmarshal(buffer, &result)
	if err != nil {
		return nil, nil
	}
	return &result, err
}
