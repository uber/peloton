package stapi

import (
	"time"
)

// JobRecord correspond to a peloton job
type JobRecord struct {
	JobID        string    //`cql:"job_id"`
	JobConfig    string    //`cql:"job_config"`
	Owner        string    //`cql:"owner"`
	CreatedTime  time.Time //`cql:"create_time"`
	Labels       string    //`cql:"labels"`
	CompleteTime time.Time //`cql:"complete_time"`
	TaskSummary  string    //`cql:"task_summary"`
}

// TaskRecord correspond to a peloton task
type TaskRecord struct {
	TaskID     string    //`cql:"task_id"`
	JobID      string    //`cql:"job_id"`
	TaskState  string    //`cql:"task_state"`
	TaskHost   string    //`cql:"task_host"`
	InstanceID int       //`cql:"instance_id"`
	TaskInfo   string    //`cql:"task_info"`
	CreateTime time.Time //`cql:"create_time"`
	UpdateTime time.Time //`cql:"update_time"`
}

// TaskEventRecords tracks a peloton task's state transitions
type TaskEventRecords struct {
	TaskID  string
	Records []TaskEventRecord
}

// TaskEventRecord tracks a peloton task state transition
type TaskEventRecord struct {
	FromState string
	ToState   string
	DateTime  time.Time
	Duration  time.Duration
}
