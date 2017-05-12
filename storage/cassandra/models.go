package cassandra

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"peloton/api/job"
	"peloton/api/respool"
	"peloton/api/task"

	"code.uber.internal/infra/peloton/util"

	log "github.com/Sirupsen/logrus"
)

// JobRecord correspond to a peloton job
type JobRecord struct {
	JobID        string
	JobConfig    string
	Owner        string
	CreatedTime  time.Time
	Labels       string
	CompleteTime time.Time
	RespoolID    string
}

// GetJobConfig returns the unmarshaled job.JobConfig
func (j *JobRecord) GetJobConfig() (*job.JobConfig, error) {
	result, err := util.UnmarshalToType(j.JobConfig, reflect.TypeOf(job.JobConfig{}))
	if err != nil {
		return nil, err
	}
	return result.(*job.JobConfig), err
}

// TaskRecord correspond to a peloton task
type TaskRecord struct {
	TaskID        string
	JobID         string
	TaskGoalState string
	TaskState     string
	TaskHost      string
	InstanceID    int
	TaskInfo      string
	CreateTime    time.Time
	UpdateTime    time.Time
}

// GetTaskInfo returns the unmarshaled task.TaskInfo
func (t *TaskRecord) GetTaskInfo() (*task.TaskInfo, error) {
	result, err := util.UnmarshalToType(t.TaskInfo, reflect.TypeOf(task.TaskInfo{}))
	if err != nil {
		return nil, err
	}
	return result.(*task.TaskInfo), err
}

// TaskStateChangeRecords tracks a peloton task's state transition events
type TaskStateChangeRecords struct {
	TaskID string
	Events []string
}

// GetStateChangeRecords returns the TaskStateChangeRecord array
func (t *TaskStateChangeRecords) GetStateChangeRecords() ([]*TaskStateChangeRecord, error) {
	var result []*TaskStateChangeRecord
	for _, e := range t.Events {
		rec, err := util.UnmarshalToType(e, reflect.TypeOf(TaskStateChangeRecord{}))
		if err != nil {
			return nil, err
		}
		result = append(result, rec.(*TaskStateChangeRecord))
	}
	return result, nil
}

// TaskStateChangeRecord tracks a peloton task state transition
type TaskStateChangeRecord struct {
	TaskState   string
	EventTime   time.Time
	TaskHost    string
	TaskID      string
	MesosTaskID string
}

// FrameworkInfoRecord tracks the framework info
type FrameworkInfoRecord struct {
	FrameworkName string
	FrameworkID   string
	MesosStreamID string
	UpdateTime    time.Time
	UpdateHost    string
}

// Resource pool (to be added)

// SetObjectField sets a field in object with the fieldname with the value
func SetObjectField(object interface{}, fieldName string, value interface{}) error {
	objValue := reflect.ValueOf(object).Elem()
	objFieldValue := objValue.FieldByName(fieldName)

	if !objFieldValue.IsValid() {
		return fmt.Errorf("Field %v is invalid, not found in object", fieldName)
	}
	if !objFieldValue.CanSet() {
		return fmt.Errorf("Field %v cannot be set", fieldName)
	}

	objFieldType := objFieldValue.Type()
	val := reflect.ValueOf(value)
	if objFieldType != val.Type() {
		return fmt.Errorf("Provided value type didn't match obj field type, Field %v val %v", fieldName, value)
	}
	objFieldValue.Set(val)
	return nil
}

// FillObject fills the data from DB into an object
func FillObject(data map[string]interface{}, object interface{}, objType reflect.Type) error {
	objectFields := getAllFieldInLowercase(objType)
	log.Debugf("objectFields : %v", objectFields)
	for fieldName, value := range data {
		_, contains := objectFields[strings.ToLower(fieldName)]
		if !contains {
			return fmt.Errorf("Field %v not found in object", fieldName)
		}
		err := SetObjectField(object, objectFields[strings.ToLower(fieldName)], value)
		if err != nil {
			return err
		}
	}
	return nil
}

// For a struct type, returns a mapping from the lowercase of the field name to field name.
// This is needed as C* returns a map that the field name is all in lower case
func getAllFieldInLowercase(objType reflect.Type) map[string]string {
	var result = make(map[string]string)
	for i := 0; i < objType.NumField(); i++ {
		result[strings.ToLower(objType.Field(i).Name)] = objType.Field(i).Name
	}
	return result
}

// ResourcePoolRecord corresponds to a peloton resource pool
type ResourcePoolRecord struct {
	ID                 string
	ResourcePoolConfig string
	Owner              string
	CreateTime         time.Time
	UpdateTime         time.Time
}

// GetResourcePoolConfig returns the unmarshaled respool.ResourceConfig
func (r *ResourcePoolRecord) GetResourcePoolConfig() (*respool.ResourcePoolConfig, error) {
	result, err := util.UnmarshalToType(r.ResourcePoolConfig, reflect.TypeOf(respool.ResourcePoolConfig{}))
	if err != nil {
		return nil, err
	}
	return result.(*respool.ResourcePoolConfig), err
}

// JobRuntimeRecord contains job runtime info
type JobRuntimeRecord struct {
	JobID       string
	JobRuntime  string
	CreatedTime time.Time
	UpdateTime  time.Time
	JobState    string
}

// GetJobRuntime returns the job.Runtime from a JobRecord table record
func (t *JobRuntimeRecord) GetJobRuntime() (*job.RuntimeInfo, error) {
	val, err := util.UnmarshalToType(t.JobRuntime, reflect.TypeOf(job.RuntimeInfo{}))
	if err != nil {
		log.WithError(err).
			WithField("JobRuntime", t.JobRuntime).
			Error("Unmarshal to RuntimeInfo failed")
		return nil, err
	}
	result := val.(*job.RuntimeInfo)
	if result.TaskStats == nil {
		result.TaskStats = make(map[string]uint32)
	}
	return result, err
}

// PersistentVolumeRecord contains persistent volume info.
type PersistentVolumeRecord struct {
	ID            string
	JobID         string
	InstanceID    int
	Hostname      string
	State         string
	GoalState     string
	SizeMB        int
	ContainerPath string
	CreateTime    time.Time
	UpdateTime    time.Time
}
