package cassandra

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/storage/querybuilder"

	"code.uber.internal/infra/peloton/util"

	log "github.com/Sirupsen/logrus"
)

// JobConfigRecord correspond to a peloton job config.
type JobConfigRecord struct {
	JobID       querybuilder.UUID `cql:"job_id"`
	Version     int
	CreatedTime time.Time         `cql:"created_time"`
	RespoolID   querybuilder.UUID `cql:"repool_id"`
	Labels      string
	Owner       string
	Config      string
}

// GetJobConfig returns the unmarshaled job.JobConfig
func (j *JobConfigRecord) GetJobConfig() (*job.JobConfig, error) {
	result, err := util.UnmarshalToType(j.Config, reflect.TypeOf(job.JobConfig{}))
	if err != nil {
		return nil, err
	}
	return result.(*job.JobConfig), err
}

// TaskRuntimeRecord correspond to a peloton task
type TaskRuntimeRecord struct {
	JobID          querybuilder.UUID `cql:"job_id"`
	InstanceID     int               `cql:"instance_id"`
	UpdateTime     time.Time         `cql:"update_time"`
	CurrentVersion int               `cql:"current_version"`
	GoalVersion    int               `cql:"goal_version"`
	CurrentState   string            `cql:"current_state"`
	MesosTaskID    string            `cql:"mesos_task_id"`
	GoalState      string            `cql:"goal_state"`
	StartTime      time.Time         `cql:"start_time"`
	CompletionTime time.Time         `cql:"completion_time"`
	Host           string
	Ports          map[string]int
	Message        string
	Reason         string
	FailureCount   int    `cql:"failure_count"`
	VolumeID       string `cql:"volume_id"`
}

// GetTaskRuntime returns the unmarshaled task.TaskInfo
func (t *TaskRuntimeRecord) GetTaskRuntime() (*task.RuntimeInfo, error) {
	ports := map[string]uint32{}
	for n, p := range t.Ports {
		ports[n] = uint32(p)
	}
	var mesosID *mesos_v1.TaskID
	if t.MesosTaskID != "" {
		mesosID = &mesos_v1.TaskID{
			Value: &t.MesosTaskID,
		}
	}
	return &task.RuntimeInfo{
		State:          task.TaskState(task.TaskState_value[t.CurrentState]),
		MesosTaskId:    mesosID,
		StartTime:      formatTime(t.StartTime),
		CompletionTime: formatTime(t.CompletionTime),
		Host:           t.Host,
		Ports:          ports,
		GoalState:      task.TaskState(task.TaskState_value[t.GoalState]),
		Message:        t.Message,
		Reason:         t.Reason,
		FailureCount:   uint32(t.FailureCount),
		VolumeID:       &peloton.VolumeID{Value: t.VolumeID},
	}, nil
}

// TaskConfigRecord correspond to a peloton task config
type TaskConfigRecord struct {
	JobID      querybuilder.UUID `cql:"job_id"`
	Version    int
	InstanceID int `cql:"instance_id"`
	Config     string
}

// GetTaskConfig returns the unmarshaled task.TaskInfo
func (t *TaskConfigRecord) GetTaskConfig() (*task.TaskConfig, error) {
	result, err := util.UnmarshalToType(t.Config, reflect.TypeOf(task.TaskConfig{}))
	if err != nil {
		return nil, err
	}
	return result.(*task.TaskConfig), err
}

// TaskStateChangeRecords tracks a peloton task's state transition events
type TaskStateChangeRecords struct {
	JobID      querybuilder.UUID `cql:"job_id"`
	InstanceID int               `cql:"instance_id"`
	Events     []string
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
	TaskState   string    `cql:"task_state"`
	EventTime   time.Time `cql:"event_time"`
	TaskHost    string    `cql:"task_host"`
	JobID       string    `cql:"job_id"`
	InstanceID  uint32    `cql:"instance_id"`
	MesosTaskID string    `cql:"mesos_task_id"`
}

// FrameworkInfoRecord tracks the framework info
type FrameworkInfoRecord struct {
	FrameworkName string    `cql:"framework_name"`
	FrameworkID   string    `cql:"framework_id"`
	MesosStreamID string    `cql:"mesos_stream_id"`
	UpdateTime    time.Time `cql:"update_time"`
	UpdateHost    string    `cql:"update_host"`
}

// Resource pool (to be added)

// UpgradeRecord tracks the upgrade info
type UpgradeRecord struct {
	Instances []int
	Progress  int
}

// GetProcessingInstances returns a list of tasks currently being upgraded.
func (r *UpgradeRecord) GetProcessingInstances() []uint32 {
	p := make([]uint32, len(r.Instances))
	for i, v := range r.Instances {
		p[i] = uint32(v)
	}
	return p
}

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
		t := objType.Field(i).Tag.Get("cql")
		if t == "" {
			t = strings.ToLower(objType.Field(i).Name)
		}
		result[t] = objType.Field(i).Name
	}
	return result
}

// ResourcePoolRecord corresponds to a peloton resource pool
type ResourcePoolRecord struct {
	RespoolID     string `cql:"respool_id"`
	RespoolConfig string `cql:"respool_config"`
	Owner         string
	CreationTime  time.Time `cql:"creation_time"`
	UpdateTime    time.Time `cql:"update_time"`
}

// GetResourcePoolConfig returns the unmarshaled respool.ResourceConfig
func (r *ResourcePoolRecord) GetResourcePoolConfig() (*respool.ResourcePoolConfig, error) {
	result, err := util.UnmarshalToType(r.RespoolConfig, reflect.TypeOf(respool.ResourcePoolConfig{}))
	if err != nil {
		return nil, err
	}
	return result.(*respool.ResourcePoolConfig), err
}

// JobRuntimeRecord contains job runtime info
type JobRuntimeRecord struct {
	JobID          querybuilder.UUID `cql:"job_id"`
	JobState       string            `cql:"job_state"`
	CurrentVersion int               `cql:"current_version"`
	CreationTime   time.Time         `cql:"creation_time"`
	StartTime      time.Time         `cql:"start_time"`
	CompletionTime time.Time         `cql:"completion_time"`
	UpdateTime     time.Time         `cql:"update_time"`
	TaskStats      map[string]int    `cql:"task_stats"`
}

// GetJobRuntime returns the job.Runtime from a JobRecord table record
func (t *JobRuntimeRecord) GetJobRuntime() (*job.RuntimeInfo, error) {
	stats := map[string]uint32{}
	for s, c := range t.TaskStats {
		stats[s] = uint32(c)
	}
	return &job.RuntimeInfo{
		State:          job.JobState(job.JobState_value[t.JobState]),
		CreationTime:   formatTime(t.CreationTime),
		StartTime:      formatTime(t.StartTime),
		CompletionTime: formatTime(t.CompletionTime),
		TaskStats:      stats,
	}, nil
}

// PersistentVolumeRecord contains persistent volume info.
type PersistentVolumeRecord struct {
	VolumeID      string `cql:"volume_id"`
	JobID         string `cql:"job_id"`
	InstanceID    int    `cql:"instance_id"`
	Hostname      string
	State         string
	GoalState     string    `cql:"goal_state"`
	SizeMB        int       `cql:"size_mb"`
	ContainerPath string    `cql:"container_path"`
	CreateTime    time.Time `cql:"creation_time"`
	UpdateTime    time.Time `cql:"update_time"`
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}
