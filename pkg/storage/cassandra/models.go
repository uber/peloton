// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cassandra

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/storage/querybuilder"

	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

// JobConfigRecord correspond to a peloton job config.
type JobConfigRecord struct {
	JobID        querybuilder.UUID `cql:"job_id"`
	Version      int
	CreationTime time.Time `cql:"creation_time"`
	Config       []byte
	ConfigAddOn  []byte `cql:"config_addon"`
}

// GetJobConfig returns the unmarshaled job.JobConfig
func (j *JobConfigRecord) GetJobConfig() (*job.JobConfig, error) {
	configBuffer, err := uncompress(j.Config)
	if err != nil {
		return nil, err
	}
	config := &job.JobConfig{}
	return config, proto.Unmarshal(configBuffer, config)
}

// GetConfigAddOn returns the unmarshaled models.ConfigAddOn
func (j *JobConfigRecord) GetConfigAddOn() (*models.ConfigAddOn, error) {
	addOn := &models.ConfigAddOn{}
	return addOn, proto.Unmarshal(j.ConfigAddOn, addOn)
}

// TaskRuntimeRecord correspond to a peloton task
type TaskRuntimeRecord struct {
	JobID       querybuilder.UUID `cql:"job_id"`
	InstanceID  int               `cql:"instance_id"`
	Version     int64
	UpdateTime  time.Time `cql:"update_time"`
	State       string
	RuntimeInfo []byte `cql:"runtime_info"`
}

// GetTaskRuntime returns the unmarshaled task.TaskInfo
func (t *TaskRuntimeRecord) GetTaskRuntime() (*task.RuntimeInfo, error) {
	runtime := &task.RuntimeInfo{}
	return runtime, proto.Unmarshal(t.RuntimeInfo, runtime)
}

// TaskConfigRecord correspond to a peloton task config
type TaskConfigRecord struct {
	JobID        querybuilder.UUID `cql:"job_id"`
	Version      int
	InstanceID   int       `cql:"instance_id"`
	CreationTime time.Time `cql:"creation_time"`
	Config       []byte
	ConfigAddOn  []byte `cql:"config_addon"`
}

// GetTaskConfig returns the unmarshaled task.TaskInfo
func (t *TaskConfigRecord) GetTaskConfig() (*task.TaskConfig, error) {
	config := &task.TaskConfig{}
	return config, proto.Unmarshal(t.Config, config)
}

// GetConfigAddOn returns the unmarshaled models.ConfigAddOn
func (t *TaskConfigRecord) GetConfigAddOn() (*models.ConfigAddOn, error) {
	addOn := &models.ConfigAddOn{}
	return addOn, proto.Unmarshal(t.ConfigAddOn, addOn)
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
	TaskState          string `cql:"task_state"`
	EventTime          string `cql:"event_time"`
	TaskHost           string `cql:"task_host"`
	JobID              string `cql:"job_id"`
	InstanceID         uint32 `cql:"instance_id"`
	MesosTaskID        string `cql:"mesos_task_id"`
	Message            string `cql:"message"`
	Healthy            string `cql:"healthy"`
	Reason             string `cql:"reason"`
	AgentID            string `cql:"agent_id"`
	PrevMesosTaskID    string `cql:"prev_mesos_task_id"`
	DesiredMesosTaskID string `cql:"desired_mesos_task_id"`
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

// UpdateViewRecord tracks the job update info from materialized view
type UpdateViewRecord struct {
	UpdateID     querybuilder.UUID `cql:"update_id"`
	JobID        querybuilder.UUID `cql:"job_id"`
	CreationTime time.Time         `cql:"creation_time"`
}

// UpdateRecord tracks the job update info
type UpdateRecord struct {
	UpdateID             querybuilder.UUID `cql:"update_id"`
	UpdateOptions        []byte            `cql:"update_options"`
	State                string            `cql:"update_state"`
	PrevState            string            `cql:"update_prev_state"`
	Type                 string            `cql:"update_type"`
	JobID                querybuilder.UUID `cql:"job_id"`
	InstancesTotal       int               `cql:"instances_total"`
	InstancesCurrent     []int             `cql:"instances_current"`
	InstancesUpdated     []int             `cql:"instances_updated"`
	InstancesAdded       []int             `cql:"instances_added"`
	InstancesRemoved     []int             `cql:"instances_removed"`
	InstancesDone        int               `cql:"instances_done"`
	InstancesFailed      int               `cql:"instances_failed"`
	JobConfigVersion     int64             `cql:"job_config_version"`
	PrevJobConfigVersion int64             `cql:"job_config_prev_version"`
	CreationTime         time.Time         `cql:"creation_time"`
	UpdateTime           time.Time         `cql:"update_time"`
	OpaqueData           string            `cql:"opaque_data"`
	CompletionTime       string            `cql:"completion_time"`
}

// GetUpdateConfig unmarshals and returns the configuration of the job update.
func (u *UpdateRecord) GetUpdateConfig() (*update.UpdateConfig, error) {
	config := &update.UpdateConfig{}
	return config, proto.Unmarshal(u.UpdateOptions, config)
}

// GetProcessingInstances returns a list of tasks currently being updated.
func (u *UpdateRecord) GetProcessingInstances() []uint32 {
	p := make([]uint32, len(u.InstancesCurrent))
	for i, v := range u.InstancesCurrent {
		p[i] = uint32(v)
	}
	return p
}

// GetInstancesUpdated returns a list of tasks to be updated
func (u *UpdateRecord) GetInstancesUpdated() []uint32 {
	p := make([]uint32, len(u.InstancesUpdated))
	for i, v := range u.InstancesUpdated {
		p[i] = uint32(v)
	}
	return p
}

// GetInstancesAdded returns a list of tasks to be added
func (u *UpdateRecord) GetInstancesAdded() []uint32 {
	p := make([]uint32, len(u.InstancesAdded))
	for i, v := range u.InstancesAdded {
		p[i] = uint32(v)
	}
	return p
}

// GetInstancesRemoved returns a list of tasks to be added
func (u *UpdateRecord) GetInstancesRemoved() []uint32 {
	p := make([]uint32, len(u.InstancesRemoved))
	for i, v := range u.InstancesRemoved {
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
	for fieldName, value := range data {
		_, contains := objectFields[strings.ToLower(fieldName)]
		if !contains {
			log.WithField("field", fieldName).
				Debug("Field not found in object")
			continue
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
// TODO: Add versioning.
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
	JobID       querybuilder.UUID `cql:"job_id"`
	State       string            `cql:"state"`
	UpdateTime  time.Time         `cql:"update_time"`
	RuntimeInfo []byte            `cql:"runtime_info"`
}

// GetJobRuntime returns the job.Runtime from a JobRecord table record
func (t *JobRuntimeRecord) GetJobRuntime() (*job.RuntimeInfo, error) {
	runtime := &job.RuntimeInfo{}
	return runtime, proto.Unmarshal(t.RuntimeInfo, runtime)
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
