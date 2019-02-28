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
	"compress/gzip"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/suite"
)

type TestModelsSuite struct {
	suite.Suite
}

func TestModels(t *testing.T) {
	suite.Run(t, new(TestModelsSuite))
}

// TestJobConfigRecord_GetJobConfig tests JobConfigRecord.GetJobConfig
func (suite *TestModelsSuite) TestJobConfigRecord_GetJobConfig() {
	var sla = job.SlaConfig{
		Priority:                22,
		MaximumRunningInstances: 6,
		Preemptible:             false,
	}
	var jobConfig = &job.JobConfig{
		OwningTeam:    "uber",
		LdapGroups:    []string{"money", "team6", "otto"},
		SLA:           &sla,
		InstanceCount: uint32(6),
		DefaultConfig: &task.TaskConfig{},
	}
	jobConfigMarshalled, _ := proto.Marshal(jobConfig)
	record := &JobConfigRecord{
		Config: jobConfigMarshalled,
	}
	jobConfigUnMarshalled, err := record.GetJobConfig()
	suite.NoError(err)
	suite.Equal(jobConfig, jobConfigUnMarshalled)

	// simulate uncompression failure due to checksum errors
	record = &JobConfigRecord{
		Config: badCheckSumData,
	}
	_, err = record.GetJobConfig()
	suite.Equal(err, gzip.ErrChecksum)
}

// TestJobConfigRecord_GetConfigAddOn tests JobConfigRecord.GetConfigAddOn
func (suite *TestModelsSuite) TestJobConfigRecord_GetConfigAddOn() {
	jobConfigAddOn := &models.ConfigAddOn{
		SystemLabels: []*peloton.Label{
			{
				Key:   "testLabelKey",
				Value: "testLabelValue",
			},
		},
	}
	jobConfigAddOnMarshalled, _ := proto.Marshal(jobConfigAddOn)
	record := &JobConfigRecord{
		ConfigAddOn: jobConfigAddOnMarshalled,
	}
	jobConfigAddOnUnMarshalled, err := record.GetConfigAddOn()
	suite.NoError(err)
	suite.Equal(jobConfigAddOn, jobConfigAddOnUnMarshalled)
}

// TestTaskRuntimeRecord_GetTaskRuntime tests TaskRuntimeRecord.GetTaskRuntime
func (suite *TestModelsSuite) TestTaskRuntimeRecord_GetTaskRuntime() {
	tID := "testTaskID"
	taskRuntime := &task.RuntimeInfo{
		PrevMesosTaskId:    nil,
		MesosTaskId:        &mesos.TaskID{Value: &tID},
		DesiredMesosTaskId: &mesos.TaskID{Value: &tID},
		State:              task.TaskState_INITIALIZED,
		Revision: &peloton.ChangeLog{
			Version: 1,
		},
	}
	taskRuntimeMarshalled, _ := proto.Marshal(taskRuntime)
	record := &TaskRuntimeRecord{
		RuntimeInfo: taskRuntimeMarshalled,
	}
	taskRuntimeUnMarshalled, err := record.GetTaskRuntime()
	suite.NoError(err)
	suite.Equal(taskRuntime, taskRuntimeUnMarshalled)
}

// TestTaskConfigRecord_GetTaskConfig tests TaskConfigRecord.GetTaskConfig
func (suite *TestModelsSuite) TestTaskConfigRecord_GetTaskConfig() {
	taskConfig := &task.TaskConfig{
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000,
		},
	}
	taskConfigMarshalled, _ := proto.Marshal(taskConfig)
	record := &TaskConfigRecord{
		Config: taskConfigMarshalled,
	}
	taskConfigUnMarshalled, err := record.GetTaskConfig()
	suite.NoError(err)
	suite.Equal(taskConfig, taskConfigUnMarshalled)
}

// TestTaskConfigRecord_GetConfigAddOn tests TaskConfigRecord.GetConfigAddOn
func (suite *TestModelsSuite) TestTaskConfigRecord_GetConfigAddOn() {
	taskConfigAddOn := &models.ConfigAddOn{
		SystemLabels: []*peloton.Label{
			{
				Key:   "testLabelKey",
				Value: "testLabelValue",
			},
		},
	}
	jobConfigAddOnMarshalled, _ := proto.Marshal(taskConfigAddOn)
	record := &JobConfigRecord{
		ConfigAddOn: jobConfigAddOnMarshalled,
	}
	jobConfigAddOnUnMarshalled, err := record.GetConfigAddOn()
	suite.NoError(err)
	suite.Equal(taskConfigAddOn, jobConfigAddOnUnMarshalled)
}

// TestTaskStateChangeRecords_GetStateChangeRecords tests
// TaskStateChangeRecords.GetStateChangeRecords
func (suite *TestModelsSuite) TestTaskStateChangeRecords_GetStateChangeRecords() {
	taskStates := []string{
		task.TaskState_INITIALIZED.String(),
		task.TaskState_PENDING.String(),
		task.TaskState_RUNNING.String(),
		task.TaskState_FAILED.String(),
		task.TaskState_RUNNING.String(),
		task.TaskState_SUCCEEDED.String(),
		task.TaskState_LOST.String(),
	}
	var events []string
	for _, state := range taskStates {
		buffer, _ := json.Marshal(TaskStateChangeRecord{TaskState: state})
		events = append(events, string(buffer))
	}
	record := &TaskStateChangeRecords{
		Events: events,
	}

	stateChanges, err := record.GetStateChangeRecords()
	suite.NoError(err)
	for i, stateChange := range stateChanges {
		suite.Equal(taskStates[i], stateChange.TaskState)
	}

	// Test error
	record = &TaskStateChangeRecords{
		Events: []string{"teststring"},
	}
	_, err = record.GetStateChangeRecords()
	suite.Error(err)
}

// TestUpdateRecord tests methods of receiver UpdateRecord
func (suite *TestModelsSuite) TestUpdateRecord() {
	updateConfig := &update.UpdateConfig{
		BatchSize: 5,
	}
	instancesBeingProcessed := []int{1, 2, 3, 4, 5}
	instancesAdded := []int{1, 2, 3, 4, 5}
	instancesRemoved := []int{1, 2, 3, 4, 5}
	instancesUpdated := []int{1, 2, 3, 4, 5}

	updateConfigMarshalled, _ := proto.Marshal(updateConfig)
	record := &UpdateRecord{
		UpdateOptions:    updateConfigMarshalled,
		InstancesCurrent: instancesBeingProcessed,
		InstancesAdded:   instancesAdded,
		InstancesRemoved: instancesRemoved,
		InstancesUpdated: instancesUpdated,
	}

	updateConfigUnMarshalled, err := record.GetUpdateConfig()
	suite.NoError(err)
	suite.Equal(updateConfig.BatchSize, updateConfigUnMarshalled.BatchSize)

	for i, instance := range record.GetProcessingInstances() {
		suite.Equal(instancesBeingProcessed[i], int(instance))
	}
	for i, instance := range record.GetInstancesAdded() {
		suite.Equal(instancesAdded[i], int(instance))
	}
	for i, instance := range record.GetInstancesRemoved() {
		suite.Equal(instancesRemoved[i], int(instance))
	}
	for i, instance := range record.GetInstancesUpdated() {
		suite.Equal(instancesUpdated[i], int(instance))
	}
}

// TestSetObjectField tests SetObjectField method
func (suite *TestModelsSuite) TestSetObjectField() {
	jobConfigRecord := &JobConfigRecord{
		Version: 1,
	}
	err := SetObjectField(jobConfigRecord, "Version", 2)
	suite.NoError(err)
	suite.Equal(2, jobConfigRecord.Version)

	// Test setting invalid field
	err = SetObjectField(jobConfigRecord, "name", "myjob")
	suite.Error(err)

	// Test setting field to incorrect type
	err = SetObjectField(jobConfigRecord, "Version", "invalid")
	suite.Error(err)
}

// TestFillObject tests FillObject method
func (suite *TestModelsSuite) TestFillObject() {
	jobConfigRecord := JobConfigRecord{}
	data := make(map[string]interface{})
	data["Version"] = 1
	data["MyKey"] = "MyValue"
	err := FillObject(data, &jobConfigRecord, reflect.TypeOf(jobConfigRecord))
	suite.NoError(err)
	suite.Equal(data["Version"], jobConfigRecord.Version)

	// Test SetObjectField error
	data["Version"] = "Invalid Type"
	err = FillObject(data, &jobConfigRecord, reflect.TypeOf(jobConfigRecord))
	suite.Error(err)
}

// TestResourcePoolRecord_GetResourcePoolConfig tests
// ResourcePoolRecord.GetResourcePoolConfig
func (suite *TestModelsSuite) TestResourcePoolRecord_GetResourcePoolConfig() {
	respoolConfig := &respool.ResourcePoolConfig{
		Name:        "TestResourcePool_1",
		ChangeLog:   nil,
		Description: "test resource pool",
		LdapGroups:  []string{"l1", "l2"},
		OwningTeam:  "team1",
		Parent:      nil,
		Policy:      1,
		Resources: []*respool.ResourceConfig{
			{
				Kind:        "cpu",
				Limit:       1000.0,
				Reservation: 100.0,
				Share:       1.0,
			},
			{
				Kind:        "gpu",
				Limit:       4.0,
				Reservation: 2.0,
				Share:       1.0,
			},
		},
	}

	buffer, _ := json.Marshal(respoolConfig)
	record := ResourcePoolRecord{
		RespoolConfig: string(buffer),
	}
	respoolConfigUnMarshalled, err := record.GetResourcePoolConfig()
	suite.NoError(err)
	suite.Equal(respoolConfig, respoolConfigUnMarshalled)

	// Test UnMarshal error
	record = ResourcePoolRecord{
		RespoolConfig: "Test data",
	}
	_, err = record.GetResourcePoolConfig()
	suite.Error(err)
}

// TestJobRuntimeRecord_GetJobRuntime tests JobRuntimeRecord.GetJobRuntime
func (suite *TestModelsSuite) TestJobRuntimeRecord_GetJobRuntime() {
	now := time.Now()
	jobRuntimeInfo := &job.RuntimeInfo{
		State:        job.JobState_INITIALIZED,
		CreationTime: now.Format(time.RFC3339Nano),
		GoalState:    job.JobState_SUCCEEDED,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(now.UnixNano()),
			UpdatedAt: uint64(now.UnixNano()),
			Version:   1,
		},
	}
	jobRuntimeInfoMarshalled, _ := proto.Marshal(jobRuntimeInfo)
	record := &JobRuntimeRecord{
		RuntimeInfo: jobRuntimeInfoMarshalled,
	}
	jobRuntimeInfoUnMarshalled, err := record.GetJobRuntime()
	suite.NoError(err)
	suite.Equal(jobRuntimeInfo, jobRuntimeInfoUnMarshalled)
}
