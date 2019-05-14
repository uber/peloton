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

package jobconfig

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"strings"
	"testing"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/util"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

const (
	maxTasksPerJob = 100000
)

const (
	newConfig                        = "testdata/new_config.yaml"
	oldConfig                        = "testdata/old_config.yaml"
	invalidNewConfig                 = "testdata/invalid_new_config.yaml"
	oldConfigWithoutDefaultCmd       = "testdata/old_config_without_default_cmd.yaml"
	invalidNewConfigWithouDefaultCmd = "testdata/invalid_new_config_without_cmd.yaml"
)

func TestValidateTaskConfigSuccess(t *testing.T) {
	// No error if there is a default task config
	taskConfig := task.TaskConfig{
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000,
		},
		Ports: []*task.PortConfig{
			{
				Name:    "port",
				EnvName: "PORT",
			},
		},
		Command: &mesos.CommandInfo{
			Value: util.PtrPrintf("echo Hello"),
		},
	}
	jobConfig := job.JobConfig{
		Name:          fmt.Sprintf("TestJob_1"),
		InstanceCount: 10,
		DefaultConfig: &taskConfig,
	}

	err := ValidateConfig(&jobConfig, maxTasksPerJob)
	assert.NoError(t, err)

	// No error if all instance configs exist
	instances := make(map[uint32]*task.TaskConfig)
	for i := uint32(0); i < 10; i++ {
		instances[i] = &taskConfig
	}
	jobConfig = job.JobConfig{
		Name:           fmt.Sprintf("TestJob_1"),
		InstanceCount:  10,
		InstanceConfig: instances,
	}
	err = ValidateConfig(&jobConfig, maxTasksPerJob)
	assert.NoError(t, err)
}

func TestValidateTaskConfigFailure(t *testing.T) {
	// Error if there is any instance config is missing
	taskConfig := task.TaskConfig{
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000,
		},
	}
	instances := make(map[uint32]*task.TaskConfig)
	for i := uint32(0); i < 10; i++ {
		instances[i] = &taskConfig
	}
	jobConfig := job.JobConfig{
		Name:           fmt.Sprintf("TestJob_1"),
		InstanceCount:  20,
		InstanceConfig: instances,
	}
	err := ValidateConfig(&jobConfig, maxTasksPerJob)
	assert.Error(t, err)
}

func TestValidateTaskConfigFailureMaxTasksPerJob(t *testing.T) {
	jobConfig := job.JobConfig{
		Name:          fmt.Sprintf("TestJob_1"),
		InstanceCount: maxTasksPerJob + 1,
	}
	err := ValidateConfig(&jobConfig, maxTasksPerJob)
	assert.Error(t, err)
}

func TestValidateTaskConfigFailureStateless(t *testing.T) {
	jobConfig := job.JobConfig{
		Name:          fmt.Sprintf("TestJob_1"),
		Type:          job.JobType_SERVICE,
		InstanceCount: 10,
		DefaultConfig: &task.TaskConfig{},
		SLA: &job.SlaConfig{
			MaxRunningTime: 1,
		},
	}
	err := ValidateConfig(&jobConfig, maxTasksPerJob)
	assert.Error(t, err)
}

func TestValidateTaskConfigFailureBatch(t *testing.T) {
	jobConfig := job.JobConfig{
		Name:          fmt.Sprintf("TestJob_1"),
		InstanceCount: 10,
		DefaultConfig: &task.TaskConfig{
			HealthCheck: &task.HealthCheckConfig{
				Enabled: true,
			},
			Command: &mesos.CommandInfo{
				Value: util.PtrPrintf("echo Hello"),
			},
		},
	}
	err := ValidateConfig(&jobConfig, maxTasksPerJob)
	assert.Error(t, err)
	assert.EqualError(t, err, "code:invalid-argument message:Invalid config for instance 0, "+
		"code:invalid-argument message:Batch job task should not set health check ")
}

func TestValidateTaskConfigFailureMaxInstances(t *testing.T) {
	// No error if there is a default task config
	taskConfig := task.TaskConfig{
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000,
		},
		Ports: []*task.PortConfig{
			{
				Name:    "port",
				EnvName: "PORT",
			},
		},
		Command: &mesos.CommandInfo{
			Value: util.PtrPrintf("echo Hello"),
		},
	}
	sla := job.SlaConfig{
		Preemptible:             false,
		Priority:                1,
		MaximumRunningInstances: 20,
	}

	jobConfig := job.JobConfig{
		Name:          fmt.Sprintf("TestJob_1"),
		InstanceCount: 10,
		SLA:           &sla,
		DefaultConfig: &taskConfig,
	}

	err := ValidateConfig(&jobConfig, maxTasksPerJob)
	assert.Error(t, err)
	assert.EqualError(t, err, errMaxInstancesTooBig.Error())
}

func TestValidateTaskConfigMaxFailureRetries(t *testing.T) {
	// No error if there is a default task config
	taskConfig := task.TaskConfig{
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000,
		},
		Ports: []*task.PortConfig{
			{
				Name:    "port",
				EnvName: "PORT",
			},
		},
		Command: &mesos.CommandInfo{
			Value: util.PtrPrintf("echo Hello"),
		},
		RestartPolicy: &task.RestartPolicy{
			MaxFailures: 200,
		},
	}

	jobConfig := job.JobConfig{
		Name:          fmt.Sprintf("TestJob_1"),
		InstanceCount: 10,
		DefaultConfig: &taskConfig,
	}

	assert.Equal(
		t,
		int(jobConfig.GetDefaultConfig().GetRestartPolicy().GetMaxFailures()),
		200,
	)
	err := ValidateConfig(&jobConfig, maxTasksPerJob)
	assert.Equal(
		t,
		int(jobConfig.GetDefaultConfig().GetRestartPolicy().GetMaxFailures()),
		100,
	)
	assert.NoError(t, err)

	taskConfig = task.TaskConfig{
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000,
		},
		Ports: []*task.PortConfig{
			{
				Name:    "port",
				EnvName: "PORT",
			},
		},
		Command: &mesos.CommandInfo{
			Value: util.PtrPrintf("echo Hello"),
		},
		RestartPolicy: &task.RestartPolicy{
			MaxFailures: 200,
		},
	}

	taskConfig2 := task.TaskConfig{
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000,
		},
		Ports: []*task.PortConfig{
			{
				Name:    "port",
				EnvName: "PORT",
			},
		},
		Command: &mesos.CommandInfo{
			Value: util.PtrPrintf("echo Hello"),
		},
		RestartPolicy: &task.RestartPolicy{
			MaxFailures: 5,
		},
	}

	// No error if all instance configs exist
	instances := make(map[uint32]*task.TaskConfig)
	for i := uint32(0); i < 10; i++ {
		if i == 5 {
			instances[i] = &taskConfig2
			continue
		}
		instances[i] = &taskConfig
	}
	jobConfig = job.JobConfig{
		Name:           fmt.Sprintf("TestJob_1"),
		InstanceCount:  10,
		InstanceConfig: instances,
	}

	// resets max task retry failures to 10.
	assert.Equal(
		t,
		int(jobConfig.GetInstanceConfig()[0].GetRestartPolicy().GetMaxFailures()),
		200,
	)
	assert.Equal(
		t,
		int(jobConfig.GetInstanceConfig()[5].GetRestartPolicy().GetMaxFailures()),
		5,
	)

	err = ValidateConfig(&jobConfig, maxTasksPerJob)
	assert.Equal(
		t,
		int(jobConfig.GetInstanceConfig()[0].GetRestartPolicy().GetMaxFailures()),
		_maxTaskRetries,
	)
	assert.Equal(
		t,
		int(jobConfig.GetInstanceConfig()[5].GetRestartPolicy().GetMaxFailures()),
		5,
	)
	assert.NoError(t, err)
}

func TestValidateTaskConfigFailureMinInstances(t *testing.T) {
	// No error if there is a default task config
	taskConfig := task.TaskConfig{
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000,
		},
		Ports: []*task.PortConfig{
			{
				Name:    "port",
				EnvName: "PORT",
			},
		},
		Command: &mesos.CommandInfo{
			Value: util.PtrPrintf("echo Hello"),
		},
	}
	sla := job.SlaConfig{
		Preemptible:             false,
		Priority:                1,
		MaximumRunningInstances: 8,
		MinimumRunningInstances: 9,
	}
	jobConfig := job.JobConfig{
		Name:          fmt.Sprintf("TestJob_1"),
		InstanceCount: 10,
		SLA:           &sla,
		DefaultConfig: &taskConfig,
	}

	err := ValidateConfig(&jobConfig, maxTasksPerJob)
	assert.Error(t, err)
	assert.EqualError(t, err, errMinInstancesTooBig.Error())
}

func TestValidateTaskConfigFailureForPortConfig(t *testing.T) {
	taskConfig := task.TaskConfig{
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000,
		},
		Ports: []*task.PortConfig{
			{
				Name: "port",
			},
		},
	}
	jobConfig := job.JobConfig{
		Name:          fmt.Sprintf("TestJob_1"),
		InstanceCount: 10,
		DefaultConfig: &taskConfig,
	}

	err := ValidateConfig(&jobConfig, maxTasksPerJob)
	assert.Error(t, err)
	assert.EqualError(t, err, errPortEnvNameMissing.Error())
}

// TestValidatePortConfig_Failure verifies validatePortConfig
// throws errPortNameMissing when name is not specified
// in PortConfig.
func TestValidatePortConfig_Failure(t *testing.T) {
	taskConfig := &task.TaskConfig{
		Ports: []*task.PortConfig{
			{
				Value: uint32(80),
			},
		},
	}

	err := validatePortConfig(taskConfig)
	assert.Error(t, err, errPortNameMissing.Error())
}

// TestValidatePortConfig_FailureMissingEnvName verifies
// validatePortConfig throws errPortEnvNameMissing when
// using dynamic port allocation but environment name is
// not specified.
func TestValidatePortConfig_FailureMissingEnvName(t *testing.T) {
	taskConfig := &task.TaskConfig{
		Ports: []*task.PortConfig{
			{
				Name: "system",
			},
		},
	}

	err := validatePortConfig(taskConfig)
	assert.Error(t, err, errPortEnvNameMissing.Error())
}

// TestValidatePortConfigMissingEnvName_NoFailureCustomExecutor
// verifies validatePortConfig does not throws errPortEnvNameMissing
// when custom executor (thermos) is used.
func TestValidatePortConfigMissingEnvName_NoFailureCustomExecutor(t *testing.T) {
	taskConfig := &task.TaskConfig{
		Executor: &mesos.ExecutorInfo{
			Type: mesos.ExecutorInfo_CUSTOM.Enum(),
		},
		Ports: []*task.PortConfig{
			{
				Name: "system",
			},
		},
	}

	err := validatePortConfig(taskConfig)
	assert.NoError(t, err)
}

func TestValidateTaskConfigWithInvalidFieldType(t *testing.T) {
	// Validates task config field type is string/ptr/slice/bool, otherwise
	// we cannot distinguish between unset value and default value through
	// reflection.
	taskConfig := &task.TaskConfig{}
	val := reflect.ValueOf(taskConfig).Elem()
	for i := 0; i < val.NumField(); i++ {
		if strings.HasPrefix(
			val.Type().Field(i).Name,
			common.ReservedProtobufFieldPrefix,
		) {
			continue
		}

		kind := val.Field(i).Kind()
		assert.True(t,
			kind == reflect.String || kind == reflect.
				Ptr || kind == reflect.Slice || kind == reflect.
				Bool || kind == reflect.Uint32)
	}
}

func TestValidateInvalidUpdateConfig(t *testing.T) {
	oldConfig := getConfig(oldConfig, t)

	invalidNewConfig := getConfig(invalidNewConfig, t)
	invalidNewConfig.RespoolID = &peloton.ResourcePoolID{Value: "different"}
	invalidNewConfig.Type = job.JobType_SERVICE

	err := ValidateUpdatedConfig(oldConfig, invalidNewConfig, maxTasksPerJob)
	assert.Error(t, err)
	expectedErrors := `9 errors occurred:

* updating Name not supported
* updating Labels not supported
* updating OwningTeam not supported
* updating RespoolID not supported
* updating Type not supported
* updating LdapGroups not supported
* updating DefaultConfig not supported
* new instance count can't be less
* existing instance config can't be updated`
	assert.Equal(t, err.Error(), expectedErrors)
}

func TestValidateValidUpdateConfig(t *testing.T) {
	oldConfig := getConfig(oldConfig, t)
	validNewConfig := getConfig(newConfig, t)
	err := ValidateUpdatedConfig(oldConfig, validNewConfig, maxTasksPerJob)
	assert.NoError(t, err)
}

func TestValidateInvalidUpdateConfigWithoutCmd(t *testing.T) {
	oldConfig := getConfig(oldConfigWithoutDefaultCmd, t)
	invalidNewConfig := getConfig(invalidNewConfigWithouDefaultCmd, t)
	err := ValidateUpdatedConfig(oldConfig, invalidNewConfig, maxTasksPerJob)
	assert.Error(t, err)
	expectedErrors := `1 error occurred:

* code:invalid-argument message:missing command info for instance 3`
	assert.Equal(t, err.Error(), expectedErrors)
}

func TestValidateInvalidUpdateConfigJobType(t *testing.T) {
	oldConfig := getConfig(oldConfig, t)

	invalidNewConfig := getConfig(newConfig, t)
	invalidNewConfig.Type = job.JobType_DAEMON

	err := ValidateUpdatedConfig(oldConfig, invalidNewConfig, maxTasksPerJob)
	assert.Error(t, err)
	expectedErrors := `2 errors occurred:

* updating Type not supported
* code:invalid-argument message:invalid job type: DAEMON`
	assert.Equal(t, err.Error(), expectedErrors)
}

func getConfig(config string, t *testing.T) *job.JobConfig {
	var jobConfig job.JobConfig
	buffer, err := ioutil.ReadFile(config)
	assert.NoError(t, err)
	err = yaml.Unmarshal(buffer, &jobConfig)
	assert.NoError(t, err)
	return &jobConfig
}

func TestValidateStatelessJobConfig(t *testing.T) {
	testCases := []struct {
		job.SlaConfig
		error
	}{
		{
			SlaConfig: job.SlaConfig{MaximumRunningInstances: 1},
			error:     errIncorrectMaxInstancesSLA,
		},
		{
			SlaConfig: job.SlaConfig{MinimumRunningInstances: 1},
			error:     errIncorrectMinInstancesSLA,
		},
		{
			SlaConfig: job.SlaConfig{MaxRunningTime: 1},
			error:     errIncorrectMaxRunningTimeSLA,
		},
		{
			SlaConfig: job.SlaConfig{Revocable: true, Preemptible: false},
			error:     errIncorrectRevocableSLA,
		},
		{},
	}

	for _, testCase := range testCases {
		jobConfig := job.JobConfig{
			Name:          fmt.Sprintf("TestJob_1"),
			InstanceCount: 10,
			DefaultConfig: &task.TaskConfig{},
			SLA:           &testCase.SlaConfig,
		}
		err := validateStatelessJobConfig(&jobConfig)
		assert.Equal(t, err, testCase.error)
	}

}

func TestValidateStatelessTaskConfig(t *testing.T) {
	testCases := []struct {
		task.PreemptionPolicy
		error
	}{
		{
			PreemptionPolicy: task.PreemptionPolicy{KillOnPreempt: true},
			error:            errKillOnPreemptNotFalse,
		},
		{
			PreemptionPolicy: task.PreemptionPolicy{KillOnPreempt: false},
		},
	}

	for _, testCase := range testCases {
		taskConfig := task.TaskConfig{
			PreemptionPolicy: &testCase.PreemptionPolicy,
		}
		err := validateStatelessTaskConfig(&taskConfig)
		assert.Equal(t, err, testCase.error)
	}
}

func TestValidateBatchTaskConfig(t *testing.T) {
	testCases := []struct {
		task.HealthCheckConfig
		error
	}{
		{
			HealthCheckConfig: task.HealthCheckConfig{Enabled: true},
			error:             errIncorrectHealthCheck,
		},
	}
	for _, testCase := range testCases {
		taskConfig := task.TaskConfig{
			HealthCheck: &testCase.HealthCheckConfig,
		}
		err := validateBatchTaskConfig(&taskConfig)
		assert.Equal(t, err, testCase.error)
	}
}

// TestValidateTaskConfigFailureBatchExecutorConfig tests validation of
// batch job config, and verifies it throws an error when executor info
// is present.
func TestValidateTaskConfigFailureBatchExecutorConfig(t *testing.T) {
	executorType := mesos.ExecutorInfo_CUSTOM
	jobConfig := job.JobConfig{
		Name:          fmt.Sprintf("TestJob_10"),
		InstanceCount: 10,
		DefaultConfig: &task.TaskConfig{
			Command: &mesos.CommandInfo{
				Value: util.PtrPrintf("echo Hello"),
			},
			Executor: &mesos.ExecutorInfo{
				Type: &executorType,
			},
		},
	}
	err := ValidateConfig(&jobConfig, maxTasksPerJob)
	assert.Error(t, err, "Batch job task should not include executor config")
}

// TestValidateTaskConfigStatelessExecutorConfig tests validation of
// stateless job config, and verifies no error is returned when executor
// info is present.
func TestValidateTaskConfigStatelessExecutorConfig(t *testing.T) {
	executorType := mesos.ExecutorInfo_CUSTOM
	jobConfig := job.JobConfig{
		Name:          fmt.Sprintf("TestJob_10"),
		Type:          job.JobType_SERVICE,
		InstanceCount: 10,
		DefaultConfig: &task.TaskConfig{
			Command: &mesos.CommandInfo{
				Value: util.PtrPrintf("while true; do echo Hello; sleep 10; done"),
			},
			Executor: &mesos.ExecutorInfo{
				Type: &executorType,
				Data: []byte{1, 2, 3, 4},
			},
		},
	}
	err := ValidateConfig(&jobConfig, maxTasksPerJob)
	assert.NoError(t, err)
}

// TestValidateTaskConfigFailureStatelessExecutorConfig tests validation of
// stateless job config, and verifies it throws an error when no data is
// present in the executor info.
func TestValidateTaskConfigFailureStatelessExecutorConfig(t *testing.T) {
	executorType := mesos.ExecutorInfo_CUSTOM
	jobConfig := job.JobConfig{
		Name:          fmt.Sprintf("TestJob_10"),
		Type:          job.JobType_SERVICE,
		InstanceCount: 10,
		DefaultConfig: &task.TaskConfig{
			Command: &mesos.CommandInfo{
				Value: util.PtrPrintf("while true; do echo Hello; sleep 10; done"),
			},
			Executor: &mesos.ExecutorInfo{
				Type: &executorType,
			},
		},
	}
	err := ValidateConfig(&jobConfig, maxTasksPerJob)
	assert.Error(t, err, "Data field not set in executor config")

	jobConfig = job.JobConfig{
		Name:          fmt.Sprintf("TestJob_10"),
		Type:          job.JobType_SERVICE,
		InstanceCount: 10,
		DefaultConfig: &task.TaskConfig{
			Command: &mesos.CommandInfo{
				Value: util.PtrPrintf("while true; do echo Hello; sleep 10; done"),
			},
			Executor: &mesos.ExecutorInfo{
				Type: &executorType,
				Data: []byte{},
			},
		},
	}
	err = ValidateConfig(&jobConfig, maxTasksPerJob)
	assert.Error(t, err, "Data field not set in executor config")
}

func TestValidatePreemptionPolicy(t *testing.T) {
	tt := []struct {
		name       string
		taskConfig *task.TaskConfig
		jobConfig  *job.JobConfig
		instanceID uint32
		wantErr    error
	}{
		{
			name: "preemption policy unknown should pass",
			taskConfig: &task.TaskConfig{
				PreemptionPolicy: &task.PreemptionPolicy{
					Type: task.PreemptionPolicy_TYPE_INVALID,
				},
			},
			jobConfig:  nil,
			instanceID: 0,
			wantErr:    nil,
		},
		{
			name: "preemption policy override if same within min instance" +
				" count should pass",
			taskConfig: &task.TaskConfig{
				PreemptionPolicy: &task.PreemptionPolicy{
					Type: task.PreemptionPolicy_TYPE_PREEMPTIBLE,
				},
			},
			jobConfig: &job.JobConfig{
				SLA: &job.SlaConfig{
					Preemptible:             true,
					MinimumRunningInstances: 10,
				},
			},
			instanceID: 0,
			wantErr:    nil,
		},
		{
			name: "preemption policy override if different but greater than" +
				" min instance count should pass",
			taskConfig: &task.TaskConfig{
				PreemptionPolicy: &task.PreemptionPolicy{
					Type: task.PreemptionPolicy_TYPE_PREEMPTIBLE,
				},
			},
			jobConfig: &job.JobConfig{
				SLA: &job.SlaConfig{
					Preemptible:             false,
					MinimumRunningInstances: 10,
				},
			},
			instanceID: 11,
			wantErr:    nil,
		},
		{
			name: "preemption policy override if different but less than" +
				" min instance count should pass",
			taskConfig: &task.TaskConfig{
				PreemptionPolicy: &task.PreemptionPolicy{
					Type: task.PreemptionPolicy_TYPE_PREEMPTIBLE,
				},
			},
			jobConfig: &job.JobConfig{
				SLA: &job.SlaConfig{
					Preemptible:             false,
					MinimumRunningInstances: 10,
				},
			},
			instanceID: 5,
			wantErr:    errInvalidPreemptionOverride,
		},
		{
			name: "preemption policy override if different but less than" +
				" min instance count should pass",
			taskConfig: &task.TaskConfig{
				PreemptionPolicy: &task.PreemptionPolicy{
					Type: task.PreemptionPolicy_TYPE_NON_PREEMPTIBLE,
				},
			},
			jobConfig: &job.JobConfig{
				SLA: &job.SlaConfig{
					Preemptible:             true,
					MinimumRunningInstances: 10,
				},
			},
			instanceID: 5,
			wantErr:    errInvalidPreemptionOverride,
		},
	}

	for _, test := range tt {
		err := validatePreemptionPolicy(
			test.instanceID,
			test.taskConfig,
			test.jobConfig,
		)
		if test.wantErr == nil {
			assert.Nil(t, err, test.name)
			continue
		}
		assert.EqualError(t, test.wantErr, err.Error(), test.name)
	}
}
