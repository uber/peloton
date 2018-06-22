package jobconfig

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/suite"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/util"
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

type TaskConfigTestSuite struct {
	suite.Suite
	jobID *peloton.JobID
}

func (suite *TaskConfigTestSuite) SetupTest() {
	suite.jobID = &peloton.JobID{
		Value: "4d8ef238-3747-11e7-a919-92ebcb67fe33",
	}
}

func (suite *TaskConfigTestSuite) TearDownTest() {
}

func TestJobManagerTaskConfig(t *testing.T) {
	suite.Run(t, new(TaskConfigTestSuite))
}

func (suite *TaskConfigTestSuite) TestValidateTaskConfigSuccess() {
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

	err := ValidateTaskConfig(&jobConfig, maxTasksPerJob)
	suite.NoError(err)

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
	err = ValidateTaskConfig(&jobConfig, maxTasksPerJob)
	suite.NoError(err)
}

func (suite *TaskConfigTestSuite) TestValidateTaskConfigFailure() {
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
	err := ValidateTaskConfig(&jobConfig, maxTasksPerJob)
	suite.Error(err)
}

func (suite *TaskConfigTestSuite) TestValidateTaskConfigFailureMaxTasksPerJob() {
	jobConfig := job.JobConfig{
		Name:          fmt.Sprintf("TestJob_1"),
		InstanceCount: maxTasksPerJob + 1,
	}
	err := ValidateTaskConfig(&jobConfig, maxTasksPerJob)
	suite.Error(err)
}

func (suite *TaskConfigTestSuite) TestValidateTaskConfigFailureMaxInstances() {
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

	err := ValidateTaskConfig(&jobConfig, maxTasksPerJob)
	suite.Error(err)
}

func (suite *TaskConfigTestSuite) TestValidateTaskConfigMaxFailureRetries() {
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

	suite.Equal(int(jobConfig.GetDefaultConfig().GetRestartPolicy().GetMaxFailures()), 200)
	err := ValidateTaskConfig(&jobConfig, maxTasksPerJob)
	suite.Equal(int(jobConfig.GetDefaultConfig().GetRestartPolicy().GetMaxFailures()), 100)
	suite.NoError(err)

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
	suite.Equal(int(jobConfig.GetInstanceConfig()[0].GetRestartPolicy().GetMaxFailures()), 200)
	suite.Equal(int(jobConfig.GetInstanceConfig()[5].GetRestartPolicy().GetMaxFailures()), 5)

	err = ValidateTaskConfig(&jobConfig, maxTasksPerJob)
	suite.Equal(int(jobConfig.GetInstanceConfig()[0].GetRestartPolicy().GetMaxFailures()), _maxTaskRetries)
	suite.Equal(int(jobConfig.GetInstanceConfig()[5].GetRestartPolicy().GetMaxFailures()), 5)
	suite.NoError(err)
}

func (suite *TaskConfigTestSuite) TestValidateTaskConfigFailureMinInstances() {
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

	err := ValidateTaskConfig(&jobConfig, maxTasksPerJob)
	suite.Error(err)
}

func (suite *TaskConfigTestSuite) TestValidateTaskConfigFailureForPortConfig() {
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

	err := ValidateTaskConfig(&jobConfig, maxTasksPerJob)
	suite.Error(err)
}

func (suite *TaskConfigTestSuite) TestValidatePortConfigFailure() {
	portConfigs := []*task.PortConfig{
		{
			Value: uint32(80),
		},
	}

	err := validatePortConfig(portConfigs)
	suite.Error(err)
}

func (suite *TaskConfigTestSuite) TestValidateTaskConfigWithInvalidFieldType() {
	// Validates task config field type is string/ptr/slice/bool, otherwise
	// we cannot distinguish between unset value and default value through
	// reflection.
	taskConfig := &task.TaskConfig{}
	val := reflect.ValueOf(taskConfig).Elem()
	for i := 0; i < val.NumField(); i++ {
		kind := val.Field(i).Kind()
		suite.True(kind == reflect.String || kind == reflect.
			Ptr || kind == reflect.Slice || kind == reflect.Bool)
	}
}

func (suite *TaskConfigTestSuite) TestValidateInvalidUpdateConfig() {
	oldConfig := getConfig(oldConfig, suite.T())
	invalidNewConfig := getConfig(invalidNewConfig, suite.T())
	err := ValidateUpdatedConfig(oldConfig, invalidNewConfig, maxTasksPerJob)
	suite.Error(err)
	expectedErrors := `7 errors occurred:

* updating Name not supported
* updating Labels not supported
* updating OwningTeam not supported
* updating LdapGroups not supported
* updating DefaultConfig not supported
* new instance count can't be less
* existing instance config can't be updated`
	suite.Equal(err.Error(), expectedErrors)
}

func (suite *TaskConfigTestSuite) TestValidateValidUpdateConfig() {
	oldConfig := getConfig(oldConfig, suite.T())
	validNewConfig := getConfig(newConfig, suite.T())
	err := ValidateUpdatedConfig(oldConfig, validNewConfig, maxTasksPerJob)
	suite.NoError(err)
}

func (suite *TaskConfigTestSuite) TestValdiateInvalidUpdateConfigWithoutCmd() {
	oldConfig := getConfig(oldConfigWithoutDefaultCmd, suite.T())
	invalidNewConfig := getConfig(invalidNewConfigWithouDefaultCmd, suite.T())
	err := ValidateUpdatedConfig(oldConfig, invalidNewConfig, maxTasksPerJob)
	suite.Error(err)
	expectedErrors := `1 error occurred:

* missing command info for instance 3`
	suite.Equal(err.Error(), expectedErrors)
}
