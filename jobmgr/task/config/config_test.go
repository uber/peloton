package config

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/suite"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

	"code.uber.internal/infra/peloton/util"
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

	err := ValidateTaskConfig(&jobConfig)
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
	err = ValidateTaskConfig(&jobConfig)
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
	err := ValidateTaskConfig(&jobConfig)
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
		Sla:           &sla,
		DefaultConfig: &taskConfig,
	}

	err := ValidateTaskConfig(&jobConfig)
	suite.Error(err)
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
		Sla:           &sla,
		DefaultConfig: &taskConfig,
	}

	err := ValidateTaskConfig(&jobConfig)
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

	err := ValidateTaskConfig(&jobConfig)
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
	// Validates task config field type is string/ptr/slice, otherwise
	// we cannot distinguish between unset value and default value through
	// reflection.
	taskConfig := &task.TaskConfig{}
	val := reflect.ValueOf(taskConfig).Elem()
	for i := 0; i < val.NumField(); i++ {
		kind := val.Field(i).Kind()
		suite.True(kind == reflect.String || kind == reflect.Ptr || kind == reflect.Slice)
	}
}
