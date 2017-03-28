package task

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/suite"

	"peloton/api/job"
	"peloton/api/task"
)

type TaskConfigTestSuite struct {
	suite.Suite
}

func (suite *TaskConfigTestSuite) SetupTest() {
}

func (suite *TaskConfigTestSuite) TearDownTest() {
}

func TestJobManagerTaskConfig(t *testing.T) {
	suite.Run(t, new(TaskConfigTestSuite))
}

func (suite *TaskConfigTestSuite) TestGetTaskConfigOutOfRange() {
	jobConfig := job.JobConfig{
		Name:          fmt.Sprintf("TestJob_1"),
		InstanceCount: 10,
	}

	taskConfig, err := GetTaskConfig(&jobConfig, 11)
	suite.Error(err)
	suite.True(taskConfig == nil)
}

func (suite *TaskConfigTestSuite) TestGetTaskConfigNoInstanceConfig() {
	defaultConfig := task.TaskConfig{
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000,
		},
	}
	jobConfig := job.JobConfig{
		Name:          fmt.Sprintf("TestJob_1"),
		InstanceCount: 10,
		DefaultConfig: &defaultConfig,
	}

	taskConfig, err := GetTaskConfig(&jobConfig, 1)
	suite.NoError(err)
	suite.True(reflect.DeepEqual(*taskConfig, defaultConfig))
}

func (suite *TaskConfigTestSuite) TestGetTaskConfigNoDefaultConfig() {
	instanceConfig := task.TaskConfig{
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000,
		},
	}
	jobConfig := job.JobConfig{
		Name:          fmt.Sprintf("TestJob_1"),
		InstanceCount: 10,
		InstanceConfig: map[uint32]*task.TaskConfig{
			0: &instanceConfig,
		},
	}

	taskConfig, err := GetTaskConfig(&jobConfig, 0)
	suite.NoError(err)
	suite.True(reflect.DeepEqual(*taskConfig, instanceConfig))

	taskConfig, err = GetTaskConfig(&jobConfig, 1)
	suite.NoError(err)
	suite.Equal(*taskConfig, task.TaskConfig{})
}

func (suite *TaskConfigTestSuite) TestGetTaskConfigInstanceOverride() {
	defaultConfig := task.TaskConfig{
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000,
		},
	}
	instanceConfig := task.TaskConfig{
		Name: "Instance_0",
		Resource: &task.ResourceConfig{
			CpuLimit:    1,
			MemLimitMb:  100,
			DiskLimitMb: 2000,
			FdLimit:     3000,
		},
	}
	jobConfig := job.JobConfig{
		Name:          fmt.Sprintf("TestJob_1"),
		InstanceCount: 10,
		DefaultConfig: &defaultConfig,
		InstanceConfig: map[uint32]*task.TaskConfig{
			0: &instanceConfig,
		},
	}

	taskConfig, err := GetTaskConfig(&jobConfig, 0)
	suite.NoError(err)
	suite.Equal(taskConfig.Name, instanceConfig.Name)
	suite.True(reflect.DeepEqual(taskConfig.Resource, instanceConfig.Resource))

	taskConfig, err = GetTaskConfig(&jobConfig, 1)
	suite.NoError(err)
	suite.Equal(taskConfig.Name, "")
	suite.True(reflect.DeepEqual(taskConfig.Resource, defaultConfig.Resource))
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
