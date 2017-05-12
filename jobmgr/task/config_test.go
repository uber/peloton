package task

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/suite"

	mesos "mesos/v1"
	"peloton/api/job"
	"peloton/api/peloton"
	"peloton/api/task"

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

func (suite *TaskConfigTestSuite) TestGetTaskConfigOutOfRange() {
	jobConfig := job.JobConfig{
		Name:          fmt.Sprintf("TestJob_1"),
		InstanceCount: 10,
	}

	taskConfig, err := GetTaskConfig(suite.jobID, &jobConfig, 11)
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

	taskConfig, err := GetTaskConfig(suite.jobID, &jobConfig, 1)
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

	taskConfig, err := GetTaskConfig(suite.jobID, &jobConfig, 0)
	suite.NoError(err)
	suite.True(reflect.DeepEqual(*taskConfig, instanceConfig))

	taskConfig, err = GetTaskConfig(suite.jobID, &jobConfig, 1)
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

	taskConfig, err := GetTaskConfig(suite.jobID, &jobConfig, 0)
	suite.NoError(err)
	suite.Equal(taskConfig.Name, instanceConfig.Name)
	suite.True(reflect.DeepEqual(taskConfig.Resource, instanceConfig.Resource))

	taskConfig, err = GetTaskConfig(suite.jobID, &jobConfig, 1)
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

func (suite *TaskConfigTestSuite) TestGetTaskConfigWithoutExistingEnviron() {
	jobConfig := job.JobConfig{
		Name:          fmt.Sprintf("TestJob_1"),
		InstanceCount: 10,
		DefaultConfig: &task.TaskConfig{
			Command: &mesos.CommandInfo{
				Value: util.PtrPrintf("echo Hello"),
			},
		},
	}

	for i := uint32(0); i < 10; i++ {
		taskConfig, err := GetTaskConfig(suite.jobID, &jobConfig, i)
		suite.NoError(err)
		suite.Equal(*taskConfig.Command.Environment,
			mesos.Environment{
				Variables: []*mesos.Environment_Variable{
					{
						Name:  util.PtrPrintf(PelotonJobID),
						Value: &suite.jobID.Value,
					},
					{
						Name:  util.PtrPrintf(PelotonInstanceID),
						Value: util.PtrPrintf("%d", i),
					},
					{
						Name:  util.PtrPrintf(PelotonTaskID),
						Value: util.PtrPrintf("%s-%d", suite.jobID.Value, i),
					},
				},
			},
		)
	}
}

func (suite *TaskConfigTestSuite) TestGetTaskConfigWithExistingEnviron() {
	jobConfig := job.JobConfig{
		Name:          fmt.Sprintf("TestJob_1"),
		InstanceCount: 10,
		DefaultConfig: &task.TaskConfig{
			Command: &mesos.CommandInfo{
				Value: util.PtrPrintf("echo Hello"),
				Environment: &mesos.Environment{
					Variables: []*mesos.Environment_Variable{
						{
							Name:  util.PtrPrintf("PATH"),
							Value: util.PtrPrintf("/usr/bin;/usr/sbin"),
						},
					},
				},
			},
		},
	}

	for i := uint32(0); i < 10; i++ {
		taskConfig, err := GetTaskConfig(suite.jobID, &jobConfig, i)
		suite.NoError(err)
		suite.Equal(*taskConfig.Command.Environment,
			mesos.Environment{
				Variables: []*mesos.Environment_Variable{
					{
						Name:  util.PtrPrintf("PATH"),
						Value: util.PtrPrintf("/usr/bin;/usr/sbin"),
					},
					{
						Name:  util.PtrPrintf(PelotonJobID),
						Value: &suite.jobID.Value,
					},
					{
						Name:  util.PtrPrintf(PelotonInstanceID),
						Value: util.PtrPrintf("%d", i),
					},
					{
						Name:  util.PtrPrintf(PelotonTaskID),
						Value: util.PtrPrintf("%s-%d", suite.jobID.Value, i),
					},
				},
			},
		)
	}
}
