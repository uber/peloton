package taskconfig

import (
	"testing"

	mesos_v1 "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/util"
	"github.com/stretchr/testify/assert"
)

func TestMergeNoInstanceConfig(t *testing.T) {
	defaultConfig := &task.TaskConfig{
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000,
		},
	}

	assert.Equal(t, defaultConfig, Merge(defaultConfig, nil))
}

func TestMergeNoDefaultConfig(t *testing.T) {
	instanceConfig := &task.TaskConfig{
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000,
		},
	}

	assert.Equal(t, instanceConfig, Merge(nil, instanceConfig))
	assert.Equal(t, (*task.TaskConfig)(nil), Merge(nil, nil))
}

func TestMergeInstanceOverride(t *testing.T) {
	defaultConfig := &task.TaskConfig{
		Name: "Instance_X",
		Resource: &task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000,
		},
	}
	instanceConfig := &task.TaskConfig{
		Resource: &task.ResourceConfig{
			CpuLimit:    1,
			MemLimitMb:  100,
			DiskLimitMb: 2000,
			FdLimit:     3000,
		},
	}

	assert.Equal(t, instanceConfig.Resource, Merge(defaultConfig, instanceConfig).Resource)
	assert.Equal(t, defaultConfig.Name, Merge(defaultConfig, instanceConfig).Name)
}

func TestMergeWithExistingEnviron(t *testing.T) {
	defaultConfig := &task.TaskConfig{
		Command: &mesos_v1.CommandInfo{
			Value: util.PtrPrintf("echo Hello"),
			Environment: &mesos_v1.Environment{
				Variables: []*mesos_v1.Environment_Variable{
					{
						Name:  util.PtrPrintf("PATH"),
						Value: util.PtrPrintf("/usr/bin;/usr/sbin"),
					},
				},
			},
		},
	}

	assert.Equal(t, &mesos_v1.Environment{
		Variables: []*mesos_v1.Environment_Variable{
			{
				Name:  util.PtrPrintf("PATH"),
				Value: util.PtrPrintf("/usr/bin;/usr/sbin"),
			},
		},
	}, Merge(defaultConfig, nil).Command.Environment)
}
