package placement

import (
	"testing"

	mesos "mesos/v1"

	"peloton/api/task"
	"peloton/api/task/config"

	"github.com/stretchr/testify/assert"
)

func TestCreateLaunchTasksRequest(t *testing.T) {
	var taskconfig0 = config.TaskConfig{
		Name: "testjob",
		Resource: &config.ResourceConfig{
			CpuLimit:    25,
			MemLimitMb:  700,
			DiskLimitMb: 200,
			FdLimit:     100,
		},
	}
	var taskid0 = "testjob-0-a98a067d-f556-11e6-a8b3-98e0d987e631"
	var runtime0 = task.RuntimeInfo{
		TaskId: &mesos.TaskID{
			Value: &taskid0,
		},
	}
	var taskconfig1 = config.TaskConfig{
		Name: "testjob",
		Resource: &config.ResourceConfig{
			CpuLimit:    15,
			MemLimitMb:  300,
			DiskLimitMb: 200,
			FdLimit:     100,
		},
	}
	var taskid1 = "testjob-1-a98a067d-f556-11e6-a8b3-98e0d987e631"
	var runtime1 = task.RuntimeInfo{
		TaskId: &mesos.TaskID{
			Value: &taskid1,
		},
	}
	lt := createLaunchableTasks(
		[]*task.TaskInfo{
			{
				InstanceId: 0,
				Config:     &taskconfig0,
				Runtime:    &runtime0,
			},
			{
				InstanceId: 1,
				Config:     &taskconfig1,
				Runtime:    &runtime1,
			},
		})

	assert.Equal(t, 2, len(lt))
	assert.Equal(t, lt[0].TaskId.GetValue(), taskid0)
	assert.ObjectsAreEqualValues(lt[0].Config, taskconfig0)
	assert.Equal(t, lt[1].TaskId.GetValue(), taskid1)
	assert.ObjectsAreEqualValues(lt[1].Config, taskconfig1)
}

func TestCreateLaunchableTasks(t *testing.T) {
	var hostname = "testhost"
	var agentID = "testagentid"
	var offerID = "testofferid"
	var offer = &mesos.Offer{
		Hostname: &hostname,
		AgentId:  &mesos.AgentID{Value: &agentID},
		Id:       &mesos.OfferID{Value: &offerID},
	}
	var taskconfig0 = config.TaskConfig{
		Name: "testjob",
		Resource: &config.ResourceConfig{
			CpuLimit:    25,
			MemLimitMb:  700,
			DiskLimitMb: 200,
			FdLimit:     100,
		},
	}
	var taskid0 = "testjob-0-a98a067d-f556-11e6-a8b3-98e0d987e631"
	var runtime0 = task.RuntimeInfo{
		TaskId: &mesos.TaskID{
			Value: &taskid0,
		},
	}
	var taskconfig1 = config.TaskConfig{
		Name: "testjob",
		Resource: &config.ResourceConfig{
			CpuLimit:    15,
			MemLimitMb:  300,
			DiskLimitMb: 200,
			FdLimit:     100,
		},
	}
	var taskid1 = "testjob-1-a98a067d-f556-11e6-a8b3-98e0d987e631"
	var runtime1 = task.RuntimeInfo{
		TaskId: &mesos.TaskID{
			Value: &taskid1,
		},
	}
	var tasks = []*task.TaskInfo{
		{
			InstanceId: 0,
			Config:     &taskconfig0,
			Runtime:    &runtime0,
		},
		{
			InstanceId: 1,
			Config:     &taskconfig1,
			Runtime:    &runtime1,
		},
	}

	ltr := createLaunchTasksRequest(tasks, offer)

	assert.Equal(t, ltr.Hostname, hostname)
	assert.Equal(t, ltr.AgentId.GetValue(), agentID)
	assert.Equal(t, ltr.OfferIds[0].GetValue(), offerID)
	assert.Equal(t, 2, len(ltr.Tasks))
}

func TestGroupTasksByResource(t *testing.T) {
	tid1 := "testjob-0-a98a067d-f556-11e6-a8b3-98e0d987e631"
	runtime1 := task.RuntimeInfo{
		TaskId: &mesos.TaskID{
			Value: &tid1,
		},
	}
	config1 := config.TaskConfig{
		Name: "testjob",
		Resource: &config.ResourceConfig{
			CpuLimit:    10,
			MemLimitMb:  10,
			DiskLimitMb: 10,
			FdLimit:     10,
		},
	}

	t1 := &task.TaskInfo{
		InstanceId: 0,
		Config:     &config1,
		Runtime:    &runtime1,
	}

	// t2 has the same config of t1
	tid2 := "testjob-1-a98a067d-f556-11e6-a8b3-98e0d987e631"
	runtime2 := task.RuntimeInfo{
		TaskId: &mesos.TaskID{
			Value: &tid2,
		},
	}

	t2 := &task.TaskInfo{
		InstanceId: 0,
		Config:     &config1,
		Runtime:    &runtime2,
	}
	result := groupTasksByResource([]*task.TaskInfo{t1, t2})
	assert.Equal(t, 1, len(result))
	key1 := config1.GetResource().String()
	assert.Contains(t, result, key1)
	group1 := result[key1]
	assert.NotNil(t, group1)
	assert.Equal(t, 2, len(group1.tasks))
	assert.Equal(t, config1.GetResource(), group1.resourceConfig)

	// t3 has a different resource config
	config3 := config.TaskConfig{
		Name: "testjob",
		Resource: &config.ResourceConfig{
			CpuLimit:    10,
			MemLimitMb:  20,
			DiskLimitMb: 20,
			FdLimit:     20,
		},
	}
	tid3 := "testjob-2-a98a067d-f556-11e6-a8b3-98e0d987e631"
	runtime3 := task.RuntimeInfo{
		TaskId: &mesos.TaskID{
			Value: &tid3,
		},
	}
	t3 := &task.TaskInfo{
		InstanceId: 2,
		Config:     &config3,
		Runtime:    &runtime3,
	}

	// t4 has a nil resource config
	config4 := config.TaskConfig{
		Name: "testjob",
	}
	tid4 := "testjob-3-a98a067d-f556-11e6-a8b3-98e0d987e631"
	runtime4 := task.RuntimeInfo{
		TaskId: &mesos.TaskID{
			Value: &tid4,
		},
	}
	t4 := &task.TaskInfo{
		InstanceId: 3,
		Config:     &config4,
		Runtime:    &runtime4,
	}

	result = groupTasksByResource([]*task.TaskInfo{t1, t2, t3, t4})
	assert.Equal(t, 3, len(result))

	key1 = config1.GetResource().String()
	assert.Contains(t, result, key1)
	group1 = result[key1]
	assert.NotNil(t, group1)
	assert.Equal(t, 2, len(group1.tasks))
	assert.Equal(t, config1.GetResource(), group1.resourceConfig)

	key3 := config3.GetResource().String()
	assert.Contains(t, result, key3)
	group3 := result[key3]
	assert.NotNil(t, group3)
	assert.Equal(t, 1, len(group3.tasks))
	assert.Equal(t, config3.GetResource(), group3.resourceConfig)

	key4 := config4.GetResource().String()
	assert.Contains(t, result, key4)
	group4 := result[key4]
	assert.NotNil(t, group4)
	assert.Equal(t, 1, len(group4.tasks))
	assert.Nil(t, group4.resourceConfig)
}
