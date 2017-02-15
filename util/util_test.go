package util

import (
	"fmt"
	"peloton/api/job"
	"peloton/api/task"
	"peloton/api/task/config"
	"testing"

	mesos_v1 "mesos/v1"

	"github.com/stretchr/testify/assert"
)

func TestGetOfferScalarResourceSummary(t *testing.T) {
	var offer = &mesos_v1.Offer{
		Resources: []*mesos_v1.Resource{
			NewMesosResourceBuilder().WithName("cpu").WithRole("peloton").WithValue(3.5).Build(),
			NewMesosResourceBuilder().WithName("mem").WithRole("peloton").WithValue(300.0).Build(),
			NewMesosResourceBuilder().WithName("mem").WithRole("*").WithValue(800.0).Build(),
			NewMesosResourceBuilder().WithName("mem").WithRole("*").WithValue(400.0).Build(),
			NewMesosResourceBuilder().WithName("cpu").WithValue(44.5).Build(),
			NewMesosResourceBuilder().WithName("disk").WithRole("*").WithValue(2000.0).Build(),
			NewMesosResourceBuilder().WithName("disk").WithRole("aurora").WithValue(1000.0).Build(),
		},
	}
	result := GetOfferScalarResourceSummary(offer)
	fmt.Println(result)

	assert.Equal(t, len(result), 3)
	assert.Equal(t, result["peloton"]["cpu"], 3.5)
	assert.Equal(t, result["peloton"]["mem"], 300.0)
	assert.Equal(t, result["*"]["mem"], 1200.0)
	assert.Equal(t, result["*"]["disk"], 2000.0)
	assert.Equal(t, result["*"]["cpu"], 44.5)
	assert.Equal(t, result["aurora"]["disk"], 1000.0)
}

func TestCanTakeTask(t *testing.T) {
	var offer = &mesos_v1.Offer{
		Resources: []*mesos_v1.Resource{
			NewMesosResourceBuilder().WithName("cpus").WithRole("*").WithValue(3.5).Build(),
			NewMesosResourceBuilder().WithName("mem").WithRole("peloton").WithValue(300.0).Build(),
			NewMesosResourceBuilder().WithName("mem").WithRole("*").WithValue(800.0).Build(),
			NewMesosResourceBuilder().WithName("mem").WithRole("*").WithValue(400.0).Build(),
			NewMesosResourceBuilder().WithName("cpus").WithValue(44.5).Build(),
			NewMesosResourceBuilder().WithName("disk").WithRole("*").WithValue(2000.0).Build(),
			NewMesosResourceBuilder().WithName("disk").WithRole("aurora").WithValue(1000.0).Build(),
		},
	}
	var jobID = job.JobID{Value: "TestJob_0"}
	var taskConfig = config.TaskConfig{
		Resource: &config.ResourceConfig{
			CpuLimit:    25,
			MemLimitMb:  700,
			DiskLimitMb: 200,
			FdLimit:     100,
		},
	}
	taskID := fmt.Sprintf("%s-%d-0", jobID, 20)
	var taskInfo = task.TaskInfo{
		Config:     &taskConfig,
		InstanceId: 20,
		JobId:      &jobID,
		Runtime: &task.RuntimeInfo{
			TaskId: &mesos_v1.TaskID{
				Value: &taskID,
			},
		},
	}
	offerSummary := GetOfferScalarResourceSummary(offer)
	ok := CanTakeTask(&offerSummary, &taskInfo)
	assert.True(t, ok)

	assert.Equal(t, len(offerSummary), 3)
	assert.Equal(t, offerSummary["peloton"]["mem"], 300.0)
	assert.Equal(t, offerSummary["*"]["mem"], 500.0)
	assert.Equal(t, offerSummary["*"]["disk"], 1800.0)
	assert.Equal(t, offerSummary["*"]["cpus"], 23.0)
	assert.Equal(t, offerSummary["aurora"]["disk"], 1000.0)

	ok = CanTakeTask(&offerSummary, &taskInfo)
	assert.False(t, ok)
}

func TestParseTaskID(t *testing.T) {
	jobID, instanceID, err := ParseTaskID("Test-1234")
	assert.Equal(t, jobID, "Test")
	assert.Equal(t, instanceID, 1234)
	assert.Nil(t, err)

	jobID, instanceID, err = ParseTaskID("a2342-Test_3-52344")
	assert.Equal(t, jobID, "a2342-Test_3")
	assert.Equal(t, instanceID, 52344)
	assert.Nil(t, err)

	jobID, instanceID, err = ParseTaskID("a234Test_3_52344")
	assert.Equal(t, jobID, "a234Test_3_52344")
	assert.Equal(t, instanceID, 0)
	assert.NotNil(t, err)

	jobID, instanceID, err = ParseTaskID("a234Test_3-52344qw")
	assert.NotNil(t, err)
}

func TestParseTaskIDFromMesosTaskID(t *testing.T) {
	taskID, err := ParseTaskIDFromMesosTaskID("Test-1234-11da214")
	assert.Equal(t, taskID, "Test-1234")
	assert.Nil(t, err)

	taskID, err = ParseTaskIDFromMesosTaskID("a2342-Test_3-52344-agdjhg3u4")
	assert.NotNil(t, err)

	taskID, err = ParseTaskIDFromMesosTaskID("Test-0")
	assert.Equal(t, taskID, "Test-0")
	assert.Nil(t, err)

	taskID, err = ParseTaskIDFromMesosTaskID("Test_1234-223_wde2")
	assert.NotNil(t, err)

	taskID, err = ParseTaskIDFromMesosTaskID("Test_123a")
	assert.NotNil(t, err)

	taskID, err = ParseTaskIDFromMesosTaskID("test1006-170-057fbf96-e7f1-11e6-943a-a45e60eeffd5")
	assert.Equal(t, taskID, "test1006-170")
	assert.Nil(t, err)
}

func TestNonGPUResources(t *testing.T) {
	rs := getMesosScalarResources(map[string]float64{
		"cpus": 1.0,
		"mem":  2.0,
		"disk": 3.0,
		"gpus": 0.0,
	})

	assert.Equal(t, 3, len(rs))
}

func TestLowerThanEspilonResources(t *testing.T) {
	rs := getMesosScalarResources(map[string]float64{
		"cpus": 1.0,
		"mem":  2.0,
		"disk": 3.0,
		"gpus": ResourceEspilon / 2.0,
	})

	assert.Equal(t, 3, len(rs))
}

func TestGPUResources(t *testing.T) {
	rs := getMesosScalarResources(map[string]float64{
		"cpus": 1.0,
		"mem":  2.0,
		"disk": 3.0,
		"gpus": 1.0,
	})

	assert.Equal(t, 4, len(rs))
}
