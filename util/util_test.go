package util_test

import (
	"code.uber.internal/infra/peloton/util"
	"fmt"
	"github.com/stretchr/testify/assert"
	"mesos/v1"
	"peloton/job"
	"peloton/task"
	"testing"
)

func TestGetOfferScalarResourceSummary(t *testing.T) {
	var offer = &mesos_v1.Offer{
		Resources: []*mesos_v1.Resource{
			util.NewMesosResourceBuilder().WithName("cpu").WithRole("peloton").WithValue(3.5).Build(),
			util.NewMesosResourceBuilder().WithName("mem").WithRole("peloton").WithValue(300.0).Build(),
			util.NewMesosResourceBuilder().WithName("mem").WithRole("*").WithValue(800.0).Build(),
			util.NewMesosResourceBuilder().WithName("mem").WithRole("*").WithValue(400.0).Build(),
			util.NewMesosResourceBuilder().WithName("cpu").WithValue(44.5).Build(),
			util.NewMesosResourceBuilder().WithName("disk").WithRole("*").WithValue(2000.0).Build(),
			util.NewMesosResourceBuilder().WithName("disk").WithRole("aurora").WithValue(1000.0).Build(),
		},
	}
	result := util.GetOfferScalarResourceSummary(offer)
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
			util.NewMesosResourceBuilder().WithName("cpus").WithRole("*").WithValue(3.5).Build(),
			util.NewMesosResourceBuilder().WithName("mem").WithRole("peloton").WithValue(300.0).Build(),
			util.NewMesosResourceBuilder().WithName("mem").WithRole("*").WithValue(800.0).Build(),
			util.NewMesosResourceBuilder().WithName("mem").WithRole("*").WithValue(400.0).Build(),
			util.NewMesosResourceBuilder().WithName("cpus").WithValue(44.5).Build(),
			util.NewMesosResourceBuilder().WithName("disk").WithRole("*").WithValue(2000.0).Build(),
			util.NewMesosResourceBuilder().WithName("disk").WithRole("aurora").WithValue(1000.0).Build(),
		},
	}
	var jobID = job.JobID{Value: "TestJob_0"}
	var jobConfig = job.JobConfig{
		Name:       "TestJob_0",
		OwningTeam: "team6",
		LdapGroups: []string{"money", "team6", "otto"},
		Resource: &job.ResourceConfig{
			CpusLimit:   25,
			MemLimitMb:  700,
			DiskLimitMb: 200,
			FdLimit:     100,
		},
	}

	var taskInfo = task.TaskInfo{
		JobConfig:  &jobConfig,
		InstanceId: 20,
		JobId:      &jobID,
	}
	offerSummary := util.GetOfferScalarResourceSummary(offer)
	ok := util.CanTakeTask(&offerSummary, &taskInfo)
	assert.True(t, ok)

	assert.Equal(t, len(offerSummary), 3)
	assert.Equal(t, offerSummary["peloton"]["mem"], 300.0)
	assert.Equal(t, offerSummary["*"]["mem"], 500.0)
	assert.Equal(t, offerSummary["*"]["disk"], 1800.0)
	assert.Equal(t, offerSummary["*"]["cpus"], 23.0)
	assert.Equal(t, offerSummary["aurora"]["disk"], 1000.0)

	ok = util.CanTakeTask(&offerSummary, &taskInfo)
	assert.False(t, ok)
}
