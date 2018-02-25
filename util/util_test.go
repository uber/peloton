package util

import (
	"fmt"
	"testing"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"github.com/pborman/uuid"
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

func TestConvertLabels(t *testing.T) {
	pelotonLabels := []*peloton.Label{
		{
			Key:   "key",
			Value: "value",
		},
	}

	mesosLabels := ConvertLabels(pelotonLabels)

	assert.Equal(t, len(pelotonLabels), len(mesosLabels.GetLabels()))
	assert.Equal(t, pelotonLabels[0].GetKey(), mesosLabels.GetLabels()[0].GetKey())
	assert.Equal(t, pelotonLabels[0].GetValue(), mesosLabels.GetLabels()[0].GetValue())
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
	assert.Equal(t, taskID, "")
	assert.NotNil(t, err)

	taskID, err = ParseTaskIDFromMesosTaskID("a2342-Test_3-" + uuid.NewUUID().String())
	assert.NotNil(t, err)
	assert.Equal(t, taskID, "")

	taskID, err = ParseTaskIDFromMesosTaskID("a2342-Test-3-" + uuid.NewUUID().String())
	assert.Nil(t, err)
	assert.Equal(t, taskID, "a2342-Test-3")

	taskID, err = ParseTaskIDFromMesosTaskID("Test-0")
	assert.Equal(t, taskID, "")
	assert.NotNil(t, err)

	taskID, err = ParseTaskIDFromMesosTaskID("Test_1234-223_wde2")
	assert.NotNil(t, err)

	taskID, err = ParseTaskIDFromMesosTaskID("Test_123a")
	assert.NotNil(t, err)

	taskID, err = ParseTaskIDFromMesosTaskID("test1006-170-057fbf96-e7f1-11e6-943a-a45e60eeffd5")
	assert.Equal(t, taskID, "test1006-170")
	assert.Nil(t, err)
}

func TestNonGPUResources(t *testing.T) {
	rs := CreateMesosScalarResources(map[string]float64{
		"cpus": 1.0,
		"mem":  2.0,
		"disk": 3.0,
		"gpus": 0.0,
	}, "*")

	assert.Equal(t, 3, len(rs))
}

func TestLowerThanEspilonResources(t *testing.T) {
	rs := CreateMesosScalarResources(map[string]float64{
		"cpus": 1.0,
		"mem":  2.0,
		"disk": 3.0,
		"gpus": ResourceEpsilon / 2.0,
	}, "*")

	assert.Equal(t, 3, len(rs))
}

func TestGPUResources(t *testing.T) {
	rs := CreateMesosScalarResources(map[string]float64{
		"cpus": 1.0,
		"mem":  2.0,
		"disk": 3.0,
		"gpus": 1.0,
	}, "*")

	assert.Equal(t, 4, len(rs))
}

func TestGetTaskType(t *testing.T) {
	tt := []struct {
		cfg *task.TaskConfig
		tt  resmgr.TaskType
	}{

		{
			cfg: &task.TaskConfig{
				Controller: true,
			},
			tt: resmgr.TaskType_CONTROLLER,
		},
		{
			cfg: &task.TaskConfig{
				Volume: &task.PersistentVolumeConfig{},
			},
			tt: resmgr.TaskType_STATEFUL,
		},
		{
			cfg: &task.TaskConfig{},
			tt:  resmgr.TaskType_BATCH,
		},
	}

	for _, test := range tt {
		assert.Equal(t, test.tt, getTaskType(test.cfg))
	}
}
