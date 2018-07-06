package util

import (
	"errors"
	"fmt"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
)

const (
	testSecretPath = "/tmp/secret"
	testSecretStr  = "top-secret-token"
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

func TestParseRunID(t *testing.T) {
	mesosTaskID := uuid.New() + "-1-" + uuid.New()
	runID, err := ParseRunID(mesosTaskID)
	assert.Equal(t, runID, -1)
	assert.Error(t, err)

	mesosTaskID = uuid.New() + "-1-abc"
	runID, err = ParseRunID(mesosTaskID)
	assert.Equal(t, runID, -1)
	assert.Error(t, err)

	mesosTaskID = uuid.New() + "-1-1"
	runID, err = ParseRunID(mesosTaskID)
	assert.Equal(t, runID, 1)
	assert.NoError(t, err)

	mesosTaskID = ""
	runID, err = ParseRunID(mesosTaskID)
	assert.Equal(t, runID, -1)
	assert.NoError(t, err)
}

func TestParseTaskID(t *testing.T) {
	ID := uuid.New()
	testTable := []struct {
		msg           string
		pelotonTaskID string
		jobID         string
		instanceID    int
		err           error
	}{
		{
			msg:           "Correct pelotonTaskID - uuid-int",
			pelotonTaskID: ID + "-1234",
			jobID:         ID,
			instanceID:    1234,
			err:           nil,
		},
		{
			msg:           "Incorrect pelotonTaskID - uuid_text",
			pelotonTaskID: ID + "-1234test",
			jobID:         "",
			instanceID:    -1,
			err:           errors.New("unable to parse instanceID " + ID + "-1234test"),
		},
		{
			msg:           "Incorrect pelotonTaskID - text-int",
			pelotonTaskID: "Test-1234",
			jobID:         "",
			instanceID:    -1,
			err:           errors.New("invalid pelotonTaskID Test-1234"),
		},
		{
			msg:           "Incorrect pelotonTaskID - text",
			pelotonTaskID: "Test",
			jobID:         "",
			instanceID:    -1,
			err:           errors.New("invalid pelotonTaskID Test"),
		},
		{
			msg:           "Incorrect pelotonTaskID - text_int",
			pelotonTaskID: "Test_1234",
			jobID:         "",
			instanceID:    -1,
			err:           errors.New("invalid pelotonTaskID Test_1234"),
		},
		{
			msg:           "Incorrect pelotonTaskID - text_text",
			pelotonTaskID: "Test_1234test",
			jobID:         "",
			instanceID:    -1,
			err:           errors.New("invalid pelotonTaskID Test_1234test"),
		},
	}

	for _, tt := range testTable {
		jobID, instanceID, err := ParseTaskID(tt.pelotonTaskID)
		assert.Equal(t, jobID, tt.jobID)
		assert.Equal(t, instanceID, tt.instanceID)
		assert.Equal(t, err, tt.err)
	}
}

func TestParseTaskIDFromMesosTaskID(t *testing.T) {
	ID := uuid.New()
	testTable := []struct {
		msg           string
		mesosTaskID   string
		pelotonTaskID string
		err           error
	}{
		{
			msg:           "Correct mesosTaskID uuid-instanceid-runid(uuid)",
			mesosTaskID:   ID + "-170-" + ID,
			pelotonTaskID: ID + "-170",
			err:           nil,
		},
		{
			msg:           "Correct mesosTaskID uuid-instanceid-runid(int)",
			mesosTaskID:   ID + "-170-1",
			pelotonTaskID: ID + "-170",
			err:           nil,
		},
		{
			msg:           "Incorrect mesosTaskID text-instanceid-runid(int)",
			mesosTaskID:   "Test-170-1",
			pelotonTaskID: "",
			err:           errors.New("invalid mesostaskID Test-170-1"),
		},
		{
			msg:           "Incorrect mesosTaskID text",
			mesosTaskID:   "Test",
			pelotonTaskID: "",
			err:           errors.New("invalid mesostaskID Test"),
		},
		{
			msg:           "Incorrect mesosTaskID uuid",
			mesosTaskID:   ID,
			pelotonTaskID: "",
			err:           errors.New("invalid mesostaskID " + ID),
		},
		{
			msg:           "Incorrect mesosTaskID uuid-text",
			mesosTaskID:   ID + "-test",
			pelotonTaskID: "",
			err:           errors.New("unable to parse instanceID " + ID),
		},
		{
			msg:           "Incorrect mesosTaskID uuid-text",
			mesosTaskID:   ID + "-test-1",
			pelotonTaskID: "",
			err:           errors.New("unable to parse instanceID " + ID + "-test"),
		},
	}

	for _, tt := range testTable {
		pelotonTaskID, err := ParseTaskIDFromMesosTaskID(tt.mesosTaskID)
		assert.Equal(t, pelotonTaskID, tt.pelotonTaskID)
		assert.Equal(t, err, tt.err)
	}
}

func TestParseJobAndInstanceID(t *testing.T) {
	ID := uuid.New()
	testTable := []struct {
		msg         string
		mesosTaskID string
		jobID       string
		instanceID  int
		err         error
	}{
		{
			msg:         "Correct mesosTaskID uuid-instanceid-runid(uuid)",
			mesosTaskID: ID + "-1-" + ID,
			jobID:       ID,
			instanceID:  1,
			err:         nil,
		},
		{
			msg:         "Correct mesosTaskID uuid-instanceid-runid(int)",
			mesosTaskID: ID + "-1-" + "1",
			jobID:       ID,
			instanceID:  1,
			err:         nil,
		},
		{
			msg:         "Incorrect mesosTaskID uuid-text-runid(int)",
			mesosTaskID: ID + "-test-" + "1",
			jobID:       "",
			instanceID:  -1,
			err:         errors.New("unable to parse instanceID " + ID + "-test"),
		},
	}

	for _, tt := range testTable {
		jobID, instanceID, err := ParseJobAndInstanceID(tt.mesosTaskID)
		assert.Equal(t, jobID, tt.jobID)
		assert.Equal(t, instanceID, tt.instanceID)
		assert.Equal(t, err, tt.err)
	}
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

func TestContains(t *testing.T) {
	list := []string{"a", "b", "c"}
	assert.True(t, Contains(list, "a"))
	assert.False(t, Contains(list, "d"))
}

// TestCreateHostInfo tests the HostInfo from Mesos Agent Info
func TestCreateHostInfo(t *testing.T) {
	// Testing if AgentInfo is nil
	res := CreateHostInfo("", nil)
	// HostInfo should be nil
	assert.Nil(t, res)
	hostname := "host1"
	// Creating Valid Agent Info
	res = CreateHostInfo(hostname, &mesos.AgentInfo{
		Hostname: &hostname,
		Id: &mesos.AgentID{
			Value: &hostname,
		},
		Resources:  nil,
		Attributes: nil,
	})
	// Valid Host Info should be returned
	assert.NotNil(t, res)
	// Validating host name
	assert.Equal(t, res.Hostname, hostname)
}

// createTaskConfigWithSecret creates TaskConfig with a test secret volume.
func createTaskConfigWithSecret() *task.TaskConfig {
	mesosContainerizer := mesos.ContainerInfo_MESOS
	return &task.TaskConfig{
		Container: &mesos.ContainerInfo{
			Type: &mesosContainerizer,
			Volumes: []*mesos.Volume{
				CreateSecretVolume(testSecretPath, testSecretStr),
			},
		},
	}
}

// TestRemoveSecretVolumesFromJobConfig tests separating secret volumes from
// jobconfig
func TestRemoveSecretVolumesFromJobConfig(t *testing.T) {
	secretVolumes := RemoveSecretVolumesFromJobConfig(&job.JobConfig{})
	assert.Equal(t, len(secretVolumes), 0)

	jobConfig := &job.JobConfig{
		DefaultConfig: createTaskConfigWithSecret(),
	}
	// add a non-secret container volume to default config
	volumeMode := mesos.Volume_RO
	testPath := "/test"
	volume := &mesos.Volume{
		Mode:          &volumeMode,
		ContainerPath: &testPath,
	}
	jobConfig.GetDefaultConfig().GetContainer().Volumes =
		append(jobConfig.GetDefaultConfig().GetContainer().Volumes, volume)
	secretVolumes = RemoveSecretVolumesFromJobConfig(jobConfig)
	assert.False(t, ConfigHasSecretVolumes(jobConfig.GetDefaultConfig()))
	assert.Equal(t, len(secretVolumes), 1)
	assert.Equal(t, []byte(testSecretStr),
		secretVolumes[0].GetSource().GetSecret().GetValue().GetData())
}
