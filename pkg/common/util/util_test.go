// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/yarpc/yarpcerrors"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"

	"github.com/uber/peloton/pkg/common"
)

const (
	testSecretPath = "/tmp/secret"
	testSecretStr  = "top-secret-token"
)

func TestGetOfferScalarResourceSummary(t *testing.T) {
	var offer = &mesos.Offer{
		Resources: []*mesos.Resource{
			NewMesosResourceBuilder().WithName("cpu").WithRole("peloton").WithValue(3.5).Build(),
			NewMesosResourceBuilder().WithName("cpu").WithRole("peloton").WithValue(4).
				WithRevocable(&mesos.Resource_RevocableInfo{}).Build(),
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
	assert.Equal(t, result["peloton"]["cpu"], 7.5)
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

// TestSubtractSliceNil tests the subtract of nil slice as input
func TestSubtractSliceNil(t *testing.T) {
	var s1, s2 []uint32

	assert.Nil(t, SubtractSlice(s1, s2))
}

// TestSubtractSlice tests subtract of two slices s1 - s2
func TestSubtractSlice(t *testing.T) {
	slice1 := []uint32{0, 1, 2, 3}
	slice2 := []uint32{3, 4, 5}

	result := SubtractSlice(slice1, slice2)
	assert.Equal(t, 3, len(result))
	assert.Equal(t, uint32(0), result[0])
	assert.Equal(t, uint32(1), result[1])
	assert.Equal(t, uint32(2), result[2])
}

// TestIntersectSliceNil tests intersect of nil slice as input
func TestIntersectSliceNil(t *testing.T) {
	var s1, s2 []uint32

	assert.Nil(t, IntersectSlice(s1, s2))
}

// TestIntersectSlice tests the intersect of s1 n s2
func TestIntersectSlice(t *testing.T) {
	slice1 := []uint32{0, 1, 2, 3}
	slice2 := []uint32{3, 4, 5}

	result := IntersectSlice(slice1, slice2)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, uint32(3), result[0])
}

func TestParseRunID(t *testing.T) {
	mesosTaskID := uuid.New() + "-1-" + uuid.New()
	runID, err := ParseRunID(mesosTaskID)
	assert.Equal(t, runID, uint64(0))
	assert.Error(t, err)

	mesosTaskID = uuid.New() + "-1-abc"
	runID, err = ParseRunID(mesosTaskID)
	assert.Equal(t, runID, uint64(0))
	assert.Error(t, err)

	mesosTaskID = uuid.New() + "-1-1"
	runID, err = ParseRunID(mesosTaskID)
	assert.Equal(t, runID, uint64(1))
	assert.NoError(t, err)

	mesosTaskID = ""
	runID, err = ParseRunID(mesosTaskID)
	assert.Equal(t, runID, uint64(0))
	assert.Error(t, err)
}

func TestParseTaskID(t *testing.T) {
	ID := uuid.New()
	testTable := []struct {
		msg           string
		pelotonTaskID string
		jobID         string
		instanceID    uint32
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
			instanceID:    0,
			err:           yarpcerrors.InvalidArgumentErrorf("unable to parse instanceID " + ID + "-1234test"),
		},
		{
			msg:           "Incorrect pelotonTaskID - text-int",
			pelotonTaskID: "Test-1234",
			jobID:         "",
			instanceID:    0,
			err:           yarpcerrors.InvalidArgumentErrorf("invalid pelotonTaskID Test-1234"),
		},
		{
			msg:           "Incorrect pelotonTaskID - text",
			pelotonTaskID: "Test",
			jobID:         "",
			instanceID:    0,
			err:           yarpcerrors.InvalidArgumentErrorf("invalid pelotonTaskID Test"),
		},
		{
			msg:           "Incorrect pelotonTaskID - text_int",
			pelotonTaskID: "Test_1234",
			jobID:         "",
			instanceID:    0,
			err:           yarpcerrors.InvalidArgumentErrorf("invalid pelotonTaskID Test_1234"),
		},
		{
			msg:           "Incorrect pelotonTaskID - text_text",
			pelotonTaskID: "Test_1234test",
			jobID:         "",
			instanceID:    0,
			err:           yarpcerrors.InvalidArgumentErrorf("invalid pelotonTaskID Test_1234test"),
		},
	}

	for _, tt := range testTable {
		jobID, instanceID, err := ParseTaskID(tt.pelotonTaskID)
		assert.Equal(t, jobID, tt.jobID, tt.msg)
		assert.Equal(t, instanceID, tt.instanceID, tt.msg)
		assert.Equal(t, err, tt.err, tt.msg)
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
			err:           yarpcerrors.InvalidArgumentErrorf("invalid mesostaskID Test-170-1"),
		},
		{
			msg:           "Incorrect mesosTaskID text",
			mesosTaskID:   "Test",
			pelotonTaskID: "",
			err:           yarpcerrors.InvalidArgumentErrorf("invalid mesostaskID Test"),
		},
		{
			msg:           "Incorrect mesosTaskID uuid",
			mesosTaskID:   ID,
			pelotonTaskID: "",
			err:           yarpcerrors.InvalidArgumentErrorf("invalid mesostaskID " + ID),
		},
		{
			msg:           "Incorrect mesosTaskID uuid-text",
			mesosTaskID:   ID + "-test",
			pelotonTaskID: "",
			err:           yarpcerrors.InvalidArgumentErrorf("unable to parse instanceID " + ID),
		},
		{
			msg:           "Incorrect mesosTaskID uuid-text",
			mesosTaskID:   ID + "-test-1",
			pelotonTaskID: "",
			err:           yarpcerrors.InvalidArgumentErrorf("unable to parse instanceID " + ID + "-test"),
		},
	}

	for _, tt := range testTable {
		pelotonTaskID, err := ParseTaskIDFromMesosTaskID(tt.mesosTaskID)
		assert.Equal(t, pelotonTaskID, tt.pelotonTaskID, tt.msg)
		assert.Equal(t, err, tt.err, tt.msg)
	}
}

func TestParseJobAndInstanceID(t *testing.T) {
	ID := uuid.New()
	testTable := []struct {
		msg         string
		mesosTaskID string
		jobID       string
		instanceID  uint32
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
			instanceID:  0,
			err:         yarpcerrors.InvalidArgumentErrorf("unable to parse instanceID " + ID + "-test"),
		},
	}

	for _, tt := range testTable {
		jobID, instanceID, err := ParseJobAndInstanceID(tt.mesosTaskID)
		assert.Equal(t, jobID, tt.jobID, tt.msg)
		assert.Equal(t, instanceID, tt.instanceID, tt.msg)
		assert.Equal(t, err, tt.err, tt.msg)
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

func TestConvertInstanceIDListToInstanceIDRange(t *testing.T) {
	instances := []uint32{0, 1, 4, 5, 6, 10}
	instanceRange := ConvertInstanceIDListToInstanceRange(instances)
	assert.Equal(t, 3, len(instanceRange))
}

func TestContains(t *testing.T) {
	list := []string{"a", "b", "c"}
	assert.True(t, Contains(list, "a"))
	assert.False(t, Contains(list, "d"))
}

func TestContainsTaskState(t *testing.T) {
	list := []task.TaskState{task.TaskState_PLACING, task.TaskState_READY, task.TaskState_DELETED}
	assert.True(t, ContainsTaskState(list, task.TaskState_READY))
	assert.False(t, ContainsTaskState(list, task.TaskState_FAILED))
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

// helper function to generate random volume
func getVolume() *mesos.Volume {
	volumeMode := mesos.Volume_RO
	testPath := "/test"
	return &mesos.Volume{
		Mode:          &volumeMode,
		ContainerPath: &testPath,
	}
}

// createTaskConfigWithSecret creates TaskConfig with a test secret volume.
func createTaskConfigWithSecret() *task.TaskConfig {
	mesosContainerizer := mesos.ContainerInfo_MESOS
	return &task.TaskConfig{
		Container: &mesos.ContainerInfo{
			Type: &mesosContainerizer,
			Volumes: []*mesos.Volume{
				getVolume(),
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
		InstanceConfig: map[uint32]*task.TaskConfig{
			0: createTaskConfigWithSecret(),
		},
	}
	// add a non-secret container volume to default config
	jobConfig.GetDefaultConfig().GetContainer().Volumes =
		append(jobConfig.GetDefaultConfig().GetContainer().Volumes, getVolume())
	secretVolumes = RemoveSecretVolumesFromJobConfig(jobConfig)
	assert.False(t, ConfigHasSecretVolumes(jobConfig.GetDefaultConfig()))
	assert.Equal(t, len(secretVolumes), 1)
	assert.Equal(t, []byte(testSecretStr),
		secretVolumes[0].GetSource().GetSecret().GetValue().GetData())
	assert.False(t, ConfigHasSecretVolumes(jobConfig.GetInstanceConfig()[0]))
}

// TestConfigHasSecretVolumes tests if task config has secret volumes
func TestConfigHasSecretVolumes(t *testing.T) {
	cfgWithSecret := createTaskConfigWithSecret()
	cfgWithoutSecret := &task.TaskConfig{}

	assert.True(t, ConfigHasSecretVolumes(cfgWithSecret))
	assert.False(t, ConfigHasSecretVolumes(cfgWithoutSecret))
}

// Test PtrPrintf
func TestPtrPrintf(t *testing.T) {
	assert.Equal(t,
		"Thi$ test is 1ame",
		*PtrPrintf("Thi%s %v is %dame", "$", "test", 1))
}

// Test Max/Min for uint32s
func TestMaxMinUint32(t *testing.T) {
	min := uint32(15)
	max := uint32(25)
	testTable := []struct {
		value    uint32
		minValue uint32
		maxValue uint32
	}{
		{value: 10, minValue: 10, maxValue: 25},
		{value: 15, minValue: 15, maxValue: 25},
		{value: 20, minValue: 15, maxValue: 25},
		{value: 25, minValue: 15, maxValue: 25},
		{value: 30, minValue: 15, maxValue: 30},
	}
	for _, tst := range testTable {
		assert.Equal(t, tst.minValue, Min(tst.value, min))
		assert.Equal(t, tst.maxValue, Max(tst.value, max))
	}
}

// Test mesos to Peloton task state translation
func TestMesosStateToPelotonState(t *testing.T) {
	testTable := []struct {
		m mesos.TaskState
		p task.TaskState
	}{
		{m: mesos.TaskState_TASK_STAGING, p: task.TaskState_LAUNCHED},
		{m: mesos.TaskState_TASK_STARTING, p: task.TaskState_STARTING},
		{m: mesos.TaskState_TASK_RUNNING, p: task.TaskState_RUNNING},
		{m: mesos.TaskState_TASK_KILLING, p: task.TaskState_RUNNING},
		{m: mesos.TaskState_TASK_FINISHED, p: task.TaskState_SUCCEEDED},
		{m: mesos.TaskState_TASK_FAILED, p: task.TaskState_FAILED},
		{m: mesos.TaskState_TASK_KILLED, p: task.TaskState_KILLED},
		{m: mesos.TaskState_TASK_LOST, p: task.TaskState_LOST},
		{m: mesos.TaskState_TASK_ERROR, p: task.TaskState_FAILED},
		{m: 10345, p: task.TaskState_INITIALIZED},
	}
	for _, tst := range testTable {
		assert.Equal(t, tst.p, MesosStateToPelotonState(tst.m))
	}
}

// Test JSON deserialization wrapper
func TestUnmarshalToType(t *testing.T) {
	type Foo struct {
		Field1 string
		Field2 []int
	}
	obj := Foo{Field1: "abc", Field2: []int{1, 2, 3}}
	str, _ := json.Marshal(obj)
	result, err := UnmarshalToType(string(str), reflect.TypeOf(obj))
	assert.NoError(t, err)
	assert.Equal(t, &obj, result)

	_, err = UnmarshalToType(string(str), reflect.TypeOf(true))
	assert.Error(t, err)
}

// Test check for job state being terminal
func TestJobTerminalState(t *testing.T) {
	jobTerminalStates := map[job.JobState]bool{
		job.JobState_FAILED:    true,
		job.JobState_KILLED:    true,
		job.JobState_SUCCEEDED: true,
		job.JobState_DELETED:   true,
	}
	for s := range job.JobState_name {
		_, isTerm := jobTerminalStates[job.JobState(s)]
		assert.Equal(t, isTerm, IsPelotonJobStateTerminal(job.JobState(s)))
	}
}

// Test check for task state being terminal
func TestTaskTerminalState(t *testing.T) {
	taskTerminalStates := map[task.TaskState]bool{
		task.TaskState_FAILED:    true,
		task.TaskState_KILLED:    true,
		task.TaskState_SUCCEEDED: true,
		task.TaskState_LOST:      true,
		task.TaskState_DELETED:   true,
	}
	for s := range task.TaskState_name {
		_, isTerm := taskTerminalStates[task.TaskState(s)]
		assert.Equal(t, isTerm, IsPelotonStateTerminal(task.TaskState(s)))
	}
}

func TestTaskThrottled(t *testing.T) {
	tests := []struct {
		state   task.TaskState
		message string
		result  bool
	}{
		{
			state:   task.TaskState_KILLED,
			message: common.TaskThrottleMessage,
			result:  true,
		},
		{
			state:   task.TaskState_KILLED,
			message: "not throttled",
			result:  false,
		},
		{
			state:   task.TaskState_RUNNING,
			message: common.TaskThrottleMessage,
			result:  false,
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.result, IsTaskThrottled(test.state, test.message))
	}
}

// Test check for pod state being terminal
func TestPodTerminalState(t *testing.T) {
	podTerminalStates := map[pod.PodState]bool{
		pod.PodState_POD_STATE_FAILED:    true,
		pod.PodState_POD_STATE_KILLED:    true,
		pod.PodState_POD_STATE_SUCCEEDED: true,
		pod.PodState_POD_STATE_LOST:      true,
		pod.PodState_POD_STATE_DELETED:   true,
	}
	for s := range pod.PodState_name {
		_, isTerm := podTerminalStates[pod.PodState(s)]
		assert.Equal(t, isTerm, IsPelotonPodStateTerminal(pod.PodState(s)))
	}
}

// TestCreateMesosTaskID tests CreateMesosTaskID
func TestCreateMesosTaskID(t *testing.T) {
	tests := []struct {
		jobID      string
		instanceID uint32
		runID      uint64
		result     string
	}{{
		jobID:      "5f9b61a6-b290-49ef-899e-6e42dc5aabd3",
		instanceID: 0,
		runID:      1,
		result:     "5f9b61a6-b290-49ef-899e-6e42dc5aabd3-0-1",
	},
		{
			jobID:      "5f9b61a6-b290-49ef-899e-6e42dc5aabd3",
			instanceID: 3,
			runID:      1,
			result:     "5f9b61a6-b290-49ef-899e-6e42dc5aabd3-3-1",
		},

		{
			jobID:      "5f9b61a6-b290-49ef-899e-6e42dc5aabd3",
			instanceID: 3,
			runID:      3,
			result:     "5f9b61a6-b290-49ef-899e-6e42dc5aabd3-3-3",
		},
	}

	for _, test := range tests {
		mesosTaskID := CreateMesosTaskID(
			&peloton.JobID{Value: test.jobID},
			test.instanceID,
			test.runID)
		assert.Equal(t, mesosTaskID.GetValue(), test.result)
	}
}

// TestGetDereferencedJobIDsList tests GetDereferencedJobIDsList
func TestGetDereferencedJobIDsList(t *testing.T) {
	jobIDs := []*peloton.JobID{
		{Value: "test-1"},
		{Value: "test-2"},
	}
	expectedIds := GetDereferencedJobIDsList(jobIDs)
	assert.Equal(t, 2, len(expectedIds))
}

// ConvertTimestampToUnixSecondsSuccess tests ConvertTimestampToUnixSeconds
// success case
func ConvertTimestampToUnixSecondsSuccess(t *testing.T) {
	ts := "2019-03-28T00:30:06Z"
	u, err := ConvertTimestampToUnixSeconds(ts)

	assert.NoError(t, err)
	assert.Equal(t, int64(1553733006), u)
}

// ConvertTimestampToUnixSecondsFailure tests ConvertTimestampToUnixSeconds
// failure case
func ConvertTimestampToUnixSecondsFailure(t *testing.T) {
	ts := "2019-03-28T00:30:06"
	_, err := ConvertTimestampToUnixSeconds(ts)

	assert.Error(t, err)
}
