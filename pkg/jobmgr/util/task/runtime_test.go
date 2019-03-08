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

package task

import (
	"testing"

	"github.com/stretchr/testify/assert"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"

	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
)

func TestRegenerateMesosTaskRuntime(t *testing.T) {
	testTable := []struct {
		jobID              string
		instanceID         uint32
		curMesosTaskID     string
		desiredMesosTaskID string
		newMesosTaskID     string
		initHealthState    task.HealthState
	}{
		{
			jobID:              "b64fd26b-0e39-41b7-b22a-205b69f247bd",
			instanceID:         0,
			curMesosTaskID:     "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1690f7cf-9691-42ea-8fd3-7e417246b830",
			desiredMesosTaskID: "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
			newMesosTaskID:     "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
			initHealthState:    task.HealthState_DISABLED,
		},
		{
			jobID:              "b64fd26b-0e39-41b7-b22a-205b69f247bd",
			instanceID:         0,
			curMesosTaskID:     "",
			desiredMesosTaskID: "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
			newMesosTaskID:     "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
			initHealthState:    task.HealthState_HEALTH_UNKNOWN,
		},
		{
			jobID:              "b64fd26b-0e39-41b7-b22a-205b69f247bd",
			instanceID:         0,
			curMesosTaskID:     "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-2",
			desiredMesosTaskID: "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-2",
			newMesosTaskID:     "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-3",
			initHealthState:    task.HealthState_HEALTH_UNKNOWN,
		},
	}

	for _, tt := range testTable {
		runtime := &task.RuntimeInfo{
			MesosTaskId:        &mesos.TaskID{Value: &tt.curMesosTaskID},
			DesiredMesosTaskId: &mesos.TaskID{Value: &tt.desiredMesosTaskID},
		}
		RegenerateMesosTaskRuntime(
			&peloton.JobID{Value: tt.jobID},
			tt.instanceID,
			runtime,
			tt.initHealthState,
		)

		assert.Equal(t, runtime.State, task.TaskState_INITIALIZED)
		assert.Equal(t, *runtime.PrevMesosTaskId.Value, tt.curMesosTaskID)
		assert.Equal(t, *runtime.MesosTaskId.Value, tt.newMesosTaskID)
		assert.Equal(t, *runtime.DesiredMesosTaskId.Value, tt.newMesosTaskID)
		assert.Equal(t, runtime.Healthy, tt.initHealthState)

		assert.Empty(t, runtime.AgentID)
		assert.Empty(t, runtime.StartTime)
		assert.Empty(t, runtime.CompletionTime)
		assert.Empty(t, runtime.Host)
		assert.Empty(t, runtime.Ports)
		assert.Empty(t, runtime.TerminationStatus)
	}
}

func TestRegenerateMesosTaskIDDiff(t *testing.T) {
	testTable := []struct {
		jobID              string
		instanceID         uint32
		curMesosTaskID     string
		desiredMesosTaskID string
		newMesosTaskID     string
		initHealthState    task.HealthState
	}{
		{
			jobID:              "b64fd26b-0e39-41b7-b22a-205b69f247bd",
			instanceID:         0,
			curMesosTaskID:     "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1690f7cf-9691-42ea-8fd3-7e417246b830",
			desiredMesosTaskID: "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
			newMesosTaskID:     "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
			initHealthState:    task.HealthState_DISABLED,
		},
		{
			jobID:              "b64fd26b-0e39-41b7-b22a-205b69f247bd",
			instanceID:         0,
			curMesosTaskID:     "",
			desiredMesosTaskID: "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
			newMesosTaskID:     "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
			initHealthState:    task.HealthState_HEALTH_UNKNOWN,
		},
		{
			jobID:              "b64fd26b-0e39-41b7-b22a-205b69f247bd",
			instanceID:         0,
			curMesosTaskID:     "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-2",
			desiredMesosTaskID: "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-2",
			newMesosTaskID:     "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-3",
			initHealthState:    task.HealthState_HEALTH_UNKNOWN,
		},
	}

	for _, tt := range testTable {
		runtime := &task.RuntimeInfo{
			MesosTaskId:        &mesos.TaskID{Value: &tt.curMesosTaskID},
			DesiredMesosTaskId: &mesos.TaskID{Value: &tt.desiredMesosTaskID},
		}
		diff := RegenerateMesosTaskIDDiff(
			&peloton.JobID{Value: tt.jobID},
			tt.instanceID,
			runtime,
			tt.initHealthState,
		)

		assert.Equal(t, diff[jobmgrcommon.StateField], task.TaskState_INITIALIZED)
		assert.Equal(t, *diff[jobmgrcommon.PrevMesosTaskIDField].(*mesos.TaskID).Value,
			tt.curMesosTaskID)
		assert.Equal(t, *diff[jobmgrcommon.MesosTaskIDField].(*mesos.TaskID).Value,
			tt.newMesosTaskID)
		assert.Equal(t, *diff[jobmgrcommon.DesiredMesosTaskIDField].(*mesos.TaskID).Value,
			tt.newMesosTaskID)
		assert.Equal(t, diff[jobmgrcommon.HealthyField], tt.initHealthState)

		assert.Empty(t, diff[jobmgrcommon.AgentIDField])
		assert.Empty(t, diff[jobmgrcommon.StartTimeField])
		assert.Empty(t, diff[jobmgrcommon.CompletionTimeField])
		assert.Empty(t, diff[jobmgrcommon.HostField])
		assert.Empty(t, diff[jobmgrcommon.PortsField])
		assert.Empty(t, diff[jobmgrcommon.TerminationStatusField])
	}
}

func TestGetInitialHealthState(t *testing.T) {
	testTable := []struct {
		taskConfig  *task.TaskConfig
		healthState task.HealthState
	}{
		{
			taskConfig: &task.TaskConfig{
				HealthCheck: &task.HealthCheckConfig{
					Enabled:                true,
					InitialIntervalSecs:    10,
					MaxConsecutiveFailures: 5,
					IntervalSecs:           10,
					TimeoutSecs:            5,
				},
			},
			healthState: task.HealthState_HEALTH_UNKNOWN,
		},
		{
			taskConfig:  &task.TaskConfig{},
			healthState: task.HealthState_DISABLED,
		},
		{
			taskConfig: &task.TaskConfig{
				HealthCheck: &task.HealthCheckConfig{
					Enabled:                false,
					InitialIntervalSecs:    10,
					MaxConsecutiveFailures: 5,
					IntervalSecs:           10,
					TimeoutSecs:            5,
				},
			},
			healthState: task.HealthState_DISABLED,
		},
	}

	for _, tt := range testTable {
		healthState := GetInitialHealthState(tt.taskConfig)
		assert.Equal(t, healthState, tt.healthState)
	}
}

func TestIsSystemFailure(t *testing.T) {
	testTable := []struct {
		taskRuntime     *task.RuntimeInfo
		isSystemFailure bool
	}{
		{
			&task.RuntimeInfo{
				Reason: mesos.TaskStatus_REASON_CONTAINER_LAUNCH_FAILED.String(),
			},
			true,
		},
		{
			&task.RuntimeInfo{
				Reason: mesos.TaskStatus_REASON_INVALID_OFFERS.String(),
			},
			true,
		},
		{
			&task.RuntimeInfo{
				Reason:  mesos.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED.String(),
				Message: "Container terminated with signal Broken pipe",
			},
			true,
		},
		{
			&task.RuntimeInfo{
				Reason:  mesos.TaskStatus_REASON_COMMAND_EXECUTOR_FAILED.String(),
				Message: "",
			},
			false,
		},
		{
			&task.RuntimeInfo{
				Reason: mesos.TaskState_TASK_FINISHED.String(),
			},
			false,
		},
	}

	for _, test := range testTable {
		assert.Equal(t, IsSystemFailure(test.taskRuntime), test.isSystemFailure)
	}
}

// TestGetExitStatusFromMessage tests various cases for
// GetExitStatusFromMessage
func TestGetExitStatusFromMessage(t *testing.T) {
	testTable := []struct {
		message       string
		expectedIsErr bool
		expectedCode  uint32
	}{
		{
			message:       "Command exited with status 127",
			expectedIsErr: false,
			expectedCode:  127,
		},
		{
			message:       "Command exited with status A1234",
			expectedIsErr: true,
		},
		{
			message:       "some error happened",
			expectedIsErr: true,
		},
	}
	for _, test := range testTable {
		code, err := GetExitStatusFromMessage(test.message)
		if test.expectedIsErr {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
			assert.Equal(t, test.expectedCode, code)
		}
	}
}

// TestGetSignalFromMessage tests various cases for
// GetSignalFromMessage
func TestGetSignalFromMessage(t *testing.T) {
	testTable := []struct {
		message        string
		expectedIsErr  bool
		expectedSignal string
	}{
		{
			message:        "Command terminated with signal Segmentation fault",
			expectedIsErr:  false,
			expectedSignal: "Segmentation fault",
		},
		{
			message:        "Container terminated with signal Broken pipe",
			expectedIsErr:  false,
			expectedSignal: "Broken pipe",
		},
		{
			message:       "some error happened",
			expectedIsErr: true,
		},
	}
	for _, test := range testTable {
		sig, err := GetSignalFromMessage(test.message)
		if test.expectedIsErr {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
			assert.Equal(t, test.expectedSignal, sig)
		}
	}
}
