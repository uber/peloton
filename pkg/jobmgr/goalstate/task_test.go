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

package goalstate

import (
	"fmt"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestTaskStateAndGoalState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobFactory := cachedmocks.NewMockJobFactory(ctrl)
	cachedJob := cachedmocks.NewMockJob(ctrl)
	cachedTask := cachedmocks.NewMockTask(ctrl)

	goalStateDriver := &driver{
		jobFactory: jobFactory,
		mtx:        NewMetrics(tally.NoopScope),
		cfg:        &Config{},
	}
	goalStateDriver.cfg.normalize()

	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)
	taskID := fmt.Sprintf("%s-%d", jobID.GetValue(), instanceID)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
		driver:     goalStateDriver,
	}

	taskState := cached.TaskStateVector{
		State:         pbtask.TaskState_RUNNING,
		ConfigVersion: 1,
	}
	taskGoalState := cached.TaskStateVector{
		State:         pbtask.TaskState_SUCCEEDED,
		ConfigVersion: 1,
	}

	// Test fetching the entity ID
	assert.Equal(t, taskID, taskEnt.GetID())

	// Test fetching the entity state
	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		CurrentState().Return(taskState)

	actState := taskEnt.GetState()
	assert.Equal(t, taskState, actState.(cached.TaskStateVector))

	// Test fetching the entity goal state
	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(cachedTask)

	cachedTask.EXPECT().
		GoalState().Return(taskGoalState)

	actGoalState := taskEnt.GetGoalState()
	assert.Equal(t, taskGoalState, actGoalState.(cached.TaskStateVector))

	// No cached task
	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(nil)

	actState = taskEnt.GetState()
	assert.Equal(t, pbtask.TaskState_UNKNOWN, actState.(cached.TaskStateVector).State)

	jobFactory.EXPECT().
		AddJob(jobID).
		Return(cachedJob)

	cachedJob.EXPECT().
		GetTask(instanceID).Return(nil)

	actGoalState = taskEnt.GetGoalState()
	assert.Equal(t, pbtask.TaskState_UNKNOWN, actGoalState.(cached.TaskStateVector).State)
}

func TestTaskActionList(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
	}

	tt := []struct {
		currentState pbtask.TaskState
		goalState    pbtask.TaskState
		lengthAction int
	}{
		{
			currentState: pbtask.TaskState_RUNNING,
			goalState:    pbtask.TaskState_SUCCEEDED,
			lengthAction: 0,
		},
		{
			currentState: pbtask.TaskState_INITIALIZED,
			goalState:    pbtask.TaskState_SUCCEEDED,
			lengthAction: 1,
		},
		{
			currentState: pbtask.TaskState_UNKNOWN,
			goalState:    pbtask.TaskState_SUCCEEDED,
			lengthAction: 1,
		},
	}

	for i, test := range tt {
		taskState := cached.TaskStateVector{
			State: test.currentState,
		}
		taskGoalState := cached.TaskStateVector{
			State: test.goalState,
		}

		_, _, actions := taskEnt.GetActionList(taskState, taskGoalState)
		assert.Equal(t, test.lengthAction, len(actions), "test %d fails", i)
	}
}

func TestEngineSuggestActionGoalKilled(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
	}

	tt := []struct {
		currentState         pbtask.TaskState
		configVersion        uint64
		desiredConfigVersion uint64
		action               TaskAction
	}{
		{
			currentState:         pbtask.TaskState_RUNNING,
			configVersion:        0,
			desiredConfigVersion: 0,
			action:               StopAction,
		},
		{
			currentState:         pbtask.TaskState_RUNNING,
			configVersion:        10,
			desiredConfigVersion: 10,
			action:               StopAction,
		},
		{
			currentState:         pbtask.TaskState_STARTING,
			configVersion:        10,
			desiredConfigVersion: 10,
			action:               StopAction,
		},
		{
			currentState:         pbtask.TaskState_LAUNCHED,
			configVersion:        10,
			desiredConfigVersion: 10,
			action:               StopAction,
		},
		{
			currentState:         pbtask.TaskState_KILLING,
			configVersion:        10,
			desiredConfigVersion: 10,
			action:               ExecutorShutdownAction,
		},
		{
			currentState:         pbtask.TaskState_KILLED,
			configVersion:        10,
			desiredConfigVersion: 11,
			action:               NoTaskAction,
		},
	}

	for i, test := range tt {
		a := taskEnt.suggestTaskAction(
			cached.TaskStateVector{
				State:         test.currentState,
				ConfigVersion: test.configVersion,
			},
			cached.TaskStateVector{
				State:         pbtask.TaskState_KILLED,
				ConfigVersion: test.desiredConfigVersion,
			},
		)
		assert.Equal(t, test.action, a, "test %d fails", i)
	}
}

func TestEngineSuggestActionLaunchedState(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
	}

	a := taskEnt.suggestTaskAction(
		cached.TaskStateVector{State: pbtask.TaskState_LAUNCHED, ConfigVersion: 0},
		cached.TaskStateVector{State: pbtask.TaskState_SUCCEEDED, ConfigVersion: 0})
	assert.Equal(t, LaunchRetryAction, a)
}

func TestEngineSuggestActionGoalRunning(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
	}

	tt := []struct {
		currentState         pbtask.TaskState
		configVersion        uint64
		desiredConfigVersion uint64
		action               TaskAction
	}{
		{
			currentState:         pbtask.TaskState_RUNNING,
			configVersion:        0,
			desiredConfigVersion: 0,
			action:               NoTaskAction,
		},
		{
			currentState:         pbtask.TaskState_RUNNING,
			configVersion:        0,
			desiredConfigVersion: 1,
			action:               StopAction,
		},
		{
			currentState:         pbtask.TaskState_KILLED,
			configVersion:        0,
			desiredConfigVersion: 1,
			action:               InitializeAction,
		},
		{
			currentState:         pbtask.TaskState_INITIALIZED,
			configVersion:        0,
			desiredConfigVersion: 0,
			action:               StartAction,
		},
		{
			currentState:         pbtask.TaskState_INITIALIZED,
			configVersion:        123,
			desiredConfigVersion: 123,
			action:               StartAction,
		},
		{
			currentState:         pbtask.TaskState_STARTING,
			configVersion:        0,
			desiredConfigVersion: 0,
			action:               LaunchRetryAction,
		},
		{
			currentState:         pbtask.TaskState_LAUNCHED,
			configVersion:        0,
			desiredConfigVersion: 0,
			action:               LaunchRetryAction,
		},
	}

	for i, test := range tt {
		a := taskEnt.suggestTaskAction(
			cached.TaskStateVector{
				State:         test.currentState,
				ConfigVersion: test.configVersion,
			},
			cached.TaskStateVector{
				State:         pbtask.TaskState_RUNNING,
				ConfigVersion: test.desiredConfigVersion,
			},
		)
		assert.Equal(t, test.action, a, "test %d fails", i)
	}
}

// Task with goal state FAILED should always invoke TaskStateInvalidAction
func TestEngineSuggestActionGoalFailed(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
	}

	testStates := []pbtask.TaskState{
		pbtask.TaskState_INITIALIZED,
		pbtask.TaskState_PENDING,
		pbtask.TaskState_LAUNCHED,
		pbtask.TaskState_STARTING,
		pbtask.TaskState_RUNNING,
		pbtask.TaskState_SUCCEEDED,
		pbtask.TaskState_FAILED,
		pbtask.TaskState_LOST,
		pbtask.TaskState_KILLING,
		pbtask.TaskState_KILLED,
	}

	for i, state := range testStates {
		a := taskEnt.suggestTaskAction(
			cached.TaskStateVector{State: state, ConfigVersion: 0},
			cached.TaskStateVector{State: pbtask.TaskState_FAILED, ConfigVersion: 0})
		assert.Equal(t, TaskStateInvalidAction, a, "test %d fails", i)
	}

}

func TestEngineSuggestActionGoalDeleted(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
	}

	tt := []struct {
		currentState         pbtask.TaskState
		configVersion        uint64
		desiredConfigVersion uint64
		action               TaskAction
	}{
		{
			currentState:         pbtask.TaskState_RUNNING,
			configVersion:        0,
			desiredConfigVersion: 0,
			action:               StopAction,
		},
		{
			currentState:         pbtask.TaskState_RUNNING,
			configVersion:        10,
			desiredConfigVersion: 10,
			action:               StopAction,
		},
		{
			currentState:         pbtask.TaskState_STARTING,
			configVersion:        10,
			desiredConfigVersion: 10,
			action:               StopAction,
		},
		{
			currentState:         pbtask.TaskState_LAUNCHED,
			configVersion:        10,
			desiredConfigVersion: 10,
			action:               StopAction,
		},
		{
			currentState:         pbtask.TaskState_KILLING,
			configVersion:        10,
			desiredConfigVersion: 10,
			action:               ExecutorShutdownAction,
		},
		{
			currentState:         pbtask.TaskState_KILLED,
			configVersion:        10,
			desiredConfigVersion: 11,
			action:               DeleteAction,
		},
		{
			currentState:         pbtask.TaskState_FAILED,
			configVersion:        10,
			desiredConfigVersion: 11,
			action:               DeleteAction,
		},
		{
			currentState:         pbtask.TaskState_SUCCEEDED,
			configVersion:        10,
			desiredConfigVersion: 11,
			action:               DeleteAction,
		},
		{
			currentState:         pbtask.TaskState_LOST,
			configVersion:        10,
			desiredConfigVersion: 11,
			action:               DeleteAction,
		},
	}

	for i, test := range tt {
		a := taskEnt.suggestTaskAction(
			cached.TaskStateVector{
				State:         test.currentState,
				ConfigVersion: test.configVersion,
			},
			cached.TaskStateVector{
				State:         pbtask.TaskState_DELETED,
				ConfigVersion: test.desiredConfigVersion,
			},
		)
		assert.Equal(t, test.action, a, "test %d fails", i)
	}
}

// TestEngineSuggestActionRestart tests task action suggestion
// given mesos id and desired mesos id
func TestEngineSuggestActionRestart(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	instanceID := uint32(0)

	taskEnt := &taskEntity{
		jobID:      jobID,
		instanceID: instanceID,
	}

	tt := []struct {
		currentState       pbtask.TaskState
		desiredState       pbtask.TaskState
		mesosTaskID        string
		desiredMesosTaskID string
		action             TaskAction
	}{
		{
			currentState:       pbtask.TaskState_KILLING,
			desiredState:       pbtask.TaskState_KILLED,
			mesosTaskID:        "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
			desiredMesosTaskID: "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
			action:             ExecutorShutdownAction,
		},
		{
			currentState:       pbtask.TaskState_RUNNING,
			desiredState:       pbtask.TaskState_RUNNING,
			mesosTaskID:        "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
			desiredMesosTaskID: "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-2",
			action:             StopAction,
		},
		{
			currentState:       pbtask.TaskState_KILLED,
			desiredState:       pbtask.TaskState_KILLED,
			mesosTaskID:        "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
			desiredMesosTaskID: "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-2",
			action:             NoTaskAction,
		},
		{
			currentState:       pbtask.TaskState_KILLED,
			desiredState:       pbtask.TaskState_RUNNING,
			mesosTaskID:        "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
			desiredMesosTaskID: "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-2",
			action:             InitializeAction,
		},
		{
			currentState:       pbtask.TaskState_KILLED,
			desiredState:       pbtask.TaskState_KILLED,
			mesosTaskID:        "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
			desiredMesosTaskID: "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
			action:             NoTaskAction,
		},
		{
			currentState:       pbtask.TaskState_RUNNING,
			desiredState:       pbtask.TaskState_KILLED,
			mesosTaskID:        "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-1",
			desiredMesosTaskID: "b64fd26b-0e39-41b7-b22a-205b69f247bd-0-2",
			action:             StopAction,
		},
	}

	for i, test := range tt {
		a := taskEnt.suggestTaskAction(
			cached.TaskStateVector{
				State:       test.currentState,
				MesosTaskID: &mesos_v1.TaskID{Value: &test.mesosTaskID},
			},
			cached.TaskStateVector{
				State:       test.desiredState,
				MesosTaskID: &mesos_v1.TaskID{Value: &test.desiredMesosTaskID},
			},
		)
		assert.Equal(t, test.action, a, "test %d fails", i)
	}
}
