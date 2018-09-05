package cached

import (
	"testing"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"github.com/stretchr/testify/assert"
)

func TestUpdateStrategyIsInstanceComplete(t *testing.T) {
	tests := []struct {
		taskRuntime          *pbtask.RuntimeInfo
		desiredConfigVersion uint64
		completed            bool
	}{
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				GoalState:            pbtask.TaskState_RUNNING,
				ConfigVersion:        1,
				DesiredConfigVersion: 2,
				Healthy:              pbtask.HealthState_HEALTHY,
			},
			desiredConfigVersion: 2,
			completed:            false,
		},
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				GoalState:            pbtask.TaskState_RUNNING,
				ConfigVersion:        2,
				DesiredConfigVersion: 2,
				Healthy:              pbtask.HealthState_DISABLED,
			},
			desiredConfigVersion: 2,
			completed:            true,
		},
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				GoalState:            pbtask.TaskState_RUNNING,
				ConfigVersion:        2,
				DesiredConfigVersion: 2,
				Healthy:              pbtask.HealthState_UNHEALTHY,
			},
			desiredConfigVersion: 2,
			completed:            false,
		},
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				GoalState:            pbtask.TaskState_RUNNING,
				ConfigVersion:        2,
				DesiredConfigVersion: 2,
				Healthy:              pbtask.HealthState_HEALTHY,
			},
			desiredConfigVersion: 2,
			completed:            true,
		},
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_PENDING,
				GoalState:            pbtask.TaskState_RUNNING,
				ConfigVersion:        2,
				DesiredConfigVersion: 2,
			},
			desiredConfigVersion: 2,
			completed:            false,
		},
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_KILLED,
				GoalState:            pbtask.TaskState_KILLED,
				ConfigVersion:        2,
				DesiredConfigVersion: 2,
			},
			desiredConfigVersion: 2,
			completed:            true,
		},
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_KILLED,
				GoalState:            pbtask.TaskState_KILLED,
				ConfigVersion:        1,
				DesiredConfigVersion: 2,
			},
			desiredConfigVersion: 2,
			completed:            true,
		},
	}

	for id, test := range tests {
		strategy := &updateStrategy{}
		assert.Equal(
			t,
			strategy.IsInstanceComplete(test.desiredConfigVersion, test.taskRuntime),
			test.completed,
			"test %d fails", id)
	}
}

func TestUpdateStrategyIsInstanceInProgress(t *testing.T) {
	tests := []struct {
		taskRuntime          *pbtask.RuntimeInfo
		desiredConfigVersion uint64
		inProgress           bool
	}{
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				GoalState:            pbtask.TaskState_RUNNING,
				ConfigVersion:        1,
				DesiredConfigVersion: 2,
				Healthy:              pbtask.HealthState_HEALTHY,
			},
			desiredConfigVersion: 2,
			inProgress:           true,
		},
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				GoalState:            pbtask.TaskState_RUNNING,
				ConfigVersion:        2,
				DesiredConfigVersion: 2,
				Healthy:              pbtask.HealthState_DISABLED,
			},
			desiredConfigVersion: 2,
			inProgress:           false,
		},
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				GoalState:            pbtask.TaskState_RUNNING,
				ConfigVersion:        2,
				DesiredConfigVersion: 2,
				Healthy:              pbtask.HealthState_UNHEALTHY,
			},
			desiredConfigVersion: 2,
			inProgress:           true,
		},
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				GoalState:            pbtask.TaskState_RUNNING,
				ConfigVersion:        2,
				DesiredConfigVersion: 2,
				Healthy:              pbtask.HealthState_HEALTHY,
			},
			desiredConfigVersion: 2,
			inProgress:           false,
		},
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_PENDING,
				GoalState:            pbtask.TaskState_RUNNING,
				ConfigVersion:        2,
				DesiredConfigVersion: 2,
			},
			desiredConfigVersion: 2,
			inProgress:           true,
		},
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_KILLED,
				GoalState:            pbtask.TaskState_KILLED,
				ConfigVersion:        2,
				DesiredConfigVersion: 2,
			},
			desiredConfigVersion: 2,
			inProgress:           false,
		},
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_KILLED,
				GoalState:            pbtask.TaskState_KILLED,
				ConfigVersion:        1,
				DesiredConfigVersion: 2,
			},
			desiredConfigVersion: 2,
			inProgress:           false,
		},
	}

	for id, test := range tests {
		strategy := &updateStrategy{}
		assert.Equal(
			t,
			strategy.IsInstanceInProgress(test.desiredConfigVersion, test.taskRuntime),
			test.inProgress,
			"test %d fails", id)
	}
}

func TestUpdateStrategyGetRuntimeDiff(t *testing.T) {
	configVersion := uint64(2)
	jobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{Version: configVersion},
	}
	strategy := &updateStrategy{}
	runtimeDiff := strategy.GetRuntimeDiff(nil, jobConfig)
	assert.Equal(
		t,
		runtimeDiff[DesiredConfigVersionField].(uint64),
		configVersion,
	)
}
