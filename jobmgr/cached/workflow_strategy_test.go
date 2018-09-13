package cached

import (
	"testing"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"

	"github.com/stretchr/testify/assert"
)

// TestUpdateStrategyIsInstanceComplete tests IsInstanceComplete
// for updateStrategy
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
		strategy := newUpdateStrategy()
		assert.Equal(
			t,
			strategy.IsInstanceComplete(test.desiredConfigVersion, test.taskRuntime),
			test.completed,
			"test %d fails", id)
	}
}

// TestUpdateStrategyIsInstanceInProgress tests IsInstanceInProgress
// for updateStrategy
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
		strategy := newUpdateStrategy()
		assert.Equal(
			t,
			strategy.IsInstanceInProgress(test.desiredConfigVersion, test.taskRuntime),
			test.inProgress,
			"test %d fails", id)
	}
}

// TestUpdateStrategyGetRuntimeDiff tests GetRuntimeDiff
// for updateStrategy
func TestUpdateStrategyGetRuntimeDiff(t *testing.T) {
	configVersion := uint64(2)
	jobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{Version: configVersion},
	}
	strategy := newUpdateStrategy()
	runtimeDiff := strategy.GetRuntimeDiff(jobConfig)
	assert.Equal(
		t,
		runtimeDiff[jobmgrcommon.DesiredConfigVersionField].(uint64),
		configVersion,
	)
	assert.Equal(
		t,
		runtimeDiff[jobmgrcommon.MessageField].(string),
		_updateTaskMessage,
	)
	assert.Len(t, runtimeDiff, 2)
}

// TestRestartStrategyIsInstanceComplete tests IsInstanceComplete
// for restartStrategy
func TestRestartStrategyIsInstanceComplete(t *testing.T) {
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
		strategy := newRestartStrategy()
		assert.Equal(
			t,
			strategy.IsInstanceComplete(test.desiredConfigVersion, test.taskRuntime),
			test.completed,
			"test %d fails", id)
	}
}

// TestRestartStrategyIsInstanceInProgress tests IsInstanceInProgress
// for restartStrategy
func TestRestartStrategyIsInstanceInProgress(t *testing.T) {
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
		strategy := newRestartStrategy()
		assert.Equal(
			t,
			strategy.IsInstanceInProgress(test.desiredConfigVersion, test.taskRuntime),
			test.inProgress,
			"test %d fails", id)
	}
}

// TestRestartStrategyGetRuntimeDiff tests GetRuntimeDiff
// for restartStrategy
func TestRestartStrategyGetRuntimeDiff(t *testing.T) {
	configVersion := uint64(2)
	jobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{Version: configVersion},
		Type:      pbjob.JobType_SERVICE,
	}
	strategy := newRestartStrategy()
	runtimeDiff := strategy.GetRuntimeDiff(jobConfig)
	assert.Equal(
		t,
		runtimeDiff[jobmgrcommon.DesiredConfigVersionField].(uint64),
		configVersion,
	)
	assert.Equal(
		t,
		runtimeDiff[jobmgrcommon.MessageField].(string),
		_restartTaskMessage,
	)
	assert.Equal(
		t,
		runtimeDiff[jobmgrcommon.GoalStateField].(pbtask.TaskState),
		getDefaultTaskGoalState(jobConfig.GetType()),
	)
}

// TestStartStrategyIsInstanceComplete tests IsInstanceComplete
// for startStrategy
func TestStartStrategyIsInstanceComplete(t *testing.T) {
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
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_KILLED,
				GoalState:            pbtask.TaskState_RUNNING,
				ConfigVersion:        1,
				DesiredConfigVersion: 1,
			},
			desiredConfigVersion: 2,
			completed:            false,
		},
	}

	for id, test := range tests {
		strategy := newStartStrategy()
		assert.Equal(
			t,
			strategy.IsInstanceComplete(test.desiredConfigVersion, test.taskRuntime),
			test.completed,
			"test %d fails", id)
	}
}

// TestStartStrategyIsInstanceInProgress tests IsInstanceInProgress
// for startStrategy
func TestStartStrategyIsInstanceInProgress(t *testing.T) {
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
		strategy := newStartStrategy()
		assert.Equal(
			t,
			strategy.IsInstanceInProgress(test.desiredConfigVersion, test.taskRuntime),
			test.inProgress,
			"test %d fails", id)
	}
}

// TestRestartStrategyGetRuntimeDiff tests GetRuntimeDiff
// for startStrategy
func TestStartStrategyGetRuntimeDiff(t *testing.T) {
	configVersion := uint64(2)
	jobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{Version: configVersion},
		Type:      pbjob.JobType_SERVICE,
	}
	strategy := newStartStrategy()
	runtimeDiff := strategy.GetRuntimeDiff(jobConfig)
	assert.Equal(
		t,
		runtimeDiff[jobmgrcommon.ConfigVersionField].(uint64),
		configVersion,
	)
	assert.Equal(
		t,
		runtimeDiff[jobmgrcommon.DesiredConfigVersionField].(uint64),
		configVersion,
	)
	assert.Equal(
		t,
		runtimeDiff[jobmgrcommon.MessageField].(string),
		_startTaskMessage,
	)
	assert.Equal(
		t,
		runtimeDiff[jobmgrcommon.GoalStateField].(pbtask.TaskState),
		getDefaultTaskGoalState(jobConfig.GetType()),
	)
}

// TestStopStrategyIsInstanceComplete tests IsInstanceComplete
// for stopStrategy
func TestStopStrategyIsInstanceComplete(t *testing.T) {
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
			completed:            false,
		},
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				GoalState:            pbtask.TaskState_KILLED,
				ConfigVersion:        2,
				DesiredConfigVersion: 2,
				Healthy:              pbtask.HealthState_UNHEALTHY,
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
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_KILLED,
				GoalState:            pbtask.TaskState_RUNNING,
				ConfigVersion:        1,
				DesiredConfigVersion: 1,
			},
			desiredConfigVersion: 2,
			completed:            false,
		},
	}

	for id, test := range tests {
		strategy := newStopStrategy()
		assert.Equal(
			t,
			strategy.IsInstanceComplete(test.desiredConfigVersion, test.taskRuntime),
			test.completed,
			"test %d fails", id)
	}
}

// TestStopStrategyIsInstanceInProgress tests IsInstanceInProgress
// for stopStrategy
func TestStopStrategyIsInstanceInProgress(t *testing.T) {
	tests := []struct {
		taskRuntime          *pbtask.RuntimeInfo
		desiredConfigVersion uint64
		inProgress           bool
	}{
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				GoalState:            pbtask.TaskState_KILLED,
				ConfigVersion:        2,
				DesiredConfigVersion: 2,
				Healthy:              pbtask.HealthState_HEALTH_UNKNOWN,
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
				Healthy:              pbtask.HealthState_HEALTH_UNKNOWN,
			},
			desiredConfigVersion: 2,
			inProgress:           false,
		},
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_KILLING,
				GoalState:            pbtask.TaskState_KILLED,
				ConfigVersion:        2,
				DesiredConfigVersion: 2,
			},
			desiredConfigVersion: 2,
			inProgress:           true,
		},
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				GoalState:            pbtask.TaskState_KILLED,
				ConfigVersion:        1,
				DesiredConfigVersion: 1,
			},
			desiredConfigVersion: 2,
			inProgress:           false,
		},
		{
			taskRuntime: &pbtask.RuntimeInfo{
				State:                pbtask.TaskState_RUNNING,
				GoalState:            pbtask.TaskState_KILLED,
				ConfigVersion:        2,
				DesiredConfigVersion: 2,
			},
			desiredConfigVersion: 2,
			inProgress:           true,
		},
	}

	for id, test := range tests {
		strategy := newStartStrategy()
		assert.Equal(
			t,
			strategy.IsInstanceInProgress(test.desiredConfigVersion, test.taskRuntime),
			test.inProgress,
			"test %d fails", id)
	}
}

// TestStopStrategyGetRuntimeDiff tests GetRuntimeDiff
// for stopStrategy
func TestStopStrategyGetRuntimeDiff(t *testing.T) {
	configVersion := uint64(2)
	jobConfig := &pbjob.JobConfig{
		ChangeLog: &peloton.ChangeLog{Version: configVersion},
		Type:      pbjob.JobType_SERVICE,
	}
	strategy := newStopStrategy()
	runtimeDiff := strategy.GetRuntimeDiff(jobConfig)

	assert.Equal(
		t,
		runtimeDiff[jobmgrcommon.DesiredConfigVersionField].(uint64),
		configVersion,
	)
	assert.Equal(
		t,
		runtimeDiff[jobmgrcommon.MessageField].(string),
		_stopTaskMessage,
	)
	assert.Equal(
		t,
		runtimeDiff[jobmgrcommon.GoalStateField].(pbtask.TaskState),
		pbtask.TaskState_KILLED,
	)
}
