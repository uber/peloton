package task

import (
	"testing"

	"github.com/stretchr/testify/assert"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"
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
