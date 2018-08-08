package task

import (
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
)

func TestGetTaskType(t *testing.T) {
	tt := []struct {
		cfg      *task.TaskConfig
		jobType  job.JobType
		taskType resmgr.TaskType
	}{
		{
			cfg: &task.TaskConfig{
				Volume: &task.PersistentVolumeConfig{},
			},
			jobType:  job.JobType_SERVICE,
			taskType: resmgr.TaskType_STATEFUL,
		},
		{
			cfg:      &task.TaskConfig{},
			jobType:  job.JobType_BATCH,
			taskType: resmgr.TaskType_BATCH,
		},
		{
			cfg:      &task.TaskConfig{},
			jobType:  job.JobType_SERVICE,
			taskType: resmgr.TaskType_STATELESS,
		},
	}

	for _, test := range tt {
		assert.Equal(t, test.taskType, getTaskType(test.cfg, test.jobType))
	}
}

func TestConvertTaskToResMgrTask(t *testing.T) {
	taskInfo := &task.TaskInfo{
		InstanceId: 0,
		JobId:      &peloton.JobID{Value: uuid.New()},
		Config: &task.TaskConfig{
			Ports: []*task.PortConfig{{Name: "http", Value: 0}},
		},
		Runtime: &task.RuntimeInfo{
			State: task.TaskState_RUNNING,
			Host:  "hostname",
		},
	}

	jobConfig := &job.JobConfig{
		SLA: &job.SlaConfig{},
	}
	rmTask := ConvertTaskToResMgrTask(taskInfo, jobConfig)
	assert.Equal(t, taskInfo.JobId.Value, rmTask.JobId.Value)
	assert.Equal(t, uint32(len(taskInfo.Config.Ports)), rmTask.NumPorts)
	assert.Equal(t, taskInfo.GetRuntime().GetHost(), rmTask.GetHostname())
}

func TestConvertToResMgrGangs(t *testing.T) {
	jobConfig := &job.JobConfig{
		SLA: &job.SlaConfig{
			MinimumRunningInstances: 2,
		},
	}

	gangs := ConvertToResMgrGangs(
		[]*task.TaskInfo{
			{
				InstanceId: 0,
			},
			{
				InstanceId: 1,
			},
			{
				InstanceId: 2,
			},
			{
				InstanceId: 3,
			}},
		jobConfig)

	assert.Len(t, gangs, 3)
}
