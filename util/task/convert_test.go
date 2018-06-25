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
		cfg     *task.TaskConfig
		jobType job.JobType
		tt      resmgr.TaskType
	}{
		{
			cfg: &task.TaskConfig{
				Volume: &task.PersistentVolumeConfig{},
			},
			jobType: job.JobType_SERVICE,
			tt:      resmgr.TaskType_STATEFUL,
		},
		{
			cfg:     &task.TaskConfig{},
			jobType: job.JobType_BATCH,
			tt:      resmgr.TaskType_BATCH,
		},
		{
			cfg:     &task.TaskConfig{},
			jobType: job.JobType_SERVICE,
			tt:      resmgr.TaskType_STATELESS,
		},
	}

	for _, test := range tt {
		assert.Equal(t, test.tt, getTaskType(test.cfg, test.jobType))
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
		},
	}

	jobConfig := &job.JobConfig{
		SLA: &job.SlaConfig{},
	}
	rmTask := ConvertTaskToResMgrTask(taskInfo, jobConfig)
	assert.Equal(t, rmTask.JobId.Value, taskInfo.JobId.Value)
	assert.Equal(t, rmTask.NumPorts, uint32(len(taskInfo.Config.Ports)))
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
