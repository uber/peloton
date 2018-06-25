package task

import (
	"github.com/stretchr/testify/assert"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
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
