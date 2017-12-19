package tracked

import (
	"context"
	"testing"

	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestJob(t *testing.T) {
	j := &job{
		id: &peloton.JobID{Value: uuid.NewRandom().String()},
		m: &manager{
			mtx: NewMetrics(tally.NoopScope),
		},
	}

	reschedule, err := j.RunAction(context.Background(), JobNoAction)
	assert.False(t, reschedule)
	assert.NoError(t, err)

	jobRuntime := &pb_job.RuntimeInfo{
		State:     pb_job.JobState_RUNNING,
		GoalState: pb_job.JobState_SUCCEEDED,
	}
	jobConfig := &pb_job.JobConfig{
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		InstanceCount: 1,
	}
	jobInfo := &pb_job.JobInfo{
		Runtime: jobRuntime,
		Config:  jobConfig,
	}
	j.updateRuntime(jobInfo)

	runtime, err := j.GetJobRuntime(context.Background())
	assert.Equal(t, jobRuntime, runtime)
	assert.NoError(t, err)

	config, err := j.getConfig()
	assert.Equal(t, jobConfig.RespoolID, config.respoolID)
	assert.Equal(t, jobConfig.InstanceCount, config.instanceCount)
	assert.NoError(t, err)

	j.ClearJobRuntime()
	assert.Nil(t, j.runtime)
}
