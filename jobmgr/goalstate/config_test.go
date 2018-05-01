package goalstate

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigNormalize(t *testing.T) {
	c := Config{}

	c.normalize()

	assert.Equal(t, _defaultMaxRetryDelay, c.MaxRetryDelay)
	assert.Equal(t, _defaultFailureRetryDelay, c.FailureRetryDelay)
	assert.Equal(t, _defaultLaunchTimeRetryDuration, c.LaunchTimeout)
	assert.Equal(t, _defaultJobRuntimeUpdateInterval, c.JobBatchRuntimeUpdateInterval)
	assert.Equal(t, _defaultJobRuntimeUpdateInterval, c.JobServiceRuntimeUpdateInterval)
	assert.Equal(t, _defaultJobWorkerThreads, c.NumWorkerJobThreads)
	assert.Equal(t, _defaultTaskWorkerThreads, c.NumWorkerTaskThreads)
}
