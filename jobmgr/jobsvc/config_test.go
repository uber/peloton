package jobsvc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigNormalize(t *testing.T) {
	c := Config{}
	c.normalize()
	assert.Equal(t, _defaultMaxTasksPerJob, c.MaxTasksPerJob)
}
