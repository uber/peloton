package goalstate

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigNormalize(t *testing.T) {
	c := Config{}

	c.normalize()

	assert.Equal(t, _defaultMaxRetryDelay, c.MaxRetryDelay)
	assert.Equal(t, _defaultSuccessRetryDelay, c.SuccessRetryDelay)
	assert.Equal(t, _defaultFailureRetryDelay, c.FailureRetryDelay)
}
