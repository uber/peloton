package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigNormalize(t *testing.T) {
	c := ArchiverConfig{}

	c.Normalize()

	assert.Equal(t, _defaultArchiveInterval, c.ArchiveInterval)
	assert.Equal(t, _defaultMaxArchiveEntries, c.MaxArchiveEntries)
	assert.Equal(t, _defaultArchiveAge, c.ArchiveAge)
	assert.Equal(t, _defaultPelotonClientTimeout, c.PelotonClientTimeout)
	assert.Equal(t, _defaultArchiveStepSize, c.ArchiveStepSize)
	assert.Equal(t, _defaultMaxRetryAttemptsJobQuery, c.MaxRetryAttemptsJobQuery)
	assert.Equal(t, _defaultRetryIntervalJobQuery, c.RetryIntervalJobQuery)
	assert.Equal(t, _defaultBootstrapDelay, c.BootstrapDelay)
}
