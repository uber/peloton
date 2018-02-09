package archiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigNormalize(t *testing.T) {
	c := Config{}

	c.Normalize()

	assert.Equal(t, _defaultArchiveInterval, c.ArchiveInterval)
	assert.Equal(t, _defaultMaxArchiveEntries, c.MaxArchiveEntries)
	assert.Equal(t, _defaultArchiveAge, c.ArchiveAge)
	assert.Equal(t, _defaultPelotonClientTimeout, c.PelotonClientTimeout)
}
