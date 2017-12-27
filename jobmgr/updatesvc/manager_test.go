package updatesvc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStartStopManager(t *testing.T) {
	m := NewManager(nil, Config{})

	assert.NoError(t, m.Start())
	assert.NoError(t, m.Stop())
}
