package mimir

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/peloton/mimir-lib/model/metrics"
)

func TestDerivation_Calculate(t *testing.T) {
	derivation := free(CPUAvailable, CPUReserved)
	metricSet := metrics.NewSet()
	metricSet.Add(CPUAvailable, 200.0)
	metricSet.Add(CPUReserved, 50.0)
	derivation.Calculate(CPUFree, metricSet)
	assert.Equal(t, 150.0, metricSet.Get(CPUFree))
}

func TestDerivation_Dependencies(t *testing.T) {
	derivation := free(CPUAvailable, CPUReserved)
	assert.Equal(t, []metrics.Type{CPUAvailable, CPUReserved}, derivation.Dependencies())
}
