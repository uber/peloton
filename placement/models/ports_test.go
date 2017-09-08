package models

import (
	"testing"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"github.com/stretchr/testify/assert"
)

func setupPortRangeVariables() *PortRange {
	begin := uint64(31000)
	end := uint64(31006)
	portRange := NewPortRange(&mesos_v1.Value_Range{
		Begin: &begin,
		End:   &end,
	})
	return portRange
}

func TestPortRange_NumPorts(t *testing.T) {
	portRange := setupPortRangeVariables()
	assert.Equal(t, uint32(7), portRange.NumPorts())
}

func TestPortRange_TakePorts_take_no_ports_from_range(t *testing.T) {
	portRange := setupPortRangeVariables()
	begin, end := portRange.begin, portRange.end
	ports := portRange.TakePorts(0)
	assert.Equal(t, 0, len(ports))
	assert.Equal(t, begin, portRange.begin)
	assert.Equal(t, end, portRange.end)
}

func TestPortRange_TakePorts(t *testing.T) {
	portRange := setupPortRangeVariables()
	assert.Equal(t, uint32(31000), portRange.begin)
	assert.Equal(t, uint32(31007), portRange.end)
	assert.Equal(t, uint32(7), portRange.NumPorts())

	ports1 := portRange.TakePorts(3)
	assert.Equal(t, uint32(4), portRange.NumPorts())
	assert.Equal(t, 3, len(ports1))
	assert.Equal(t, uint32(31000), ports1[0])
	assert.Equal(t, uint32(31001), ports1[1])
	assert.Equal(t, uint32(31002), ports1[2])

	ports2 := portRange.TakePorts(3)
	assert.Equal(t, uint32(1), portRange.NumPorts())
	assert.Equal(t, 3, len(ports2))
	assert.Equal(t, uint32(31003), ports2[0])
	assert.Equal(t, uint32(31004), ports2[1])
	assert.Equal(t, uint32(31005), ports2[2])

	ports3 := portRange.TakePorts(3)
	assert.Equal(t, uint32(0), portRange.NumPorts())
	assert.Equal(t, 1, len(ports3))
	assert.Equal(t, uint32(31006), ports3[0])

	ports4 := portRange.TakePorts(3)
	assert.Equal(t, uint32(0), portRange.NumPorts())
	assert.Equal(t, 0, len(ports4))
}
