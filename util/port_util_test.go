package util

import (
	"testing"

	"github.com/stretchr/testify/assert"

	mesos_v1 "code.uber.internal/infra/peloton/.gen/mesos/v1"
)

func TestGetPortsNumFromOfferMap(t *testing.T) {
	portSet := map[uint32]bool{
		1000: true,
		1002: true,
	}
	rs := []*mesos_v1.Resource{
		NewMesosResourceBuilder().WithName("cpus").WithValue(1.0).Build(),
		NewMesosResourceBuilder().
			WithName("ports").
			WithType(mesos_v1.Value_RANGES).
			WithRanges(CreatePortRanges(portSet)).
			Build(),
	}

	offer1 := &mesos_v1.Offer{
		Resources: rs,
	}
	offer2 := &mesos_v1.Offer{
		Resources: rs,
	}

	result := GetPortsNumFromOfferMap(
		map[string]*mesos_v1.Offer{
			"o1": offer1,
			"o2": offer2,
		},
	)
	assert.Equal(t, result, uint32(4))
}

// This tests bidirectional transformation between set of available port and
// ranges in Mesos resource.
func TestPortRanges(t *testing.T) {
	// Empty range.
	rs := NewMesosResourceBuilder().
		WithName("ports").
		WithType(mesos_v1.Value_RANGES).
		Build()
	portSet := ExtractPortSet(rs)
	assert.Empty(t, portSet)

	// Unmatch resource name.
	rs = NewMesosResourceBuilder().
		WithName("foo").
		WithType(mesos_v1.Value_RANGES).
		Build()
	portSet = ExtractPortSet(rs)
	assert.Empty(t, portSet)
}

// This tests bidirectional transformation between set of available port and
// ranges in Mesos resource.
func TestPortSetTransformation(t *testing.T) {

	portSetTestCases := []struct {
		in       map[uint32]bool
		rangeLen int
	}{
		{getPortSet(), 0},
		{getPortSet(1000, 1000), 1},
		{getPortSet(1000, 1001), 2},
		{getPortSet(1000, 1003), 4},
		{getPortSet(1000, 1001, 1003, 1004), 4},
		{getPortSet(1000, 1000, 2000, 2000, 3000, 3000, 4000, 4000), 4},
		{getPortSet(1000, 2000, 3000, 4000), 2002},
		{getPortSet(1000, 2000, 2001, 3000), 2001},
		{getPortSet(1000, 1001, 2000, 2001, 3000, 3001, 4000, 4000), 7},
	}

	for _, tt := range portSetTestCases {
		ranges := CreatePortRanges(tt.in)
		assert.Equal(t, tt.rangeLen, len(ranges.Range))

		rs := NewMesosResourceBuilder().
			WithName("ports").
			WithType(mesos_v1.Value_RANGES).
			WithRanges(ranges).
			Build()
		portSet := ExtractPortSet(rs)
		assert.Equal(t, tt.in, portSet)
	}
}

// helper function for getting a set of ports from ranges of integers.
func getPortSet(ranges ...uint32) map[uint32]bool {

	result := make(map[uint32]bool)
	begin := uint32(0)
	for index, num := range ranges {
		if index%2 == 0 {
			begin = num
		} else {
			for i := begin; i <= num; i++ {
				result[i] = true
			}
			begin = uint32(0)
		}
	}

	if begin != uint32(0) {
		panic("Odd number of input arguments!")
	}

	return result
}
