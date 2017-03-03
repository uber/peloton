package scalar

import (
	mesos "mesos/v1"
	"peloton/api/task/config"
	"testing"

	"code.uber.internal/infra/peloton/util"
	"github.com/stretchr/testify/assert"
)

const zeroEpsilon = 0.000001

func TestContains(t *testing.T) {
	// An empty Resources should container another empty one.
	empty1 := Resources{}
	empty2 := Resources{}
	assert.True(t, empty1.Contains(&empty1))
	assert.True(t, empty1.Contains(&empty2))

	r1 := Resources{
		CPU: 1.0,
	}
	assert.True(t, r1.Contains(&r1))
	assert.False(t, empty1.Contains(&r1))
	assert.True(t, r1.Contains(&empty1))

	r2 := Resources{
		Mem: 1.0,
	}
	assert.False(t, r1.Contains(&r2))
	assert.False(t, r2.Contains(&r1))

	r3 := Resources{
		CPU:  1.0,
		Mem:  1.0,
		Disk: 1.0,
		GPU:  1.0,
	}
	assert.False(t, r1.Contains(&r3))
	assert.False(t, r2.Contains(&r3))
	assert.True(t, r3.Contains(&r1))
	assert.True(t, r3.Contains(&r2))
	assert.True(t, r3.Contains(&r3))
}

func TestHasGPU(t *testing.T) {
	empty := Resources{}
	assert.False(t, empty.HasGPU())

	r1 := Resources{
		CPU: 1.0,
	}
	assert.False(t, r1.HasGPU())

	r2 := Resources{
		CPU: 1.0,
		GPU: 1.0,
	}
	assert.True(t, r2.HasGPU())
}

func TestAdd(t *testing.T) {
	empty := Resources{}
	r1 := Resources{
		CPU: 1.0,
	}

	empty.Add(&empty)
	assert.InEpsilon(t, 0.0, empty.CPU, zeroEpsilon)
	assert.InEpsilon(t, 0.0, empty.Mem, zeroEpsilon)
	assert.InEpsilon(t, 0.0, empty.Disk, zeroEpsilon)
	assert.InEpsilon(t, 0.0, empty.GPU, zeroEpsilon)

	result := Resources{}
	result.Add(&r1)
	assert.InEpsilon(t, 1.0, result.CPU, zeroEpsilon)
	assert.InEpsilon(t, 0.0, result.Mem, zeroEpsilon)
	assert.InEpsilon(t, 0.0, result.Disk, zeroEpsilon)
	assert.InEpsilon(t, 0.0, result.GPU, zeroEpsilon)

	r2 := Resources{
		CPU:  4.0,
		Mem:  3.0,
		Disk: 2.0,
		GPU:  1.0,
	}
	result.Add(&r2)
	assert.InEpsilon(t, 5.0, result.CPU, zeroEpsilon)
	assert.InEpsilon(t, 3.0, result.Mem, zeroEpsilon)
	assert.InEpsilon(t, 2.0, result.Disk, zeroEpsilon)
	assert.InEpsilon(t, 1.0, result.GPU, zeroEpsilon)
}

func TestTrySubtract(t *testing.T) {
	empty := Resources{}
	r1 := Resources{
		CPU:  1.0,
		Mem:  2.0,
		Disk: 3.0,
		GPU:  4.0,
	}

	res := empty.TrySubtract(&empty)
	assert.True(t, res)
	assert.InEpsilon(t, 0.0, empty.CPU, zeroEpsilon)
	assert.InEpsilon(t, 0.0, empty.Mem, zeroEpsilon)
	assert.InEpsilon(t, 0.0, empty.Disk, zeroEpsilon)
	assert.InEpsilon(t, 0.0, empty.GPU, zeroEpsilon)

	res = empty.TrySubtract(&r1)
	assert.False(t, res)
	assert.InEpsilon(t, 0.0, empty.CPU, zeroEpsilon)
	assert.InEpsilon(t, 0.0, empty.Mem, zeroEpsilon)
	assert.InEpsilon(t, 0.0, empty.Disk, zeroEpsilon)
	assert.InEpsilon(t, 0.0, empty.GPU, zeroEpsilon)

	r2 := r1
	res = r2.TrySubtract(&r1)
	assert.True(t, res)
	assert.InEpsilon(t, 0.0, r2.CPU, zeroEpsilon)
	assert.InEpsilon(t, 0.0, r2.Mem, zeroEpsilon)
	assert.InEpsilon(t, 0.0, r2.Disk, zeroEpsilon)
	assert.InEpsilon(t, 0.0, r2.GPU, zeroEpsilon)

	res = r1.TrySubtract(&empty)
	assert.True(t, res)
	assert.InEpsilon(t, 1.0, r1.CPU, zeroEpsilon)
	assert.InEpsilon(t, 2.0, r1.Mem, zeroEpsilon)
	assert.InEpsilon(t, 3.0, r1.Disk, zeroEpsilon)
	assert.InEpsilon(t, 4.0, r1.GPU, zeroEpsilon)

	r3 := Resources{
		CPU:  5.0,
		Mem:  6.0,
		Disk: 7.0,
		GPU:  8.0,
	}

	r3.TrySubtract(&r1)
	assert.InEpsilon(t, 4.0, r3.CPU, zeroEpsilon)
	assert.InEpsilon(t, 4.0, r3.Mem, zeroEpsilon)
	assert.InEpsilon(t, 4.0, r3.Disk, zeroEpsilon)
	assert.InEpsilon(t, 4.0, r3.GPU, zeroEpsilon)

	// r3 is more than r1
	res = r1.TrySubtract(&r3)
	assert.False(t, res)
	assert.InEpsilon(t, 1.0, r1.CPU, zeroEpsilon)
	assert.InEpsilon(t, 2.0, r1.Mem, zeroEpsilon)
	assert.InEpsilon(t, 3.0, r1.Disk, zeroEpsilon)
	assert.InEpsilon(t, 4.0, r1.GPU, zeroEpsilon)
}

func TestFromOfferMap(t *testing.T) {
	rs := []*mesos.Resource{
		util.NewMesosResourceBuilder().WithName("cpus").WithValue(1.0).Build(),
		util.NewMesosResourceBuilder().WithName("mem").WithValue(2.0).Build(),
		util.NewMesosResourceBuilder().WithName("disk").WithValue(3.0).Build(),
		util.NewMesosResourceBuilder().WithName("gpus").WithValue(4.0).Build(),
		util.NewMesosResourceBuilder().WithName("custom").WithValue(5.0).Build(),
	}

	offer := mesos.Offer{
		Resources: rs,
	}

	result := FromOfferMap(map[string]*mesos.Offer{"o1": &offer})
	assert.InEpsilon(t, 1.0, result.CPU, zeroEpsilon)
	assert.InEpsilon(t, 2.0, result.Mem, zeroEpsilon)
	assert.InEpsilon(t, 3.0, result.Disk, zeroEpsilon)
	assert.InEpsilon(t, 4.0, result.GPU, zeroEpsilon)

	result = FromOfferMap(map[string]*mesos.Offer{
		"o1": &offer,
		"o2": &offer,
	})
	assert.InEpsilon(t, 2.0, result.CPU, zeroEpsilon)
	assert.InEpsilon(t, 4.0, result.Mem, zeroEpsilon)
	assert.InEpsilon(t, 6.0, result.Disk, zeroEpsilon)
	assert.InEpsilon(t, 8.0, result.GPU, zeroEpsilon)
}

func TestFromResourceConfig(t *testing.T) {
	result := FromResourceConfig(&config.ResourceConfig{
		CpuLimit:    1.0,
		MemLimitMb:  2.0,
		DiskLimitMb: 3.0,
		GpuLimit:    4.0,
	})
	assert.InEpsilon(t, 1.0, result.CPU, zeroEpsilon)
	assert.InEpsilon(t, 2.0, result.Mem, zeroEpsilon)
	assert.InEpsilon(t, 3.0, result.Disk, zeroEpsilon)
	assert.InEpsilon(t, 4.0, result.GPU, zeroEpsilon)
}

func TestMinimum(t *testing.T) {
	r1 := Minimum(
		Resources{
			CPU: 1.0,
			Mem: 2.0,
		},
		Resources{
			CPU:  2.0,
			Mem:  2.0,
			Disk: 2.0,
		},
	)

	assert.InEpsilon(t, 1.0, r1.CPU, zeroEpsilon)
	assert.InEpsilon(t, 2.0, r1.Mem, zeroEpsilon)
	assert.InEpsilon(t, 0.0, r1.Disk, zeroEpsilon)
	assert.InEpsilon(t, 0.0, r1.GPU, zeroEpsilon)
}

func TestNonEmptyFields(t *testing.T) {
	r1 := Resources{}
	assert.True(t, r1.Empty())
	assert.Empty(t, r1.NonEmptyFields())

	r2 := Resources{
		CPU:  1.0,
		Disk: 2.0,
	}
	assert.False(t, r2.Empty())
	assert.Equal(t, []string{"cpus", "disk"}, r2.NonEmptyFields())
}
