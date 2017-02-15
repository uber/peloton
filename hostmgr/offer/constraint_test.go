package offer

import (
	"testing"

	"code.uber.internal/infra/peloton/util"
	"github.com/stretchr/testify/assert"

	mesos "mesos/v1"

	"peloton/api/task/config"
	"peloton/private/hostmgr/hostsvc"
)

func TestEffectiveLimit(t *testing.T) {
	// this deprecated semantic is equivalent to a single constraint with same limit.
	c := &Constraint{
		hostsvc.Constraint{
			Limit: 0,
		},
	}

	assert.Equal(t, uint32(1), c.effectiveLimit())

	c.Limit = 1
	assert.Equal(t, uint32(1), c.effectiveLimit())

	c.Limit = 10
	assert.Equal(t, uint32(10), c.effectiveLimit())
}

// Check that offers satisfy constraint w/ GPU are properly matched.
func TestConstraintMatchGPU(t *testing.T) {
	gpuConstraint := &Constraint{
		hostsvc.Constraint{
			Limit: 1,
			ResourceConstraint: &hostsvc.ResourceConstraint{
				Minimum: &config.ResourceConfig{
					CpuLimit:    1.0,
					MemLimitMb:  1.0,
					DiskLimitMb: 1.0,
					GpuLimit:    1.0,
				},
			},
		},
	}

	gpuOffer := &mesos.Offer{
		Resources: []*mesos.Resource{
			util.NewMesosResourceBuilder().WithName("cpus").WithValue(1.0).Build(),
			util.NewMesosResourceBuilder().WithName("mem").WithValue(1.0).Build(),
			util.NewMesosResourceBuilder().WithName("disk").WithValue(1.0).Build(),
			util.NewMesosResourceBuilder().WithName("gpus").WithValue(1.0).Build(),
		},
	}

	assert.True(t, gpuConstraint.match(map[string]*mesos.Offer{"o1": gpuOffer}))
}

// Check that offers satisfy constraint w/ GPU but not other resources are not matched.
func TestConstraintNotEnoughMem(t *testing.T) {
	gpuConstraint := &Constraint{
		hostsvc.Constraint{
			Limit: 1,
			ResourceConstraint: &hostsvc.ResourceConstraint{
				Minimum: &config.ResourceConfig{
					CpuLimit:    1.0,
					MemLimitMb:  2.0,
					DiskLimitMb: 1.0,
					GpuLimit:    1.0,
				},
			},
		},
	}

	gpuOffer := &mesos.Offer{
		Resources: []*mesos.Resource{
			util.NewMesosResourceBuilder().WithName("cpus").WithValue(1.0).Build(),
			util.NewMesosResourceBuilder().WithName("mem").WithValue(1.0).Build(),
			util.NewMesosResourceBuilder().WithName("disk").WithValue(1.0).Build(),
			util.NewMesosResourceBuilder().WithName("gpus").WithValue(1.0).Build(),
		},
	}

	assert.False(t, gpuConstraint.match(map[string]*mesos.Offer{"o1": gpuOffer}))
}

// Check that offers w/o enough GPU but not other resources are not matched.
func TestConstraintNotEnoughGPU(t *testing.T) {
	gpuConstraint := &Constraint{
		hostsvc.Constraint{
			Limit: 1,
			ResourceConstraint: &hostsvc.ResourceConstraint{
				Minimum: &config.ResourceConfig{
					CpuLimit:    1.0,
					MemLimitMb:  1.0,
					DiskLimitMb: 1.0,
					GpuLimit:    2.0,
				},
			},
		},
	}

	gpuOffer := &mesos.Offer{
		Resources: []*mesos.Resource{
			util.NewMesosResourceBuilder().WithName("cpus").WithValue(2.0).Build(),
			util.NewMesosResourceBuilder().WithName("mem").WithValue(2.0).Build(),
			util.NewMesosResourceBuilder().WithName("disk").WithValue(2.0).Build(),
			util.NewMesosResourceBuilder().WithName("gpus").WithValue(1.0).Build(),
		},
	}

	assert.False(t, gpuConstraint.match(map[string]*mesos.Offer{"o1": gpuOffer}))
}

// Check that offers satisfy constraint w/o GPU are matched.
func TestConstraintNoGPU(t *testing.T) {
	gpuConstraint := &Constraint{
		hostsvc.Constraint{
			Limit: 1,
			ResourceConstraint: &hostsvc.ResourceConstraint{
				Minimum: &config.ResourceConfig{
					CpuLimit:    1.0,
					MemLimitMb:  1.0,
					DiskLimitMb: 1.0,
				},
			},
		},
	}

	gpuOffer := &mesos.Offer{
		Resources: []*mesos.Resource{
			util.NewMesosResourceBuilder().WithName("cpus").WithValue(2.0).Build(),
			util.NewMesosResourceBuilder().WithName("mem").WithValue(2.0).Build(),
			util.NewMesosResourceBuilder().WithName("disk").WithValue(2.0).Build(),
		},
	}

	assert.True(t, gpuConstraint.match(map[string]*mesos.Offer{"o1": gpuOffer}))
}

// Check that offers satisfy other constraints but w/ GPU are not matched to non-GPU task.
func TestGPUOfferNonGPUTask(t *testing.T) {
	gpuConstraint := &Constraint{
		hostsvc.Constraint{
			Limit: 1,
			ResourceConstraint: &hostsvc.ResourceConstraint{
				Minimum: &config.ResourceConfig{
					CpuLimit:    1.0,
					MemLimitMb:  1.0,
					DiskLimitMb: 1.0,
				},
			},
		},
	}

	gpuOffer := &mesos.Offer{
		Resources: []*mesos.Resource{
			util.NewMesosResourceBuilder().WithName("cpus").WithValue(2.0).Build(),
			util.NewMesosResourceBuilder().WithName("mem").WithValue(2.0).Build(),
			util.NewMesosResourceBuilder().WithName("disk").WithValue(2.0).Build(),
			util.NewMesosResourceBuilder().WithName("gpus").WithValue(2.0).Build(),
		},
	}

	assert.False(t, gpuConstraint.match(map[string]*mesos.Offer{"o1": gpuOffer}))
}
