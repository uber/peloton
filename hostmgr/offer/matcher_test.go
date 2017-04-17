package offer

import (
	"testing"

	"github.com/stretchr/testify/suite"

	mesos "mesos/v1"
	"peloton/api/task"
	"peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/util"
)

type ConstraintTestSuite struct {
	suite.Suite
}

func (suite *ConstraintTestSuite) TestEffectiveHostLimit() {
	// this deprecated semantic is equivalent to a single constraints
	// with same limit.
	c := &hostsvc.Constraint{
		HostLimit: 0,
	}

	suite.Equal(uint32(1), effectiveHostLimit(c))

	c.HostLimit = 1
	suite.Equal(uint32(1), effectiveHostLimit(c))

	c.HostLimit = 10
	suite.Equal(uint32(10), effectiveHostLimit(c))
}

// Check that offers satisfy constraints w/ GPU are properly matched.
func (suite *ConstraintTestSuite) TestConstraintMatchGPU() {
	gpuConstraint := &hostsvc.Constraint{
		HostLimit: 1,
		ResourceConstraint: &hostsvc.ResourceConstraint{
			Minimum: &task.ResourceConfig{
				CpuLimit:    1.0,
				MemLimitMb:  1.0,
				DiskLimitMb: 1.0,
				GpuLimit:    1.0,
			},
		},
	}

	gpuOffer := &mesos.Offer{
		Resources: []*mesos.Resource{
			util.NewMesosResourceBuilder().
				WithName("cpus").
				WithValue(1.0).
				Build(),
			util.NewMesosResourceBuilder().
				WithName("mem").
				WithValue(1.0).
				Build(),
			util.NewMesosResourceBuilder().
				WithName("disk").
				WithValue(1.0).
				Build(),
			util.NewMesosResourceBuilder().
				WithName("gpus").
				WithValue(1.0).
				Build(),
		},
	}

	suite.Equal(
		Matched,
		matchConstraint(
			map[string]*mesos.Offer{"o1": gpuOffer},
			gpuConstraint,
			nil))
}

// Check that offers satisfy constraints w/ GPU but not other resources are not matched.
func (suite *ConstraintTestSuite) TestConstraintNotEnoughMem() {
	gpuConstraint := &hostsvc.Constraint{
		HostLimit: 1,
		ResourceConstraint: &hostsvc.ResourceConstraint{
			Minimum: &task.ResourceConfig{
				CpuLimit:    1.0,
				MemLimitMb:  2.0,
				DiskLimitMb: 1.0,
				GpuLimit:    1.0,
			},
		},
	}

	gpuOffer := &mesos.Offer{
		Resources: []*mesos.Resource{
			util.NewMesosResourceBuilder().
				WithName("cpus").
				WithValue(1.0).
				Build(),
			util.NewMesosResourceBuilder().
				WithName("mem").
				WithValue(1.0).
				Build(),
			util.NewMesosResourceBuilder().
				WithName("disk").
				WithValue(1.0).
				Build(),
			util.NewMesosResourceBuilder().
				WithName("gpus").
				WithValue(1.0).
				Build(),
		},
	}

	suite.Equal(
		InsufficientResources,
		matchConstraint(
			map[string]*mesos.Offer{"o1": gpuOffer},
			gpuConstraint,
			nil))
}

// Check that offers w/o enough GPU but not other resources are not matched.
func (suite *ConstraintTestSuite) TestConstraintNotEnoughGPU() {
	gpuConstraint := &hostsvc.Constraint{
		HostLimit: 1,
		ResourceConstraint: &hostsvc.ResourceConstraint{
			Minimum: &task.ResourceConfig{
				CpuLimit:    1.0,
				MemLimitMb:  1.0,
				DiskLimitMb: 1.0,
				GpuLimit:    2.0,
			},
		},
	}

	gpuOffer := &mesos.Offer{
		Resources: []*mesos.Resource{
			util.NewMesosResourceBuilder().
				WithName("cpus").
				WithValue(2.0).
				Build(),
			util.NewMesosResourceBuilder().
				WithName("mem").
				WithValue(2.0).
				Build(),
			util.NewMesosResourceBuilder().
				WithName("disk").
				WithValue(2.0).
				Build(),
			util.NewMesosResourceBuilder().
				WithName("gpus").
				WithValue(1.0).
				Build(),
		},
	}

	suite.Equal(
		InsufficientResources,
		matchConstraint(
			map[string]*mesos.Offer{"o1": gpuOffer},
			gpuConstraint,
			nil))
}

// Check that offers satisfy constraints w/o GPU are matched.
func (suite *ConstraintTestSuite) TestConstraintNoGPU() {
	gpuConstraint := &hostsvc.Constraint{
		HostLimit: 1,
		ResourceConstraint: &hostsvc.ResourceConstraint{
			Minimum: &task.ResourceConfig{
				CpuLimit:    1.0,
				MemLimitMb:  1.0,
				DiskLimitMb: 1.0,
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

	suite.Equal(
		Matched,
		matchConstraint(
			map[string]*mesos.Offer{"o1": gpuOffer},
			gpuConstraint,
			nil))
}

// Check that offers satisfy other constraints but w/ GPU are not matched to non-GPU task.
func (suite *ConstraintTestSuite) TestGPUOfferNonGPUTask() {
	gpuConstraint := &hostsvc.Constraint{
		HostLimit: 1,
		ResourceConstraint: &hostsvc.ResourceConstraint{
			Minimum: &task.ResourceConfig{
				CpuLimit:    1.0,
				MemLimitMb:  1.0,
				DiskLimitMb: 1.0,
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

	suite.Equal(
		MismatchGPU,
		matchConstraint(
			map[string]*mesos.Offer{"o1": gpuOffer},
			gpuConstraint,
			nil))
}

func TestConstraintTestSuite(t *testing.T) {
	suite.Run(t, new(ConstraintTestSuite))
}
