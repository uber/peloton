package atop

import (
	"fmt"

	"github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
	"github.com/uber/peloton/aurorabridge/common"
	"github.com/uber/peloton/aurorabridge/label"

	"go.uber.org/thriftrw/ptr"
)

// NewPodSpec creates a new PodSpec.
func NewPodSpec(t *api.TaskConfig) (*pod.PodSpec, error) {
	jobKeyLabel := label.Build(label.NewAuroraJobKey(t.GetJob()))

	constraint, err := newConstraint(jobKeyLabel, t.GetConstraints())
	if err != nil {
		return nil, fmt.Errorf("new constraint: %s", err)
	}

	return &pod.PodSpec{
		PodName:        nil, // Unused.
		Labels:         []*peloton.Label{jobKeyLabel},
		InitContainers: nil, // Unused.
		Containers: []*pod.ContainerSpec{{
			Name:           "", // Unused.
			Resource:       newResourceSpec(t.GetResources()),
			Container:      newMesosContainerInfo(t.GetContainer()),
			Command:        nil, // TODO(codyg): Executor data instead?
			LivenessCheck:  nil, // TODO(codyg): Figure this default.
			ReadinessCheck: nil, // Unused.
			Ports:          newPortSpecs(t.GetResources()),
		}},
		Constraint:             constraint,
		RestartPolicy:          nil,   // Unused.
		Volume:                 nil,   // Unused.
		PreemptionPolicy:       nil,   // Unused.
		Controller:             false, // Unused.
		KillGracePeriodSeconds: 0,     // Unused.
		Revocable:              t.GetTier() == common.Revocable,
	}, nil
}

func newResourceSpec(rs []*api.Resource) *pod.ResourceSpec {
	if len(rs) == 0 {
		return nil
	}

	result := &pod.ResourceSpec{}
	for _, r := range rs {
		if r.IsSetNumCpus() {
			result.CpuLimit = r.GetNumCpus()
		}
		if r.IsSetRamMb() {
			result.MemLimitMb = float64(r.GetRamMb())
		}
		if r.IsSetDiskMb() {
			result.DiskLimitMb = float64(r.GetDiskMb())
		}
		if r.IsSetNumGpus() {
			result.GpuLimit = float64(r.GetNumGpus())
		}
		// Note: Aurora API does not include fd_limit.
	}
	return result
}

func newPortSpecs(rs []*api.Resource) []*pod.PortSpec {
	var result []*pod.PortSpec
	for _, r := range rs {
		if r.IsSetNamedPort() {
			result = append(result, &pod.PortSpec{Name: r.GetNamedPort()})
		}
	}
	return result
}

func newConstraint(
	jobKeyLabel *peloton.Label,
	cs []*api.Constraint,
) (*pod.Constraint, error) {

	var result []*pod.Constraint
	for _, c := range cs {
		if c.GetConstraint().IsSetLimit() {
			if c.GetName() != common.MesosHostAttr {
				return nil, fmt.Errorf(
					"constraint %s: only host limit constraints supported", c.GetName())
			}
			r := newHostLimitConstraint(jobKeyLabel, c.GetConstraint().GetLimit().GetLimit())
			result = append(result, r)
		} else if c.GetConstraint().IsSetValue() {
			if c.GetConstraint().GetValue().GetNegated() {
				return nil, fmt.Errorf(
					"constraint %s: negated value constraints not supported", c.GetName())
			}
			r := newHostConstraint(c.GetName(), c.GetConstraint().GetValue().GetValues())
			result = append(result, r)
		}
	}
	return joinConstraints(result, _andOp), nil
}

// newHostLimitConstraint creates a custom pod constraint for restricting no more than
// n instances of k on a single host.
func newHostLimitConstraint(jobKeyLabel *peloton.Label, n int32) *pod.Constraint {
	return &pod.Constraint{
		Type: pod.Constraint_CONSTRAINT_TYPE_LABEL,
		LabelConstraint: &pod.LabelConstraint{
			Kind:        pod.LabelConstraint_LABEL_CONSTRAINT_KIND_POD,
			Condition:   pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_LESS_THAN,
			Label:       jobKeyLabel,
			Requirement: uint32(n),
		},
	}
}

// newHostConstraint maps Aurora value constraints into host constraints.
func newHostConstraint(name string, values map[string]struct{}) *pod.Constraint {
	var result []*pod.Constraint
	for v := range values {
		result = append(result, &pod.Constraint{
			Type: pod.Constraint_CONSTRAINT_TYPE_LABEL,
			LabelConstraint: &pod.LabelConstraint{
				Kind:      pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
				Condition: pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_EQUAL,
				Label: &peloton.Label{
					Key:   name,
					Value: v,
				},
				Requirement: 1,
			},
		})
	}
	return joinConstraints(result, _orOp)
}

// joinOp is an enum for describing how to join a list of constraints.
type joinOp int

const (
	_andOp joinOp = iota + 1
	_orOp
)

// joinConstraints converts cs into a single constraint joined by op.
func joinConstraints(cs []*pod.Constraint, op joinOp) *pod.Constraint {
	if len(cs) == 0 {
		return nil
	}
	if len(cs) == 1 {
		return cs[0]
	}

	switch op {
	case _andOp:
		return &pod.Constraint{
			Type: pod.Constraint_CONSTRAINT_TYPE_AND,
			AndConstraint: &pod.AndConstraint{
				Constraints: cs,
			},
		}
	case _orOp:
		return &pod.Constraint{
			Type: pod.Constraint_CONSTRAINT_TYPE_OR,
			OrConstraint: &pod.OrConstraint{
				Constraints: cs,
			},
		}
	default:
		panic(fmt.Sprintf("unknown join op: %v", op))
	}
}

func newMesosContainerInfo(c *api.Container) *mesos_v1.ContainerInfo {
	if c == nil {
		return nil
	}

	result := &mesos_v1.ContainerInfo{}
	if c.IsSetMesos() {
		result.Type = mesos_v1.ContainerInfo_MESOS.Enum()
		result.Mesos = &mesos_v1.ContainerInfo_MesosInfo{
			Image: newMesosImage(c.GetMesos().GetImage()),
		}
		result.Volumes = newMesosVolumes(c.GetMesos().GetVolumes())
	}
	if c.IsSetDocker() {
		result.Type = mesos_v1.ContainerInfo_DOCKER.Enum()
		result.Docker = &mesos_v1.ContainerInfo_DockerInfo{
			Image:      ptr.String(c.GetDocker().GetImage()),
			Parameters: newMesosDockerParameters(c.GetDocker().GetParameters()),
		}
	}
	return result
}

func newMesosImage(i *api.Image) *mesos_v1.Image {
	if i == nil {
		return nil
	}

	result := &mesos_v1.Image{}
	if i.IsSetDocker() {
		result.Type = mesos_v1.Image_DOCKER.Enum()
		result.Docker = &mesos_v1.Image_Docker{
			Name: ptr.String(
				fmt.Sprintf("%s:%s", i.GetDocker().GetName(), i.GetDocker().GetTag())),
		}
	}
	if i.IsSetAppc() {
		result.Type = mesos_v1.Image_APPC.Enum()
		result.Appc = &mesos_v1.Image_Appc{
			Name: ptr.String(i.GetAppc().GetName()),
			Id:   ptr.String(i.GetAppc().GetImageId()),
		}
	}
	return result
}

func newMesosVolumes(vs []*api.Volume) []*mesos_v1.Volume {
	var result []*mesos_v1.Volume
	for _, v := range vs {
		result = append(result, &mesos_v1.Volume{
			ContainerPath: ptr.String(v.GetContainerPath()),
			HostPath:      ptr.String(v.GetHostPath()),
			Mode:          newMesosVolumeMode(v.GetMode()).Enum(),
		})
	}
	return result
}

func newMesosVolumeMode(mode api.Mode) mesos_v1.Volume_Mode {
	if mode == api.ModeRw {
		return mesos_v1.Volume_RW
	}
	return mesos_v1.Volume_RO
}

func newMesosDockerParameters(ps []*api.DockerParameter) []*mesos_v1.Parameter {
	var result []*mesos_v1.Parameter
	for _, p := range ps {
		result = append(result, &mesos_v1.Parameter{
			Key:   ptr.String(p.GetName()),
			Value: ptr.String(p.GetValue()),
		})
	}
	return result
}
