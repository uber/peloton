package atop

import (
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod"
	"code.uber.internal/infra/peloton/.gen/thrift/aurora/api"
	"code.uber.internal/infra/peloton/aurorabridge/common"
	"code.uber.internal/infra/peloton/aurorabridge/fixture"
	"code.uber.internal/infra/peloton/aurorabridge/label"
	"github.com/stretchr/testify/assert"
	"go.uber.org/thriftrw/ptr"
)

// Ensures that PodSpec container resources are set.
func TestNewPodSpec_ContainersResource(t *testing.T) {
	var (
		cpu  float64 = 2
		mem  int64   = 256
		disk int64   = 512
		gpu  int64   = 1
	)

	p, err := NewPodSpec(&api.TaskConfig{
		Resources: []*api.Resource{
			&api.Resource{NumCpus: &cpu},
			&api.Resource{RamMb: &mem},
			&api.Resource{DiskMb: &disk},
			&api.Resource{NumGpus: &gpu},
		},
	})
	assert.NoError(t, err)

	assert.Len(t, p.Containers, 1)
	r := p.Containers[0].GetResource()

	assert.Equal(t, float64(cpu), r.GetCpuLimit())
	assert.Equal(t, float64(mem), r.GetMemLimitMb())
	assert.Equal(t, float64(disk), r.GetDiskLimitMb())
	assert.Equal(t, float64(gpu), r.GetGpuLimit())
}

// Ensures that PodSpec pod-per-host limit constraints are translated from
// Aurora host limits.
func TestNewPodSpec_HostLimitConstraint(t *testing.T) {
	var (
		n int32 = 1
		k       = fixture.AuroraJobKey()
	)

	jobKeyLabel := label.Build(label.NewAuroraJobKey(k))

	p, err := NewPodSpec(&api.TaskConfig{
		Job: k,
		Constraints: []*api.Constraint{{
			Name: ptr.String(common.MesosHostAttr),
			Constraint: &api.TaskConstraint{
				Limit: &api.LimitConstraint{Limit: &n},
			},
		}},
	})
	assert.NoError(t, err)

	assert.Contains(t, p.Labels, jobKeyLabel)

	assert.Equal(t, &pod.Constraint{
		Type: pod.Constraint_CONSTRAINT_TYPE_LABEL,
		LabelConstraint: &pod.LabelConstraint{
			Kind:        pod.LabelConstraint_LABEL_CONSTRAINT_KIND_POD,
			Condition:   pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_LESS_THAN,
			Label:       jobKeyLabel,
			Requirement: uint32(n),
		},
	}, p.GetConstraint())
}

// Ensures that PodSpec host constraints are translated from Aurora value
// constraints.
func TestNewPodSpec_ValueConstraints(t *testing.T) {
	var (
		name = "sku"
		v1   = "abc123"
		v2   = "xyz456"
	)

	p, err := NewPodSpec(&api.TaskConfig{
		Constraints: []*api.Constraint{{
			Name: &name,
			Constraint: &api.TaskConstraint{
				Value: &api.ValueConstraint{
					Values: map[string]struct{}{
						v1: struct{}{},
						v2: struct{}{},
					},
				},
			},
		}},
	})
	assert.NoError(t, err)

	c := p.GetConstraint()
	assert.Equal(t, pod.Constraint_CONSTRAINT_TYPE_OR, c.GetType())
	assert.ElementsMatch(t, []*pod.Constraint{
		{
			Type: pod.Constraint_CONSTRAINT_TYPE_LABEL,
			LabelConstraint: &pod.LabelConstraint{
				Kind:      pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
				Condition: pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_EQUAL,
				Label: &peloton.Label{
					Key:   name,
					Value: v1,
				},
				Requirement: 1,
			},
		}, {
			Type: pod.Constraint_CONSTRAINT_TYPE_LABEL,
			LabelConstraint: &pod.LabelConstraint{
				Kind:      pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
				Condition: pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_EQUAL,
				Label: &peloton.Label{
					Key:   name,
					Value: v2,
				},
				Requirement: 1,
			},
		},
	}, c.GetOrConstraint().GetConstraints())
}
