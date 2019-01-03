package atop

import (
	"fmt"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	"code.uber.internal/infra/peloton/.gen/thrift/aurora/api"
	"code.uber.internal/infra/peloton/aurorabridge/common"
	"code.uber.internal/infra/peloton/aurorabridge/label"
)

// NewJobSpecFromJobUpdateRequest creates a new JobSpec.
func NewJobSpecFromJobUpdateRequest(
	r *api.JobUpdateRequest,
	respoolID *peloton.ResourcePoolID,
) (*stateless.JobSpec, error) {

	p, err := NewPodSpec(r.GetTaskConfig())
	if err != nil {
		return nil, fmt.Errorf("new pod spec: %s", err)
	}

	aml, err := label.NewAuroraMetadata(r.GetTaskConfig().GetMetadata())
	if err != nil {
		return nil, fmt.Errorf("new aurora metadata label: %s", err)
	}

	return &stateless.JobSpec{
		Revision:      nil, // Unused.
		Name:          NewJobName(r.GetTaskConfig().GetJob()),
		Owner:         r.GetTaskConfig().GetOwner().GetUser(),
		OwningTeam:    "",  // Unused.
		LdapGroups:    nil, // Unused.
		Description:   "",  // Unused.
		Labels:        []*peloton.Label{label.Build(aml)},
		InstanceCount: uint32(r.GetInstanceCount()),
		Sla:           newSLASpec(r.GetTaskConfig(), r.GetSettings().GetMaxFailedInstances()),
		DefaultSpec:   p,
		InstanceSpec:  nil, // TODO(codyg): Pinned instance support.
		RespoolId:     respoolID,
	}, nil
}

func newSLASpec(t *api.TaskConfig, maxFailedInstances int32) *stateless.SlaSpec {
	return &stateless.SlaSpec{
		Priority:                    uint32(t.GetPriority()),
		Preemptible:                 t.GetTier() == common.Preemptible,
		Revocable:                   t.GetTier() == common.Revocable,
		MaximumUnavailableInstances: uint32(maxFailedInstances),
	}
}
