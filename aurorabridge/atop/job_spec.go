// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package atop

import (
	"fmt"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
	"github.com/uber/peloton/aurorabridge/common"
	"github.com/uber/peloton/aurorabridge/label"
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
		Labels:        []*peloton.Label{aml},
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
