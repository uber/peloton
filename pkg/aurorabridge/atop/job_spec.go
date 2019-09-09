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
	"math"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/common/config"

	"github.com/uber/peloton/pkg/aurorabridge/common"
	"github.com/uber/peloton/pkg/aurorabridge/label"
)

// NewJobSpecFromJobUpdateRequest creates a new JobSpec.
func NewJobSpecFromJobUpdateRequest(
	r *api.JobUpdateRequest,
	respoolID *peloton.ResourcePoolID,
	c config.ThermosExecutorConfig,
) (*stateless.JobSpec, error) {

	if !r.IsSetTaskConfig() {
		return nil, fmt.Errorf("task config is not set in job update request")
	}

	p, err := NewPodSpec(r.GetTaskConfig(), c)
	if err != nil {
		return nil, fmt.Errorf("new pod spec: %s", err)
	}

	// build labels for role, environment and job_name, used for task
	// querying by partial job key (e.g. getTasksWithoutConfigs)
	l := []*peloton.Label{
		label.NewAuroraJobKeyRole(r.GetTaskConfig().GetJob().GetRole()),
		label.NewAuroraJobKeyEnvironment(r.GetTaskConfig().GetJob().GetEnvironment()),
		label.NewAuroraJobKeyName(r.GetTaskConfig().GetJob().GetName()),
		common.BridgeJobLabel,
	}

	return &stateless.JobSpec{
		Revision:      nil, // Unused.
		Name:          NewJobName(r.GetTaskConfig().GetJob()),
		Owner:         r.GetTaskConfig().GetOwner().GetUser(),
		OwningTeam:    r.GetTaskConfig().GetOwner().GetUser(),
		LdapGroups:    nil, // Unused.
		Description:   "",  // Unused.
		Labels:        l,
		InstanceCount: uint32(r.GetInstanceCount()),
		Sla:           newSLASpec(r.GetTaskConfig(), getMaxUnavailableInstances(r)),
		DefaultSpec:   p,
		InstanceSpec:  nil, // TODO(codyg): Pinned instance support.
		RespoolId:     respoolID,
	}, nil
}

// getMaxUnavailableInstances calculates MaximumUnavailableInstances based on
// JobUpdateRequest. For jobs with instance_count<10, maxUnavailableInstances
// is set to 1.
func getMaxUnavailableInstances(r *api.JobUpdateRequest) uint32 {
	instanceCount := float64(r.GetInstanceCount())
	return uint32(math.Max(0.1*instanceCount, 1.0))
}

func newSLASpec(t *api.TaskConfig, maxUnavailableInstances uint32) *stateless.SlaSpec {
	preemptible := false
	revocable := false

	switch t.GetTier() {
	case common.Preemptible:
		preemptible = false
		revocable = false
	case common.Revocable:
		preemptible = true
		revocable = true
	case common.Preferred:
		preemptible = false
		revocable = false
	}

	// Map aurora's preemptible task to Peloton non-revocable + non-preemptible
	// task, such that tasks use resources <= reservation (not elastic resources)
	// and are not subject to preemption
	return &stateless.SlaSpec{
		Priority:                    uint32(t.GetPriority()),
		Preemptible:                 preemptible,
		Revocable:                   revocable,
		MaximumUnavailableInstances: maxUnavailableInstances,
	}
}
