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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
	"github.com/uber/peloton/pkg/aurorabridge/common"
	"github.com/uber/peloton/pkg/aurorabridge/fixture"
	"github.com/uber/peloton/pkg/aurorabridge/label"
	"github.com/uber/peloton/pkg/common/config"
	"go.uber.org/thriftrw/ptr"
)

// Ensures that PodSpec pod-per-host limit constraints are translated from
// Aurora host limits.
func TestNewPodSpec_HostLimitConstraint(t *testing.T) {
	var (
		n int32 = 1
		k       = fixture.AuroraJobKey()
	)

	jobKeyLabel := label.NewAuroraJobKey(k)

	p, err := NewPodSpec(
		&api.TaskConfig{
			Job: k,
			Constraints: []*api.Constraint{{
				Name: ptr.String(common.MesosHostAttr),
				Constraint: &api.TaskConstraint{
					Limit: &api.LimitConstraint{Limit: &n},
				},
			}},
		},
		config.ThermosExecutorConfig{},
	)
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

	p, err := NewPodSpec(
		&api.TaskConfig{
			Constraints: []*api.Constraint{{
				Name: &name,
				Constraint: &api.TaskConstraint{
					Value: &api.ValueConstraint{
						Values: map[string]struct{}{
							v1: {},
							v2: {},
						},
					},
				},
			}},
		},
		config.ThermosExecutorConfig{},
	)
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
