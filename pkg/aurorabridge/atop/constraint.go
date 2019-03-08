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

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
	"github.com/uber/peloton/pkg/aurorabridge/common"
)

// NewConstraint creates a new Constraint.
func NewConstraint(
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
