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

package ptoa

import (
	"fmt"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/common"

	"go.uber.org/thriftrw/ptr"
)

// NewConstraints creates one or many Constraint objects
// (either ValueConstraint or LimitConstraint) based on input
// peloton pod.Constraint.
func NewConstraints(constraint *pod.Constraint) ([]*api.Constraint, error) {
	if constraint == nil {
		return nil, nil
	}

	var constraints []*api.Constraint

	// Acccording to atop.NewConstraint(), if we see an "And" Constraint,
	// assume there are multiple Aurora Constraints, otherwise assume
	// there is one single Aurora Constraint.
	if constraint.GetType() == pod.Constraint_CONSTRAINT_TYPE_AND {
		for _, c := range constraint.GetAndConstraint().GetConstraints() {
			ac, err := newConstraint(c)
			if err != nil {
				return nil, err
			}
			constraints = append(constraints, ac)
		}
	} else {
		ac, err := newConstraint(constraint)
		if err != nil {
			return nil, err
		}
		constraints = append(constraints, ac)
	}

	return constraints, nil
}

// newConstraint creates either ValueConstraint or LimitConstraint based
// on input type.
func newConstraint(constraint *pod.Constraint) (*api.Constraint, error) {
	// If see a single "Label" Constraint, it's either created from
	// ValueConstraint (with single value) or LimitConstraint,
	// determine using Label Constraint Kind.
	//
	// A ValueConstraint would look like:
	//
	// pod.LabelConstraint{
	//   Kind: pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST
	//   Condition: pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_EQUAL
	//   Label: peloton.Label{
	//     Key: <constraint.name>
	//     Value: <constraint.constraint.value.values[0]>
	//   },
	//   Requirement: 1
	// }
	//
	// A LimitConstraint would look like:
	//
	// pod.LabelConstraint{
	//   Kind: LabelConstraint_LABEL_CONSTRAINT_KIND_POD
	//   Condition: LabelConstraint_LABEL_CONSTRAINT_CONDITION_LESS_THAN
	//   Label: <jobKeyLabel>
	//   Requirement: <constraint.constraint.limit.limit>
	// }
	if constraint.GetType() == pod.Constraint_CONSTRAINT_TYPE_LABEL {
		if constraint.GetLabelConstraint().GetKind() ==
			pod.LabelConstraint_LABEL_CONSTRAINT_KIND_POD {
			// Assume KIND_POD is LimitConstraint, verifies
			// the label is a valid job key
			_, err := NewJobKey(constraint.GetLabelConstraint().GetLabel().GetValue())
			if err != nil {
				return nil, fmt.Errorf("not a valid limit constraint")
			}

			return &api.Constraint{
				Name: ptr.String(common.MesosHostAttr),
				Constraint: &api.TaskConstraint{
					Limit: &api.LimitConstraint{
						Limit: ptr.Int32(int32(constraint.GetLabelConstraint().GetRequirement())),
					},
				},
			}, nil
		} else if constraint.GetLabelConstraint().GetKind() ==
			pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST {
			return &api.Constraint{
				Name: ptr.String(constraint.GetLabelConstraint().GetLabel().GetKey()),
				Constraint: &api.TaskConstraint{
					Value: &api.ValueConstraint{
						Values: map[string]struct{}{
							constraint.GetLabelConstraint().GetLabel().GetValue(): {},
						},
					},
				},
			}, nil
		}
	}

	// If see a single "Or" Constraint, assume it's created from
	// ValueConstraint with multiple values. Throw an error if
	// any of the "sub"-Constraints is not ValueConstraint
	// (determining using Label Constraint Kind).
	if constraint.GetType() == pod.Constraint_CONSTRAINT_TYPE_OR {
		values := make(map[string]struct{})
		var name string

		for _, c := range constraint.GetOrConstraint().GetConstraints() {
			if c.GetLabelConstraint().GetKind() !=
				pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST {
				return nil, fmt.Errorf("expect label constraint kind host")
			}

			name = c.GetLabelConstraint().GetLabel().GetKey()
			values[c.GetLabelConstraint().GetLabel().GetValue()] = struct{}{}
		}

		return &api.Constraint{
			Name: ptr.String(name),
			Constraint: &api.TaskConstraint{
				Value: &api.ValueConstraint{
					Values: values,
				},
			},
		}, nil
	}

	return nil, fmt.Errorf("unexpected constraint type %d", constraint.GetType())
}
