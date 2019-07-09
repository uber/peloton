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

package constraints

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/pkg/common"
)

func makeLabelConstraint(
	kind pod.LabelConstraint_Kind,
	key, value string,
	cond pod.LabelConstraint_Condition,
	req uint32,
) *pod.Constraint {
	return &pod.Constraint{
		Type: pod.Constraint_CONSTRAINT_TYPE_LABEL,
		LabelConstraint: &pod.LabelConstraint{
			Kind:        kind,
			Condition:   cond,
			Label:       &peloton.Label{Key: key, Value: value},
			Requirement: req,
		},
	}
}

func makeAndConstraint(c ...*pod.Constraint) *pod.Constraint {
	return &pod.Constraint{
		Type: pod.Constraint_CONSTRAINT_TYPE_AND,
		AndConstraint: &pod.AndConstraint{
			Constraints: c,
		},
	}
}

func makeOrConstraint(c ...*pod.Constraint) *pod.Constraint {
	return &pod.Constraint{
		Type: pod.Constraint_CONSTRAINT_TYPE_OR,
		OrConstraint: &pod.OrConstraint{
			Constraints: c,
		},
	}
}

func TestIsNonExclusiveConstraint(t *testing.T) {
	hostExcl := makeLabelConstraint(
		pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
		common.PelotonExclusiveNodeLabel, "storage",
		0, 0)
	hostNonExcl := makeLabelConstraint(
		pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
		"non-exclusive-label", "storage",
		0, 0)
	hostExcl2 := makeLabelConstraint(
		pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
		common.PelotonExclusiveNodeLabel, "cpu",
		0, 0)
	hostNonExcl2 := makeLabelConstraint(
		pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
		"non-exclusive-label", "cpu",
		0, 0)
	podExcl := makeLabelConstraint(
		pod.LabelConstraint_LABEL_CONSTRAINT_KIND_POD,
		common.PelotonExclusiveNodeLabel, "storage",
		0, 0)
	podNonExcl := makeLabelConstraint(
		pod.LabelConstraint_LABEL_CONSTRAINT_KIND_POD,
		"non-exclusive-label", "storage",
		0, 0)
	podExcl2 := makeLabelConstraint(
		pod.LabelConstraint_LABEL_CONSTRAINT_KIND_POD,
		common.PelotonExclusiveNodeLabel, "cpu",
		0, 0)
	podNonExcl2 := makeLabelConstraint(
		pod.LabelConstraint_LABEL_CONSTRAINT_KIND_POD,
		"non-exclusive-label", "cpu",
		0, 0)

	for _, tc := range []struct {
		msg        string
		constraint *pod.Constraint
		expected   bool
	}{
		{
			msg:        "host label constraint with exclusive",
			constraint: hostExcl,
			expected:   false,
		},
		{
			msg:        "host label constraint with non-exclusive",
			constraint: hostNonExcl,
			expected:   true,
		},
		{
			msg:        "host label and constraint with exclusive",
			constraint: makeAndConstraint(hostExcl2, hostNonExcl, hostExcl),
			expected:   false,
		},
		{
			msg:        "host label or constraint with none exclusive",
			constraint: makeOrConstraint(hostNonExcl, hostNonExcl2),
			expected:   true,
		},
		{
			msg:        "host label or constraint with exclusive",
			constraint: makeOrConstraint(hostNonExcl, hostExcl, hostNonExcl2),
			expected:   false,
		},
		{
			msg:        "pod label constraint with exclusive",
			constraint: podExcl,
			expected:   true,
		},
		{
			msg:        "pod label constraint with non-exclusive",
			constraint: podNonExcl,
			expected:   true,
		},
		{
			msg:        "pod label and constraint with exclusive",
			constraint: makeAndConstraint(podExcl2, podExcl),
			expected:   true,
		},
		{
			msg:        "pod label or constraint with none exclusive",
			constraint: makeOrConstraint(podNonExcl, podNonExcl2),
			expected:   true,
		},
		{
			msg:        "pod label or constraint with exclusive",
			constraint: makeOrConstraint(podNonExcl, podExcl, podNonExcl2),
			expected:   true,
		},
	} {
		got := IsNonExclusiveConstraint(tc.constraint)
		assert.Equal(t, tc.expected, got, tc.msg)
	}
}

func TestEvaluator(t *testing.T) {
	host1 := "test-host1"
	host2 := "test-host2"
	rack1 := "test-rack1"
	rack2 := "test-rack2"
	rackLabel := "rack"

	empty := LabelValues{}
	hostLabels1 := LabelValues{
		common.HostNameKey: {
			host1: 1,
		},
		rackLabel: {
			rack1: 1,
		},
	}
	hostLabels2 := LabelValues{
		common.HostNameKey: {
			host2: 1,
		},
		rackLabel: {
			rack2: 1,
		},
	}

	ev := NewEvaluator(pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST)

	for _, tc := range []struct {
		msg         string
		constraint  *pod.Constraint
		labelValues LabelValues
		expected    EvaluateResult
		expectedErr error
	}{
		{
			msg: "mismatched kind not applicable",
			constraint: makeLabelConstraint(
				pod.LabelConstraint_LABEL_CONSTRAINT_KIND_POD,
				"foo", "bar",
				0, 0),
			labelValues: empty,
			expected:    EvaluateResultNotApplicable,
		},
		{
			msg: "matched host affinity",
			constraint: makeLabelConstraint(
				pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
				common.HostNameKey, host1,
				pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_EQUAL, 1),
			labelValues: hostLabels1,
			expected:    EvaluateResultMatch,
		},
		{
			msg: "mismatched host affinity",
			constraint: makeLabelConstraint(
				pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
				common.HostNameKey, host1,
				pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_EQUAL, 1),
			labelValues: hostLabels2,
			expected:    EvaluateResultMismatch,
		},
		{
			msg: "matched host anti-affinity",
			constraint: makeLabelConstraint(
				pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
				common.HostNameKey, host1,
				pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_LESS_THAN, 1),
			labelValues: hostLabels1,
			expected:    EvaluateResultMismatch,
		},
		{
			msg: "mismatched host anti-affinity",
			constraint: makeLabelConstraint(
				pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
				common.HostNameKey, host1,
				pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_LESS_THAN, 1),
			labelValues: hostLabels2,
			expected:    EvaluateResultMatch,
		},
		{
			msg: "matched rack affinity",
			constraint: makeLabelConstraint(
				pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
				rackLabel, rack1,
				pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_GREATER_THAN, 0),
			labelValues: hostLabels1,
			expected:    EvaluateResultMatch,
		},
		{
			msg: "mismatched rack affinity",
			constraint: makeLabelConstraint(
				pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
				rackLabel, rack1,
				pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_GREATER_THAN, 0),
			labelValues: hostLabels2,
			expected:    EvaluateResultMismatch,
		},
		{
			msg: "AndConstraint passes when all constraints pass",
			constraint: makeAndConstraint(
				makeLabelConstraint(
					pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
					common.HostNameKey, host1,
					pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_EQUAL, 1),
				makeLabelConstraint(
					pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
					rackLabel, rack1,
					pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_GREATER_THAN, 0)),
			labelValues: hostLabels1,
			expected:    EvaluateResultMatch,
		},
		{
			msg: "AndConstraint fails when some constraints fail",
			constraint: makeAndConstraint(
				makeLabelConstraint(
					pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
					common.HostNameKey, host1,
					pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_EQUAL, 1),
				makeLabelConstraint(
					pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
					rackLabel, rack2,
					pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_GREATER_THAN, 0)),
			labelValues: hostLabels1,
			expected:    EvaluateResultMismatch,
		},
		{
			msg: "AndConstraint not applicable",
			constraint: makeAndConstraint(
				makeLabelConstraint(
					pod.LabelConstraint_LABEL_CONSTRAINT_KIND_POD,
					"foo", "bar",
					0, 0),
				makeLabelConstraint(
					pod.LabelConstraint_LABEL_CONSTRAINT_KIND_POD,
					"foo", "bar",
					0, 0)),
			labelValues: hostLabels1,
			expected:    EvaluateResultNotApplicable,
		},
		{
			msg: "OrConstraint passes when some constraints pass",
			constraint: makeOrConstraint(
				makeLabelConstraint(
					pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
					common.HostNameKey, host1,
					pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_EQUAL, 1),
				makeLabelConstraint(
					pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
					rackLabel, rack2,
					pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_GREATER_THAN, 0)),
			labelValues: hostLabels1,
			expected:    EvaluateResultMatch,
		},
		{
			msg: "OrConstraint fails when all constraints fail",
			constraint: makeOrConstraint(
				makeLabelConstraint(
					pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
					common.HostNameKey, host2,
					pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_EQUAL, 1),
				makeLabelConstraint(
					pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
					rackLabel, rack2,
					pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_GREATER_THAN, 0)),
			labelValues: hostLabels1,
			expected:    EvaluateResultMismatch,
		},
		{
			msg: "OrConstraint not applicable",
			constraint: makeOrConstraint(
				makeLabelConstraint(
					pod.LabelConstraint_LABEL_CONSTRAINT_KIND_POD,
					"foo", "bar",
					0, 0),
				makeLabelConstraint(
					pod.LabelConstraint_LABEL_CONSTRAINT_KIND_POD,
					"foo", "bar",
					0, 0)),
			labelValues: hostLabels1,
			expected:    EvaluateResultNotApplicable,
		},
		{
			msg: "unknownconditionenum",
			constraint: makeLabelConstraint(
				pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST,
				common.HostNameKey, host1,
				pod.LabelConstraint_Condition(-1), 1),
			labelValues: hostLabels1,
			expected:    EvaluateResultNotApplicable,
			expectedErr: ErrUnknownLabelCondition,
		},
		{
			msg: "unknownconstrainttypeenum",
			constraint: &pod.Constraint{
				Type: pod.Constraint_Type(-1),
			},
			labelValues: hostLabels1,
			expected:    EvaluateResultNotApplicable,
			expectedErr: ErrUnknownConstraintType,
		},
	} {
		got, err := ev.Evaluate(tc.constraint, tc.labelValues)
		if tc.expectedErr != nil {
			assert.Error(t, err)
			assert.Equal(t, tc.expectedErr, err)
		} else {
			assert.NoError(t, err)
		}
		assert.Equal(t, tc.expected, got, tc.msg)
	}
}
