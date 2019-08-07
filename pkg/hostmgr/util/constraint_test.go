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

package util

import (
	"testing"

	mesospb "github.com/uber/peloton/.gen/mesos/v1"
	pelotonpb "github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/constraints"
	common_constraints_mocks "github.com/uber/peloton/pkg/common/constraints/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

// ConstraintTestSuite is test suite for constraint util package.
type ConstraintTestSuite struct {
	suite.Suite

	ctrl *gomock.Controller
}

// TestConstraintTestSuite runs ConstraintTestSuite.
func TestConstraintTestSuite(t *testing.T) {
	suite.Run(t, new(ConstraintTestSuite))
}

// SetupTest is setup function for this suite.
func (suite *ConstraintTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
}

// TearDownTest is tear down function for this suite.
func (suite *ConstraintTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestMatchSchedulingConstraint tests scheduling constraint matching result.
func (suite *ConstraintTestSuite) TestMatchSchedulingConstraint() {
	exclusiveAttrName := common.PelotonExclusiveAttributeName

	testCases := map[string]struct {
		hostname     string
		additionalLV constraints.LabelValues
		attributes   []*mesospb.Attribute
		constraint   *task.Constraint
		evaluated    bool
		evalRes      constraints.EvaluateResult
		evalErr      error
		expectedRes  hostsvc.HostFilterResult
	}{
		"non-exclusive-constraint-with-exclusive-attribute": {
			hostname: "host1",
			attributes: []*mesospb.Attribute{
				{
					Name: &exclusiveAttrName,
				},
			},
			expectedRes: hostsvc.HostFilterResult_MISMATCH_CONSTRAINTS,
		},
		"empty-constraint": {
			hostname:    "host1",
			expectedRes: hostsvc.HostFilterResult_MATCH,
		},
		"constraint-evaluation-failure": {
			hostname: "host1",
			additionalLV: constraints.LabelValues{
				"label2": map[string]uint32{"value2": 1},
			},
			constraint: &task.Constraint{
				Type: task.Constraint_LABEL_CONSTRAINT,
				LabelConstraint: &task.LabelConstraint{
					Kind:      task.LabelConstraint_HOST,
					Condition: task.LabelConstraint_CONDITION_EQUAL,
					Label: &pelotonpb.Label{
						Key:   "label1",
						Value: "value1",
					},
					Requirement: 1,
				},
			},
			evaluated:   true,
			evalErr:     errors.New("some bad things heppaned"),
			expectedRes: hostsvc.HostFilterResult_MISMATCH_CONSTRAINTS,
		},
		"evaluation-result-as-match": {
			hostname: "host1",
			additionalLV: constraints.LabelValues{
				"label2": map[string]uint32{"value2": 1},
			},
			constraint: &task.Constraint{
				Type: task.Constraint_LABEL_CONSTRAINT,
				LabelConstraint: &task.LabelConstraint{
					Kind:      task.LabelConstraint_HOST,
					Condition: task.LabelConstraint_CONDITION_EQUAL,
					Label: &pelotonpb.Label{
						Key:   "label1",
						Value: "value1",
					},
					Requirement: 1,
				},
			},
			evaluated:   true,
			evalRes:     constraints.EvaluateResultMatch,
			expectedRes: hostsvc.HostFilterResult_MATCH,
		},
		"evaluation-result-as-not-applicable": {
			hostname: "host1",
			additionalLV: constraints.LabelValues{
				"label2": map[string]uint32{"value2": 1},
			},
			constraint: &task.Constraint{
				Type: task.Constraint_LABEL_CONSTRAINT,
				LabelConstraint: &task.LabelConstraint{
					Kind:      task.LabelConstraint_HOST,
					Condition: task.LabelConstraint_CONDITION_EQUAL,
					Label: &pelotonpb.Label{
						Key:   "label1",
						Value: "value1",
					},
					Requirement: 1,
				},
			},
			evaluated:   true,
			evalRes:     constraints.EvaluateResultNotApplicable,
			expectedRes: hostsvc.HostFilterResult_MATCH,
		},
		"evaluation-result-as-default": {
			hostname: "host1",
			additionalLV: constraints.LabelValues{
				"label2": map[string]uint32{"value2": 1},
			},
			constraint: &task.Constraint{
				Type: task.Constraint_LABEL_CONSTRAINT,
				LabelConstraint: &task.LabelConstraint{
					Kind:      task.LabelConstraint_HOST,
					Condition: task.LabelConstraint_CONDITION_EQUAL,
					Label: &pelotonpb.Label{
						Key:   "label1",
						Value: "value1",
					},
					Requirement: 1,
				},
			},
			evaluated:   true,
			evalRes:     -1,
			expectedRes: hostsvc.HostFilterResult_MISMATCH_CONSTRAINTS,
		},
	}

	for tcName, tc := range testCases {
		mockEvaluator := common_constraints_mocks.NewMockEvaluator(suite.ctrl)
		if tc.evaluated {
			mockEvaluator.EXPECT().
				Evaluate(tc.constraint, gomock.Any()).Return(tc.evalRes, tc.evalErr)
		}

		res := MatchSchedulingConstraint(
			tc.hostname,
			tc.additionalLV,
			tc.attributes,
			tc.constraint,
			mockEvaluator,
		)

		suite.EqualValues(tc.expectedRes, res, "test case: %s", tcName)
	}
}
