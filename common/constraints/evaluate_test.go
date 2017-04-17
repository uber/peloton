package constraints

import (
	"testing"

	"github.com/stretchr/testify/suite"

	mesos "mesos/v1"
	"peloton/api/task"
)

type testCase struct {
	constraint  *task.Constraint
	labelValues LabelValues
	expected    EvaluateResult
	expectedErr error
	msg         string
}

const (
	_testHost1 = "test-host1"
	_testHost2 = "test-host2"
	_testRack  = "test-rack"
	_rackLabel = "rack"
)

type EvaluatorTestSuite struct {
	suite.Suite
}

func (suite *EvaluatorTestSuite) TestKindEvaluator() {
	// Define reusable test cases.
	empty := LabelValues(make(map[string]map[string]uint32))
	hostLabels1 := LabelValues(map[string]map[string]uint32{
		HostNameKey: {
			_testHost1: 1,
		},
		_rackLabel: {
			_testRack: 1,
		},
	})

	hostLabels2 := LabelValues(map[string]map[string]uint32{
		HostNameKey: {
			_testHost2: 1,
		},
		_rackLabel: {
			_testRack: 1,
		},
	})

	kindMismatch := testCase{
		expected:    EvaluateResultNotApplicable,
		msg:         "Mismatched kind should evaluate to true",
		labelValues: empty,
		constraint: &task.Constraint{
			Type: task.Constraint_LABEL_CONSTRAINT,
			LabelConstraint: &task.LabelConstraint{
				Kind: task.LabelConstraint_TASK,
			},
		},
	}

	tmp1, tmp2 := HostNameKey, _testHost1
	hostLabel := mesos.Label{
		Key:   &tmp1,
		Value: &tmp2,
	}

	tmp3, tmp4 := _rackLabel, _testRack
	rackLabel := mesos.Label{
		Key:   &tmp3,
		Value: &tmp4,
	}

	// A host affinity constraint which only passes on host1.
	hostAffinityConstraint := &task.LabelConstraint{
		Kind:        task.LabelConstraint_HOST,
		Label:       &hostLabel,
		Requirement: 1,
		Condition:   task.LabelConstraint_CONDITION_EQUAL,
	}

	hostAffinityMatch := testCase{
		expected:    EvaluateResultMatch,
		msg:         "Matched host affinity case expects true",
		labelValues: hostLabels1,
		constraint: &task.Constraint{
			Type:            task.Constraint_LABEL_CONSTRAINT,
			LabelConstraint: hostAffinityConstraint,
		},
	}

	hostAffinityMismatch := testCase{
		expected:    EvaluateResultMismatch,
		msg:         "Mismatched host affinity case expects false",
		labelValues: hostLabels2,
		constraint: &task.Constraint{
			Type:            task.Constraint_LABEL_CONSTRAINT,
			LabelConstraint: hostAffinityConstraint,
		},
	}

	// A rack affinity constraint which only maps to host1.
	rackAffinityConstraint := &task.LabelConstraint{
		Kind:        task.LabelConstraint_HOST,
		Label:       &rackLabel,
		Requirement: 0,
		Condition:   task.LabelConstraint_CONDITION_GREATER_THAN,
	}

	rackAffinityMatch1 := testCase{
		expected:    EvaluateResultMatch,
		msg:         "Matched host affinity case expects true",
		labelValues: hostLabels1,
		constraint: &task.Constraint{
			Type:            task.Constraint_LABEL_CONSTRAINT,
			LabelConstraint: rackAffinityConstraint,
		},
	}

	rackAffinityMatch2 := rackAffinityMatch1
	rackAffinityMatch2.labelValues = hostLabels2

	// A host anti-affinity constraint which only passes on host1.
	hostAntiAffinityConstraint := &task.LabelConstraint{
		Kind:        task.LabelConstraint_HOST,
		Label:       &hostLabel,
		Requirement: 1,
		Condition:   task.LabelConstraint_CONDITION_LESS_THAN,
	}

	hostAntiAffinityMismatch := testCase{
		expected:    EvaluateResultMismatch,
		msg:         "Mismatched host anti-affinity case",
		labelValues: hostLabels1,
		constraint: &task.Constraint{
			Type:            task.Constraint_LABEL_CONSTRAINT,
			LabelConstraint: hostAntiAffinityConstraint,
		},
	}

	hostAntiAffinityMatch := testCase{
		expected:    EvaluateResultMatch,
		msg:         "Matched host anti-affinity case",
		labelValues: hostLabels2,
		constraint: &task.Constraint{
			Type:            task.Constraint_LABEL_CONSTRAINT,
			LabelConstraint: hostAntiAffinityConstraint,
		},
	}

	andMatch := testCase{
		expected:    EvaluateResultMatch,
		msg:         "AndConstraint passes when all constraint passes",
		labelValues: hostLabels1,
		constraint: &task.Constraint{
			Type: task.Constraint_AND_CONSTRAINT,
			AndConstraint: &task.AndConstraint{
				Constraints: []*task.Constraint{
					hostAffinityMatch.constraint,
					rackAffinityMatch1.constraint,
				},
			},
		},
	}

	andMismatch := testCase{
		expected:    EvaluateResultMismatch,
		msg:         "AndConstraint fails when some constraint fails",
		labelValues: hostLabels1,
		constraint: &task.Constraint{
			Type: task.Constraint_AND_CONSTRAINT,
			AndConstraint: &task.AndConstraint{
				Constraints: []*task.Constraint{
					rackAffinityMatch1.constraint,
					hostAntiAffinityMismatch.constraint,
				},
			},
		},
	}

	andNotApplicable := testCase{
		expected:    EvaluateResultNotApplicable,
		msg:         "AndConstraint not applicable",
		labelValues: hostLabels1,
		constraint: &task.Constraint{
			Type: task.Constraint_AND_CONSTRAINT,
			AndConstraint: &task.AndConstraint{
				Constraints: []*task.Constraint{
					kindMismatch.constraint,
					kindMismatch.constraint,
				},
			},
		},
	}

	// orMatch is almost the same of andMismatch but using `or`, so it
	// actually passes
	orMatch := testCase{
		expected:    EvaluateResultMatch,
		msg:         "Or passes in mixture of pass and fail",
		labelValues: hostLabels1,
		constraint: &task.Constraint{
			Type: task.Constraint_OR_CONSTRAINT,
			OrConstraint: &task.OrConstraint{
				Constraints: []*task.Constraint{
					rackAffinityMatch1.constraint,
					hostAntiAffinityMismatch.constraint,
				},
			},
		},
	}

	// orMismatch is almost the same of andMismatch but using `or`, so it
	// actually passes
	orMismatch := testCase{
		expected:    EvaluateResultMismatch,
		msg:         "Or fails when all constraint fails",
		labelValues: hostLabels1,
		constraint: &task.Constraint{
			Type: task.Constraint_OR_CONSTRAINT,
			OrConstraint: &task.OrConstraint{
				Constraints: []*task.Constraint{
					andMismatch.constraint,
					hostAntiAffinityMismatch.constraint,
				},
			},
		},
	}

	orNotApplicable := testCase{
		expected:    EvaluateResultNotApplicable,
		msg:         "OrConstraint not applicable",
		labelValues: hostLabels1,
		constraint: &task.Constraint{
			Type: task.Constraint_OR_CONSTRAINT,
			AndConstraint: &task.AndConstraint{
				Constraints: []*task.Constraint{
					kindMismatch.constraint,
					kindMismatch.constraint,
				},
			},
		},
	}
	unknownConditionEnum := testCase{
		expected:    EvaluateResultNotApplicable,
		expectedErr: ErrUnknownLabelCondition,
		msg:         "UnknownConditionEnum",

		constraint: &task.Constraint{
			Type: task.Constraint_LABEL_CONSTRAINT,
			LabelConstraint: &task.LabelConstraint{
				Kind:        task.LabelConstraint_HOST,
				Label:       &hostLabel,
				Requirement: 1,
				Condition:   task.LabelConstraint_Condition(-1),
			},
		},
	}
	unknownConstraintTypeEnum := testCase{
		expected:    EvaluateResultNotApplicable,
		expectedErr: ErrUnknownConstraintType,
		msg:         "UnknownConstraintTypeEnum",

		constraint: &task.Constraint{
			Type: task.Constraint_Type(-1),
		},
	}

	table := []testCase{
		kindMismatch,
		hostAffinityMatch,
		hostAffinityMismatch,
		rackAffinityMatch1,
		rackAffinityMatch2,
		hostAntiAffinityMatch,
		hostAntiAffinityMismatch,
		andMatch,
		andMismatch,
		andNotApplicable,
		orMatch,
		orMismatch,
		orNotApplicable,
		unknownConditionEnum,
		unknownConstraintTypeEnum,
	}
	e := NewEvaluator(task.LabelConstraint_HOST)

	for _, tt := range table {
		actual, err := e.Evaluate(tt.constraint, tt.labelValues)
		if tt.expectedErr != nil {
			suite.Error(err)
			suite.Equal(tt.expectedErr, err)
		} else {
			suite.NoError(err)
		}
		suite.Equal(tt.expected, actual, tt.msg)
	}
}

func TestEvaluatorTestSuite(t *testing.T) {
	suite.Run(t, new(EvaluatorTestSuite))
}
