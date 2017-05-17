package constraints

import (
	"errors"

	log "github.com/Sirupsen/logrus"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

// Evaluator is the interface to evaluate whether given LabelValueSet satisifies
// given constraint.
type Evaluator interface {
	// Evaluate returns true if given constraint is satisfied on
	// these labelValues.
	Evaluate(
		constraint *task.Constraint,
		labelValues LabelValues,
	) (EvaluateResult, error)
}

// EvaluateResult is an enum indicating various possible result.
type EvaluateResult int

const (
	// EvaluateResultMatch indicates that given constraint fully matched
	// input labelValues.
	EvaluateResultMatch EvaluateResult = iota
	// EvaluateResultMismatch indicates that given constraint mismatched
	// input labelValues.
	EvaluateResultMismatch
	// EvaluateResultNotApplicable indicates that given constraint is not
	// applicable to input values.
	EvaluateResultNotApplicable
)

var (
	// ErrUnknownConstraintType is the error when unknown Constraint.Type
	// enum is processed.
	ErrUnknownConstraintType = errors.New(
		"unknown enum value for Constraint.Type")
	// ErrUnknownLabelCondition is the error when unknown
	// LabelConstraint.Condition enum is processed.
	ErrUnknownLabelCondition = errors.New(
		"unknown enum value for LabelConstraint.Condition")
)

// evaluator implements Evaluator by filtering out any constraint which has a
// different kind.
type evaluator task.LabelConstraint_Kind

// NewEvaluator return a new instance of evaluator which filters out constraints
// of different kind.
func NewEvaluator(kind task.LabelConstraint_Kind) Evaluator {
	return evaluator(kind)
}

// Evaluate takes given constraints and labels, and evaluate whether all parts
// in the given kind matches the input.
func (e evaluator) Evaluate(
	constraint *task.Constraint,
	labelValues LabelValues,
) (EvaluateResult, error) {

	switch constraint.GetType() {
	case task.Constraint_AND_CONSTRAINT:
		return e.evaluateAndConstraint(
			constraint.GetAndConstraint(), labelValues)
	case task.Constraint_OR_CONSTRAINT:
		return e.evaluateOrConstraint(
			constraint.GetOrConstraint(), labelValues)
	case task.Constraint_LABEL_CONSTRAINT:
		return e.evaluateLabelConstraint(
			constraint.GetLabelConstraint(), labelValues)
	}

	log.WithField("type", constraint.GetType()).
		Error(ErrUnknownConstraintType.Error())
	return EvaluateResultNotApplicable, ErrUnknownConstraintType
}

func (e evaluator) evaluateAndConstraint(
	andConstraint *task.AndConstraint,
	labelValues LabelValues,
) (EvaluateResult, error) {

	result := EvaluateResultNotApplicable
	for _, c := range andConstraint.GetConstraints() {
		subResult, err := e.Evaluate(c, labelValues)
		if err != nil {
			return EvaluateResultNotApplicable, err
		}
		if subResult == EvaluateResultMismatch {
			return EvaluateResultMismatch, nil
		} else if subResult == EvaluateResultMatch {
			// If at least one constraint is relevant, we will
			// consider this is a potential match.
			result = EvaluateResultMatch
		}
	}
	return result, nil
}

func (e evaluator) evaluateOrConstraint(
	orConstraint *task.OrConstraint,
	labelValues LabelValues,
) (EvaluateResult, error) {

	result := EvaluateResultNotApplicable
	for _, c := range orConstraint.GetConstraints() {
		subResult, err := e.Evaluate(c, labelValues)
		if err != nil {
			return EvaluateResultNotApplicable, err
		}
		if subResult == EvaluateResultMatch {
			return EvaluateResultMatch, nil
		} else if subResult == EvaluateResultMismatch {
			// If at least one constraint is relevant, we will
			// consider this is a potential mismatch.
			result = EvaluateResultMismatch
		}
	}
	return result, nil
}

func (e evaluator) evaluateLabelConstraint(
	labelConstraint *task.LabelConstraint,
	labelValues LabelValues,
) (EvaluateResult, error) {

	// If kind of LabelConstraint does not match, returns not applicable
	// which will not short-circuit any And/Or constraint evaluation.
	if labelConstraint.GetKind() != task.LabelConstraint_Kind(e) {
		return EvaluateResultNotApplicable, nil
	}

	count := valueCount(labelConstraint.GetLabel(), labelValues)
	requirement := labelConstraint.GetRequirement()

	match := false

	switch labelConstraint.Condition {
	case task.LabelConstraint_CONDITION_LESS_THAN:
		match = count < requirement
	case task.LabelConstraint_CONDITION_EQUAL:
		match = count == requirement
	case task.LabelConstraint_CONDITION_GREATER_THAN:
		match = count > requirement
	default:
		log.WithField("type", labelConstraint.Condition).
			Error(ErrUnknownLabelCondition.Error())
		return EvaluateResultNotApplicable, ErrUnknownLabelCondition
	}
	if match {
		return EvaluateResultMatch, nil
	}

	return EvaluateResultMismatch, nil
}

func valueCount(label *peloton.Label, labelValues LabelValues) uint32 {
	return labelValues[label.GetKey()][label.GetValue()]
}
