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
	"errors"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/pkg/common"

	log "github.com/sirupsen/logrus"
)

// Evaluator is the interface to evaluate whether given LabelValueSet satisifies
// given constraint.
type Evaluator interface {
	// Evaluate returns true if given constraint is satisfied on
	// these labelValues.
	Evaluate(
		constraint *pod.Constraint,
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
type evaluator pod.LabelConstraint_Kind

// NewEvaluator return a new instance of evaluator which filters out constraints
// of different kind.
func NewEvaluator(kind pod.LabelConstraint_Kind) Evaluator {
	return evaluator(kind)
}

// Evaluate takes given constraints and labels, and evaluate whether all parts
// in the given kind matches the input.
func (e evaluator) Evaluate(
	constraint *pod.Constraint,
	labelValues LabelValues,
) (EvaluateResult, error) {
	switch constraint.GetType() {
	case pod.Constraint_CONSTRAINT_TYPE_AND:
		return e.evaluateAndConstraint(
			constraint.GetAndConstraint(), labelValues)
	case pod.Constraint_CONSTRAINT_TYPE_OR:
		return e.evaluateOrConstraint(
			constraint.GetOrConstraint(), labelValues)
	case pod.Constraint_CONSTRAINT_TYPE_LABEL:
		return e.evaluateLabelConstraint(
			constraint.GetLabelConstraint(), labelValues)
	default:
		log.WithField("type", constraint.GetType()).
			Error(ErrUnknownConstraintType.Error())
		return EvaluateResultNotApplicable, ErrUnknownConstraintType
	}
}

func (e evaluator) evaluateAndConstraint(
	andConstraint *pod.AndConstraint,
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
	orConstraint *pod.OrConstraint,
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
	labelConstraint *pod.LabelConstraint,
	labelValues LabelValues,
) (EvaluateResult, error) {
	// If kind of LabelConstraint does not match, returns not applicable
	// which will not short-circuit any And/Or constraint evaluation.
	if labelConstraint.GetKind() != pod.LabelConstraint_Kind(e) {
		return EvaluateResultNotApplicable, nil
	}

	count := valueCount(labelConstraint.GetLabel(), labelValues)
	requirement := labelConstraint.GetRequirement()

	match := false

	switch labelConstraint.Condition {
	case pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_LESS_THAN:
		match = count < requirement
	case pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_EQUAL:
		match = count == requirement
	case pod.LabelConstraint_LABEL_CONSTRAINT_CONDITION_GREATER_THAN:
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

// IsNonExclusiveConstraint returns true if all components of the constraint
// specification do not use a host label constraint for exclusive attribute.
func IsNonExclusiveConstraint(constraint *pod.Constraint) bool {
	if constraint == nil {
		return true
	}

	var toEval []*pod.Constraint
	switch constraint.GetType() {
	case pod.Constraint_CONSTRAINT_TYPE_AND:
		toEval = constraint.GetAndConstraint().GetConstraints()
	case pod.Constraint_CONSTRAINT_TYPE_OR:
		toEval = constraint.GetOrConstraint().GetConstraints()
	case pod.Constraint_CONSTRAINT_TYPE_LABEL:
		lc := constraint.GetLabelConstraint()
		if lc != nil && lc.GetKind() == pod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST &&
			lc.GetLabel().GetKey() == common.PelotonExclusiveNodeLabel {
			return false
		}
		return true
	}
	for _, c := range toEval {
		if !IsNonExclusiveConstraint(c) {
			return false
		}
	}
	return true
}
