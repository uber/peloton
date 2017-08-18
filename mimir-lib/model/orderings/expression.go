// @generated AUTO GENERATED - DO NOT EDIT!
// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package orderings

import (
	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
	"math"
)

// Metric will create an ordering which will order groups based on their value of the given metric type.
func Metric(source Source, metricType metrics.MetricType) Custom {
	return &byMetric{
		source:     source,
		metricType: metricType,
	}
}

// Relation will create an ordering which will order groups based on the number of their relations matching
// the given pattern.
func Relation(pattern labels.LabelTemplate) Custom {
	return &byRelation{
		pattern: pattern,
	}
}

// Label will create an ordering which will order groups based on the number of their labels matching
// the given pattern.
func Label(pattern labels.LabelTemplate) Custom {
	return &byLabel{
		pattern: pattern,
	}
}

// Constant will return a tuple score which will always return a tuple of length one with the given constant.
func Constant(constant float64) Custom {
	return &byConstant{
		constant: constant,
	}
}

// Negate will negate the given tuple.
func Negate(subExpression Custom) Custom {
	return &byNegate{
		subExpression: subExpression,
	}
}

// Inverse will invert the given tuple.
func Inverse(subExpression Custom) Custom {
	return &byInverse{
		subExpression: subExpression,
	}
}

// Summation will take the tuples of the sub expressions and return a tuple which will have the length of the smallest
// tuple returned from the sub expressions where each entry is the summation of the corresponding entry in the
// tuple from the sub expressions.
func Summation(subExpressions ...Custom) Custom {
	return &bySummation{
		subExpressions: subExpressions,
	}
}

// Multiply will take the tuples of the sub expressions and return a tuple which will have the length of the smallest
// tuple returned from the sub expressions where each entry is the multiplication of the corresponding entry in the
// tuple from the sub expressions.
func Multiply(subExpressions ...Custom) Custom {
	return &byMultiply{
		subExpressions: subExpressions,
	}
}

// Map will change the tuple according to which bucket each entry of the tuple falls.
func Map(mapping *Mapping, subExpression Custom) Custom {
	return &byMap{
		mapping:       mapping,
		subExpression: subExpression,
	}
}

// Concatenate will take a list of orderings and then make a concatenation that will behave like a lexicographic
// ordering.
func Concatenate(subExpressions ...Custom) Custom {
	return &byConcatenate{
		subExpressions: subExpressions,
	}
}

type byMetric struct {
	source     Source
	metricType metrics.MetricType
}

func (by *byMetric) Tuple(group *placement.Group, entity *placement.Entity) []float64 {
	switch by.source {
	case EntitySource:
		return []float64{entity.Metrics.Get(by.metricType)}
	case GroupSource:
		return []float64{group.Metrics.Get(by.metricType)}
	}
	return []float64{0.0}
}

type byRelation struct {
	pattern labels.LabelTemplate
}

func (by *byRelation) Tuple(group *placement.Group, entity *placement.Entity) []float64 {
	return []float64{float64(group.Relations.Count(by.pattern.Instantiate()))}
}

type byLabel struct {
	pattern labels.LabelTemplate
}

func (by *byLabel) Tuple(group *placement.Group, entity *placement.Entity) []float64 {
	return []float64{float64(group.Labels.Count(by.pattern.Instantiate()))}
}

type byConstant struct {
	constant float64
}

func (by *byConstant) Tuple(group *placement.Group, entity *placement.Entity) []float64 {
	return []float64{by.constant}
}

type byNegate struct {
	subExpression Custom
}

func (by *byNegate) Tuple(group *placement.Group, entity *placement.Entity) []float64 {
	tuple := by.subExpression.Tuple(group, entity)
	for i := range tuple {
		tuple[i] = -tuple[i]
	}
	return tuple
}

type byInverse struct {
	subExpression Custom
}

func (by *byInverse) Tuple(group *placement.Group, entity *placement.Entity) []float64 {
	tuple := by.subExpression.Tuple(group, entity)
	for i := range tuple {
		if tuple[i] == 0.0 {
			tuple[i] = math.Inf(1)
			continue
		}
		tuple[i] = 1.0 / tuple[i]
	}
	return tuple
}

type bySummation struct {
	subExpressions []Custom
}

func (by *bySummation) Tuple(group *placement.Group, entity *placement.Entity) []float64 {
	tuples := make([][]float64, 0, len(by.subExpressions))
	minLength := math.MaxInt64
	for _, subExpression := range by.subExpressions {
		t := subExpression.Tuple(group, entity)
		if len(t) < minLength {
			minLength = len(t)
		}
		tuples = append(tuples, t)
	}
	var result []float64
	if len(by.subExpressions) > 0 {
		result = make([]float64, minLength)
	}
	for i := range result {
		result[i] = 0.0
	}
	for _, tuple := range tuples {
		for i := range result {
			result[i] += tuple[i]
		}
	}
	return result
}

type byMultiply struct {
	subExpressions []Custom
}

func (by *byMultiply) Tuple(group *placement.Group, entity *placement.Entity) []float64 {
	tuples := make([][]float64, 0, len(by.subExpressions))
	minLength := math.MaxInt64
	for _, subExpression := range by.subExpressions {
		t := subExpression.Tuple(group, entity)
		if len(t) < minLength {
			minLength = len(t)
		}
		tuples = append(tuples, t)
	}
	var result []float64
	if len(by.subExpressions) > 0 {
		result = make([]float64, minLength)
	}
	for i := range result {
		result[i] = 1.0
	}
	for _, tuple := range tuples {
		for i := range result {
			result[i] *= tuple[i]
		}
	}
	return result
}

type byMap struct {
	mapping       *Mapping
	subExpression Custom
}

func (by *byMap) Tuple(group *placement.Group, entity *placement.Entity) []float64 {
	tuple := by.subExpression.Tuple(group, entity)
	for i := range tuple {
		tuple[i] = by.mapping.Map(tuple[i])
	}
	return tuple
}

type byConcatenate struct {
	subExpressions []Custom
}

func (by *byConcatenate) Tuple(group *placement.Group, entity *placement.Entity) []float64 {
	var tuple []float64
	for _, subExpression := range by.subExpressions {
		tuple = append(tuple, subExpression.Tuple(group, entity)...)
	}
	return tuple
}
