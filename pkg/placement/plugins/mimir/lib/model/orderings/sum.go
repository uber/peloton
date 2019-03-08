// @generated AUTO GENERATED - DO NOT EDIT! 117d51fa2854b0184adc875246a35929bbbf0a91

// Copyright (c) 2018 Uber Technologies, Inc.
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
	"math"

	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

// Sum will take the tuples of the sub expressions and return a tuple which will have the length of the smallest
// tuple returned from the sub expressions where each entry is the summation of the corresponding entry in the
// tuple from the sub expressions.
func Sum(subExpressions ...placement.Ordering) placement.Ordering {
	return &SumCustom{
		SubExpressions: subExpressions,
	}
}

// SumCustom can create a tuple of floats which is the summation of tuples created in the sub-expressions.
// The resulting will have the same length as the shortest tuple returned in the sub-expressions and each entry will
// be the summation of the entries of the other tuples at the same index.
type SumCustom struct {
	SubExpressions []placement.Ordering
}

// Tuple returns a tuple of floats created from the group, scope groups and the entity.
func (custom *SumCustom) Tuple(group *placement.Group, scopeSet *placement.ScopeSet, entity *placement.Entity) []float64 {
	tuples := make([][]float64, 0, len(custom.SubExpressions))
	minLength := math.MaxInt64
	for _, subExpression := range custom.SubExpressions {
		t := subExpression.Tuple(group, scopeSet, entity)
		if len(t) < minLength {
			minLength = len(t)
		}
		tuples = append(tuples, t)
	}
	var result []float64
	if len(custom.SubExpressions) > 0 {
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
