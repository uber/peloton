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
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
)

// Custom is used to create a custom ordering with. It can return a slice of floats to do lexicographic orderings.
type Custom interface {
	// Tuple returns a tuple of floats created from the group and the entity.
	Tuple(group *placement.Group, entity *placement.Entity) []float64
}

// NewCustomOrdering creates a lexicographic ordering from a tuple score implementation.
func NewCustomOrdering(order Custom) placement.Ordering {
	return &customOrdering{
		scorer: order,
	}
}

type customOrdering struct {
	scorer Custom
}

func (order *customOrdering) Less(group1, group2 *placement.Group, entity *placement.Entity) bool {
	tuple1 := order.scorer.Tuple(group1, entity)
	tuple2 := order.scorer.Tuple(group2, entity)
	for i := range tuple1 {
		if tuple1[i] < tuple2[i] {
			return true
		}
	}
	return false
}
