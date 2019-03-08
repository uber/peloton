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

package metrics

import (
	"fmt"
)

type mark int

const (
	temporary = 1
	permanent = 2
)

func reverse(s []Type) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func topSortVisit(metricType Type, marks map[string]mark, order []Type) ([]Type, error) {
	if marks[metricType.Name] == permanent {
		return order, nil
	}
	if marks[metricType.Name] == temporary {
		return nil, fmt.Errorf("the metric type %v is forming a cycle", metricType)
	}
	marks[metricType.Name] = temporary
	if metricType.Derivation() != nil {
		for _, dependency := range metricType.Derivation().Dependencies() {
			var err error
			order, err = topSortVisit(dependency, marks, order)
			if err != nil {
				return nil, err
			}
		}
	}
	marks[metricType.Name] = permanent
	return append(order, metricType), nil
}

// TopSort takes a list of metric types and returns a order to visit the metric types in where any metric type is before
// its dependent metric types. If the metric types dependencies form a cycle it will be detected and an error will be
// returned.
func TopSort(unmarked ...Type) ([]Type, error) {
	marks := make(map[string]mark, len(unmarked))
	order := make([]Type, 0, len(unmarked))
	var err error
	for len(unmarked) > 0 {
		node := unmarked[0]
		unmarked = unmarked[1:]
		order, err = topSortVisit(node, marks, order)
		if err != nil {
			return nil, err
		}
	}
	reverse(order)
	return order, err
}
