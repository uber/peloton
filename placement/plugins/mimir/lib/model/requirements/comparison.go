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

package requirements

import "fmt"

// Comparison represents a condition on two numbers.
type Comparison string

const (
	// LessThan requires the occurrences of a label to be less than (<) the required occurrences.
	LessThan Comparison = "less_than"

	// LessThanEqual requires the occurrences of a label to be less than or equal to (<=) the required occurrences.
	LessThanEqual Comparison = "less_than_equal"

	// Equal requires the occurrences of a label to be equal to (=) the required occurrences.
	Equal Comparison = "equal"

	// GreaterThanEqual requires the occurrences of a label to be greater than or equal to (>=) the required occurrences.
	GreaterThanEqual Comparison = "greater_than_equal"

	// GreaterThan requires the occurrences of a label to be greater than (>) the required occurrences.
	GreaterThan Comparison = "greater_than"
)

// Compare evaluates how the value a compares to the value b given the type of the comparison.
func (comparison Comparison) Compare(a, b float64) (bool, error) {
	switch comparison {
	case LessThan:
		return a < b, nil
	case LessThanEqual:
		return a <= b, nil
	case Equal:
		return a == b, nil
	case GreaterThanEqual:
		return a >= b, nil
	case GreaterThan:
		return a > b, nil
	}
	return false, fmt.Errorf("unknown %T '%v'", comparison, string(comparison))
}
