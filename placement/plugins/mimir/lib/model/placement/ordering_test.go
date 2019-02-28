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

package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLess_returns_false_when_tuple1_is_nil(t *testing.T) {
	assert.False(t, Less(nil, nil))
}

func TestLess_returns_true_when_tuple1_is_not_nil_and_tuple2_is_nil(t *testing.T) {
	assert.True(t, Less([]float64{}, nil))
}

func TestLess_if_tuple1_is_equal_to_a_prefix_of_tuple2_then_tuple1_is_least(t *testing.T) {
	assert.True(t, Less([]float64{0.0}, []float64{0.0, 1.0}))
}

func TestLess_if_tuple2_is_equal_to_a_prefix_of_tuple1_then_tuple1_is_not_least(t *testing.T) {
	assert.False(t, Less([]float64{0.0, 1.0}, []float64{0.0}))
}

func TestLess_if_tuple1_first_element_is_larger_rest_are_less(t *testing.T) {
	assert.False(t, Less([]float64{0.0, 2.0}, []float64{-1.0, 3.0}))
}

func TestNameOrdering(t *testing.T) {
	group1 := NewGroup("a")
	group2 := NewGroup("b")
	ordering := NameOrdering()

	tuple1 := ordering.Tuple(group1, nil, nil)
	tuple2 := ordering.Tuple(group2, nil, nil)

	assert.True(t, Less(tuple1, tuple2))
	assert.False(t, Less(tuple2, tuple1))
}
