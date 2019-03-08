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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestComparison_Compare_0_LessThan_1(t *testing.T) {
	result, err := LessThan.Compare(0, 1)
	assert.Nil(t, err)
	assert.True(t, result)
}

func TestComparison_Compare_1_Not_LessThan_0(t *testing.T) {
	result, err := LessThan.Compare(1, 0)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestComparison_Compare_0_LessThanEqual_1(t *testing.T) {
	result, err := LessThanEqual.Compare(0, 1)
	assert.Nil(t, err)
	assert.True(t, result)
}

func TestComparison_Compare_1_Not_LessEqualEqual_0(t *testing.T) {
	result, err := LessThanEqual.Compare(1, 0)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestComparison_Compare_1_Equal_1(t *testing.T) {
	result, err := Equal.Compare(1, 1)
	assert.Nil(t, err)
	assert.True(t, result)
}

func TestComparison_Compare_0_Not_Equal_1(t *testing.T) {
	result, err := Equal.Compare(0, 1)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestComparison_Compare_1_GreaterThanEqual_0(t *testing.T) {
	result, err := GreaterThanEqual.Compare(1, 0)
	assert.Nil(t, err)
	assert.True(t, result)
}

func TestComparison_Compare_0_Not_GreaterThanEqual_1(t *testing.T) {
	result, err := GreaterThanEqual.Compare(0, 1)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestComparison_Compare_1_GreaterThan_0(t *testing.T) {
	result, err := GreaterThan.Compare(1, 0)
	assert.Nil(t, err)
	assert.True(t, result)
}

func TestComparison_Compare_0_Not_GreaterThan_1(t *testing.T) {
	result, err := GreaterThan.Compare(0, 1)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestComparison_Compare_Invalid_Comparison_Value(t *testing.T) {
	_, err := Comparison("invalid").Compare(1, 0)
	assert.NotNil(t, err)
}
