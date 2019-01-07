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

package generation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRandomImpl_Uniform(t *testing.T) {
	random := NewRandom(42)
	now := time.Duration(0)
	value1 := random.Uniform(now)
	value2 := random.Uniform(now)
	assert.Equal(t, value1, value2)
}

func TestRandomImpl_Norm(t *testing.T) {
	random := NewRandom(42)
	now := time.Duration(0)
	value1 := random.Norm(now)
	value2 := random.Norm(now)
	assert.Equal(t, value1, value2)
}

func TestRandomImpl_Exp(t *testing.T) {
	random := NewRandom(42)
	now := time.Duration(0)
	value1 := random.Exp(now)
	value2 := random.Exp(now)
	assert.Equal(t, value1, value2)
}

func TestRandomImpl_Perm(t *testing.T) {
	random := NewRandom(42)
	now := time.Duration(0)
	value1 := random.Perm(now, 42)
	value2 := random.Perm(now, 42)
	assert.Equal(t, value1, value2)
}

func TestGaussian_Value(t *testing.T) {
	random := NewRandom(42)
	now := time.Duration(0)
	distribution := NewConstantGaussian(1.0, 0.0)

	value1 := distribution.Value(random, now)
	value2 := distribution.Value(random, now)
	assert.Equal(t, value1, value2)
	assert.Equal(t, 1.0, value1)
}

func TestUniformDiscrete_Value(t *testing.T) {
	random := NewRandom(42)
	now := time.Duration(0)
	distribution := NewUniformDiscrete(1.0)

	value1 := distribution.Value(random, now)
	value2 := distribution.Value(random, now)
	assert.Equal(t, value1, value2)
	assert.Equal(t, 1.0, value1)
}

func TestDiscrete_Value(t *testing.T) {
	random := NewRandom(42)
	now := time.Duration(0)
	distribution := NewDiscrete(map[float64]float64{
		1.0: 1.0,
	})

	value1 := distribution.Value(random, now)
	value2 := distribution.Value(random, now)
	assert.Equal(t, value1, value2)
	assert.Equal(t, 1.0, value1)
}

func TestConstant_Value(t *testing.T) {
	random := NewRandom(42)
	now := time.Duration(0)
	constant := NewConstant(42.0)

	assert.Equal(t, 42.0, constant.Value(random, now))
}

func TestConstant_NewValue(t *testing.T) {
	random := NewRandom(42)
	now := time.Duration(0)
	constant := NewConstant(42.0)

	assert.Equal(t, 42.0, constant.Value(random, now))
	constant.NewValue(21.0)
	assert.Equal(t, 21.0, constant.Value(random, now))
}

func TestConstant_CurrentValue(t *testing.T) {
	constant := NewConstant(42.0)

	assert.Equal(t, 42.0, constant.CurrentValue())
}
