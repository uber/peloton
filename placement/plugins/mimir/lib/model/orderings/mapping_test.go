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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMapping_Buckets(t *testing.T) {
	mapping, err := NewMapping(
		NewBucket(
			NewEndpoint(math.Inf(-1), false),
			NewEndpoint(0, false),
			0,
		),
		NewBucket(
			NewEndpoint(0, true),
			NewEndpoint(math.Inf(1), false),
			1,
		),
	)

	assert.NoError(t, err)
	expected, actual := mapping.buckets, mapping.Buckets()
	assert.Equal(t, expected, actual)
	actual[1] = NewBucket(
		NewEndpoint(0, true),
		NewEndpoint(math.Inf(1), false),
		42,
	)
	assert.NotEqual(t, expected, actual)
}

func TestMapping_Map(t *testing.T) {
	mapping, err := NewMapping(
		NewBucket(
			NewEndpoint(math.Inf(-1), false),
			NewEndpoint(0, false),
			0,
		),
		NewBucket(
			NewEndpoint(0, true),
			NewEndpoint(math.Inf(1), false),
			1,
		),
	)

	assert.NoError(t, err)
	assert.Equal(t, 0.0, mapping.Map(-10.0))
	assert.Equal(t, 0.0, mapping.Map(-5.0))
	assert.Equal(t, 0.0, mapping.Map(-0.0001))
	assert.Equal(t, 0.0, mapping.Map(-0.0))
	assert.Equal(t, 0.0, mapping.Map(0.0))
	assert.Equal(t, 1.0, mapping.Map(0.0000001))
	assert.Equal(t, 1.0, mapping.Map(0.001))
	assert.Equal(t, 1.0, mapping.Map(1.0))
	assert.Equal(t, 1.0, mapping.Map(10.0))
}

func TestMapping_MapPanicsOnInvalidMapping(t *testing.T) {
	mapping := &Mapping{
		buckets: []*Bucket{
			NewBucket(
				NewEndpoint(math.Inf(-1), false),
				NewEndpoint(0, true),
				0,
			),
			NewBucket(
				NewEndpoint(0, true),
				NewEndpoint(math.Inf(1), false),
				1,
			),
		},
	}

	f := func() {
		mapping.Map(0.0)
	}
	assert.Panics(t, f)
}

func TestNewMapping_NoBuckets(t *testing.T) {
	_, err := NewMapping()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "the buckets do not cover any part of")
}

func TestNewMapping_InvalidBucket(t *testing.T) {
	mapping, err := NewMapping(
		NewBucket(
			NewEndpoint(math.Inf(-1), false),
			NewEndpoint(0, false),
			0,
		),
		NewBucket(
			NewEndpoint(1, true),
			NewEndpoint(0, true),
			0,
		),
		NewBucket(
			NewEndpoint(1, false),
			NewEndpoint(math.Inf(-1), false),
			0,
		),
	)

	assert.Error(t, err)
	assert.Nil(t, mapping)
}

func TestNewMapping_Covered1(t *testing.T) {
	mapping, err := NewMapping(
		NewBucket(
			NewEndpoint(math.Inf(-1), false),
			NewEndpoint(0, false),
			0,
		),
		NewBucket(
			NewEndpoint(0, true),
			NewEndpoint(math.Inf(1), false),
			1,
		),
	)
	assert.NoError(t, err)
	assert.NotNil(t, mapping)
}

func TestNewMapping_Covered2(t *testing.T) {
	mapping, err := NewMapping(
		NewBucket(
			NewEndpoint(math.Inf(-1), false),
			NewEndpoint(0, true),
			0,
		),
		NewBucket(
			NewEndpoint(0, false),
			NewEndpoint(math.Inf(1), false),
			1,
		),
	)
	assert.NoError(t, err)
	assert.NotNil(t, mapping)
}

func TestNewMapping_Covered3(t *testing.T) {
	mapping, err := NewMapping(
		NewBucket(
			NewEndpoint(math.Inf(-1), false),
			NewEndpoint(math.Inf(1), false),
			0,
		),
	)
	assert.NoError(t, err)
	assert.NotNil(t, mapping)
}

func TestNewMapping_Overlapping(t *testing.T) {
	_, err := NewMapping(
		NewBucket(
			NewEndpoint(math.Inf(-1), false),
			NewEndpoint(0, false),
			0,
		),
		NewBucket(
			NewEndpoint(0, false),
			NewEndpoint(math.Inf(1), false),
			1,
		),
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "overlapping")
}

func TestNewMapping_Overlapping2(t *testing.T) {
	_, err := NewMapping(
		NewBucket(
			NewEndpoint(0, true),
			NewEndpoint(1, true),
			0,
		),
		NewBucket(
			NewEndpoint(0, false),
			NewEndpoint(1, false),
			1,
		),
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "do not cover exactly")
}

func TestNewMapping_Overlapping3(t *testing.T) {
	_, err := NewMapping(
		NewBucket(
			NewEndpoint(math.Inf(-1), false),
			NewEndpoint(math.Inf(1), false),
			0,
		),
		NewBucket(
			NewEndpoint(math.Inf(-1), false),
			NewEndpoint(math.Inf(1), false),
			1,
		),
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "overlapping")
}

func TestNewMapping_MultipleOverlapping(t *testing.T) {
	_, err := NewMapping(
		NewBucket(
			NewEndpoint(math.Inf(-1), false),
			NewEndpoint(0, false),
			0,
		),
		NewBucket(
			NewEndpoint(0, true),
			NewEndpoint(3, false),
			0.5,
		),
		NewBucket(
			NewEndpoint(2, true),
			NewEndpoint(math.Inf(1), false),
			1,
		),
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "overlapping")
}

func TestNewMapping_NotCoveredPointHole(t *testing.T) {
	_, err := NewMapping(
		NewBucket(
			NewEndpoint(math.Inf(-1), false),
			NewEndpoint(0, true),
			0,
		),
		NewBucket(
			NewEndpoint(0, true),
			NewEndpoint(math.Inf(1), false),
			1,
		),
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "do not cover exactly")
}

func TestNewMapping_NotCoveredBigHole1(t *testing.T) {
	_, err := NewMapping(
		NewBucket(
			NewEndpoint(math.Inf(-1), false),
			NewEndpoint(0, true),
			0,
		),
		NewBucket(
			NewEndpoint(1, true),
			NewEndpoint(math.Inf(1), false),
			1,
		),
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "do not cover exactly")
}

func TestNewMapping_NotCoveredBigHole2(t *testing.T) {
	_, err := NewMapping(
		NewBucket(
			NewEndpoint(math.Inf(-1), false),
			NewEndpoint(0, true),
			0,
		),
		NewBucket(
			NewEndpoint(0, false),
			NewEndpoint(2, true),
			1,
		),
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "do not cover exactly")
}

func TestNewMapping_NotCoveredBigHole3(t *testing.T) {
	_, err := NewMapping(
		NewBucket(
			NewEndpoint(2, false),
			NewEndpoint(3, false),
			1,
		),
		NewBucket(
			NewEndpoint(0, false),
			NewEndpoint(1, false),
			0,
		),
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "do not cover exactly")
}

func TestNewMapping_NotCoveredBigHole4(t *testing.T) {
	_, err := NewMapping(
		NewBucket(
			NewEndpoint(math.Inf(-1), false),
			NewEndpoint(0, true),
			-1,
		),
		NewBucket(
			NewEndpoint(0, false),
			NewEndpoint(0, false),
			0,
		),
		NewBucket(
			NewEndpoint(0, true),
			NewEndpoint(1, false),
			1,
		),
		NewBucket(
			NewEndpoint(10, false),
			NewEndpoint(20, true), 10,
		),
		NewBucket(
			NewEndpoint(20, false),
			NewEndpoint(math.Inf(1), false),
			1,
		),
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "do not cover exactly")
}
