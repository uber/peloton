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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBucket_String(t *testing.T) {
	bucket := NewBucket(
		NewEndpoint(0, false),
		NewEndpoint(1, true),
		0,
	)

	assert.Equal(t, "[0;1[", bucket.String())
}

func TestBucket_Contains(t *testing.T) {
	bucket := NewBucket(
		NewEndpoint(0, false),
		NewEndpoint(1, true),
		0,
	)

	assert.False(t, bucket.Contains(-0.5))
	assert.True(t, bucket.Contains(0))
	assert.True(t, bucket.Contains(0.5))
	assert.False(t, bucket.Contains(1))
	assert.False(t, bucket.Contains(1.5))
}

func TestBucket_StartEndAndValue(t *testing.T) {
	bucket := NewBucket(
		NewEndpoint(0, false),
		NewEndpoint(1, true),
		42,
	)

	assert.Equal(t, 42.0, bucket.Value())
	assert.Equal(t, 0.0, bucket.Start().Value())
	assert.Equal(t, false, bucket.Start().Open())
	assert.Equal(t, 1.0, bucket.End().Value())
	assert.Equal(t, true, bucket.End().Open())
}

func TestBucket_ValidateStartLargerThanEnd(t *testing.T) {
	bucket := NewBucket(
		NewEndpoint(1, false),
		NewEndpoint(0, true),
		0,
	)

	err := bucket.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "start larger than the end")
}

func TestBucket_ValidateEmptyBucket(t *testing.T) {
	bucket := NewBucket(
		NewEndpoint(0, false),
		NewEndpoint(0, true),
		0,
	)

	err := bucket.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "bucket is empty")
}
