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

import "fmt"

// Bucket represents an interval that maps to a given value.
type Bucket struct {
	start *Endpoint
	end   *Endpoint
	value float64
}

// NewBucket creates a new bucket from a start and end point and a value that the bucket should have.
func NewBucket(start, end *Endpoint, value float64) *Bucket {
	return &Bucket{
		start: start,
		end:   end,
		value: value,
	}
}

// Start returns the start endpoint of the bucket.
func (bucket *Bucket) Start() *Endpoint {
	return bucket.start
}

// End returns the end endpoint of the bucket.
func (bucket *Bucket) End() *Endpoint {
	return bucket.end
}

// Contains returns true iff the given value is contained in the bucket.
func (bucket *Bucket) Contains(value float64) bool {
	if (bucket.Start().Open() && value <= bucket.Start().Value()) || value < bucket.Start().Value() {
		return false
	}
	if (bucket.End().Open() && bucket.End().Value() <= value) || bucket.End().Value() < value {
		return false
	}
	return true
}

// Value returns the value of the bucket.
func (bucket *Bucket) Value() float64 {
	return bucket.value
}

// Validate will check if the bucket is valid, i.e. the start is before the end and the bucket is non-empty.
func (bucket *Bucket) Validate() error {
	if bucket.start.value > bucket.end.value {
		return fmt.Errorf("the bucket has a start larger than the end value")
	}
	if bucket.start.value >= bucket.end.value && (bucket.start.open || bucket.end.open) {
		return fmt.Errorf("the bucket is empty")
	}
	return nil
}

// String will return a string representation of the interval of the bucket.
func (bucket *Bucket) String() string {
	result := ""
	if bucket.start.open {
		result += "]"
	} else {
		result += "["
	}
	result += fmt.Sprintf("%v;%v", bucket.start.value, bucket.end.value)
	if bucket.end.open {
		result += "["
	} else {
		result += "]"
	}
	return result
}
