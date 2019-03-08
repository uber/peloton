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
	"fmt"
	"math"
	"sort"
)

// Mapping represents a mapping of an interval from ]-inf;inf[ onto a finite set of float values.
type Mapping struct {
	buckets []*Bucket
}

// Buckets returns a copy of the list of buckets.
func (mapping *Mapping) Buckets() []*Bucket {
	result := make([]*Bucket, 0, len(mapping.buckets))
	for _, bucket := range mapping.buckets {
		result = append(result, bucket)
	}
	return result
}

// Map will map the given value into another value defined by the mapping.
func (mapping *Mapping) Map(value float64) float64 {
	for _, bucket := range mapping.buckets {
		if bucket.Contains(value) {
			return bucket.Value()
		}
	}
	// This will never happens since all mapping types are created using the new mapping factory.
	panic("the value does not map to any buckets")
}

type byStart []*Bucket

func (buckets byStart) Len() int {
	return len(buckets)
}

func (buckets byStart) Less(i, j int) bool {
	if buckets[i].start.value < buckets[j].start.value {
		return true
	}
	if buckets[i].start.value > buckets[j].start.value {
		return false
	}
	// Closed starting endpoint is less than an open starting endpoint
	if !buckets[i].start.Open() && buckets[j].start.Open() {
		return true
	}
	return false
}

func (buckets byStart) Swap(i, j int) {
	buckets[i], buckets[j] = buckets[j], buckets[i]
}

// NewMapping will create a new mapping from a list of buckets. The buckets will be checked to not overlap and to
// ensure that they cover the whole range [-inf;inf], if this is not the case an error is returned.
func NewMapping(buckets ...*Bucket) (*Mapping, error) {
	sortByStart := make(byStart, 0, len(buckets))
	sortByStart = append(sortByStart, buckets...)
	sort.Sort(sortByStart)
	var lastBucket *Bucket
	for _, bucket := range sortByStart {
		err := bucket.Validate()
		if err != nil {
			return nil, err
		}
		if lastBucket != nil {
			if bucket.start.value > lastBucket.end.value {
				return nil, fmt.Errorf(
					"the buckets do not cover exactly [-inf;inf], there is a hole between bucket %v and %v",
					lastBucket, bucket)
			}
			if bucket.start.value < lastBucket.end.value {
				return nil, fmt.Errorf("the buckets %v and %v are overlapping", lastBucket, bucket)
			}
			if bucket.start.Open() && lastBucket.end.Open() {
				return nil, fmt.Errorf(
					"the buckets do not cover exactly [-inf;inf], the point %v is missing between bucket %v and %v",
					bucket.start.value, lastBucket, bucket)
			}
			if !bucket.start.Open() && !lastBucket.end.Open() {
				return nil, fmt.Errorf(
					"the buckets are overlapping, the point %v is in both bucket %v and %v",
					bucket.start.value, lastBucket, bucket)
			}
		} else {
			if !math.IsInf(bucket.start.value, -1) || bucket.start.Open() {
				return nil, fmt.Errorf("the buckets do not cover exactly [-inf;inf], the first bucket is %v",
					bucket)
			}
		}
		lastBucket = bucket
	}
	if lastBucket == nil {
		return nil, fmt.Errorf("the buckets do not cover any part of [-inf;inf], as no buckets where provided")
	}
	if !math.IsInf(lastBucket.end.value, 1) || lastBucket.end.Open() {
		return nil, fmt.Errorf("the buckets do not cover exactly [-inf;inf], the last bucket is %v", lastBucket)
	}
	return &Mapping{
		buckets: sortByStart,
	}, nil
}
