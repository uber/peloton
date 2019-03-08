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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTopSort_DetectCycle(t *testing.T) {
	metricTypes := make([]Type, 3)
	for i := range metricTypes {
		metricTypes[i].Name = fmt.Sprintf("metric_type%v", i)
		metricTypes[i].Unit = "unit"
		metricTypes[i].derivation = &derivation{}
	}
	for i := range metricTypes {
		next := metricTypes[(i+1)%3]
		d := metricTypes[i].derivation.(*derivation)
		d.dependencies = []Type{next}
	}

	_, err := TopSort(metricTypes[2], metricTypes[1], metricTypes[0])
	assert.Error(t, err, "is forming a cycle")
}

func TestTopSort_ChainDAG(t *testing.T) {
	metricTypes := make([]Type, 3)
	for i := range metricTypes {
		metricTypes[i].Name = fmt.Sprintf("metric_type%v", i)
		metricTypes[i].Unit = "unit"
		metricTypes[i].derivation = &derivation{}
	}
	for i := range metricTypes {
		if i >= len(metricTypes)-1 {
			break
		}
		next := metricTypes[(i+1)%3]
		d := metricTypes[i].derivation.(*derivation)
		d.dependencies = []Type{next}
	}

	order, err := TopSort(metricTypes...)
	assert.NoError(t, err)
	assert.NotNil(t, order)
}
