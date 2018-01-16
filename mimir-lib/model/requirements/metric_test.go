// @generated AUTO GENERATED - DO NOT EDIT! 9f8b9e47d86b5e1a3668856830c149e768e78415
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
	"fmt"
	"testing"

	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
	"github.com/stretchr/testify/assert"
)

func hostWithDiskResources() *metrics.MetricSet {
	set := metrics.NewMetricSet()
	set.Add(metrics.DiskTotal, 2*metrics.TiB)
	set.Add(metrics.DiskUsed, 542*metrics.GiB)
	set.Add(metrics.DiskFree, 482*metrics.GiB)

	return set
}

func TestMetricRequirement_String_and_Composite(t *testing.T) {
	requirement := NewMetricRequirement(metrics.DiskFree, GreaterThanEqual, 256*metrics.GiB)

	assert.Equal(t, fmt.Sprintf("disk_free should be greater_than_equal %v bytes", 256*metrics.GiB),
		requirement.String())
	composite, name := requirement.Composite()
	assert.False(t, composite)
	assert.Equal(t, "metric", name)
}

func TestMetricRequirement_Fulfilled_FulfilledOnSetWithEnoughOfTheResource(t *testing.T) {
	group := placement.NewGroup("group")
	group.Metrics = hostWithDiskResources()

	requirement := NewMetricRequirement(metrics.DiskFree, GreaterThanEqual, 256*metrics.GiB)

	transcript := placement.NewTranscript("transcript")
	assert.True(t, requirement.Passed(group, nil, nil, transcript))
	assert.Equal(t, 1, transcript.GroupsPassed)
	assert.Equal(t, 0, transcript.GroupsFailed)
}

func TestMetricRequirement_Fulfilled_NotFulfilledOnSetWithTooLittleOfTheResource(t *testing.T) {
	group := placement.NewGroup("group")
	group.Metrics = hostWithDiskResources()

	requirement := NewMetricRequirement(metrics.DiskFree, GreaterThanEqual, 512*metrics.GiB)

	assert.False(t, requirement.Passed(group, nil, nil, nil))
}

func TestMetricRequirement_Fulfilled_IsUnfulfilledForInvalidComparison(t *testing.T) {
	group := placement.NewGroup("group")
	group.Metrics = hostWithDiskResources()

	requirement := NewMetricRequirement(metrics.DiskFree, Comparison("invalid"), 256*metrics.GiB)

	assert.False(t, requirement.Passed(group, nil, nil, nil))
}
