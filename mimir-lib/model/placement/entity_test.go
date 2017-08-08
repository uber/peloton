// @generated AUTO GENERATED - DO NOT EDIT!
// Copyright (c) 2017 Uber Technologies, Inc.
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
	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"code.uber.internal/infra/peloton/mimir-lib/model/requirements"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEntity_Fulfilled_true_for_empty_requirements(t *testing.T) {
	entity := NewEntity("entity")
	group := NewGroup("group")

	assert.True(t, entity.Fulfilled(group, nil))
}

func TestEntity_Fulfilled_false_for_unfulfilled_metric_requirement(t *testing.T) {
	entity := NewEntity("entity")
	entity.MetricRequirements = append(entity.MetricRequirements, requirements.NewMetricRequirement(
		metrics.DiskFree, requirements.GreaterThanEqual, 256*metrics.GiB))
	group := NewGroup("group")

	assert.False(t, entity.Fulfilled(group, nil))
}

func TestEntity_Fulfilled_false_for_unfulfilled_affinity_requirement(t *testing.T) {
	entity := NewEntity("entity")
	entity.AffinityRequirement = requirements.NewOrRequirement(
		requirements.NewLabelRequirement(
			labels.NewLabel("host", "*"),
			labels.NewLabel("volume-types", "zfs"),
			requirements.Equal,
			1,
		),
	)
	group := NewGroup("group")
	group.Labels.Add(labels.NewLabel("host", "some-host"))

	assert.False(t, entity.Fulfilled(group, nil))
}
