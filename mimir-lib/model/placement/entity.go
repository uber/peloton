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
)

// Entity represents an task, process or some entity that should run on a group.
type Entity struct {
	Name                string
	Relocateable        bool
	Ordering            Ordering
	AffinityRequirement requirements.AffinityRequirement
	MetricRequirements  []*requirements.MetricRequirement
	Relations           *labels.LabelBag
	Metrics             *metrics.MetricSet
}

// Fulfilled checks if the requirements are fulfilled by the given group and will update the result into the transcript.
func (entity *Entity) Fulfilled(group *Group, transcript *requirements.Transcript) bool {
	passed := true
	for _, metricRequirement := range entity.MetricRequirements {
		if !metricRequirement.Fulfilled(group.Metrics, transcript.Subscript(metricRequirement)) {
			passed = false
		}
	}
	if entity.AffinityRequirement != nil {
		if !entity.AffinityRequirement.Fulfilled(
			group.Labels, group.Relations, transcript.Subscript(entity.AffinityRequirement)) {
			passed = false
		}
	}
	if passed {
		transcript.IncPassed()
	} else {
		transcript.IncFailed()
	}
	return passed
}

// NewEntity will create a new entity with the given name and creation time.
func NewEntity(name string) *Entity {
	return &Entity{
		Name:                name,
		Relocateable:        true,
		AffinityRequirement: requirements.NewAndRequirement(),
		MetricRequirements:  []*requirements.MetricRequirement{},
		Relations:           labels.NewLabelBag(),
		Metrics:             metrics.NewMetricSet(),
	}
}
