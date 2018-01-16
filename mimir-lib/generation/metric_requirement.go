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

package generation

import (
	"time"

	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
	"code.uber.internal/infra/peloton/mimir-lib/model/requirements"
)

// NewMetricRequirementBuilder will create a new metrics requirement builder requiring the metric to fulfill the
// requirement.
func NewMetricRequirementBuilder(metricType metrics.MetricType, comparison requirements.Comparison,
	value Distribution) RequirementBuilder {
	return &metricRequirementBuilder{
		metricType: metricType,
		comparison: comparison,
		value:      value,
	}
}

type metricRequirementBuilder struct {
	metricType metrics.MetricType
	comparison requirements.Comparison
	value      Distribution
}

func (builder *metricRequirementBuilder) Generate(random Random, time time.Duration) placement.Requirement {
	return requirements.NewMetricRequirement(builder.metricType, builder.comparison, builder.value.Value(random, time))
}
