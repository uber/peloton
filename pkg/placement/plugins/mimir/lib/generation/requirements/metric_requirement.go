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

package requirements

import (
	"time"

	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/generation"
	gPlacement "github.com/uber/peloton/pkg/placement/plugins/mimir/lib/generation/placement"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
	mPlacement "github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/requirements"
)

// NewMetricRequirementBuilder will create a new metrics requirement builder requiring the metric to fulfill the
// requirement.
func NewMetricRequirementBuilder(metricType metrics.Type, comparison requirements.Comparison,
	value generation.Distribution) gPlacement.RequirementBuilder {
	return &metricRequirementBuilder{
		metricType: metricType,
		comparison: comparison,
		value:      value,
	}
}

type metricRequirementBuilder struct {
	metricType metrics.Type
	comparison requirements.Comparison
	value      generation.Distribution
}

func (builder *metricRequirementBuilder) Generate(random generation.Random, time time.Duration) mPlacement.Requirement {
	return requirements.NewMetricRequirement(builder.metricType, builder.comparison, builder.value.Value(random, time))
}
