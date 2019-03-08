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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/generation"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/requirements"
)

func TestOrRequirementBuilder_Generate(t *testing.T) {
	builder := NewOrRequirementBuilder(
		NewMetricRequirementBuilder(
			metrics.DiskFree,
			requirements.GreaterThanEqual,
			generation.NewConstantGaussian(2.0*metrics.GiB, 0.0)))
	requirement, ok := builder.Generate(generation.NewRandom(42), time.Duration(0)).(*requirements.OrRequirement)

	assert.True(t, ok)
	assert.Equal(t, 1, len(requirement.Requirements))
	_, ok = requirement.Requirements[0].(*requirements.MetricRequirement)
	assert.True(t, ok)
}
