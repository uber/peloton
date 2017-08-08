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

package generation

import (
	"code.uber.internal/infra/peloton/mimir-lib/model/requirements"
	"math/rand"
	"time"
)

type andRequirementBuilder struct {
	affinityRequirementBuilders []AffinityRequirementBuilder
}

// NewAndRequirementBuilder will create a new and requirement builder for generating and requirements.
func NewAndRequirementBuilder(subRequirements ...AffinityRequirementBuilder) AffinityRequirementBuilder {
	return &andRequirementBuilder{
		affinityRequirementBuilders: subRequirements,
	}
}

func (builder *andRequirementBuilder) Generate(random *rand.Rand, time time.Time) requirements.AffinityRequirement {
	subRequirements := make([]requirements.AffinityRequirement, 0, len(builder.affinityRequirementBuilders))
	for _, subBuilder := range builder.affinityRequirementBuilders {
		subRequirements = append(subRequirements, subBuilder.Generate(random, time))
	}
	return requirements.NewAndRequirement(subRequirements...)
}
