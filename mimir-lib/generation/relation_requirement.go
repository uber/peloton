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
	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/requirements"
	"math/rand"
	"time"
)

type relationRequirementBuilder struct {
	appliesTo   labels.LabelTemplate
	relation    labels.LabelTemplate
	comparison  requirements.Comparison
	occurrences int
}

func (builder *relationRequirementBuilder) Generate(random *rand.Rand, time time.Time) requirements.AffinityRequirement {
	return requirements.NewRelationRequirement(
		builder.appliesTo.Instantiate(),
		builder.relation.Instantiate(),
		builder.comparison,
		builder.occurrences,
	)
}

// NewRelationRequirementBuilder will create a new relation requirement builder applying to a groups with a given label
// requiring that the relations occurrences to fulfill the comparison.
func NewRelationRequirementBuilder(appliesTo, relation labels.LabelTemplate, comparison requirements.Comparison,
	occurrences int) AffinityRequirementBuilder {
	return &relationRequirementBuilder{
		appliesTo:   appliesTo,
		relation:    relation,
		comparison:  comparison,
		occurrences: occurrences,
	}
}
