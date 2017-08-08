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
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func TestEntityBuilder_Generate(t *testing.T) {
	random := rand.New(rand.NewSource(42))
	entityBuilder, variables := CreateSchemalessEntityBuilder()

	variables.
		Bind(Instance.Name(), "somestore").
		Bind(Datacenter.Name(), "dc1")
	entities := CreateSchemalessEntities(random, entityBuilder, variables, 4, 4)

	for _, entity := range entities {
		assert.Equal(t, 2, entity.Relations.Size())
		assert.Equal(t, 2, entity.Metrics.Size())
		assert.Equal(t, 2, len(entity.MetricRequirements))
		assert.NotNil(t, entity.AffinityRequirement)
	}
}
