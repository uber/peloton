// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ptoa

import (
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/stretchr/testify/assert"
	"go.uber.org/thriftrw/ptr"
)

// TestNewRange checks success case for NewRange util function.
func TestNewRange(t *testing.T) {
	instances := []*pod.InstanceIDRange{
		{From: 3, To: 5},
		{From: 7, To: 9},
	}

	r := NewRange(instances)
	assert.Equal(t, []*api.Range{
		{First: ptr.Int32(3), Last: ptr.Int32(5)},
		{First: ptr.Int32(7), Last: ptr.Int32(9)},
	}, r)
}

// TestNewRange_Empty checks NewRange util function when empty instanceIDRange
// is passed.
func TestNewRange_Empty(t *testing.T) {
	r := NewRange(nil)
	assert.NotNil(t, r)
	assert.Len(t, r, 0)
}
