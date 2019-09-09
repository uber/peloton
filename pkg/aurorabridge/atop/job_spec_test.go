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

package atop

import (
	"testing"

	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/stretchr/testify/assert"
	"go.uber.org/thriftrw/ptr"
)

func TestGetMaxUnavailableInstances(t *testing.T) {
	testCases := []struct {
		name   string
		req    *api.JobUpdateRequest
		expect uint32
	}{
		{
			name:   "instance count larger than 10 (0)",
			req:    &api.JobUpdateRequest{InstanceCount: ptr.Int32(25)},
			expect: 2,
		},
		{
			name:   "instance count larger than 10 (1)",
			req:    &api.JobUpdateRequest{InstanceCount: ptr.Int32(29)},
			expect: 2,
		},
		{
			name:   "instance count larger than 10 (2)",
			req:    &api.JobUpdateRequest{InstanceCount: ptr.Int32(30)},
			expect: 3,
		},
		{
			name:   "instance count less than 10 (0)",
			req:    &api.JobUpdateRequest{InstanceCount: ptr.Int32(5)},
			expect: 1,
		},
		{
			name:   "instance count less than 10 (1)",
			req:    &api.JobUpdateRequest{InstanceCount: ptr.Int32(1)},
			expect: 1,
		},
		{
			name:   "instance count less than 10 (2)",
			req:    &api.JobUpdateRequest{InstanceCount: ptr.Int32(0)},
			expect: 1,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			maxUnavailableInstances := getMaxUnavailableInstances(tt.req)
			assert.Equal(t, tt.expect, maxUnavailableInstances)
		})
	}
}
