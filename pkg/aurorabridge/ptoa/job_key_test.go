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

	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/atop"

	"github.com/stretchr/testify/assert"
	"go.uber.org/thriftrw/ptr"
)

func TestNewJobKey_Success(t *testing.T) {
	k := &api.JobKey{
		Role:        ptr.String("test-role"),
		Environment: ptr.String("test-env"),
		Name:        ptr.String("test-name"),
	}
	n := atop.NewJobName(k)

	r, err := NewJobKey(n)
	assert.NoError(t, err)
	assert.Equal(t, k, r)
}

func TestNewJobKey_Error(t *testing.T) {
	ns := []string{
		"invalid/name",
		"invalid//name",
	}

	for _, n := range ns {
		_, err := NewJobKey(n)
		assert.Error(t, err)
	}
}
