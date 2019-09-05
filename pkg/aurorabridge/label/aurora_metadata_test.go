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

package label

import (
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/common"

	"github.com/stretchr/testify/assert"
	"go.uber.org/thriftrw/ptr"
)

func TestAuroraMetadata(t *testing.T) {
	input := []*api.Metadata{
		{
			Key:   ptr.String("test-key-1"),
			Value: ptr.String("test-value-1"),
		},
		{
			Key:   ptr.String("test-key-2"),
			Value: ptr.String("test-value-2"),
		},
	}

	l := NewAuroraMetadataLabels(input)
	assert.Equal(t, []*peloton.Label{
		{
			Key:   "org.apache.aurora.metadata.test-key-1",
			Value: "test-value-1",
		},
		{
			Key:   "org.apache.aurora.metadata.test-key-2",
			Value: "test-value-2",
		},
	}, l)

	// append some noise to make sure ParseAuroraMetadata can filter it
	// correctly
	l = append(l, []*peloton.Label{
		{
			Key:   "test-extra-key-1",
			Value: "test-extra-value-1",
		},
	}...)

	output := ParseAuroraMetadata(l)
	assert.Equal(t, input, output)
}

// TestGetUdeployGpuLimit tests GetUdeployGpuLimit function
func TestGetUdeployGpuLimit(t *testing.T) {
	// Expect success
	m1 := []*api.Metadata{
		{
			Key:   ptr.String(common.AuroraGpuResourceKey),
			Value: ptr.String("2"),
		},
	}
	g, err := GetUdeployGpuLimit(m1)
	assert.NoError(t, err)
	assert.NotNil(t, g)
	assert.Equal(t, float64(2), *g)
	assert.True(t, IsGpuConfig(m1, nil))

	// Expect empty label to return nil
	m2 := []*api.Metadata{}
	g, err = GetUdeployGpuLimit(m2)
	assert.NoError(t, err)
	assert.Nil(t, g)
	assert.False(t, IsGpuConfig(m2, nil))

	rs := []*api.Resource{
		{
			NumGpus: ptr.Int64(int64(2)),
		},
	}
	assert.True(t, IsGpuConfig(m2, rs))

	// Expect invalid value to fail
	m3 := []*api.Metadata{
		{
			Key:   ptr.String(common.AuroraGpuResourceKey),
			Value: ptr.String("blah"),
		},
	}
	_, err = GetUdeployGpuLimit(m3)
	assert.Error(t, err)
}
