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

// Ensures that PodSpec container resources are set.
func TestNewPodSpec_ContainersResource(t *testing.T) {
	var (
		cpu  float64 = 2
		mem  int64   = 256
		disk int64   = 512
		gpu  int64   = 1
	)

	md := []*api.Metadata{
		{
			Key:   ptr.String("test-key-1"),
			Value: ptr.String("test-value-1"),
		},
	}

	p, err := NewPodSpec(
		&api.TaskConfig{
			Resources: []*api.Resource{
				{NumCpus: &cpu},
				{RamMb: &mem},
				{DiskMb: &disk},
				{NumGpus: &gpu},
			},
			Metadata: md,
		},
		ThermosExecutorConfig{},
	)
	assert.NoError(t, err)

	assert.Len(t, p.Containers, 1)
	r := p.Containers[0].GetResource()

	assert.Equal(t, float64(cpu), r.GetCpuLimit())
	assert.Equal(t, float64(mem), r.GetMemLimitMb())
	assert.Equal(t, float64(disk), r.GetDiskLimitMb())
	assert.Equal(t, float64(gpu), r.GetGpuLimit())

	assert.NotNil(t, p.Containers[0].GetCommand())
	assert.NotNil(t, p.Containers[0].GetExecutor())

	assert.Len(t, p.GetLabels(), 3)
}

// TestEncodeTaskConfig_Consistency make sure encodeTaskConfig generated
// byte arrays are consistent across TaskConfigs whose some of the fields
// are different by order.
func TestEncodeTaskConfig_Consistency(t *testing.T) {
	t1 := &api.TaskConfig{
		Job: &api.JobKey{
			Role:        ptr.String("role"),
			Environment: ptr.String("environment"),
			Name:        ptr.String("name"),
		},
		Owner: &api.Identity{
			User: ptr.String("user"),
		},
		IsService:       ptr.Bool(true),
		Priority:        ptr.Int32(5),
		MaxTaskFailures: ptr.Int32(1),
		Production:      ptr.Bool(true),
		Tier:            ptr.String("revocable"),
		Resources: []*api.Resource{
			{NumCpus: ptr.Float64(1.5)},
			{RamMb: ptr.Int64(128)},
			{DiskMb: ptr.Int64(32)},
			{NamedPort: ptr.String("http")},
			{NamedPort: ptr.String("tchannel")},
			{NumGpus: ptr.Int64(2)},
		},
		Constraints: []*api.Constraint{
			{Name: ptr.String("mesos")},
			{Name: ptr.String("value")},
		},
		MesosFetcherUris: []*api.MesosFetcherURI{
			{Value: ptr.String("http://url1/")},
			{Value: ptr.String("http://url2/")},
		},
		ContactEmail: ptr.String("testuser@testdomain.com"),
		Metadata: []*api.Metadata{
			{
				Key:   ptr.String("test-key-1"),
				Value: ptr.String("test-value-1"),
			},
			{
				Key:   ptr.String("test-key-2"),
				Value: ptr.String("test-value-2"),
			},
		},
	}

	t2 := &api.TaskConfig{
		Job: &api.JobKey{
			Role:        ptr.String("role"),
			Environment: ptr.String("environment"),
			Name:        ptr.String("name"),
		},
		Owner: &api.Identity{
			User: ptr.String("user"),
		},
		IsService:       ptr.Bool(true),
		Priority:        ptr.Int32(5),
		MaxTaskFailures: ptr.Int32(1),
		Production:      ptr.Bool(true),
		Tier:            ptr.String("revocable"),
		Resources: []*api.Resource{
			{NamedPort: ptr.String("tchannel")},
			{NamedPort: ptr.String("http")},
			{RamMb: ptr.Int64(128)},
			{NumGpus: ptr.Int64(2)},
			{NumCpus: ptr.Float64(1.5)},
			{DiskMb: ptr.Int64(32)},
		},
		Constraints: []*api.Constraint{
			{Name: ptr.String("value")},
			{Name: ptr.String("mesos")},
		},
		MesosFetcherUris: []*api.MesosFetcherURI{
			{Value: ptr.String("http://url2/")},
			{Value: ptr.String("http://url1/")},
		},
		ContactEmail: ptr.String("testuser@testdomain.com"),
		Metadata: []*api.Metadata{
			{
				Key:   ptr.String("test-key-2"),
				Value: ptr.String("test-value-2"),
			},
			{
				Key:   ptr.String("test-key-1"),
				Value: ptr.String("test-value-1"),
			},
		},
	}

	b1, err := encodeTaskConfig(t1)
	assert.NoError(t, err)

	b2, err := encodeTaskConfig(t2)
	assert.NoError(t, err)

	assert.Equal(t, b1, b2)
}
