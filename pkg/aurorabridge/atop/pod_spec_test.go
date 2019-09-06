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

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/common/config"

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
		config.ThermosExecutorConfig{},
	)
	assert.NoError(t, err)

	assert.Len(t, p.Containers, 1)
	r := p.Containers[0].GetResource()

	assert.Equal(t, float64(cpu), r.GetCpuLimit())
	assert.Equal(t, float64(mem), r.GetMemLimitMb())
	assert.Equal(t, float64(disk), r.GetDiskLimitMb())
	assert.Equal(t, float64(gpu), r.GetGpuLimit())

	assert.NotNil(t, p.GetMesosSpec())
	assert.NotNil(t, p.GetMesosSpec().GetExecutorSpec())
	assert.NotNil(t, p.GetContainers()[0].GetEntrypoint())
	assert.Len(t, p.GetLabels(), 3)
}

// TestNewResourceSpec tests newResourceSpec
func TestNewResourceSpec(t *testing.T) {
	// Empty resource expect nil ResourceSpec
	rs := []*api.Resource{}
	r := newResourceSpec(rs, nil)
	assert.Nil(t, r)

	// Check regular ResourceSpec conversion
	rs = []*api.Resource{
		{
			NumCpus: ptr.Float64(2.5),
		},
		{
			RamMb: ptr.Int64(256),
		},
		{
			DiskMb: ptr.Int64(32),
		},
		{
			NumGpus: ptr.Int64(2),
		},
	}
	r = newResourceSpec(rs, nil)
	assert.Equal(t, &pod.ResourceSpec{
		CpuLimit:    float64(2.5),
		MemLimitMb:  float64(256),
		DiskLimitMb: float64(32),
		GpuLimit:    float64(2),
	}, r)

	// Check ResourceSpec conversion, with gpu limit passed in
	rs = []*api.Resource{
		{
			NumCpus: ptr.Float64(2.5),
		},
		{
			RamMb: ptr.Int64(256),
		},
		{
			DiskMb: ptr.Int64(32),
		},
		{
			NumGpus: ptr.Int64(2),
		},
	}
	r = newResourceSpec(rs, ptr.Float64(3))
	assert.Equal(t, &pod.ResourceSpec{
		CpuLimit:    float64(2.5),
		MemLimitMb:  float64(256),
		DiskLimitMb: float64(32),
		GpuLimit:    float64(3),
	}, r)
}
