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

	assert.Len(t, p.GetLabels(), 2)
}
