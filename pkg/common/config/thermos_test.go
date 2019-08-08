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

package config

import (
	"testing"

	"github.com/uber/peloton/.gen/mesos/v1"

	"github.com/stretchr/testify/assert"
	"go.uber.org/thriftrw/ptr"
)

func TestNewThermosCommandInfo(t *testing.T) {
	c := &ThermosExecutorConfig{
		Path:      "/some/path/to/executor.pex",
		Resources: "/some/path/to/res1.pex,/some/path/to/res2.pex",
		Flags:     "-flag1 -flag2",
	}

	ci := c.NewThermosCommandInfo()
	assert.NotNil(t, ci)
	assert.Equal(t, []*mesos_v1.CommandInfo_URI{
		{
			Value:      ptr.String("/some/path/to/executor.pex"),
			Executable: ptr.Bool(true),
		},
		{
			Value:      ptr.String("/some/path/to/res1.pex"),
			Executable: ptr.Bool(true),
		},
		{
			Value:      ptr.String("/some/path/to/res2.pex"),
			Executable: ptr.Bool(true),
		},
	}, ci.GetUris())
	assert.Equal(t, "${MESOS_SANDBOX=.}/executor.pex -flag1 -flag2", ci.GetValue())
	assert.True(t, ci.GetShell())
}

func TestNewThermosExecutorInfo(t *testing.T) {
	c := &ThermosExecutorConfig{
		CPU: 2.5,
		RAM: 1024,
	}
	d := []byte{0x11, 0x12, 0x13, 0x14}

	ei := c.NewThermosExecutorInfo(d)
	assert.NotNil(t, ei)
	assert.Equal(t, mesos_v1.ExecutorInfo_CUSTOM, ei.GetType())
	assert.NotNil(t, ei.GetExecutorId())
	assert.Equal(t, []*mesos_v1.Resource{
		{
			Type: mesos_v1.Value_SCALAR.Enum(),
			Name: ptr.String("cpus"),
			Scalar: &mesos_v1.Value_Scalar{
				Value: ptr.Float64(c.CPU),
			},
		},
		{
			Type: mesos_v1.Value_SCALAR.Enum(),
			Name: ptr.String("mem"),
			Scalar: &mesos_v1.Value_Scalar{
				Value: ptr.Float64(float64(c.RAM)),
			},
		},
	}, ei.GetResources())
	assert.Equal(t, d, ei.GetData())
}
