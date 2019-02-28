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

package goalstate

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigNormalize(t *testing.T) {
	c := Config{}

	c.normalize()

	assert.Equal(t, _defaultMaxRetryDelay, c.MaxRetryDelay)
	assert.Equal(t, _defaultFailureRetryDelay, c.FailureRetryDelay)
	assert.Equal(t, _defaultLaunchTimeRetryDuration, c.LaunchTimeout)
	assert.Equal(t, _defaultStartTimeRetryDuration, c.StartTimeout)
	assert.Equal(t, _defaultJobRuntimeUpdateInterval, c.JobBatchRuntimeUpdateInterval)
	assert.Equal(t, _defaultJobRuntimeUpdateInterval, c.JobServiceRuntimeUpdateInterval)
	assert.Equal(t, _defaultJobWorkerThreads, c.NumWorkerJobThreads)
	assert.Equal(t, _defaultTaskWorkerThreads, c.NumWorkerTaskThreads)
	assert.Equal(t, _defaultUpdateWorkerThreads, c.NumWorkerUpdateThreads)
}
