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

	"github.com/stretchr/testify/assert"
)

func TestConfigNormalize(t *testing.T) {
	c := ArchiverConfig{}

	c.Normalize()

	assert.Equal(t, _defaultArchiveInterval, c.ArchiveInterval)
	assert.Equal(t, _defaultMaxArchiveEntries, c.MaxArchiveEntries)
	assert.Equal(t, _defaultArchiveAge, c.ArchiveAge)
	assert.Equal(t, _defaultPelotonClientTimeout, c.PelotonClientTimeout)
	assert.Equal(t, _defaultArchiveStepSize, c.ArchiveStepSize)
	assert.Equal(t, _defaultMaxRetryAttemptsJobQuery, c.MaxRetryAttemptsJobQuery)
	assert.Equal(t, _defaultRetryIntervalJobQuery, c.RetryIntervalJobQuery)
	assert.Equal(t, _defaultBootstrapDelay, c.BootstrapDelay)
}
