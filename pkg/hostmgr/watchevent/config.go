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

package watchevent

const (
	_defaultBufferSize int = 100
	_defaultMaxClient  int = 1000
)

// Config for Watch API
type Config struct {
	// Size of per-client internal buffer
	BufferSize int `yaml:"buffer_size"`

	// Maximum number of concurrent watch clients
	MaxClient int `yaml:"max_client"`
}

func (c *Config) normalize() {
	if c.BufferSize <= 0 {
		c.BufferSize = _defaultBufferSize
	}
	if c.MaxClient <= 0 {
		c.MaxClient = _defaultMaxClient
	}
}
