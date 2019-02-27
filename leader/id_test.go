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

package leader

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServiceInstance(t *testing.T) {
	additionalEndpoints := make(map[string]Endpoint, 1)

	endpoint := Endpoint{
		Host: "10.102.141.24",
		Port: 5396,
	}
	aEndpoint := Endpoint{
		Host: "10.102.141.24",
		Port: 5396,
	}
	additionalEndpoints["http"] = aEndpoint

	serviceInstance := NewServiceInstance(endpoint, additionalEndpoints)

	// Expected aurora znode format
	expected := "{\"serviceEndpoint\":{\"host\":\"10.102.141.24\",\"port\":5396},\"additionalEndpoints\":{\"http\":{\"host\":\"10.102.141.24\",\"port\":5396}},\"status\":\"ALIVE\"}"
	assert.Equal(t, expected, serviceInstance)
}
