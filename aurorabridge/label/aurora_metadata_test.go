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

	"github.com/stretchr/testify/assert"
	"go.uber.org/thriftrw/ptr"
)

func TestAuroraMetadata(t *testing.T) {
	input := []*api.Metadata{
		&api.Metadata{
			Key:   ptr.String("test-key-1"),
			Value: ptr.String("test-value-1"),
		},
		&api.Metadata{
			Key:   ptr.String("test-key-2"),
			Value: ptr.String("test-value-2"),
		},
	}

	l, err := NewAuroraMetadata(input)
	assert.NoError(t, err)

	output, err := ParseAuroraMetadata([]*peloton.Label{l})
	assert.NoError(t, err)
	assert.Equal(t, input, output)
}
