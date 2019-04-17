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

package common

import (
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"

	"github.com/stretchr/testify/assert"
)

// TestRemoveBridgeUpdateLabel tests removeBridgeUpdateLabel util method.
func TestRemoveBridgeUpdateLabel(t *testing.T) {
	p1 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{Key: "k1", Value: "v1"},
			{Key: "k2", Value: "v2"},
		},
	}
	p2 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{Key: "k2", Value: "v2"},
			{Key: "k1", Value: "v1"},
			{Key: BridgeUpdateLabelKey, Value: "123456"},
		},
	}

	v1 := RemoveBridgeUpdateLabel(p1)
	assert.Empty(t, v1)
	assert.Equal(t, &pod.PodSpec{
		Labels: []*peloton.Label{
			{Key: "k1", Value: "v1"},
			{Key: "k2", Value: "v2"},
		},
	}, p1)

	v2 := RemoveBridgeUpdateLabel(p2)
	assert.Equal(t, "123456", v2)
	assert.Equal(t, &pod.PodSpec{
		Labels: []*peloton.Label{
			{Key: "k2", Value: "v2"},
			{Key: "k1", Value: "v1"},
		},
	}, p2)
}

// TestIsPodSpecWithoutBridgeUpdateLabelChanged tests
// IsPodSpecWithoutBridgeUpdateLabelChanged util method.
func TestIsPodSpecWithoutBridgeUpdateLabelChanged(t *testing.T) {
	p1 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{Key: "k1", Value: "v1"},
			{Key: "k2", Value: "v2"},
		},
	}
	p2 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{Key: "k2", Value: "v2"},
			{Key: "k1", Value: "v1"},
			{Key: BridgeUpdateLabelKey, Value: "123456"},
		},
	}
	p3 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{Key: BridgeUpdateLabelKey, Value: "654321"},
			{Key: "k2", Value: "v2"},
			{Key: "k1", Value: "v1"},
		},
	}
	p4 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{Key: "k2", Value: "v3"},
			{Key: "k1", Value: "v1"},
			{Key: BridgeUpdateLabelKey, Value: "123456"},
		},
	}

	assert.False(t, IsPodSpecWithoutBridgeUpdateLabelChanged(p1, p2))
	assert.False(t, IsPodSpecWithoutBridgeUpdateLabelChanged(p1, p3))
	assert.False(t, IsPodSpecWithoutBridgeUpdateLabelChanged(p2, p3))
	assert.True(t, IsPodSpecWithoutBridgeUpdateLabelChanged(p1, p4))
}
