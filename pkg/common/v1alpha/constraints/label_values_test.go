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

package constraints

import (
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"

	"github.com/uber/peloton/pkg/common"

	"github.com/stretchr/testify/assert"
)

func TestGetHostLabelValues(t *testing.T) {
	hostname := "foo.localhost"

	t.Run("count hostname and labels", func(t *testing.T) {
		labels := []*peloton.Label{
			{Key: "k0", Value: "k0-v0"},
			{Key: "k0", Value: "k0-v1"},
			{Key: "k1", Value: "k1-v0"},
			{Key: "k0", Value: "k0-v0"}}
		res := GetHostLabelValues(hostname, labels)
		assert.Equal(t, uint32(1), res[common.HostNameKey][hostname])
		assert.Equal(t, uint32(2), res["k0"]["k0-v0"])
		assert.Equal(t, uint32(1), res["k0"]["k0-v1"])
		assert.Equal(t, uint32(1), res["k1"]["k1-v0"])
	})

	t.Run("empty label value", func(t *testing.T) {
		labels := []*peloton.Label{
			{Key: "k0", Value: "k0-v0"},
			{Key: "k0"},
			{Key: "k1"}}
		res := GetHostLabelValues(hostname, labels)
		assert.Equal(t, uint32(1), res[common.HostNameKey][hostname])
		assert.Equal(t, uint32(1), res["k0"]["k0-v0"])
		assert.Equal(t, uint32(1), res["k0"][""])
		assert.Equal(t, uint32(1), res["k1"][""])
	})
}

func TestHasExclusiveLabel(t *testing.T) {
	t.Run("no exclusive label", func(t *testing.T) {
		labels := []*peloton.Label{
			{Key: "k0", Value: "k0-v0"},
		}
		ok := HasExclusiveLabel(labels)
		assert.False(t, ok)
	})

	t.Run("one exclusive label", func(t *testing.T) {
		labels := []*peloton.Label{
			{Key: "k0", Value: "k0-v0"},
			{Key: common.PelotonExclusiveNodeLabel},
		}
		ok := HasExclusiveLabel(labels)
		assert.True(t, ok)
	})

	t.Run("multiple exclusive labels", func(t *testing.T) {
		labels := []*peloton.Label{
			{Key: "k0", Value: "k0-v0"},
			{Key: common.PelotonExclusiveNodeLabel},
			{Key: common.PelotonExclusiveNodeLabel, Value: "foo"},
		}
		ok := HasExclusiveLabel(labels)
		assert.True(t, ok)
	})
}
