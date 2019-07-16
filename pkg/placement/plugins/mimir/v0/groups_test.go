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

package mimir_v0

import (
	"testing"

	"github.com/stretchr/testify/assert"

	common "github.com/uber/peloton/pkg/placement/plugins/mimir/common"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
	"github.com/uber/peloton/pkg/placement/testutil/v0"
)

func TestGroupMapper_Convert(t *testing.T) {
	offer := v0_testutil.SetupHostOffer()
	group := OfferToGroup(offer)
	assert.Equal(t, "hostname", group.Name)
	assert.Equal(t, 4800.0, group.Metrics.Get(common.CPUAvailable))
	assert.Equal(t, 128.0*metrics.GiB, group.Metrics.Get(common.MemoryAvailable))
	assert.Equal(t, 6.0*metrics.TiB, group.Metrics.Get(common.DiskAvailable))
	assert.Equal(t, 12800.0, group.Metrics.Get(common.GPUAvailable))
	assert.Equal(t, 10.0, group.Metrics.Get(common.PortsAvailable))
	assert.Equal(t, 1, group.Labels.Count(labels.NewLabel("attribute", "text")))
	assert.Equal(t, 1, group.Labels.Count(labels.NewLabel("attribute", "1")))
	assert.Equal(t, 1, group.Labels.Count(labels.NewLabel("attribute", "[31000-31009]")))
}
