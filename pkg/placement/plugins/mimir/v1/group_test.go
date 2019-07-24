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

package mimir_v1

import (
	"testing"

	"github.com/uber/peloton/pkg/hostmgr/scalar"
	common "github.com/uber/peloton/pkg/placement/plugins/mimir/common"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"

	"github.com/stretchr/testify/require"
)

func TestCreateGroup(t *testing.T) {
	t.Run("metrics are accurate", func(t *testing.T) {
		res := scalar.Resources{
			CPU:  1,
			Mem:  200,
			Disk: 1000,
			GPU:  2,
		}
		ports := uint64(1000)
		m := makeMetrics(res, ports)
		require.Equal(t, 100.0, m.Get(common.CPUAvailable))
		require.Equal(t, 200.0, m.Get(common.GPUAvailable))
		require.Equal(t, 200.0*metrics.MiB, m.Get(common.MemoryAvailable))
		require.Equal(t, 1000.0*metrics.MiB, m.Get(common.DiskAvailable))
	})

	t.Run("group name", func(t *testing.T) {
		res := scalar.Resources{
			CPU:  1,
			Mem:  200,
			Disk: 1000,
			GPU:  2,
		}
		ports := uint64(1000)
		labels := map[string]string{"l1": "k1"}
		group := CreateGroup("hostname1", res, ports, labels)
		require.Equal(t, "hostname1", group.Name)
	})
}
