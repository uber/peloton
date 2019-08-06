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

package lifecyclemgr

import (
	"testing"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/rpc"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
)

// TestNew tests constructors for v0 and v1 lifecycle managers.
func TestNew(t *testing.T) {
	tt := rpc.NewTransport()
	outbound := tt.NewOutbound(nil)
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:     common.PelotonResourceManager,
		Inbounds: nil,
		Outbounds: yarpc.Outbounds{
			common.PelotonHostManager: transport.Outbounds{
				Unary: outbound,
			},
		},
		Metrics: yarpc.MetricsConfig{
			Tally: tally.NoopScope,
		},
	})

	lm := New(api.V1Alpha, dispatcher, tally.NoopScope)

	_, ok := lm.(Manager)
	assert.True(t, ok)

	lm = New(api.V0, dispatcher, tally.NoopScope)
	_, ok = lm.(Manager)
	assert.True(t, ok)
}
