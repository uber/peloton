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
	"context"
	"strings"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	v1_hostsvc "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha/svc"
	v1_host_mocks "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha/svc/mocks"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/rpc"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/yarpcerrors"
	"golang.org/x/time/rate"
)

type v1LifecycleTestSuite struct {
	suite.Suite
	ctrl        *gomock.Controller
	ctx         context.Context
	mockHostMgr *v1_host_mocks.MockHostManagerServiceYARPCClient
	podID       string
	lm          *v1LifecycleMgr
}

func (suite *v1LifecycleTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestV1LifecycleTestSuite tests functions covered in jobmgr/task/util.go
func TestV1LifecycleTestSuite(t *testing.T) {
	suite.Run(t, new(v1LifecycleTestSuite))
}

func (suite *v1LifecycleTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.ctx = context.Background()
	suite.mockHostMgr = v1_host_mocks.
		NewMockHostManagerServiceYARPCClient(suite.ctrl)
	suite.lm = &v1LifecycleMgr{
		hostManagerV1: suite.mockHostMgr,
		lockState:     &lockState{state: 0},
	}
	suite.podID = "af647b98-0ae0-4dac-be42-c74a524dfe44-0"
}

func (suite *v1LifecycleTestSuite,
) buildKillPodsReq() *v1_hostsvc.KillPodsRequest {
	return &v1_hostsvc.KillPodsRequest{
		PodIds: []*peloton.PodID{{Value: suite.podID}},
	}
}

// TestNew tests creating an instance of v1 lifecyclemgr
func (suite *v1LifecycleTestSuite) TestNew() {
	t := rpc.NewTransport()
	outbound := t.NewOutbound(nil)
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

	_ = newV1LifecycleMgr(dispatcher)
}

// TestKill tests Kill pods.
func (suite *v1LifecycleTestSuite) TestKill() {
	suite.mockHostMgr.EXPECT().
		KillPods(gomock.Any(), suite.buildKillPodsReq())
	err := suite.lm.Kill(
		suite.ctx,
		suite.podID,
		"",
		nil,
	)
	suite.Nil(err)
}

// TestKillLock tests Kill pods is blocked when kill is locked
func (suite *v1LifecycleTestSuite) TestKillLock() {
	suite.lm.LockKill()
	err := suite.lm.Kill(
		suite.ctx,
		suite.podID,
		"",
		nil,
	)
	suite.Error(err)
}

// TestKillFailure tests KillFailure error in Kill
func (suite *v1LifecycleTestSuite) TestKillFailure() {
	suite.mockHostMgr.EXPECT().KillPods(
		gomock.Any(),
		suite.buildKillPodsReq()).
		Return(nil, yarpcerrors.InternalErrorf(randomErrorStr))
	err := suite.lm.Kill(
		suite.ctx,
		suite.podID,
		"",
		nil,
	)
	suite.Error(err)
	suite.True(yarpcerrors.IsInternal(err))
	suite.True(strings.Contains(err.Error(), randomErrorStr))
}

// TestKillRateLimit tests task kill fails due to rate limit reached
func (suite *v1LifecycleTestSuite) TestKillRateLimit() {
	err := suite.lm.Kill(
		suite.ctx,
		suite.podID,
		"",
		rate.NewLimiter(0, 0),
	)
	suite.Error(err)
	suite.True(yarpcerrors.IsResourceExhausted(err))
}

// TestShutdownExecutor tests lm.ShutdownExecutor.
func (suite *v1LifecycleTestSuite) TestShutdownExecutor() {
	err := suite.lm.ShutdownExecutor(
		suite.ctx,
		suite.podID,
		"",
		nil)
	suite.NoError(err)
}

// TestShutdownExecutor tests lm.ShutdownExecutor when kill is locked.
// Lock has no effect, since v1 does not support executor shutdown
func (suite *v1LifecycleTestSuite) TestShutdownExecutorLock() {
	suite.lm.LockKill()
	err := suite.lm.ShutdownExecutor(
		suite.ctx,
		suite.podID,
		"",
		nil)
	suite.NoError(err)
}
