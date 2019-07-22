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
	"fmt"
	"strings"
	"testing"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	v0_hostsvc "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	v0_host_mocks "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/rpc"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/yarpcerrors"
	"golang.org/x/time/rate"
)

const randomErrorStr = "random error"

type LifecycleTestSuite struct {
	suite.Suite
	ctrl        *gomock.Controller
	ctx         context.Context
	mockHostMgr *v0_host_mocks.MockInternalHostServiceYARPCClient
	mesosTaskID string
	jobID       string
	instanceID  int32
	lm          *v0LifecycleMgr
}

func (suite *LifecycleTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestLifecycleTestSuite tests functions covered in jobmgr/task/util.go
func TestLifecycleTestSuite(t *testing.T) {
	suite.Run(t, new(LifecycleTestSuite))
}

func (suite *LifecycleTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.ctx = context.Background()
	suite.mockHostMgr = v0_host_mocks.
		NewMockInternalHostServiceYARPCClient(suite.ctrl)
	suite.lm = &v0LifecycleMgr{
		hostManagerV0: suite.mockHostMgr,
	}
	suite.jobID = "af647b98-0ae0-4dac-be42-c74a524dfe44"
	suite.instanceID = 89
	suite.mesosTaskID = fmt.Sprintf(
		"%s-%d-%s",
		suite.jobID,
		suite.instanceID,
		uuid.New())

}

func (suite *LifecycleTestSuite,
) buildKillTasksReq() *v0_hostsvc.KillTasksRequest {
	return &v0_hostsvc.KillTasksRequest{
		TaskIds: []*mesos.TaskID{{Value: &suite.mesosTaskID}},
	}
}

// build a shutdown executor request
func (suite *LifecycleTestSuite,
) buildShutdownExecutorsReq() *v0_hostsvc.ShutdownExecutorsRequest {
	return &v0_hostsvc.ShutdownExecutorsRequest{
		Executors: []*v0_hostsvc.ExecutorOnAgent{
			{
				ExecutorId: &mesos.ExecutorID{Value: &suite.mesosTaskID},
				AgentId:    &mesos.AgentID{Value: &suite.mesosTaskID},
			},
		},
	}
}

// TestNew tests creating an instance of v0 lifecyclemgr
func (suite *LifecycleTestSuite) TestNew() {
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

	_ = New(dispatcher)
}

// TestKill tests successful lm.Kill
func (suite *LifecycleTestSuite) TestKill() {
	suite.mockHostMgr.EXPECT().
		KillTasks(gomock.Any(), suite.buildKillTasksReq())
	err := suite.lm.Kill(
		suite.ctx,
		suite.mesosTaskID,
		"",
		nil,
	)
	suite.Nil(err)
}

// TestKillTaskInvalidTaskIDs tests InvalidTaskIDs error in KillTask
func (suite *LifecycleTestSuite) TestKillInvalidTaskIDs() {
	// Simulate InvalidTaskIDs error
	resp := &v0_hostsvc.KillTasksResponse{
		Error: &v0_hostsvc.KillTasksResponse_Error{
			InvalidTaskIDs: &v0_hostsvc.InvalidTaskIDs{
				Message: randomErrorStr,
			},
		},
	}
	suite.mockHostMgr.EXPECT().KillTasks(
		gomock.Any(),
		suite.buildKillTasksReq()).
		Return(resp, nil)
	err := suite.lm.Kill(
		suite.ctx,
		suite.mesosTaskID,
		"",
		nil,
	)
	suite.Error(err)
	suite.True(yarpcerrors.IsInternal(err))
	suite.True(strings.Contains(err.Error(), randomErrorStr))
}

// TestKillFailure tests KillFailure error in Kill
func (suite *LifecycleTestSuite) TestKillFailure() {
	// Simulate KillFailure error
	resp := &v0_hostsvc.KillTasksResponse{
		Error: &v0_hostsvc.KillTasksResponse_Error{
			KillFailure: &v0_hostsvc.KillFailure{
				Message: randomErrorStr,
			},
		},
	}
	suite.mockHostMgr.EXPECT().KillTasks(
		gomock.Any(),
		suite.buildKillTasksReq()).
		Return(resp, nil)
	err := suite.lm.Kill(
		suite.ctx,
		suite.mesosTaskID,
		"",
		nil,
	)
	suite.Error(err)
	suite.True(yarpcerrors.IsInternal(err))
	suite.True(strings.Contains(err.Error(), randomErrorStr))
}

// TestKillRateLimit tests task kill fails due to rate limit reached
func (suite *LifecycleTestSuite) TestKillRateLimit() {
	err := suite.lm.Kill(
		suite.ctx,
		suite.mesosTaskID,
		"",
		rate.NewLimiter(0, 0),
	)
	suite.Error(err)
	suite.True(yarpcerrors.IsResourceExhausted(err))
}

// TestShutdownExecutorShutdownFailure tests ShutdownFailure error in
// suite.lm.ShutdownExecutor
func (suite *LifecycleTestSuite) TestShutdownExecutorShutdownFailure() {
	resp := &v0_hostsvc.ShutdownExecutorsResponse{
		Error: &v0_hostsvc.ShutdownExecutorsResponse_Error{
			ShutdownFailure: &v0_hostsvc.ShutdownFailure{
				Message: randomErrorStr,
			},
		},
	}
	suite.mockHostMgr.EXPECT().ShutdownExecutors(
		suite.ctx,
		suite.buildShutdownExecutorsReq()).
		Return(resp, nil)
	err := suite.lm.ShutdownExecutor(
		suite.ctx,
		suite.mesosTaskID,
		suite.mesosTaskID,
		nil)
	suite.Error(err)
	suite.True(yarpcerrors.IsInternal(err))
	suite.True(strings.Contains(err.Error(), randomErrorStr))
}

// TestExecutorShutdownRateLimit tests executor shutdown fails due to
// rate limit
func (suite *LifecycleTestSuite) TestExecutorShutdownRateLimit() {
	err := suite.lm.ShutdownExecutor(
		suite.ctx,
		suite.mesosTaskID,
		suite.mesosTaskID,
		rate.NewLimiter(0, 0),
	)
	suite.Error(err)
	suite.True(yarpcerrors.IsResourceExhausted(err))
}

// TestShutdownExecutorInvalidExecutors tests InvalidExecutors error in
// lm.ShutdownExecutor
func (suite *LifecycleTestSuite) TestShutdownExecutorInvalidExecutors() {
	// Simulate InvalidExecutors error
	resp := &v0_hostsvc.ShutdownExecutorsResponse{
		Error: &v0_hostsvc.ShutdownExecutorsResponse_Error{
			InvalidExecutors: &v0_hostsvc.InvalidExecutors{
				Message: randomErrorStr,
			},
		},
	}
	suite.mockHostMgr.EXPECT().ShutdownExecutors(
		suite.ctx, suite.buildShutdownExecutorsReq()).
		Return(resp, nil)
	err := suite.lm.ShutdownExecutor(
		suite.ctx,
		suite.mesosTaskID,
		suite.mesosTaskID,
		nil,
	)
	suite.Error(err)
	suite.True(yarpcerrors.IsInternal(err))
	suite.True(strings.Contains(err.Error(), randomErrorStr))
}
