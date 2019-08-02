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

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	v1_hostsvc "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha/svc"

	"github.com/uber/peloton/pkg/common"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
	"golang.org/x/time/rate"
)

type v1LifecycleMgr struct {
	*lockState
	// v1 client hostmgr pod operations.
	hostManagerV1 v1_hostsvc.HostManagerServiceYARPCClient
}

// newV1LifecycleMgr returns an instance of the v1 lifecycle manager.
func newV1LifecycleMgr(
	dispatcher *yarpc.Dispatcher,
) *v1LifecycleMgr {
	return &v1LifecycleMgr{
		hostManagerV1: v1_hostsvc.NewHostManagerServiceYARPCClient(
			dispatcher.ClientConfig(
				common.PelotonHostManager,
			),
		),
		lockState: &lockState{state: 0},
	}
}

// Kill tries to kill the pod using podID.
// Functionality to reserve a host is not implemented in v1.
func (l *v1LifecycleMgr) Kill(
	ctx context.Context,
	podID string,
	hostToReserve string,
	rateLimiter *rate.Limiter,
) error {
	// check lock
	if l.lockState.hasKillLock() {
		return yarpcerrors.InternalErrorf("kill op is locked")
	}

	// enforce rate limit
	if rateLimiter != nil && !rateLimiter.Allow() {
		return yarpcerrors.ResourceExhaustedErrorf(
			"rate limit reached for kill")
	}

	_, err := l.hostManagerV1.KillPods(
		ctx,
		&v1_hostsvc.KillPodsRequest{
			PodIds: []*peloton.PodID{{Value: podID}},
		},
	)
	return err
}

// ShutdownExecutor is a no-op for v1 lifecyclemgr.
// This is a mesos specific call and will only be implemented for v0 case.
func (l *v1LifecycleMgr) ShutdownExecutor(
	ctx context.Context,
	taskID string,
	agentID string,
	rateLimiter *rate.Limiter,
) error {
	return nil
}
