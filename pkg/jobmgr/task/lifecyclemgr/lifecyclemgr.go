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

	"github.com/uber/peloton/pkg/common/api"

	"go.uber.org/yarpc"
	"golang.org/x/time/rate"
)

// Manager interface defines methods to kill workloads.
type Manager interface {
	Lockable

	// Kill will kill tasks/pods using their ID.
	Kill(
		ctx context.Context,
		id string,
		hostToReserve string,
		rateLimiter *rate.Limiter,
	) error
	// ShutdownExecutor will shutdown the underlying mesos executor. This will
	// be a no-op for v1 LifecycleMgr.
	ShutdownExecutor(
		ctx context.Context,
		id string,
		agentID string,
		rateLimiter *rate.Limiter,
	) error
}

// New gets hostmgr API specific task/pod lifecycle mgr instance
func New(
	version api.Version,
	dispatcher *yarpc.Dispatcher,
) Manager {
	if version.IsV1() {
		return newV1LifecycleMgr(dispatcher)
	} else {
		return newV0LifecycleMgr(dispatcher)
	}
}
