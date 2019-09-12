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
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/common/api"

	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
	"golang.org/x/time/rate"
)

// hostmgr API timeout
const _defaultHostmgrAPITimeout = 10 * time.Second

var (
	errEmptyTasks = yarpcerrors.InvalidArgumentErrorf(
		"empty tasks infos")
	errEmptyPods = yarpcerrors.InvalidArgumentErrorf(
		"empty pods info")
	errLaunchInvalidOffer = yarpcerrors.InternalErrorf(
		"invalid offer to launch tasks")
)

// LaunchableTaskInfo contains the info of a task to be launched.
type LaunchableTaskInfo struct {
	*task.TaskInfo
	// ConfigAddOn is the task config add on.
	ConfigAddOn *models.ConfigAddOn
	// Spec is the pod spec for the pod to be launched.
	Spec *pbpod.PodSpec
}

// Manager interface defines methods to kill workloads.
type Manager interface {
	Lockable

	// Launch will launch tasks/pods using their config/spec on the specified
	// host using the acquired leaseID.
	Launch(
		ctx context.Context,
		leaseID string,
		hostname string,
		agentID string,
		tasks map[string]*LaunchableTaskInfo,
		rateLimiter *rate.Limiter,
	) error
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

	// TerminateLease will be called to terminate the acquired lease on
	// hostmgr in case of any errors. This will ensure that the hosts that
	// are leased are not freed up for placement in case we cannot place the
	// current set of tasks on them.
	TerminateLease(
		ctx context.Context,
		hostname string,
		agentID string,
		leaseID string,
	) error

	// GetTasksOnDrainingHosts gets the taskIDs of the tasks on the
	// hosts in DRAINING state
	GetTasksOnDrainingHosts(
		ctx context.Context,
		limit uint32,
		timeout uint32,
	) ([]string, error)
}

// New gets hostmgr API specific task/pod lifecycle mgr instance
func New(
	version api.Version,
	dispatcher *yarpc.Dispatcher,
	parent tally.Scope,
) Manager {
	if version.IsV1() {
		return newV1LifecycleMgr(dispatcher, parent)
	}
	return newV0LifecycleMgr(dispatcher, parent)
}
