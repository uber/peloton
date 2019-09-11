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

package hostsummary

import (
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	hostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"
	"github.com/uber/peloton/pkg/hostmgr/models"
	p2kscalar "github.com/uber/peloton/pkg/hostmgr/p2k/scalar"
)

type HostSummary interface {
	// TryMatch atomically tries to match the current host with given
	// HostFilter, and lock the host if it does.
	TryMatch(filter *hostmgr.HostFilter) Match

	// CompleteLease verifies that the leaseID on this host is still valid.
	CompleteLease(leaseID string, podToSpecMap map[string]*pbpod.PodSpec) error

	// CasStatus sets the status to new value if current value is old, otherwise
	// returns error.
	CasStatus(old, new HostStatus) error

	// GetCapacity returns the capacity of the host.
	GetCapacity() models.HostResources

	// GetAllocated returns the allocation of the host.
	GetAllocated() models.HostResources

	// GetAvailable returns the available resources of the host.
	GetAvailable() models.HostResources

	// SetCapacity sets the capacity of the host.
	SetCapacity(r models.HostResources)

	// SetAvailable sets the available resource of the host.
	SetAvailable(r models.HostResources)

	// GetVersion returns the version of the host.
	GetVersion() string

	// SetVersion sets the version of the host.
	SetVersion(v string)

	// GetHostname returns the hostname of the host.
	GetHostname() string

	// GetHostStatus returns the HostStatus of the host.
	GetHostStatus() HostStatus

	// GetHostLease creates and returns a host lease.
	GetHostLease() *hostmgr.HostLease

	// TerminateLease is called when terminating the lease on a host.
	TerminateLease(leaseID string) error

	// HandlePodEvent is called when a pod event occurs for a pod
	// that affects this host.
	HandlePodEvent(event *p2kscalar.PodEvent)

	// HoldForPod holds the host for the pod specified.
	// If an error is returned, hostsummary would guarantee that
	// the host is not held for the task.
	HoldForPod(id *peloton.PodID) error

	// ReleaseHoldForPod release the hold of host for the pod specified.
	ReleaseHoldForPod(id *peloton.PodID)

	// GetHeldPods returns a slice of pods that puts the host in held.
	GetHeldPods() []*peloton.PodID

	// DeleteExpiredHolds deletes expired held pods in a hostSummary, returns
	// whether the hostSummary is free of helds,
	// available resource,
	// and the pods held expired.
	DeleteExpiredHolds(now time.Time) (bool, models.HostResources, []*peloton.PodID)

	// CompleteLaunchPod is called when a pod is successfully launched,
	// for example to remove the ports from the available port ranges.
	CompleteLaunchPod(pod *models.LaunchablePod)

	// RecoverPodInfo updates pods info on the host, it is used only
	// when hostsummary needs to recover the info upon restart
	RecoverPodInfo(id *peloton.PodID, state pbpod.PodState, spec *pbpod.PodSpec)
}

// hostStrategy defines methods that shared by mesos/k8s hosts, but have
// different ßimplementation.
// methods in the interface assumes lock is taken.
type hostStrategy interface {
	// postCompleteLease handles actions after lease is completed.
	postCompleteLease(podToSpecMap map[string]*pbpod.PodSpec) error
}
