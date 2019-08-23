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

package hostcache

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	p2kscalar "github.com/uber/peloton/pkg/hostmgr/p2k/scalar"
	"github.com/uber/peloton/pkg/hostmgr/scalar"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/yarpcerrors"
)

// makes sure kubeletHostSummary implements HostSummary
var _ HostSummary = &kubeletHostSummary{}

// makes sure kubeletHostSummary implements hostStrategy
var _ hostStrategy = &kubeletHostSummary{}

type kubeletHostSummary struct {
	*baseHostSummary

	// pod map of PodID to resources for pods that run on this host
	podToResMap map[string]scalar.Resources
}

// newKubeletHostSummary returns a zero initialized HostSummary object.
func newKubeletHostSummary(
	hostname string,
	r *peloton.Resources,
	version string,
) HostSummary {
	rs := scalar.FromPelotonResources(r)
	ks := &kubeletHostSummary{
		podToResMap:     make(map[string]scalar.Resources),
		baseHostSummary: newBaseHostSummary(hostname, version),
	}
	ks.baseHostSummary.capacity = rs
	ks.baseHostSummary.strategy = ks
	return ks
}

// HandlePodEvent makes sure that we update the host summary according to the
// pod events that occur in the cluster.
// Add events should no-op because we already allocated the resources while
// completing the lease and launching the pods.
// Update events are not handled at the moment, but theoretically should only
// noop because a pod update can only change the image of the pod, not the
// resource profile.
// Delete events should release the resources of the pod that was deleted.
func (a *kubeletHostSummary) HandlePodEvent(event *p2kscalar.PodEvent) {
	switch event.EventType {
	case p2kscalar.AddPod, p2kscalar.UpdatePod:
		// We do not need to do anything during an Add event, as it will
		// always follow a Launch, which already populated this host summary.
		// Update events only change the image of the pod, and as such the
		// resource accounting doesn't change.
		return
	case p2kscalar.DeletePod:
		// The release error scenario is handled inside release. If the pod
		// was already deleted, ReleasePodResources no-ops, which is correct
		// here.
		a.releasePodResources(context.Background(), event.Event.PodId.Value)
		return
	default:
		log.WithField("pod_event", event).
			Error("unsupported pod event type")
	}
}

// SetCapacity sets the capacity of the host.
// For k8s, capacity is updated by host event, and allocation is
// calculated when pod is launched/killed. Therefore, whenever capacity
// changes, available resources also changes.
func (a *kubeletHostSummary) SetCapacity(r scalar.Resources) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// once capacity changes, need to recalculate available resources
	a.capacity = r
	a.available = a.calculateAvailable()
}

// SetAvailable is noop for k8s agent, since it is calculated on-flight
func (a *kubeletHostSummary) SetAvailable(r scalar.Resources) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	log.WithField("hostname", a.hostname).
		Warn("unexpected call to SetAvailable for kubeletHostSummary")
	return
}

// releasePodResources adds back resources to the current kubeletHostSummary.
// When a pod is terminal, it will be deleted and this function will be called
// to remove that pod from the host summary and free up the resources allocated
// to that pod.
func (a *kubeletHostSummary) releasePodResources(
	ctx context.Context,
	podID string,
) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, ok := a.podToResMap[podID]; !ok {
		// TODO: add failure metric
		log.WithField("podID", podID).Error("pod not found in host summary")
		return
	}
	delete(a.podToResMap, podID)
	a.calculateAllocated()
}

func (a *kubeletHostSummary) calculateAvailable() scalar.Resources {
	available, ok := a.capacity.TrySubtract(a.allocated)
	if !ok {
		// continue with available set to scalar.Resources{}. This would
		// organically fail in the following steps.
		log.WithFields(
			log.Fields{
				"allocated":   a.allocated,
				"podToResMap": a.podToResMap,
				"capacity":    a.capacity,
			},
		).Error("kubeletHostSummary: Allocated more resources than capacity")
		return scalar.Resources{}
	}
	return available
}

func (a *kubeletHostSummary) postCompleteLease(newPodToResMap map[string]scalar.Resources) error {
	// At this point the lease is terminated, the host is back in ready/held
	// status but we need to validate if the new pods can be successfully
	// launched on this host. Note that the lease has to be terminated before
	// this step irrespective of the outcome
	if err := a.validateNewPods(newPodToResMap); err != nil {
		return yarpcerrors.InvalidArgumentErrorf("pod validation failed: %s", err)
	}

	// Update podToResMap with newPodToResMap for the new pods to be launched
	// Reduce available resources by the resources required by the new pods
	a.updatePodToResMap(newPodToResMap)

	return nil
}

// validateNewPods will return an error if:
// 1. The pod already exists on the host map.
// 2. The host has insufficient resources to place new pods.
// This function assumes baseHostSummary lock is held before calling.
func (a *kubeletHostSummary) validateNewPods(
	newPodToResMap map[string]scalar.Resources,
) error {
	var needed scalar.Resources

	for podID, res := range newPodToResMap {
		if _, ok := a.podToResMap[podID]; ok {
			return fmt.Errorf("pod %v already exists on the host", podID)
		}
		needed = needed.Add(res)
	}
	if !a.available.Contains(needed) {
		return errors.New("host has insufficient resources")
	}
	return nil
}

// calculateAllocated walks through the current list of pods on this host and
// calculates total allocated resources.
// This function assumes baseHostSummary lock is held before calling.
func (a *kubeletHostSummary) calculateAllocated() {
	var allocated scalar.Resources
	var ok bool

	// calculate current allocation based on the new pods map
	for _, r := range a.podToResMap {
		allocated = allocated.Add(r)
	}
	a.allocated = allocated
	a.available, ok = a.capacity.TrySubtract(allocated)
	if !ok {
		// continue with available set to scalar.Resources{}. This would
		// organically fail in the following steps.
		log.WithFields(
			log.Fields{
				"allocated":   a.allocated,
				"podToResMap": a.podToResMap,
				"capacity":    a.capacity,
			},
		).Error("kubeletHostSummary: No enough available resources")
		// no pod can be launched onto the host due to unexpected shortage
		// of resources. Set available to be 0, wait it to be updated when
		// more pod/capacity events come.
		a.available = scalar.Resources{}
	}
}

// updatepodToResMap updates the current podToResMap with the new podToResMap
// and also recalculate available resources based on the new podToResMap.
// This function assumes baseHostSummary lock is held before calling.
func (a *kubeletHostSummary) updatePodToResMap(
	newPodToResMap map[string]scalar.Resources,
) {
	// Add new pods to the pods map.
	for podID, res := range newPodToResMap {
		a.podToResMap[podID] = res
	}
	a.calculateAllocated()

}
