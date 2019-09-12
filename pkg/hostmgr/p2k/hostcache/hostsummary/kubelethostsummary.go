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
	"errors"
	"sort"

	pbhost "github.com/uber/peloton/.gen/peloton/api/v1alpha/host"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/pkg/hostmgr/models"
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
}

// NewKubeletHostSummary returns a zero initialized HostSummary object.
func NewKubeletHostSummary(
	hostname string,
	capacity models.HostResources,
	version string,
) HostSummary {
	ks := &kubeletHostSummary{
		baseHostSummary: newBaseHostSummary(hostname, version),
	}
	ks.baseHostSummary.capacity = capacity
	ks.baseHostSummary.strategy = ks
	return ks
}

// HandlePodEvent updates pod resources and states by calling parent class,
// and recalculate available resources upon the change
func (a *kubeletHostSummary) HandlePodEvent(event *p2kscalar.PodEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.baseHostSummary.handlePodEvent(event)
	a.calculateAllocated()

	switch event.EventType {
	case p2kscalar.AddPod:
		// We recover allocated ports from AddPod events.
		// This is necessary for the initial sync or periodical resync.
		// Between syncs, we don't need to do anything for an Add event,
		// as it will always follow a Launch, which already populated this host summary.
		//
		// TODO: how can we differentiate sync with incremental changes?
		a.allocatePorts(event.Event)
		return
	case p2kscalar.UpdatePod:
		// Update events only change the image of the pod, and as such the
		// resource accounting doesn't change.
		return
	case p2kscalar.DeletePod:
		// The release error scenario is handled inside release. If the pod
		// was already deleted, ReleasePodResources no-ops, which is correct
		// here.
		a.releasePorts(event.Event)
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
func (a *kubeletHostSummary) SetCapacity(r models.HostResources) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// once capacity changes, need to recalculate available resources
	a.capacity = r
	a.available = a.calculateAvailable()
}

// SetAvailable is noop for k8s agent, since it is calculated on-flight
func (a *kubeletHostSummary) SetAvailable(r models.HostResources) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	log.WithField("hostname", a.hostname).
		Warn("unexpected call to SetAvailable for kubeletHostSummary")
	return
}

func (a *kubeletHostSummary) calculateAvailable() models.HostResources {
	available, ok := a.capacity.TrySubtract(a.allocated)
	if !ok {
		// Continue with available set to scalar.Resources{}. This would
		// organically fail in the following steps.
		log.WithFields(
			log.Fields{
				"allocated":    a.allocated,
				"podToSpecMap": a.getPodToResMap(),
				"capacity":     a.capacity,
			},
		).Error("kubeletHostSummary: Allocated more resources than capacity")
		return models.HostResources{
			Slack:    scalar.Resources{},
			NonSlack: scalar.Resources{},
		}
	}
	return available
}

func (a *kubeletHostSummary) postCompleteLease(podToSpecMap map[string]*pbpod.PodSpec) error {
	// At this point the lease is terminated, the host is back in ready/held
	// status but we need to validate if the new pods can be successfully
	// launched on this host. Note that the lease has to be terminated before
	// this step irrespective of the outcome.
	if err := a.validateEnoughResToLaunch(podToSpecMap); err != nil {
		return yarpcerrors.InvalidArgumentErrorf("pod validation failed: %s", err)
	}

	a.calculateAllocated()
	return nil
}

// validateEnoughResToLaunch will return an error if:
// a. The host has insufficient resources to place new pods.
// This function assumes baseHostSummary lock is held before calling.
func (a *kubeletHostSummary) validateEnoughResToLaunch(
	podToSpecMap map[string]*pbpod.PodSpec,
) error {
	// TODO validate slack too.
	var slackNeeded, nonSlackNeeded scalar.Resources

	podToResMap := make(map[string]scalar.Resources)
	for id, spec := range podToSpecMap {
		podToResMap[id] = scalar.FromPodSpec(spec)
	}

	for _, res := range podToResMap {
		nonSlackNeeded = nonSlackNeeded.Add(res)
	}

	if !a.available.Contains(models.HostResources{
		Slack:    slackNeeded,
		NonSlack: nonSlackNeeded,
	}) {
		return errors.New("host has insufficient resources")
	}
	return nil
}

// calculateAllocated walks through the current list of pods on this host and
// calculates total allocated resources.
// This function assumes baseHostSummary lock is held before calling.
func (a *kubeletHostSummary) calculateAllocated() {
	var slackAllocated, nonSlackallocated scalar.Resources
	var ok bool

	// calculate current allocation based on the new pods map
	// TODO: populate slackAllocated from this map.
	for _, r := range a.getPodToResMap() {
		nonSlackallocated = nonSlackallocated.Add(r)
	}
	a.allocated.NonSlack = nonSlackallocated
	a.available, ok = a.capacity.TrySubtract(models.HostResources{
		Slack:    slackAllocated,
		NonSlack: nonSlackallocated,
	})
	if !ok {
		// continue with available set to scalar.Resources{}. This would
		// organically fail in the following steps.
		log.WithFields(
			log.Fields{
				"allocated":    a.allocated,
				"podToSpecMap": a.getPodToResMap(),
				"capacity":     a.capacity,
			},
		).Error("kubeletHostSummary: No enough available resources")
		// no pod can be launched onto the host due to unexpected shortage
		// of resources. Set available to be 0, wait it to be updated when
		// more pod/capacity events come.
		a.available = models.HostResources{
			Slack:    scalar.Resources{},
			NonSlack: scalar.Resources{},
		}
	}
}

// TODO: do this for both slack and non-slack.
func (a *kubeletHostSummary) getPodToResMap() map[string]scalar.Resources {
	result := make(map[string]scalar.Resources)

	a.pods.RangePods(func(id string, p *podInfo) error {
		result[id] = scalar.FromPodSpec(
			p.spec,
		)
		return nil
	})

	return result
}

func (a *kubeletHostSummary) CompleteLaunchPod(pod *models.LaunchablePod) {
	// update available ports
	if len(pod.Ports) == 0 {
		return
	}
	ports := make([]int, 0, len(pod.Ports))
	for _, v := range pod.Ports {
		ports = append(ports, int(v))
	}
	usedRanges := toPortRanges(ports)

	a.mu.Lock()
	a.ports = subtractPortRanges(a.ports, usedRanges)
	a.mu.Unlock()
}

// toPortRanges sorts and arranges ports to a list of PortRange, in order.
func toPortRanges(ports []int) (all []*pbhost.PortRange) {
	sort.Ints(ports)
	ps := &pbhost.PortRange{Begin: uint64(ports[0]), End: uint64(ports[0])}
	for _, x := range ports {
		p := uint64(x)
		switch {
		case p <= ps.End:
		case p == ps.End+1:
			ps.End++
		default:
			all = append(all, ps)
			ps = &pbhost.PortRange{Begin: p, End: p}
		}
	}
	return append(all, ps)
}

// subtractPortRanges removes used PortRanges from avail PortRanges.
func subtractPortRanges(allAvail, allUsed []*pbhost.PortRange) (left []*pbhost.PortRange) {
	if len(allAvail) == 0 {
		return nil
	}

	var avail *pbhost.PortRange
	for (len(allAvail) > 0 || avail != nil) && len(allUsed) > 0 {
		if avail == nil {
			avail = allAvail[0]
			allAvail = allAvail[1:]
		}
		used := allUsed[0]
		if used.End < avail.Begin {
			// used to the left of avail
			allUsed = allUsed[1:]
		} else if used.End < avail.End {
			if used.Begin <= avail.Begin {
				// used overlaps the left of avail
			} else {
				// used in the middle of avail
				left = append(left, &pbhost.PortRange{Begin: avail.Begin, End: used.Begin - 1})
			}
			avail = &pbhost.PortRange{Begin: used.End + 1, End: avail.End}
			allUsed = allUsed[1:]
		} else {
			// used.End >= avail.End
			if used.Begin <= avail.Begin {
				// used covers avail
			} else if used.Begin > avail.End {
				// used to the right of avail
				left = append(left, avail)
			} else {
				// avail.Begin < used.Begin <= avail.End
				// used covers the right of avail
				left = append(left, &pbhost.PortRange{Begin: avail.Begin, End: used.Begin - 1})
			}
			avail = nil
		}
	}
	if avail != nil {
		left = append(left, avail)
	}
	return append(left, allAvail...)
}

func (a *kubeletHostSummary) allocatePorts(event *pbpod.PodEvent) {
	usedRanges := getPortRangesFromEvent(event)
	if len(usedRanges) == 0 {
		return
	}

	a.ports = subtractPortRanges(a.ports, usedRanges)
}

func getPortRangesFromEvent(event *pbpod.PodEvent) []*pbhost.PortRange {
	var ports []int
	for _, cs := range event.ContainerStatus {
		for _, v := range cs.Ports {
			ports = append(ports, int(v))
		}
	}
	if len(ports) == 0 {
		return nil
	}
	return toPortRanges(ports)
}

func (a *kubeletHostSummary) releasePorts(event *pbpod.PodEvent) {
	unusedRanges := getPortRangesFromEvent(event)
	if len(unusedRanges) == 0 {
		return
	}

	a.ports = mergePortRanges(a.ports, unusedRanges)
}

func mergePortRanges(allAvail, allUnused []*pbhost.PortRange) (merged []*pbhost.PortRange) {
	if len(allAvail) == 0 {
		return allUnused
	}

	for len(allAvail) > 0 && len(allUnused) > 0 {
		if allAvail[0].Begin < allUnused[0].Begin {
			merged = appendMerged(merged, allAvail[0])
			allAvail = allAvail[1:]
		} else {
			merged = appendMerged(merged, allUnused[0])
			allUnused = allUnused[1:]
		}
	}
	for _, x := range allAvail {
		merged = appendMerged(merged, x)
	}
	for _, x := range allUnused {
		merged = appendMerged(merged, x)
	}
	return merged
}

func appendMerged(merged []*pbhost.PortRange, r *pbhost.PortRange) []*pbhost.PortRange {
	if len(merged) == 0 {
		return []*pbhost.PortRange{r}
	}
	a := merged[len(merged)-1]
	// a.Begin <= r.Begin
	switch {
	case a.End+1 < r.Begin:
		// no overlapping
		return append(merged, r)
	case a.End < r.End:
		a.End = r.End
	}
	return merged
}
