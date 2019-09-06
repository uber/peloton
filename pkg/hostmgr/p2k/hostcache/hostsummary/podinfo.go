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
	"sync"

	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
)

// podInfo contains pod spec and current state.
type podInfo struct {
	spec  *pbpod.PodSpec
	state pbpod.PodState
}

// newPodInfo creates new podInfo object with given spec, assuming it's in
// "Launched" state.
func newPodInfo(spec *pbpod.PodSpec) *podInfo {
	return &podInfo{
		spec:  spec,
		state: pbpod.PodState_POD_STATE_LAUNCHED,
	}
}

// podInfoMap is a pod ID to pod info map.
type podInfoMap struct {
	mu sync.RWMutex

	m map[string]*podInfo
}

// newPodInfoMap initializes a new podInfoMap.
func newPodInfoMap() *podInfoMap {
	return &podInfoMap{
		m: make(map[string]*podInfo),
	}
}

// GetSize returns size of the map.
func (pm *podInfoMap) GetSize() int {
	return len(pm.m)
}

// GetPodInfo returns pod info with given pod ID.
func (pm *podInfoMap) GetPodInfo(id string) (*podInfo, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	info, ok := pm.m[id]

	return info, ok
}

// AddPodInfo adds given pod ID and pod info into the map.
func (pm *podInfoMap) AddPodInfo(id string, info *podInfo) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.m[id] = info
}

// AddPodSpec constructs a new pod info using given pod spec, assuming the pod
// is in "Launched" state, and add it into the map with given pod ID.
func (pm *podInfoMap) AddPodSpec(id string, spec *pbpod.PodSpec) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.m[id] = &podInfo{
		spec:  spec,
		state: pbpod.PodState_POD_STATE_LAUNCHED,
	}
}

// AddPodSpecs constructs new pod infos using given pod spec map, assuming all
// pods are in "Launched" state.
func (pm *podInfoMap) AddPodSpecs(podToSpecMap map[string]*pbpod.PodSpec) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for id, spec := range podToSpecMap {
		pm.m[id] = &podInfo{
			spec:  spec,
			state: pbpod.PodState_POD_STATE_LAUNCHED,
		}
	}
}

// RemovePod removes pod info with given id from map.
func (pm *podInfoMap) RemovePod(id string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	delete(pm.m, id)
}

// AnyPodExist returns true if any given pod exists.
func (pm *podInfoMap) AnyPodExist(podToSpecMap map[string]*pbpod.PodSpec) string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	for podID := range podToSpecMap {
		if _, ok := pm.m[podID]; ok {
			return podID
		}
	}
	return ""
}

// RangePods applies given function to all <podID, podInfo> pairs in the map.
func (pm *podInfoMap) RangePods(f func(podID string, info *podInfo) error) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for id, pod := range pm.m {
		if err := f(id, pod); err != nil {
			return err
		}
	}
	return nil
}
