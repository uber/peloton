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
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/pkg/hostmgr/models"

	log "github.com/sirupsen/logrus"
)

// makes sure mesosHostSummary implements HostSummary
var _ HostSummary = &mesosHostSummary{}

// makes sure mesosHostSummary implements hostStrategy
var _ hostStrategy = &mesosHostSummary{}

type mesosHostSummary struct {
	*baseHostSummary
}

func NewMesosHostSummary(hostname string) HostSummary {
	ms := &mesosHostSummary{
		// mesos does not has concept of host version
		baseHostSummary: newBaseHostSummary(hostname, ""),
	}

	ms.baseHostSummary.strategy = ms

	return ms
}

// SetCapacity sets the capacity of the host.
func (a *mesosHostSummary) SetCapacity(r models.HostResources) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// TODO: figure out how to handle available/allocated resources
	a.capacity = r
}

// SetAvailable sets available resources on a host
func (a *mesosHostSummary) SetAvailable(r models.HostResources) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var ok bool

	a.available = r
	a.allocated, ok = a.capacity.TrySubtract(r)
	if !ok {
		// continue with available set to scalar.Resources{}. This would
		// organically fail in the following steps.
		log.WithFields(
			log.Fields{
				"allocated": r,
				"capacity":  a.capacity,
				"hostname":  a.hostname,
			},
		).Error("mesosHostSummary: More available resources than capacity")
	}
}

// postCompleteLease handles actions after lease is completed
func (a *mesosHostSummary) postCompleteLease(podToSpecMap map[string]*pbpod.PodSpec) error {
	// noop for mesos
	return nil
}
