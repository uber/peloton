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

package models

import (
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"

	"github.com/uber/peloton/pkg/hostmgr/scalar"
)

// LaunchablePod is a struct contains info to launch a pod
type LaunchablePod struct {
	PodId *peloton.PodID
	Spec  *pbpod.PodSpec
	Ports map[string]uint32
}

// HostResources is a non-thread safe helper struct holding the Slack and NonSlack resources for a host.
type HostResources struct {
	Slack    scalar.Resources
	NonSlack scalar.Resources
}

// Add adds other resources onto current one.
func (r HostResources) Add(other HostResources) HostResources {
	return HostResources{
		Slack:    r.Slack.Add(other.Slack),
		NonSlack: r.NonSlack.Add(other.NonSlack),
	}
}

// TrySubtract attempts to subtract a host resource from current one
// but returns false if other has more resources.
func (r HostResources) TrySubtract(other HostResources) (HostResources, bool) {
	if !r.NonSlack.Contains(other.NonSlack) {
		return HostResources{}, false
	}
	if !r.Slack.Contains(other.Slack) {
		return HostResources{}, false
	}
	return HostResources{
		Slack:    r.Slack.Subtract(other.Slack),
		NonSlack: r.NonSlack.Subtract(other.NonSlack),
	}, true
}

// Subtract the other host resource from current one.
func (r HostResources) Subtract(other HostResources) HostResources {
	return HostResources{
		Slack:    r.Slack.Subtract(other.Slack),
		NonSlack: r.NonSlack.Subtract(other.NonSlack),
	}
}

// Contains determines whether current Resources is large enough to contain
// the other one.
func (r HostResources) Contains(other HostResources) bool {
	return r.Slack.Contains(other.Slack) && r.NonSlack.Contains(other.NonSlack)
}
