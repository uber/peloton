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
	"time"

	"github.com/uber/peloton/pkg/placement/plugins"
)

// Offer is the interface that represents a Host Offer in v0, or
// a Host Lease in v1alpha API.
type Offer interface {
	plugins.Host

	// Returns the ID of the offer (Offer ID or Lease ID)
	ID() string

	// Returns the hostname of the offer.
	Hostname() string

	// Returns the agent ID that this offer is for.
	AgentID() string

	// Returns the available port ranges for this offer.
	AvailablePortRanges() map[*PortRange]struct{}
}

// Task is the interface that represents a resource manager task. This
// is meant to wrap around the resource manager protos so we can keep the same logic
// for v0 and v1alpha API.
type Task interface {
	plugins.Task

	// This ID is either the mesos task id or the k8s pod name.
	OrchestrationID() string

	// Returns true if the task is past its placement deadline.
	IsPastDeadline(time.Time) bool

	// Increments the number of placement rounds that we have
	// tried to place this task for.
	IncRounds()

	// IsPastMaxRounds returns true if the task has been through
	// too many placement rounds.
	IsPastMaxRounds() bool

	// Returns true if the task is ready for host reservation.
	IsReadyForHostReservation() bool

	// Returns true if the task should run on revocable resources.
	IsRevocable() bool

	// Sets the matching offer for this task.
	SetPlacement(Offer)

	// Returns the offer that this tasked was matched with.
	GetPlacement() Offer

	// Returns the reason for the placement failure.
	GetPlacementFailure() string
}

// ToPluginTasks transforms an array of tasks into an array of placement
// strategy plugin tasks.
func ToPluginTasks(tasks []Task) []plugins.Task {
	result := make([]plugins.Task, len(tasks))
	for i, t := range tasks {
		result[i] = t
	}
	return result
}
