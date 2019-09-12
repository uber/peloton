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

package plugins

import (
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

// Strategy is a placment strategy that will do all the placement logic of
// assigning tasks to offers.
type Strategy interface {
	// GetTaskPlacements takes a list of assignments without any assigned offers and
	// will assign offers to the task in each assignment. This assignment is
	// returned as a map from task index to host index.
	// So for instance: map[int]int{2: 3} means that assignments[2] should
	// be assigned to hosts[3].
	// Tasks that could not be placed should map to an index of -1.
	GetTaskPlacements(tasks []Task, hosts []Host) map[int]int

	// GroupTasksByPlacementNeeds will take a list of assignments and group them into groups that
	// should use the same host filter to acquire offers from the host manager.
	// Returns a list of TasksByPlacementNeedss, which group the tasks that share the same
	// placement needs.
	GroupTasksByPlacementNeeds(tasks []Task) []*TasksByPlacementNeeds

	// ConcurrencySafe returns true iff the strategy is concurrency safe. If
	// the strategy is concurrency safe then it is safe for multiple
	// go-routines to run the GetTaskPlacements method concurrently, else only one
	// go-routine is allowed to run the GetTaskPlacements method at a time.
	ConcurrencySafe() bool
}

// PlacementNeeds is the struct that is needed to construct API calls to
// HostManager to acquire host offers/leases.
type PlacementNeeds struct {
	// The minimum available resources for each host.
	Resources scalar.Resources

	// The minimum number of ports that each host needs.
	Ports uint64

	// IsRevocable returns whether or not the host filter is for
	// revocable resources.
	Revocable bool

	// The minimum number of FDs that each host needs.
	FDs uint32

	// The maximum number of hosts required.
	MaxHosts uint32

	// A map from task/pod ID to preferred hostname.
	HostHints map[string]string

	// TODO: Constraint
	Constraint interface{}

	// TODO: RankingHint
	RankHint interface{}
}

// Task is the interface that the Strategy takes in and tries to place on
// Hosts.
type Task interface {
	// Returns the Peloton ID of the task.
	PelotonID() string

	// Tries to fit the task on the host resources passed in as arguments.
	// Returns as last argument whether or not that operation succeeded.
	// Also returns the new resources after this task was fitted onto the host.
	// NOTE: The resources returned are same as the ones passed in if the "fit"
	// failed.
	Fits(resLeft scalar.Resources, portsLeft uint64) (scalar.Resources, uint64, bool)

	// Sets the placement failure reason for this task.
	SetPlacementFailure(string)

	// Returns the mimir entity representing this task.
	// TODO: Remove this, it should definitely not be here.
	ToMimirEntity() *placement.Entity

	// Returns whether or not this task should be spread on hosts, instead of packed.
	// TODO: Remove this, it should definitely not be here.
	NeedsSpread() bool

	// Returns the preferred hostname for this task.
	PreferredHost() string

	// A task inherently has its own PlacementNeeds.
	GetPlacementNeeds() PlacementNeeds

	// Returns the resource manager v0 task.
	// NOTE: This was done to get the host reservation feature working. We
	// should figure out a way to avoid having to do this.
	GetResmgrTaskV0() *resmgr.Task
}

// Host is the interface that the host offers or leases must satisfy in order
// to be used by the placement strategy.
type Host interface {
	// Returns the free resources on this host.
	GetAvailableResources() (res scalar.Resources, ports uint64)

	// Returns the mimir group representing the host lease or offer.
	ToMimirGroup() *placement.Group
}

// Config contains strategy plugin configurations.
type Config struct {
	TaskType    resmgr.TaskType
	UseHostPool bool
}
