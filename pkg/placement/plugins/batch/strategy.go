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

package batch

import (
	log "github.com/sirupsen/logrus"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"github.com/uber/peloton/pkg/hostmgr/scalar"
	"github.com/uber/peloton/pkg/placement/models"
	"github.com/uber/peloton/pkg/placement/plugins"
)

// New creates a new batch placement strategy.
func New() plugins.Strategy {
	log.Info("Using batch placement strategy.")
	return &batch{}
}

// batch is the batch placement strategy which just fills up offers with tasks one at a time.
type batch struct{}

// GetTaskPlacements is an implementation of the placement.Strategy interface.
func (batch *batch) GetTaskPlacements(
	unassigned []*models.Assignment,
	hosts []*models.HostOffers,
) map[int]int {
	// All tasks are identical in their requirements, including
	// placementHint (see method Filters())). So just looking at
	// the first one is sufficient.
	if len(unassigned) == 0 {
		return map[int]int{}
	}

	var placements map[int]int
	ph := models.Assignments(unassigned).GetPlacementStrategy()
	if ph == job.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD_JOB {
		placements = batch.spreadTasksOnHost(unassigned, hosts)
	} else {
		// the default host assignment strategy is PACK
		placements = batch.packTasksOnHost(unassigned, hosts)
	}

	var leftOver []*models.Assignment
	for assignmentIdx, assignment := range unassigned {
		if _, isAssigned := placements[assignmentIdx]; !isAssigned {
			placements[assignmentIdx] = -1
			leftOver = append(leftOver, assignment)
		}
	}

	log.WithFields(log.Fields{
		"failed_assignments": leftOver,
		"hosts":              hosts,
		"placement_hint":     ph.String(),
		"strategy":           "batch",
	}).Info("GetTaskPlacements batch strategy returned")
	return placements
}

// Assign hosts to tasks by trying to pack as many tasks as possible
// on a single host. Returns any tasks that could not be assigned to
// a host.
// The output is a map[AssignmentIndex]HostIndex, as defined by the
// GetTaskPlacements function signature.
func (batch *batch) packTasksOnHost(
	unassigned []*models.Assignment,
	hosts []*models.HostOffers,
) map[int]int {
	placements := map[int]int{}
	totalAssignedCount := 0
	for hostIdx, host := range hosts {
		log.WithFields(log.Fields{
			"unassigned": unassigned,
			"hosts":      hosts,
		}).Debug("GetTaskPlacements batch strategy called")

		assignedCount := batch.getTasksForHost(host, unassigned[totalAssignedCount:])
		for assignmentIdx := 0; assignmentIdx < assignedCount; assignmentIdx++ {
			placementIdx := totalAssignedCount + assignmentIdx
			placements[placementIdx] = hostIdx
		}
		totalAssignedCount += assignedCount
	}
	return placements
}

// Assign exactly one task to a host, and return all tasks that
// could not be assigned (in case there are fewer hosts than tasks).
// Note that all task have identical resource and scheduling
// constraints, and each host satisifies these constraints.
// So a simple index-by-index assignment is just fine.
// The output is a map[AssignmentIndex]HostIndex, as defined by the
// GetTaskPlacements function signature.
func (batch *batch) spreadTasksOnHost(
	unassigned []*models.Assignment,
	hosts []*models.HostOffers,
) map[int]int {
	numTasks := len(hosts)
	if len(unassigned) < numTasks {
		numTasks = len(unassigned)
	}

	placements := map[int]int{}
	for i := 0; i < numTasks; i++ {
		placements[i] = i
	}
	return placements
}

// getTasksForHost tries to fit in sequence as many tasks as possible
// to the given offers in a host, and returns the indices of the
// tasks that fit on that host. getTasksForHost does not call mutate its
// inputs in any way.
// NOTE: getTasksForHost stops at the first task that failed to fit on
// the host.
// TODO (pourchet): Is the above note a bug? Or is it deliberate?
func (batch *batch) getTasksForHost(
	host *models.HostOffers,
	unassigned []*models.Assignment,
) int {
	portsLeft := host.GetAvailablePortCount()
	resLeft := scalar.FromMesosResources(host.GetOffer().GetResources())
	for i, assignment := range unassigned {
		var ok bool
		resLeft, portsLeft, ok = assignment.Fits(resLeft, portsLeft)
		if !ok {
			return i
		}
	}
	return len(unassigned)
}

// Filters is an implementation of the placement.Strategy interface.
func (batch *batch) Filters(
	assignments []*models.Assignment,
) map[*hostsvc.HostFilter][]*models.Assignment {
	filters := (models.Assignments(assignments)).GroupByHostFilter(
		func(a *models.Assignment) *hostsvc.HostFilter {
			return a.GetSimpleHostFilter()
		},
	)

	// Add quantity control to hostfilter.
	result := map[*hostsvc.HostFilter][]*models.Assignment{}
	for filter, assignments := range filters {
		filterWithQuantity := &hostsvc.HostFilter{
			ResourceConstraint:   filter.GetResourceConstraint(),
			SchedulingConstraint: filter.GetSchedulingConstraint(),
			Quantity: &hostsvc.QuantityControl{
				MaxHosts: uint32(len(assignments)),
			},
			Hint: filter.GetHint(),
		}
		result[filterWithQuantity] = assignments
	}
	return result
}

// ConcurrencySafe is an implementation of the placement.Strategy interface.
func (batch *batch) ConcurrencySafe() bool {
	return true
}
